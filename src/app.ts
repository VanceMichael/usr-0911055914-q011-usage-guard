import Koa from 'koa';
import Router from '@koa/router';
import bodyParser from 'koa-bodyparser';
import { randomUUID } from 'crypto';
import { pool } from './db';
import { redis } from './redis';
import { AppConfig } from './config';
import { processUsageEvent, UsageEventInput } from './guardrail';

interface AppState {
  requestId: string;
}

function badRequest(ctx: Koa.Context, message: string): void {
  ctx.status = 400;
  ctx.body = { error: 'invalid_request', message };
}

/** 校验并解析事件体；失败返回 null 并已写好 400 响应 */
function parseEventBody(ctx: Koa.Context): UsageEventInput | null {
  const b = (ctx.request as Koa.Request & { body?: unknown }).body;
  if (typeof b !== 'object' || b === null) {
    badRequest(ctx, 'JSON body required');
    return null;
  }
  const body = b as Record<string, unknown>;
  const str = (k: string): string | null =>
    typeof body[k] === 'string' && (body[k] as string).length > 0 ? (body[k] as string) : null;
  const int = (k: string): number | null =>
    Number.isInteger(body[k]) && (body[k] as number) >= 0 ? (body[k] as number) : null;

  const tenantId = str('tenant_id');
  const callerId = str('caller_id');
  const model = str('model');
  const idempotencyKey = str('idempotency_key');
  const inputTokens = int('input_tokens');
  const outputTokens = int('output_tokens');
  if (!tenantId || !callerId || !model || !idempotencyKey) {
    badRequest(ctx, 'tenant_id, caller_id, model, idempotency_key are required non-empty strings');
    return null;
  }
  if (inputTokens === null || outputTokens === null) {
    badRequest(ctx, 'input_tokens and output_tokens must be non-negative integers');
    return null;
  }
  if (inputTokens + outputTokens === 0) {
    badRequest(ctx, 'input_tokens + output_tokens must be > 0');
    return null;
  }
  let occurredAt = new Date();
  if (body.occurred_at !== undefined) {
    if (typeof body.occurred_at !== 'string' || Number.isNaN(Date.parse(body.occurred_at))) {
      badRequest(ctx, 'occurred_at must be an ISO-8601 timestamp');
      return null;
    }
    occurredAt = new Date(body.occurred_at);
  }
  return { tenantId, callerId, model, inputTokens, outputTokens, idempotencyKey, occurredAt };
}

export function createApp(cfg: AppConfig): Koa {
  const app = new Koa<AppState>();
  const router = new Router<AppState>();

  app.use(async (ctx, next) => {
    ctx.state.requestId = randomUUID();
    ctx.set('x-request-id', ctx.state.requestId);
    await next();
  });
  app.use(
    bodyParser({
      enableTypes: ['json'],
      onerror: (err, ctx) => {
        ctx.status = 400;
        ctx.body = { error: 'invalid_json', message: err.message };
      },
    }),
  );
  app.use(async (ctx, next) => {
    try {
      await next();
    } catch (err) {
      ctx.status = 500;
      ctx.body = { error: 'internal_error', message: (err as Error).message };
      ctx.app.emit('error', err, ctx);
    }
  });

  // ---- 健康检查 ----
  router.get(['/healthz', '/health'], (ctx) => {
    ctx.body = { status: 'ok' };
  });
  router.get('/readyz', async (ctx) => {
    const checks: Record<string, string> = {};
    try {
      await pool.query('SELECT 1');
      checks.postgres = 'ok';
    } catch (e) {
      checks.postgres = `fail: ${(e as Error).message}`;
    }
    try {
      checks.redis = (await redis.ping()) === 'PONG' ? 'ok' : 'fail';
    } catch (e) {
      checks.redis = `fail: ${(e as Error).message}`;
    }
    const ready = Object.values(checks).every((v) => v === 'ok');
    ctx.status = ready ? 200 : 503;
    ctx.body = { ready, checks };
  });

  // ---- 用量事件摄入 ----
  router.post('/v1/usage/events', async (ctx) => {
    const input = parseEventBody(ctx);
    if (!input) return;
    const outcome = await processUsageEvent(cfg, ctx.state.requestId, input);
    ctx.status = outcome.httpStatus;
    if (outcome.retryAfterSeconds !== undefined) {
      ctx.set('Retry-After', String(outcome.retryAfterSeconds));
    }
    ctx.body = {
      request_id: ctx.state.requestId,
      decision: outcome.decision,
      reason: outcome.reason,
      contract_id: outcome.contractId,
      price_version: outcome.priceVersion,
      cost_micros: outcome.costMicros === null ? null : outcome.costMicros.toString(),
      running_total_micros:
        outcome.runningTotalMicros === null ? null : outcome.runningTotalMicros.toString(),
      is_backfill: outcome.isBackfill,
      ...(outcome.retryAfterSeconds !== undefined
        ? { retry_after_seconds: outcome.retryAfterSeconds }
        : {}),
    };
  });

  // ---- 审计：决定日志查询（限速/拒绝/补发三条链路的追溯入口）----
  router.get('/v1/decisions', async (ctx) => {
    const { tenant_id, idempotency_key, decision } = ctx.query;
    const limit = Math.min(Number(ctx.query.limit ?? 100) || 100, 1000);
    const where: string[] = [];
    const params: unknown[] = [];
    if (typeof tenant_id === 'string' && tenant_id) {
      params.push(tenant_id);
      where.push(`tenant_id = $${params.length}`);
    }
    if (typeof idempotency_key === 'string' && idempotency_key) {
      params.push(idempotency_key);
      where.push(`idempotency_key = $${params.length}`);
    }
    if (typeof decision === 'string' && decision) {
      params.push(decision);
      where.push(`decision = $${params.length}`);
    }
    params.push(limit);
    const res = await pool.query(
      `SELECT id, request_id, tenant_id, caller_id, model, idempotency_key, decision, reason,
              contract_id, price_version,
              cost_micros::text, running_total_micros::text, is_backfill, created_at
         FROM decisions
         ${where.length ? `WHERE ${where.join(' AND ')}` : ''}
         ORDER BY id DESC LIMIT $${params.length}`,
      params,
    );
    ctx.body = { count: res.rows.length, decisions: res.rows };
  });

  // ---- 租户周期用量：数据库权威值 vs Redis 缓存（验证重启重建）----
  router.get('/v1/tenants/:tenantId/usage', async (ctx) => {
    const tenantId = ctx.params.tenantId;
    const res = await pool.query(
      `SELECT tenant_id, contract_id, period_from, period_to,
              total_input_tokens::text, total_output_tokens::text,
              total_cost_micros::text, event_count::text, row_version::text, updated_at
         FROM tenant_usage WHERE tenant_id = $1 ORDER BY contract_id`,
      [tenantId],
    );
    const periods = [];
    for (const row of res.rows) {
      const cache = await redis.hgetall(`${cfg.usageCachePrefix}${tenantId}:${row.contract_id}`);
      const cached = Object.keys(cache).length > 0 ? cache : null;
      periods.push({
        ...row,
        redis_cache: cached,
        cache_consistent:
          cached !== null &&
          cached.cost_micros === row.total_cost_micros &&
          cached.event_count === row.event_count,
      });
    }
    ctx.body = { tenant_id: tenantId, periods };
  });

  // ---- 合同与价格版本（采购溯源）----
  router.get('/v1/contracts', async (ctx) => {
    const { tenant_id } = ctx.query;
    const params: unknown[] = [];
    let where = '';
    if (typeof tenant_id === 'string' && tenant_id) {
      params.push(tenant_id);
      where = 'WHERE tenant_id = $1';
    }
    const contracts = await pool.query(
      `SELECT id, tenant_id, version, currency, effective_from, effective_to,
              soft_limit_micros::text, hard_limit_micros::text,
              throttle_rate_per_window, throttle_window_seconds
         FROM contracts ${where} ORDER BY tenant_id, version`,
      params,
    );
    const tiers = await pool.query(
      `SELECT contract_id, price_version, model,
              tier_from_tokens::text, tier_to_tokens::text,
              input_price_micros_per_1k::text, output_price_micros_per_1k::text
         FROM price_tiers
         ${where ? 'WHERE contract_id IN (SELECT id FROM contracts ' + where + ')' : ''}
         ORDER BY contract_id, model, tier_from_tokens`,
      params,
    );
    ctx.body = { contracts: contracts.rows, price_tiers: tiers.rows };
  });

  app.use(router.routes()).use(router.allowedMethods());
  return app;
}
