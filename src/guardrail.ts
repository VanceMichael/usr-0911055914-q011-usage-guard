import { PoolClient } from 'pg';
import { pool, toBigInt, withTransaction } from './db';
import { redis } from './redis';
import { computeCost, PriceTier } from './pricing';
import { AppConfig } from './config';

export interface UsageEventInput {
  tenantId: string;
  callerId: string;
  model: string;
  inputTokens: number;
  outputTokens: number;
  idempotencyKey: string;
  occurredAt: Date;
}

export type DecisionKind =
  | 'allowed'
  | 'duplicate'
  | 'throttled'
  | 'rejected'
  | 'rejected_no_contract';

export interface DecisionOutcome {
  decision: DecisionKind;
  httpStatus: number;
  reason: string | null;
  contractId: string | null;
  priceVersion: string | null;
  costMicros: bigint | null;
  runningTotalMicros: bigint | null;
  isBackfill: boolean;
  retryAfterSeconds?: number;
}

interface ContractRow {
  id: string;
  tenant_id: string;
  version: number;
  currency: string;
  effective_from: Date;
  effective_to: Date;
  soft_limit_micros: string;
  hard_limit_micros: string;
  throttle_rate_per_window: number;
  throttle_window_seconds: number;
}

const STATUS_BY_DECISION: Record<DecisionKind, number> = {
  allowed: 202,
  duplicate: 200,
  throttled: 429,
  rejected: 403,
  rejected_no_contract: 422,
};

/** Redis 固定窗口限速器：软阈值之上每窗口只放行 rate 个事件 */
async function allowInThrottleWindow(
  tenantId: string,
  contractId: string,
  rate: number,
  windowSeconds: number,
): Promise<{ allowed: boolean; retryAfterSeconds: number }> {
  const nowMs = Date.now();
  const windowMs = windowSeconds * 1000;
  const win = Math.floor(nowMs / windowMs);
  const key = `guard:rl:${tenantId}:${contractId}:${win}`;
  const count = await redis.incr(key);
  if (count === 1) await redis.pexpire(key, windowMs + 1000); // 窗口 + 1s 缓冲
  const retryAfterSeconds = Math.max(1, Math.ceil(((win + 1) * windowMs - nowMs) / 1000));
  return { allowed: count <= rate, retryAfterSeconds };
}

async function recordDecision(
  client: PoolClient,
  requestId: string,
  input: UsageEventInput,
  outcome: Omit<DecisionOutcome, 'httpStatus'>,
): Promise<void> {
  await client.query(
    `INSERT INTO decisions
       (request_id, tenant_id, caller_id, model, idempotency_key, decision, reason,
        contract_id, price_version, cost_micros, running_total_micros, is_backfill)
     VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12)`,
    [
      requestId,
      input.tenantId,
      input.callerId,
      input.model,
      input.idempotencyKey,
      outcome.decision,
      outcome.reason,
      outcome.contractId,
      outcome.priceVersion,
      outcome.costMicros === null ? null : outcome.costMicros.toString(),
      outcome.runningTotalMicros === null ? null : outcome.runningTotalMicros.toString(),
      outcome.isBackfill,
    ],
  );
}

function usageCacheKey(prefix: string, tenantId: string, contractId: string): string {
  return `${prefix}${tenantId}:${contractId}`;
}

/** 提交后把最新累计写回 Redis 缓存（缓存仅用于读路径与重启核对，权威仍是 PG） */
async function writeThroughCache(
  cfg: AppConfig,
  tenantId: string,
  contractId: string,
  totals: { costMicros: bigint; inputTokens: bigint; outputTokens: bigint; eventCount: bigint; rowVersion: bigint },
): Promise<void> {
  await redis.hset(usageCacheKey(cfg.usageCachePrefix, tenantId, contractId), {
    cost_micros: totals.costMicros.toString(),
    input_tokens: totals.inputTokens.toString(),
    output_tokens: totals.outputTokens.toString(),
    event_count: totals.eventCount.toString(),
    row_version: totals.rowVersion.toString(),
  });
}

/**
 * 处理一条用量事件。并发安全：同一 (tenant, contract) 的判定在
 * SELECT ... FOR UPDATE 行锁内串行，累计额只会单调增加；
 * 幂等键唯一约束保证重试风暴不会重复入账。
 */
export async function processUsageEvent(
  cfg: AppConfig,
  requestId: string,
  input: UsageEventInput,
): Promise<DecisionOutcome> {
  const isBackfill =
    Date.now() - input.occurredAt.getTime() > cfg.backfillSkewSeconds * 1000;

  // 1. 按事件发生时间定位合同（取该时间段内版本最新的一份）
  const contractRes = await pool.query<ContractRow>(
    `SELECT * FROM contracts
      WHERE tenant_id = $1 AND effective_from <= $2 AND $2 < effective_to
      ORDER BY version DESC LIMIT 1`,
    [input.tenantId, input.occurredAt],
  );
  const contract = contractRes.rows[0];
  if (!contract) {
    const outcome: DecisionOutcome = {
      decision: 'rejected_no_contract',
      httpStatus: STATUS_BY_DECISION.rejected_no_contract,
      reason: `no contract covers occurred_at for tenant ${input.tenantId}`,
      contractId: null,
      priceVersion: null,
      costMicros: null,
      runningTotalMicros: null,
      isBackfill,
    };
    await pool.query(
      `INSERT INTO decisions
         (request_id, tenant_id, caller_id, model, idempotency_key, decision, reason, is_backfill)
       VALUES ($1,$2,$3,$4,$5,$6,$7,$8)`,
      [requestId, input.tenantId, input.callerId, input.model, input.idempotencyKey,
       outcome.decision, outcome.reason, isBackfill],
    );
    return outcome;
  }

  // 2. 取该模型在合同下的阶梯价（价格版本随阶梯行返回）
  const tierRes = await pool.query<{
    price_version: string;
    tier_from_tokens: string;
    tier_to_tokens: string | null;
    input_price_micros_per_1k: string;
    output_price_micros_per_1k: string;
  }>(
    `SELECT price_version, tier_from_tokens, tier_to_tokens,
            input_price_micros_per_1k, output_price_micros_per_1k
       FROM price_tiers WHERE contract_id = $1 AND model = $2
       ORDER BY tier_from_tokens`,
    [contract.id, input.model],
  );
  if (tierRes.rows.length === 0) {
    const outcome: DecisionOutcome = {
      decision: 'rejected_no_contract',
      httpStatus: STATUS_BY_DECISION.rejected_no_contract,
      reason: `no price tier for model ${input.model} in contract ${contract.id}`,
      contractId: contract.id,
      priceVersion: null,
      costMicros: null,
      runningTotalMicros: null,
      isBackfill,
    };
    await pool.query(
      `INSERT INTO decisions
         (request_id, tenant_id, caller_id, model, idempotency_key, decision, reason, contract_id, is_backfill)
       VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9)`,
      [requestId, input.tenantId, input.callerId, input.model, input.idempotencyKey,
       outcome.decision, outcome.reason, contract.id, isBackfill],
    );
    return outcome;
  }
  const priceVersion = tierRes.rows[0].price_version;
  const tiers: PriceTier[] = tierRes.rows.map((r) => ({
    tierFromTokens: toBigInt(r.tier_from_tokens),
    tierToTokens: r.tier_to_tokens === null ? null : toBigInt(r.tier_to_tokens),
    inputPriceMicrosPer1k: toBigInt(r.input_price_micros_per_1k),
    outputPriceMicrosPer1k: toBigInt(r.output_price_micros_per_1k),
  }));

  const softLimit = toBigInt(contract.soft_limit_micros);
  const hardLimit = toBigInt(contract.hard_limit_micros);

  // 3. 事务：行锁内完成 判重 → 阈值 → 入账 → 审计
  const { outcome, totals } = await withTransaction(async (client) => {
    // 确保累计行存在并加行锁
    await client.query(
      `INSERT INTO tenant_usage (tenant_id, contract_id, period_from, period_to)
       VALUES ($1,$2,$3,$4) ON CONFLICT (tenant_id, contract_id) DO NOTHING`,
      [input.tenantId, contract.id, contract.effective_from, contract.effective_to],
    );
    const usageRes = await client.query<{
      total_input_tokens: string;
      total_output_tokens: string;
      total_cost_micros: string;
      event_count: string;
      row_version: string;
    }>(
      `SELECT total_input_tokens, total_output_tokens, total_cost_micros, event_count, row_version
         FROM tenant_usage WHERE tenant_id = $1 AND contract_id = $2 FOR UPDATE`,
      [input.tenantId, contract.id],
    );
    const usage = usageRes.rows[0];
    const runningTotal = toBigInt(usage.total_cost_micros);
    const totalTokens = toBigInt(usage.total_input_tokens) + toBigInt(usage.total_output_tokens);

    // 3a. 幂等判重：已入账事件的重放直接返回首次结果，绝不二次入账
    const dupRes = await client.query<{
      cost_micros: string;
      price_version: string;
      occurred_at: Date;
    }>(
      `SELECT cost_micros, price_version, occurred_at FROM usage_events
        WHERE tenant_id = $1 AND idempotency_key = $2`,
      [input.tenantId, input.idempotencyKey],
    );
    if (dupRes.rows.length > 0) {
      const dup = dupRes.rows[0];
      const outcome: DecisionOutcome = {
        decision: 'duplicate',
        httpStatus: STATUS_BY_DECISION.duplicate,
        reason: 'idempotency key already applied; original charge returned',
        contractId: contract.id,
        priceVersion: dup.price_version,
        costMicros: toBigInt(dup.cost_micros),
        runningTotalMicros: runningTotal,
        isBackfill,
      };
      await recordDecision(client, requestId, input, outcome);
      return {
        outcome,
        totals: null, // 重复事件不改变累计，无需写缓存
      };
    }

    // 3b. 硬阈值：当前累计已达硬顶 → 拒绝，不入账
    if (runningTotal >= hardLimit) {
      const outcome: DecisionOutcome = {
        decision: 'rejected',
        httpStatus: STATUS_BY_DECISION.rejected,
        reason: `hard limit reached: running total ${runningTotal} micros >= ${hardLimit} micros`,
        contractId: contract.id,
        priceVersion,
        costMicros: null,
        runningTotalMicros: runningTotal,
        isBackfill,
      };
      await recordDecision(client, requestId, input, outcome);
      return { outcome, totals: null };
    }

    // 3c. 软阈值：进入限速车道，Redis 短窗口计数，超限则 429
    if (runningTotal >= softLimit) {
      const gate = await allowInThrottleWindow(
        input.tenantId,
        contract.id,
        contract.throttle_rate_per_window,
        contract.throttle_window_seconds,
      );
      if (!gate.allowed) {
        const outcome: DecisionOutcome = {
          decision: 'throttled',
          httpStatus: STATUS_BY_DECISION.throttled,
          reason: `soft limit exceeded: throttle lane allows ${contract.throttle_rate_per_window} events / ${contract.throttle_window_seconds}s`,
          contractId: contract.id,
          priceVersion,
          costMicros: null,
          runningTotalMicros: runningTotal,
          isBackfill,
          retryAfterSeconds: gate.retryAfterSeconds,
        };
        await recordDecision(client, requestId, input, outcome);
        return { outcome, totals: null };
      }
    }

    // 3d. 入账：按当前累计令牌数定位阶梯，精确计价后单调累加
    const cost = computeCost(tiers, totalTokens, input.inputTokens, input.outputTokens);
    await client.query(
      `INSERT INTO usage_events
         (tenant_id, caller_id, model, input_tokens, output_tokens, idempotency_key,
          occurred_at, contract_id, price_version, cost_micros)
       VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10)`,
      [
        input.tenantId, input.callerId, input.model, input.inputTokens, input.outputTokens,
        input.idempotencyKey, input.occurredAt, contract.id, priceVersion,
        cost.costMicros.toString(),
      ],
    );
    const upd = await client.query<{
      total_input_tokens: string;
      total_output_tokens: string;
      total_cost_micros: string;
      event_count: string;
      row_version: string;
    }>(
      `UPDATE tenant_usage SET
         total_input_tokens  = total_input_tokens  + $3,
         total_output_tokens = total_output_tokens + $4,
         total_cost_micros   = total_cost_micros   + $5,
         event_count         = event_count + 1,
         row_version         = row_version + 1,
         updated_at          = now()
       WHERE tenant_id = $1 AND contract_id = $2
       RETURNING total_input_tokens, total_output_tokens, total_cost_micros, event_count, row_version`,
      [input.tenantId, contract.id, input.inputTokens, input.outputTokens, cost.costMicros.toString()],
    );
    const u = upd.rows[0];
    const newTotals = {
      costMicros: toBigInt(u.total_cost_micros),
      inputTokens: toBigInt(u.total_input_tokens),
      outputTokens: toBigInt(u.total_output_tokens),
      eventCount: toBigInt(u.event_count),
      rowVersion: toBigInt(u.row_version),
    };
    const outcome: DecisionOutcome = {
      decision: 'allowed',
      httpStatus: STATUS_BY_DECISION.allowed,
      reason: runningTotal >= softLimit ? 'applied via throttle lane (soft limit exceeded)' : null,
      contractId: contract.id,
      priceVersion,
      costMicros: cost.costMicros,
      runningTotalMicros: newTotals.costMicros,
      isBackfill,
    };
    await recordDecision(client, requestId, input, outcome);
    return { outcome, totals: newTotals };
  });

  // 4. 提交后写穿 Redis 缓存（失败不影响已提交的权威结果）
  if (totals) {
    await writeThroughCache(cfg, input.tenantId, contract.id, totals).catch(() => undefined);
  }
  return outcome;
}

/** 服务重启后从 PostgreSQL 重建 Redis 中的限额状态 */
export async function rebuildUsageCache(cfg: AppConfig): Promise<number> {
  const res = await pool.query<{
    tenant_id: string;
    contract_id: string;
    total_input_tokens: string;
    total_output_tokens: string;
    total_cost_micros: string;
    event_count: string;
    row_version: string;
  }>(`SELECT tenant_id, contract_id, total_input_tokens, total_output_tokens,
             total_cost_micros, event_count, row_version FROM tenant_usage`);

  // 清掉旧缓存再按数据库权威值重建
  let cursor = '0';
  do {
    const [next, keys] = await redis.scan(cursor, 'MATCH', `${cfg.usageCachePrefix}*`, 'COUNT', 200);
    cursor = next;
    if (keys.length > 0) await redis.del(...keys);
  } while (cursor !== '0');

  const pipeline = redis.pipeline();
  for (const r of res.rows) {
    pipeline.hset(usageCacheKey(cfg.usageCachePrefix, r.tenant_id, r.contract_id), {
      cost_micros: r.total_cost_micros,
      input_tokens: r.total_input_tokens,
      output_tokens: r.total_output_tokens,
      event_count: r.event_count,
      row_version: r.row_version,
    });
  }
  await pipeline.exec();
  return res.rows.length;
}
