import Koa from 'koa';
import Router from '@koa/router';
import { pool, migrate, rebuildState } from './db';
import { redis } from './rate';
import { processEvent, UsageEvent } from './guard';

const app = new Koa();
const router = new Router();

async function readBody(ctx: Koa.Context): Promise<string> {
  const chunks: Buffer[] = [];
  for await (const c of ctx.req) chunks.push(c as Buffer);
  return Buffer.concat(chunks).toString('utf8');
}

function validate(body: any): UsageEvent | string {
  const { tenant, caller, model, tokens, idempotency_key, occurred_at } = body ?? {};
  for (const [k, v] of Object.entries({ tenant, caller, model, idempotency_key, occurred_at })) {
    if (typeof v !== 'string' || v.length === 0) return `字段 ${k} 缺失或非法`;
  }
  if (!Number.isInteger(tokens) || tokens < 0) return 'tokens 必须为非负整数';
  if (Number.isNaN(Date.parse(occurred_at))) return 'occurred_at 必须是合法时间';
  return { tenant, caller, model, tokens: Number(tokens), idempotency_key, occurred_at } as UsageEvent;
}

router.get('/health', async (ctx) => {
  const checks: Record<string, string> = {};
  try { await pool.query('SELECT 1'); checks.postgres = 'ok'; }
  catch { checks.postgres = 'down'; }
  try { checks.redis = (await redis.ping()) === 'PONG' ? 'ok' : 'down'; }
  catch { checks.redis = 'down'; }
  const ok = Object.values(checks).every((v) => v === 'ok');
  ctx.status = ok ? 200 : 503;
  ctx.body = { status: ok ? 'ok' : 'degraded', checks };
});

router.post('/v1/usage', async (ctx) => {
  let body: any;
  try { body = JSON.parse(await readBody(ctx)); }
  catch { ctx.status = 400; ctx.body = { error: '非法 JSON' }; return; }
  const ev = validate(body);
  if (typeof ev === 'string') { ctx.status = 400; ctx.body = { error: ev }; return; }

  const d = await processEvent(ev);
  ctx.status = d.httpStatus;
  if (d.retry_after_seconds) ctx.set('Retry-After', String(d.retry_after_seconds));
  ctx.body = d;
});

router.get('/v1/tenants/:tenant/state', async (ctx) => {
  const r = await pool.query(
    `SELECT period_start, total_cost_micros, total_tokens, updated_at
     FROM tenant_state WHERE tenant=$1 ORDER BY period_start DESC`,
    [ctx.params.tenant],
  );
  ctx.body = { tenant: ctx.params.tenant, periods: r.rows };
});

router.get('/v1/tenants/:tenant/decisions', async (ctx) => {
  const limit = Math.min(Number(ctx.query.limit ?? 100), 1000);
  const r = await pool.query(
    `SELECT idempotency_key, caller, model, tokens, occurred_at, decision, reason,
            price_version, cost_micros, total_after_micros, decided_at
     FROM decisions WHERE tenant=$1 ORDER BY id DESC LIMIT $2`,
    [ctx.params.tenant, limit],
  );
  ctx.body = { tenant: ctx.params.tenant, decisions: r.rows };
});

router.get('/v1/decisions/:key', async (ctx) => {
  const r = await pool.query(
    `SELECT decision, reason, price_version, cost_micros, total_after_micros, decided_at
     FROM decisions WHERE idempotency_key=$1 ORDER BY id`,
    [ctx.params.key],
  );
  if (r.rowCount === 0) { ctx.status = 404; ctx.body = { error: '未找到该幂等键' }; return; }
  ctx.body = { idempotency_key: ctx.params.key, trail: r.rows };
});

router.post('/admin/contracts', async (ctx) => {
  let body: any;
  try { body = JSON.parse(await readBody(ctx)); }
  catch { ctx.status = 400; ctx.body = { error: '非法 JSON' }; return; }
  const { tenant, valid_from, valid_to, soft_limit_micros, hard_limit_micros,
          throttle_rpm = 60, model, price_version, effective_from, tiers } = body ?? {};
  if (!tenant || !valid_from || !valid_to || !model || !price_version || !effective_from
      || !Array.isArray(tiers) || !Number.isInteger(soft_limit_micros) || !Number.isInteger(hard_limit_micros)) {
    ctx.status = 400; ctx.body = { error: '合同字段不完整' }; return;
  }
  const client = await pool.connect();
  try {
    await client.query('BEGIN');
    const c = await client.query(
      `INSERT INTO contracts (tenant, valid_from, valid_to, soft_limit_micros, hard_limit_micros, throttle_rpm)
       VALUES ($1,$2,$3,$4,$5,$6)
       ON CONFLICT (tenant, valid_from, valid_to) DO UPDATE SET
         soft_limit_micros=EXCLUDED.soft_limit_micros,
         hard_limit_micros=EXCLUDED.hard_limit_micros,
         throttle_rpm=EXCLUDED.throttle_rpm
       RETURNING id`,
      [tenant, valid_from, valid_to, soft_limit_micros, hard_limit_micros, throttle_rpm],
    );
    await client.query(
      `INSERT INTO price_versions (contract_id, version, model, effective_from, tiers)
       VALUES ($1,$2,$3,$4,$5)
       ON CONFLICT (contract_id, model, version) DO NOTHING`,
      [c.rows[0].id, price_version, model, effective_from, JSON.stringify(tiers)],
    );
    await client.query('COMMIT');
    ctx.body = { contract_id: c.rows[0].id };
  } catch (e) {
    await client.query('ROLLBACK').catch(() => undefined);
    throw e;
  } finally { client.release(); }
});

router.post('/admin/rebuild', async (ctx) => {
  const n = await rebuildState();
  ctx.body = { rebuilt_periods: n };
});

app.use(router.routes()).use(router.allowedMethods());

const port = Number(process.env.PORT ?? 8080);

async function main(): Promise<void> {
  await redis.connect().catch(() => undefined);
  await migrate();
  const rebuilt = await rebuildState();
  app.listen(port, '0.0.0.0', () => {
    console.log(`guard listening on :${port}, 启动重建 ${rebuilt} 个账期状态`);
  });
}

main().catch((err) => {
  console.error('启动失败', err);
  process.exit(1);
});
