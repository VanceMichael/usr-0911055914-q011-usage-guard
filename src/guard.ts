import { PoolClient } from 'pg';
import { pool } from './db';
import { marginalCost, Tier } from './pricing';
import { hitWindow } from './rate';

export interface UsageEvent {
  tenant: string;
  caller: string;
  model: string;
  tokens: number;
  idempotency_key: string;
  occurred_at: string; // ISO
}

export type DecisionKind = 'allowed' | 'throttled' | 'rejected' | 'duplicate';

export interface DecisionResult {
  decision: DecisionKind;
  httpStatus: number;
  reason: string;
  billed: boolean;
  cost_micros: number;
  total_after_micros: number;
  price_version: string | null;
  retry_after_seconds?: number;
}

interface ContractRow {
  id: string;
  soft_limit_micros: string;
  hard_limit_micros: string;
  throttle_rpm: number;
}

function periodStart(iso: string): string {
  return iso.slice(0, 7) + '-01'; // 月度账期
}

async function recordDecision(
  client: PoolClient,
  ev: UsageEvent,
  d: Omit<DecisionResult, 'httpStatus'>,
  priceVersionId: number | null,
): Promise<void> {
  await client.query(
    `INSERT INTO decisions
       (idempotency_key, tenant, caller, model, tokens, occurred_at,
        decision, reason, price_version_id, price_version, cost_micros, total_after_micros)
     VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12)`,
    [ev.idempotency_key, ev.tenant, ev.caller, ev.model, ev.tokens, ev.occurred_at,
     d.decision, d.reason, priceVersionId, d.price_version, d.cost_micros, d.total_after_micros],
  );
}

export async function processEvent(ev: UsageEvent): Promise<DecisionResult> {
  const client = await pool.connect();
  try {
    await client.query('BEGIN');

    // 1) 幂等去重：同一 idempotency_key 只计费一次；重复到达返回首次决定
    const ins = await client.query(
      `INSERT INTO usage_events (idempotency_key, tenant, caller, model, tokens, occurred_at, period_start)
       VALUES ($1,$2,$3,$4,$5,$6,$7)
       ON CONFLICT (idempotency_key) DO NOTHING
       RETURNING id`,
      [ev.idempotency_key, ev.tenant, ev.caller, ev.model, ev.tokens, ev.occurred_at,
       periodStart(ev.occurred_at)],
    );
    if (ins.rowCount === 0) {
      const prev = await client.query(
        `SELECT d.decision, d.reason, d.cost_micros, d.total_after_micros, d.price_version,
                e.billed
         FROM usage_events e
         LEFT JOIN LATERAL (
           SELECT * FROM decisions WHERE idempotency_key = e.idempotency_key
           ORDER BY id ASC LIMIT 1
         ) d ON true
         WHERE e.idempotency_key = $1`,
        [ev.idempotency_key],
      );
      const p = prev.rows[0];
      const dup: DecisionResult = {
        decision: 'duplicate',
        httpStatus: 200,
        reason: `重复事件，首次决定为 ${p?.decision ?? 'unknown'}`,
        billed: false,
        cost_micros: 0,
        total_after_micros: Number(p?.total_after_micros ?? 0),
        price_version: p?.price_version ?? null,
      };
      await recordDecision(client, ev, dup, null);
      await client.query('COMMIT');
      return dup;
    }

    // 2) 锁定租户账期行，串行化累计 → 并发/乱序下总额单调不回退
    const period = periodStart(ev.occurred_at);
    await client.query(
      `INSERT INTO tenant_state (tenant, period_start) VALUES ($1,$2) ON CONFLICT DO NOTHING`,
      [ev.tenant, period],
    );
    const st = await client.query(
      `SELECT total_cost_micros, total_tokens FROM tenant_state
       WHERE tenant=$1 AND period_start=$2 FOR UPDATE`,
      [ev.tenant, period],
    );
    const totalBefore = Number(st.rows[0].total_cost_micros);
    const tokensBefore = Number(st.rows[0].total_tokens);

    // 3) 合同 + 价格版本（按 occurred_at 选择，补发旧事件用旧价）
    const ct = await client.query<ContractRow>(
      `SELECT id, soft_limit_micros, hard_limit_micros, throttle_rpm
       FROM contracts
       WHERE tenant=$1 AND valid_from <= $2 AND valid_to > $2
       ORDER BY valid_from DESC LIMIT 1`,
      [ev.tenant, ev.occurred_at],
    );
    if (ct.rowCount === 0) {
      const d: DecisionResult = {
        decision: 'rejected', httpStatus: 422, billed: false, cost_micros: 0,
        total_after_micros: totalBefore, price_version: null,
        reason: '无覆盖该时间的合同',
      };
      await recordDecision(client, ev, d, null);
      await client.query('COMMIT');
      return d;
    }
    const contract = ct.rows[0];
    const pv = await client.query(
      `SELECT id, version, tiers FROM price_versions
       WHERE contract_id=$1 AND model=$2 AND effective_from <= $3
       ORDER BY effective_from DESC LIMIT 1`,
      [contract.id, ev.model, ev.occurred_at],
    );
    if (pv.rowCount === 0) {
      const d: DecisionResult = {
        decision: 'rejected', httpStatus: 422, billed: false, cost_micros: 0,
        total_after_micros: totalBefore, price_version: null,
        reason: `模型 ${ev.model} 无生效价格版本`,
      };
      await recordDecision(client, ev, d, null);
      await client.query('COMMIT');
      return d;
    }
    const price = pv.rows[0] as { id: number; version: string; tiers: Tier[] };

    const soft = Number(contract.soft_limit_micros);
    const hard = Number(contract.hard_limit_micros);

    // 4) 硬阈值：拒绝，不计费
    if (totalBefore >= hard) {
      const d: DecisionResult = {
        decision: 'rejected', httpStatus: 429, billed: false, cost_micros: 0,
        total_after_micros: totalBefore, price_version: price.version,
        reason: `超过硬阈值 (${totalBefore}/${hard} micro-USD)`,
      };
      await recordDecision(client, ev, d, price.id);
      await client.query('COMMIT');
      return d;
    }

    // 5) 阶梯边际成本
    const cost = marginalCost(price.tiers, tokensBefore, ev.tokens);
    const totalAfter = totalBefore + cost;

    // 6) 软阈值：Redis 短窗口限速。窗口内超额 → 限速拒绝（不计费）；窗口内 → 计费但标记 throttled
    if (totalAfter > soft) {
      const hits = await hitWindow(ev.tenant);
      if (hits > contract.throttle_rpm) {
        const d: DecisionResult = {
          decision: 'throttled', httpStatus: 429, billed: false, cost_micros: 0,
          total_after_micros: totalBefore, price_version: price.version,
          reason: `超过软阈值，限速窗口 ${hits}/${contract.throttle_rpm} rpm`,
          retry_after_seconds: 60 - (Math.floor(Date.now() / 1000) % 60),
        };
        await recordDecision(client, ev, d, price.id);
        await client.query('COMMIT');
        return d;
      }
    }

    // 7) 计费累计（只增）
    await client.query(
      `UPDATE tenant_state
       SET total_cost_micros = total_cost_micros + $3,
           total_tokens      = total_tokens + $4,
           updated_at        = now()
       WHERE tenant=$1 AND period_start=$2`,
      [ev.tenant, period, cost, ev.tokens],
    );
    await client.query(
      `UPDATE usage_events SET price_version_id=$2, cost_micros=$3, billed=true
       WHERE idempotency_key=$1`,
      [ev.idempotency_key, price.id, cost],
    );
    const overSoft = totalAfter > soft;
    const d: DecisionResult = {
      decision: overSoft ? 'throttled' : 'allowed',
      httpStatus: 200,
      billed: true,
      cost_micros: cost,
      total_after_micros: totalAfter,
      price_version: price.version,
      reason: overSoft
        ? `超过软阈值 (${totalAfter}/${soft} micro-USD)，已计费并限速`
        : 'ok',
      ...(overSoft ? { retry_after_seconds: 1 } : {}),
    };
    await recordDecision(client, ev, d, price.id);
    await client.query('COMMIT');
    return d;
  } catch (err) {
    await client.query('ROLLBACK').catch(() => undefined);
    throw err;
  } finally {
    client.release();
  }
}
