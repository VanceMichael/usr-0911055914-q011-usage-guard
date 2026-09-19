#!/usr/bin/env node
/**
 * 共享种子逻辑：为指定租户创建合同 + 阶梯价（幂等）。
 * seed.mjs 与 load-test.mjs 共用；压测每次使用独立租户，保证可重复运行。
 */
import pg from 'pg';

export const DEFAULT_LIMITS = {
  softMicros: process.env.SOFT_LIMIT_MICROS ?? '500000',
  hardMicros: process.env.HARD_LIMIT_MICROS ?? '1000000',
  throttleRate: Number(process.env.THROTTLE_RATE ?? 10),
  throttleWindowSeconds: Number(process.env.THROTTLE_WINDOW_SECONDS ?? 5),
};

export async function seedTenant(tenantId, limits = DEFAULT_LIMITS) {
  const pool = new pg.Pool({
    connectionString: process.env.DATABASE_URL ?? 'postgres://guard:guard@localhost:5432/guard',
  });
  const contractId = `ctr-${tenantId}-v1`;
  try {
    await pool.query(
      `INSERT INTO contracts
         (id, tenant_id, version, currency, effective_from, effective_to,
          soft_limit_micros, hard_limit_micros, throttle_rate_per_window, throttle_window_seconds)
       VALUES ($1, $2, 1, 'USD', '2026-01-01T00:00:00Z', '2027-01-01T00:00:00Z', $3, $4, $5, $6)
       ON CONFLICT (id) DO UPDATE SET
         soft_limit_micros = EXCLUDED.soft_limit_micros,
         hard_limit_micros = EXCLUDED.hard_limit_micros,
         throttle_rate_per_window = EXCLUDED.throttle_rate_per_window,
         throttle_window_seconds = EXCLUDED.throttle_window_seconds`,
      [contractId, tenantId, limits.softMicros, limits.hardMicros,
       limits.throttleRate, limits.throttleWindowSeconds],
    );
    const tiers = [
      // claude-sonnet-5：10 万令牌后阶梯降价，价格版本 v2026.09
      [contractId, 'v2026.09', 'claude-sonnet-5', '0', '100000', '3000', '15000'],
      [contractId, 'v2026.09', 'claude-sonnet-5', '100000', null, '2400', '12000'],
      // gpt-5：单档，价格版本 v2026.08
      [contractId, 'v2026.08', 'gpt-5', '0', null, '1250', '10000'],
    ];
    for (const [cid, pv, model, from, to, pin, pout] of tiers) {
      await pool.query(
        `INSERT INTO price_tiers
           (contract_id, price_version, model, tier_from_tokens, tier_to_tokens,
            input_price_micros_per_1k, output_price_micros_per_1k)
         VALUES ($1,$2,$3,$4,$5,$6,$7)
         ON CONFLICT (contract_id, model, tier_from_tokens) DO UPDATE SET
           price_version = EXCLUDED.price_version,
           tier_to_tokens = EXCLUDED.tier_to_tokens,
           input_price_micros_per_1k = EXCLUDED.input_price_micros_per_1k,
           output_price_micros_per_1k = EXCLUDED.output_price_micros_per_1k`,
        [cid, pv, model, from, to, pin, pout],
      );
    }
    return { contractId };
  } finally {
    await pool.end();
  }
}
