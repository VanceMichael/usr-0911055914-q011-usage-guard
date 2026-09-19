import { Pool } from 'pg';

export const pool = new Pool({
  connectionString: process.env.DATABASE_URL ?? 'postgres://guard:guard@localhost:5432/guard',
  max: 20,
});

/**
 * 金额统一使用 micro-USD（1 美元 = 1_000_000 micros）的整数字段，
 * 避免浮点误差；阶梯单价以 “每 1k token 的 micros” 表示。
 */
const MIGRATIONS: string[] = [
  `CREATE TABLE IF NOT EXISTS contracts (
     id            BIGSERIAL PRIMARY KEY,
     tenant        TEXT NOT NULL,
     valid_from    TIMESTAMPTZ NOT NULL,
     valid_to      TIMESTAMPTZ NOT NULL,
     soft_limit_micros BIGINT NOT NULL,
     hard_limit_micros BIGINT NOT NULL,
     throttle_rpm  INT NOT NULL DEFAULT 60,
     created_at    TIMESTAMPTZ NOT NULL DEFAULT now()
   )`,
  `CREATE UNIQUE INDEX IF NOT EXISTS contracts_tenant_window
     ON contracts (tenant, valid_from, valid_to)`,

  `CREATE TABLE IF NOT EXISTS price_versions (
     id              BIGSERIAL PRIMARY KEY,
     contract_id     BIGINT NOT NULL REFERENCES contracts(id),
     version         TEXT NOT NULL,
     model           TEXT NOT NULL,
     effective_from  TIMESTAMPTZ NOT NULL,
     tiers           JSONB NOT NULL,  -- [{up_to: number|null, micros_per_1k: number}]
     UNIQUE (contract_id, model, version)
   )`,

  `CREATE TABLE IF NOT EXISTS usage_events (
     id               BIGSERIAL PRIMARY KEY,
     idempotency_key  TEXT NOT NULL UNIQUE,
     tenant           TEXT NOT NULL,
     caller           TEXT NOT NULL,
     model            TEXT NOT NULL,
     tokens           BIGINT NOT NULL CHECK (tokens >= 0),
     occurred_at      TIMESTAMPTZ NOT NULL,
     received_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
     period_start     DATE NOT NULL,
     price_version_id BIGINT REFERENCES price_versions(id),
     cost_micros      BIGINT NOT NULL DEFAULT 0,
     billed           BOOLEAN NOT NULL DEFAULT false
   )`,
  `CREATE INDEX IF NOT EXISTS usage_events_tenant_period
     ON usage_events (tenant, period_start)`,

  `-- 每个 (租户, 账期) 一行；总额只增不减，由行锁保证串行累计
   CREATE TABLE IF NOT EXISTS tenant_state (
     tenant            TEXT NOT NULL,
     period_start      DATE NOT NULL,
     total_cost_micros BIGINT NOT NULL DEFAULT 0,
     total_tokens      BIGINT NOT NULL DEFAULT 0,
     last_event_seq    BIGINT NOT NULL DEFAULT 0,
     updated_at        TIMESTAMPTZ NOT NULL DEFAULT now(),
     PRIMARY KEY (tenant, period_start)
   )`,

  `-- 审计：每一次决定（含重复与拒绝）都可追溯
   CREATE TABLE IF NOT EXISTS decisions (
     id               BIGSERIAL PRIMARY KEY,
     idempotency_key  TEXT NOT NULL,
     tenant           TEXT NOT NULL,
     caller           TEXT NOT NULL,
     model            TEXT NOT NULL,
     tokens           BIGINT NOT NULL,
     occurred_at      TIMESTAMPTZ NOT NULL,
     decision         TEXT NOT NULL,  -- allowed | throttled | rejected | duplicate
     reason           TEXT NOT NULL,
     price_version_id BIGINT,
     price_version    TEXT,
     cost_micros      BIGINT NOT NULL DEFAULT 0,
     total_after_micros BIGINT NOT NULL,
     decided_at       TIMESTAMPTZ NOT NULL DEFAULT now()
   )`,
  `CREATE INDEX IF NOT EXISTS decisions_tenant ON decisions (tenant, decided_at)`,
  `CREATE INDEX IF NOT EXISTS decisions_key ON decisions (idempotency_key)`,
];

const SEED = `
  INSERT INTO contracts (tenant, valid_from, valid_to, soft_limit_micros, hard_limit_micros, throttle_rpm)
  VALUES ('team-a', '2026-01-01T00:00:00Z', '2027-01-01T00:00:00Z', 100000, 200000, 20)
  ON CONFLICT (tenant, valid_from, valid_to) DO NOTHING;

  INSERT INTO price_versions (contract_id, version, model, effective_from, tiers)
  SELECT c.id, 'v2026.1', 'demo', '2026-01-01T00:00:00Z',
         '[{"up_to": 100000, "micros_per_1k": 10}, {"up_to": null, "micros_per_1k": 30}]'::jsonb
  FROM contracts c WHERE c.tenant = 'team-a'
  ON CONFLICT (contract_id, model, version) DO NOTHING;

  -- 第二个价格版本：2026-09-01 起生效，用于证明“采用的价格版本”可追溯
  INSERT INTO price_versions (contract_id, version, model, effective_from, tiers)
  SELECT c.id, 'v2026.9', 'demo', '2026-09-01T00:00:00Z',
         '[{"up_to": 50000, "micros_per_1k": 12}, {"up_to": null, "micros_per_1k": 40}]'::jsonb
  FROM contracts c WHERE c.tenant = 'team-a'
  ON CONFLICT (contract_id, model, version) DO NOTHING;
`;

export async function migrate(): Promise<void> {
  for (const sql of MIGRATIONS) await pool.query(sql);
  await pool.query(SEED);
}

/**
 * 重启后从 usage_events 重建 tenant_state：
 * 总额 = 已计费事件的成本之和（只增语义由“仅重放 billed 事件”保持）。
 */
export async function rebuildState(): Promise<number> {
  const res = await pool.query(`
    INSERT INTO tenant_state (tenant, period_start, total_cost_micros, total_tokens, last_event_seq, updated_at)
    SELECT tenant, period_start,
           SUM(cost_micros) FILTER (WHERE billed) AS cost,
           SUM(tokens)      FILTER (WHERE billed) AS tokens,
           COALESCE(MAX(id), 0), now()
    FROM usage_events
    GROUP BY tenant, period_start
    ON CONFLICT (tenant, period_start) DO UPDATE SET
      total_cost_micros = EXCLUDED.total_cost_micros,
      total_tokens      = EXCLUDED.total_tokens,
      last_event_seq    = EXCLUDED.last_event_seq,
      updated_at        = now()
    RETURNING tenant
  `);
  return res.rowCount ?? 0;
}
