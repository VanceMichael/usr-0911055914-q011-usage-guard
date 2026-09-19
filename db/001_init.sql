-- 成本护栏数据库结构（幂等，可重复执行）
-- 金额一律使用 BIGINT micros（1e-6 货币单位），杜绝浮点误差。

CREATE TABLE IF NOT EXISTS contracts (
  id                      TEXT PRIMARY KEY,            -- 如 'ctr-acme-2026'
  tenant_id               TEXT NOT NULL,
  version                 INTEGER NOT NULL,            -- 合同版本号
  currency                TEXT NOT NULL DEFAULT 'USD',
  effective_from          TIMESTAMPTZ NOT NULL,
  effective_to            TIMESTAMPTZ NOT NULL,
  soft_limit_micros       BIGINT NOT NULL,             -- 软阈值：超过后进入限速车道
  hard_limit_micros       BIGINT NOT NULL,             -- 硬阈值：超过后一律拒绝
  throttle_rate_per_window INTEGER NOT NULL DEFAULT 5, -- 限速车道：每窗口放行的事件数
  throttle_window_seconds  INTEGER NOT NULL DEFAULT 10,
  created_at              TIMESTAMPTZ NOT NULL DEFAULT now(),
  UNIQUE (tenant_id, version),
  CHECK (soft_limit_micros < hard_limit_micros),
  CHECK (effective_from < effective_to)
);

CREATE TABLE IF NOT EXISTS price_tiers (
  id                         BIGSERIAL PRIMARY KEY,
  contract_id                TEXT NOT NULL REFERENCES contracts(id),
  price_version              TEXT NOT NULL,            -- 如 'v2026.09'，采购溯源用
  model                      TEXT NOT NULL,
  tier_from_tokens           BIGINT NOT NULL,          -- 阶梯下界（周期内累计令牌，含）
  tier_to_tokens             BIGINT,                   -- 阶梯上界（不含），NULL = 无上限
  input_price_micros_per_1k  BIGINT NOT NULL,          -- 每 1k 输入令牌价格（micros）
  output_price_micros_per_1k BIGINT NOT NULL,
  UNIQUE (contract_id, model, tier_from_tokens),
  CHECK (tier_to_tokens IS NULL OR tier_to_tokens > tier_from_tokens)
);

-- 每租户每合同的周期累计（只增不减，数据库为唯一权威）
CREATE TABLE IF NOT EXISTS tenant_usage (
  tenant_id           TEXT NOT NULL,
  contract_id         TEXT NOT NULL REFERENCES contracts(id),
  period_from         TIMESTAMPTZ NOT NULL,
  period_to           TIMESTAMPTZ NOT NULL,
  total_input_tokens  BIGINT NOT NULL DEFAULT 0,
  total_output_tokens BIGINT NOT NULL DEFAULT 0,
  total_cost_micros   BIGINT NOT NULL DEFAULT 0,
  event_count         BIGINT NOT NULL DEFAULT 0,
  row_version         BIGINT NOT NULL DEFAULT 0,       -- 每次应用事件 +1，审计用
  updated_at          TIMESTAMPTZ NOT NULL DEFAULT now(),
  PRIMARY KEY (tenant_id, contract_id)
);

-- 已入账事件：幂等键唯一约束是防重风暴的最后防线
CREATE TABLE IF NOT EXISTS usage_events (
  id              BIGSERIAL PRIMARY KEY,
  tenant_id       TEXT NOT NULL,
  caller_id       TEXT NOT NULL,
  model           TEXT NOT NULL,
  input_tokens    INTEGER NOT NULL CHECK (input_tokens >= 0),
  output_tokens   INTEGER NOT NULL CHECK (output_tokens >= 0),
  idempotency_key TEXT NOT NULL,
  occurred_at     TIMESTAMPTZ NOT NULL,                -- 事件真实发生时间（可乱序/补发）
  received_at     TIMESTAMPTZ NOT NULL DEFAULT now(),
  contract_id     TEXT NOT NULL REFERENCES contracts(id),
  price_version   TEXT NOT NULL,                       -- 入账时采用的价格版本
  cost_micros     BIGINT NOT NULL,
  UNIQUE (tenant_id, idempotency_key)
);
CREATE INDEX IF NOT EXISTS idx_usage_events_tenant_time ON usage_events(tenant_id, received_at);

-- 决定审计日志：只追加，记录每一次请求的判定与当时采用的价格版本
CREATE TABLE IF NOT EXISTS decisions (
  id                   BIGSERIAL PRIMARY KEY,
  request_id           TEXT NOT NULL,                  -- 每次 HTTP 请求一个 ID
  tenant_id            TEXT NOT NULL,
  caller_id            TEXT,
  model                TEXT,
  idempotency_key      TEXT NOT NULL,
  decision             TEXT NOT NULL,                  -- allowed|duplicate|throttled|rejected|rejected_no_contract
  reason               TEXT,
  contract_id          TEXT,
  price_version        TEXT,
  cost_micros          BIGINT,                         -- 本次入账金额（未入账为 0/NULL）
  running_total_micros BIGINT,                         -- 判定时刻的周期累计
  is_backfill          BOOLEAN NOT NULL DEFAULT false, -- occurred_at 明显早于接收时间 → 补发
  created_at           TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS idx_decisions_tenant_key  ON decisions(tenant_id, idempotency_key, id);
CREATE INDEX IF NOT EXISTS idx_decisions_tenant_time ON decisions(tenant_id, created_at);
