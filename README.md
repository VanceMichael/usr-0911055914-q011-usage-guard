# 亚太 AI 用量成本护栏（usage-guard）

纯后端成本护栏：接收带租户、调用方、模型、令牌数与幂等键的用量事件，按合同时间段与阶梯价实时累计；
超过软阈值进入限速车道（429 + Retry-After），超过硬阈值拒绝（403）；每次决定与采用的价格版本写入
PostgreSQL 审计表，事件乱序/重复到达时累计额单调不回退。Redis 承担短窗口限速计数与限额状态缓存，
服务重启后自动从数据库重建。

## 架构

```
调用方 ──POST /v1/usage/events──▶ Koa API ──┬─▶ PostgreSQL（权威）
                                            │    contracts / price_tiers
                                            │    tenant_usage（行锁内单调累加）
                                            │    usage_events（幂等键唯一）
                                            │    decisions（只追加审计）
                                            └─▶ Redis
                                                 guard:rl:*   短窗口限速计数（INCR+过期）
                                                 guard:usage:* 限额状态缓存（重启后从 PG 重建）
```

并发安全模型：同一 `(tenant, contract)` 的判定在 `SELECT ... FOR UPDATE` 行锁内串行完成
「判重 → 阈值检查 → 阶梯计价 → 入账 → 审计」，因此累计额只增不减、阶梯定位精确、重试风暴不会重复入账。

## 快速开始（Docker Compose）

```bash
docker compose up --build          # api:8080 + postgres:16 + redis:7，均带健康检查
npm run seed                       # 灌入演示合同（tenant-apac-1，见 contracts/seed-contract.json）
npm run loadtest                   # 并发压测：验证限速/拒绝/补发三条链路（每次独立租户，可重复跑）
```

本地开发（无 Docker）：`npm install && npm run build`，设置 `DATABASE_URL`，
`REDIS_URL=mock://local` 可起内存版 Redis 接缝（仅沙箱验证用，生产勿用），`npm start`。
单元测试：`npm test`（阶梯计价 bigint 精确性）。

## API

| 方法 | 路径 | 说明 |
|---|---|---|
| POST | `/v1/usage/events` | 摄入用量事件，返回决定与价格版本 |
| GET | `/v1/decisions?tenant_id=&idempotency_key=&decision=&limit=` | 决定审计查询（三条链路的追溯入口） |
| GET | `/v1/tenants/:tenantId/usage` | 周期累计：数据库权威值 + Redis 缓存 + 一致性标记 |
| GET | `/v1/contracts?tenant_id=` | 合同与阶梯价（采购溯源） |
| GET | `/healthz` `/readyz` | 存活 / 就绪（PG + Redis 探活） |

### 事件与决定

请求体（`contracts/usage-event.json` 为示例）：

```json
{
  "tenant_id": "tenant-apac-1", "caller_id": "svc-chat-gateway",
  "model": "claude-sonnet-5", "input_tokens": 1000, "output_tokens": 500,
  "idempotency_key": "req-01J8ZK3E7A", "occurred_at": "2026-09-19T06:00:00Z"
}
```

| 决定 | HTTP | 含义 |
|---|---|---|
| `allowed` | 202 | 已入账，返回 `cost_micros` / `price_version` / `running_total_micros` |
| `duplicate` | 200 | 幂等键已入账，返回首次计费结果，不重复入账 |
| `throttled` | 429 | 超软阈值且限速窗口已满，带 `Retry-After`，客户端应退避重试 |
| `rejected` | 403 | 超硬阈值，拒绝且不入账 |
| `rejected_no_contract` | 422 | 事件发生时间无合同覆盖或模型无阶梯价 |

金额一律为整数 micros（1e-6 货币单位）以字符串传输，杜绝浮点误差。

## 三条链路的追溯方式

```bash
# 1. 限速链：同一幂等键 throttled →（按 Retry-After 重试）→ allowed
curl 'http://localhost:8080/v1/decisions?tenant_id=tenant-apac-1&idempotency_key=<KEY>'

# 2. 拒绝链：硬顶后每次 403 都落审计，含 price_version 与 running_total_micros
curl 'http://localhost:8080/v1/decisions?tenant_id=tenant-apac-1&decision=rejected'

# 3. 补发链：occurred_at 明显早于接收时间的事件带 is_backfill=true；重放已入账键 → duplicate
curl 'http://localhost:8080/v1/decisions?tenant_id=tenant-apac-1'   # 过滤 is_backfill / duplicate
```

`npm run loadtest` 自动构造上述全部场景并断言 21 项不变量（含「审计 allowed 之和 == 周期累计」
账实相符校验、压测期间累计额单调不回退采样、重启前 Redis 缓存与库一致）。

## 关键语义

- **幂等**：`(tenant_id, idempotency_key)` 唯一约束。已入账键的重放返回 200 + 首次结果；
  被 429/403 的键未入账，重试会被重新评估——决定链完整记录每次尝试。
- **乱序/补发**：按 `occurred_at` 定位合同版本与计费周期；累计是加法运算，乱序不影响总额；
  总额只增不减（数据库行锁内 `+=`），任何情况下不回退。
- **阶梯价**：以周期内累计令牌数定位阶梯；单事件跨档时按「先输入后输出」的确定顺序分段计价，
  结果可重放。每次入账记录 `price_version`，采购可按版本对账。
- **限速车道**：超软阈值后每 `throttle_window_seconds` 窗口只放行 `throttle_rate_per_window`
  个事件（Redis 固定窗口计数），其余 429；超硬阈值一律 403。
- **重启重建**：启动时清空 `guard:usage:*` 并按 `tenant_usage` 权威值重建缓存；
  `GET /v1/tenants/:id/usage` 的 `cache_consistent` 字段可验证一致性。

## 环境变量

| 变量 | 默认 | 说明 |
|---|---|---|
| `PORT` | 8080 | 服务端口 |
| `DATABASE_URL` | `postgres://guard:guard@localhost:5432/guard` | PostgreSQL 连接串 |
| `REDIS_URL` | `redis://localhost:6379/0` | Redis 连接串；`mock://` 前缀启用内存接缝（仅沙箱） |
| `BACKFILL_SKEW_SECONDS` | 60 | occurred_at 早于接收时间超过该值即标记补发 |
| `SOFT_LIMIT_MICROS` / `HARD_LIMIT_MICROS` / `THROTTLE_RATE` / `THROTTLE_WINDOW_SECONDS` | 500000 / 1000000 / 10 / 5 | `npm run seed` 写入合同的演示阈值 |

## 目录

```
src/            config / db / redis / pricing（纯函数阶梯计价）/ guardrail（决策引擎）/ app / main
db/001_init.sql 幂等建表（服务启动时自动执行）
scripts/        seed.mjs（灌合同）、load-test.mjs（并发压测+链路验证）、lib-seed.mjs
test/           阶梯计价单元测试（node:test）
contracts/      事件与合同 JSON 示例
```
