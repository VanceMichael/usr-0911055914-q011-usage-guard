# 亚太 AI 用量成本护栏

纯后端成本护栏：接收带租户、调用方、模型、令牌数和幂等键的用量事件，按合同时间段与阶梯价实时累计；超过软阈值限速、超过硬阈值拒绝；每次决定与采用的价格版本写入 PostgreSQL，Redis 只用于短窗口限速计数，重启后从事件表重建限额状态。

## 快速开始

```bash
docker compose up --build          # 启动 api + postgres + redis（均带健康检查）
curl http://localhost:8080/health  # {"status":"ok","checks":{"postgres":"ok","redis":"ok"}}
docker compose --profile loadtest run --rm loadtest   # 并发压测：限速/拒绝/补发三链路
```

本地开发：`npm install && npm run build && npm start`（需 `DATABASE_URL`、`REDIS_URL`）；`npm run loadtest` 跑压测。

## 接口

| 方法 | 路径 | 说明 |
|---|---|---|
| GET | `/health` | 探活，同时检查 PG 与 Redis，异常返回 503 |
| POST | `/v1/usage` | 上报用量事件，返回决定（见下） |
| GET | `/v1/tenants/:tenant/state` | 租户各账期累计（成本/令牌） |
| GET | `/v1/tenants/:tenant/decisions` | 租户决定审计流 |
| GET | `/v1/decisions/:idempotency_key` | 单事件完整决定轨迹（补发可追溯） |
| POST | `/admin/contracts` | 创建/更新合同与价格版本 |
| POST | `/admin/rebuild` | 从 `usage_events` 重建 `tenant_state`（启动时自动执行） |

### 用量事件

```json
{"tenant":"team-a","caller":"svc-demo","model":"demo","tokens":1500,
 "idempotency_key":"u-01","occurred_at":"2026-09-19T08:00:00Z"}
```

响应：`decision` ∈ `allowed | throttled | rejected | duplicate`，并携带 `price_version`、`cost_micros`、`total_after_micros`；限速/拒绝返回 429 与 `Retry-After`。金额一律为整数 micro-USD，无浮点误差。

## 关键设计

- **幂等**：`usage_events.idempotency_key` 唯一约束，`ON CONFLICT DO NOTHING`；重复事件返回首次决定并追加一条 `duplicate` 审计，绝不重复计费。
- **乱序不回退**：每个 (租户, 月账期) 一行 `tenant_state`，事务内 `SELECT ... FOR UPDATE` 串行累计；阶梯价按“事件前累计令牌数”求边际成本，与到达顺序无关，总额单调只增。
- **软阈值限速**：累计超软阈值后进入限速区——Redis `INCR guard:rate:{tenant}:{minute}`（70s 过期）短窗口计数，窗口内放行并计费、响应标记 `throttled`，窗口外 429 不计费。
- **硬阈值拒绝**：累计达硬阈值后一律 429，不计费；拒绝同样落审计。
- **价格版本可追溯**：按 `occurred_at` 选择合同与价格版本（补发旧事件用旧价），每次决定记录 `price_version_id` 与版本号。
- **重启重建**：Redis 只存分钟级窗口（可丢失）；启动时与 `/admin/rebuild` 用 `usage_events` 中已计费事件重放汇总，重建 `tenant_state`。

## 压测脚本

`scripts/load-test.js` 并发打满四条链路并断言：软阈值放行→标记→429 限速、硬阈值拒绝且不计费、50 个乱序事件 + 并发补发后总额恰为唯一事件之和、单键审计轨迹 `allowed → duplicate`、清空状态后重建一致。全部断言通过退出码为 0。
