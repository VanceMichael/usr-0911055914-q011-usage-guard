#!/usr/bin/env node
/**
 * 并发压测 + 三条链路可追溯性验证：
 *   链路一 限速：软阈值之上突发 → 429 + Retry-After → 同幂等键按窗口重试后入账
 *   链路二 拒绝：硬阈值之上 → 403，累计额冻结
 *   链路三 补发：乱序/迟到事件正常入账并标记 is_backfill；重放已入账键 → 200 duplicate
 * 同时验证：并发重试风暴下同一幂等键只入账一次、周期累计单调不回退、
 * 决定审计之和 == 周期累计（账实相符）、Redis 缓存与库一致。
 *
 * 每次运行使用独立租户（自动灌入合同与阶梯价），可重复执行。
 * 运行：npm run loadtest   （BASE_URL / DATABASE_URL 可覆盖）
 */
import { seedTenant } from './lib-seed.mjs';

const BASE_URL = process.env.BASE_URL ?? 'http://localhost:8080';
const MODEL = 'claude-sonnet-5';

const checks = [];
function check(name, ok, detail = '') {
  checks.push({ name, ok });
  console.log(`  ${ok ? 'PASS' : 'FAIL'}  ${name}${detail ? ` — ${detail}` : ''}`);
}
const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

async function api(path) {
  const res = await fetch(`${BASE_URL}${path}`);
  return { status: res.status, body: await res.json() };
}

let TENANT;
async function postEvent(evt) {
  const res = await fetch(`${BASE_URL}/v1/usage/events`, {
    method: 'POST',
    headers: { 'content-type': 'application/json' },
    body: JSON.stringify({ tenant_id: TENANT, caller_id: 'load-test', model: MODEL, ...evt }),
  });
  return { status: res.status, body: await res.json(), retryAfter: res.headers.get('retry-after') };
}

async function runPool(items, concurrency, fn) {
  const results = new Array(items.length);
  let idx = 0;
  await Promise.all(
    Array.from({ length: concurrency }, async () => {
      while (idx < items.length) {
        const i = idx++;
        results[i] = await fn(items[i], i);
      }
    }),
  );
  return results;
}

/** 发送一批事件；遇 429 按 Retry-After 等待后重试，直到放行或拿到终态（403 等） */
async function drainThroughThrottle(events, { maxRounds = 20, log = false } = {}) {
  const pending = new Map(events.map((e) => [e.idempotency_key, e]));
  const results = [];
  let rounds = 0;
  while (pending.size > 0 && rounds < maxRounds) {
    rounds++;
    const rs = await runPool([...pending.values()], 10, async (e) => ({ e, ...(await postEvent(e)) }));
    results.push(...rs);
    let waitSec = 0;
    for (const r of rs) {
      if (r.status === 429) {
        waitSec = Math.max(waitSec, Number(r.retryAfter ?? r.body.retry_after_seconds ?? 5));
      } else {
        pending.delete(r.e.idempotency_key);
      }
    }
    if (log) {
      console.log(
        `  第 ${rounds} 轮：放行 ${rs.filter((r) => r.status === 202).length}，` +
          `限速 ${rs.filter((r) => r.status === 429).length}，拒绝 ${rs.filter((r) => r.status === 403).length}`,
      );
    }
    if (pending.size > 0) await sleep(waitSec * 1000 + 200);
  }
  return { results, pending };
}

const runId = `lt-${Date.now()}`;
let keySeq = 0;
const nextKey = (tag) => `${runId}-${tag}-${keySeq++}`;
const evt = (key, inTk = 1000, outTk = 500, occurredAt = null) => ({
  idempotency_key: key,
  input_tokens: inTk,
  output_tokens: outTk,
  ...(occurredAt ? { occurred_at: occurredAt } : {}),
});

async function tenantTotal() {
  const { body } = await api(`/v1/tenants/${TENANT}/usage`);
  const p = body.periods[0];
  return p ? { cost: BigInt(p.total_cost_micros), events: BigInt(p.event_count), raw: p } : null;
}

async function decisionChain(key) {
  const { body } = await api(
    `/v1/decisions?tenant_id=${TENANT}&idempotency_key=${encodeURIComponent(key)}&limit=100`,
  );
  return body.decisions.slice().sort((a, b) => a.id - b.id);
}

async function main() {
  // 独立租户 + 专属合同：压测可重复运行，互不污染
  TENANT = process.env.TENANT ?? `tenant-${runId}`;
  await seedTenant(TENANT);
  console.log(`load test against ${BASE_URL}, tenant=${TENANT}, run=${runId}\n`);

  // ---------- 阶段 0：健康检查 ----------
  console.log('[phase 0] readiness');
  const ready = await api('/readyz');
  check('GET /readyz 200 且 postgres/redis 均 ok', ready.status === 200 && ready.body.ready === true,
    JSON.stringify(ready.body.checks));
  const baseline = (await tenantTotal()) ?? { cost: 0n, events: 0n };

  // ---------- 阶段 1：正常入账 ----------
  console.log('\n[phase 1] 正常入账（10 事件，并发 5）');
  const p1Keys = Array.from({ length: 10 }, () => nextKey('p1'));
  const p1 = await runPool(p1Keys.map((k) => evt(k)), 5, postEvent);
  check('全部 202 allowed', p1.every((r) => r.status === 202 && r.body.decision === 'allowed'));
  check('每次决定都带价格版本 v2026.09', p1.every((r) => r.body.price_version === 'v2026.09'));

  // ---------- 阶段 2：并发重试风暴 + 乱序（40 键 × 4 副本 = 160 请求）----------
  console.log('\n[phase 2] 重试风暴：40 个幂等键 × 4 副本并发乱序到达（occurred_at 随机过去化）');
  const p2Keys = Array.from({ length: 40 }, () => nextKey('p2'));
  const p2Events = p2Keys.flatMap((k) => {
    const at = new Date(Date.now() - Math.floor(Math.random() * 7200_000)).toISOString();
    return Array.from({ length: 4 }, () => evt(k, 1000, 500, at));
  });
  for (let i = p2Events.length - 1; i > 0; i--) {
    const j = Math.floor(Math.random() * (i + 1));
    [p2Events[i], p2Events[j]] = [p2Events[j], p2Events[i]];
  }
  const samples = [];
  let sampling = true;
  const sampler = (async () => {
    while (sampling) {
      const t = await tenantTotal().catch(() => null);
      if (t) samples.push(t.cost);
      await sleep(120);
    }
  })();
  const p2 = await runPool(p2Events, 20, postEvent);
  sampling = false;
  await sampler;
  const p2Allowed = p2.filter((r) => r.body.decision === 'allowed').length;
  const p2Dup = p2.filter((r) => r.body.decision === 'duplicate').length;
  check('恰好 40 个入账（allowed）', p2Allowed === 40, `allowed=${p2Allowed}`);
  check('恰好 120 个判重（duplicate）', p2Dup === 120, `duplicate=${p2Dup}`);
  let monotonic = true;
  for (let i = 1; i < samples.length; i++) if (samples[i] < samples[i - 1]) monotonic = false;
  check('压测期间累计额单调不回退', monotonic && samples.length > 1, `${samples.length} 个采样点`);
  const afterP2 = await tenantTotal();
  check('event_count 只增 40（副本未重复入账）', afterP2.events - baseline.events === 50n,
    `delta=${afterP2.events - baseline.events}`);
  const dupChain = await decisionChain(p2Keys[0]);
  check('被重放键的决定链 = allowed → duplicate×3',
    dupChain.map((d) => d.decision).join(',') === 'allowed,duplicate,duplicate,duplicate',
    dupChain.map((d) => d.decision).join(' → '));

  // ---------- 阶段 3：软阈值限速 ----------
  console.log('\n[phase 3] 软阈值之上突发 25 事件 → 限速车道（429 + Retry-After），按窗口重试至全部入账');
  const p3Keys = Array.from({ length: 25 }, () => nextKey('p3'));
  const p3 = await drainThroughThrottle(p3Keys.map((k) => evt(k)), { log: true });
  const p3Throttled = p3.results.filter((r) => r.status === 429);
  check('限速车道出现 429 throttled 且带 Retry-After',
    p3Throttled.length > 0 && p3Throttled.every((r) => r.retryAfter !== null),
    `throttled=${p3Throttled.length}`);
  check('重试后 25 个事件全部最终入账（202）',
    p3.pending.size === 0 && p3Keys.every((k) =>
      p3.results.some((r) => r.e.idempotency_key === k && r.status === 202)));
  const throttledKey = p3Throttled[0]?.e.idempotency_key ?? p3Keys[0];
  const throttleChain = await decisionChain(throttledKey);
  const chainStr = throttleChain.map((d) => d.decision).join(' → ');
  check('被限速键的决定链含 throttled → allowed（限速链路可追溯）',
    /throttled/.test(chainStr) && /allowed$/.test(chainStr), chainStr);

  // ---------- 阶段 4：补发 + 重放 ----------
  console.log('\n[phase 4] 补发 3 天前的事件 + 重放已入账幂等键');
  const p4Keys = Array.from({ length: 5 }, () => nextKey('p4'));
  const threeDaysAgo = new Date(Date.now() - 3 * 86400_000).toISOString();
  const p4 = await drainThroughThrottle(p4Keys.map((k) => evt(k, 1000, 500, threeDaysAgo)));
  const p4Final = p4Keys.map((k) =>
    p4.results.find((r) => r.e.idempotency_key === k && r.status === 202));
  check('补发事件全部 202 且 is_backfill=true',
    p4Final.every((r) => r && r.body.is_backfill === true));
  const replay = await postEvent(evt(p1Keys[0]));
  check('重放已入账键 → 200 duplicate（不二次入账）',
    replay.status === 200 && replay.body.decision === 'duplicate');
  const replayChain = await decisionChain(p1Keys[0]);
  check('重放键决定链可追溯（allowed → … → duplicate）',
    replayChain[0]?.decision === 'allowed' &&
    replayChain[replayChain.length - 1]?.decision === 'duplicate',
    replayChain.map((d) => d.decision).join(' → '));

  // ---------- 阶段 5：硬阈值拒绝 ----------
  console.log('\n[phase 5] 大额事件冲顶硬阈值 → 403 rejected，累计额冻结');
  const p5Keys = Array.from({ length: 12 }, () => nextKey('p5'));
  const p5 = await drainThroughThrottle(p5Keys.map((k) => evt(k, 5000, 2000)), { log: true });
  const p5Allowed = p5.results.filter((r) => r.status === 202).length;
  const p5Rejected = p5.results.filter((r) => r.status === 403 && r.body.decision === 'rejected').length;
  check('硬阈值之上出现 403 rejected', p5Rejected > 0, `allowed=${p5Allowed} rejected=${p5Rejected}`);
  const afterHard = await tenantTotal();
  const freezeProbe = await postEvent(evt(nextKey('p5x'), 10, 10));
  const afterProbe = await tenantTotal();
  check('冻结验证：探针事件被拒且总额不变',
    freezeProbe.status === 403 && afterProbe.cost === afterHard.cost,
    `total=${afterProbe.cost}`);
  const rejectedList = await api(`/v1/decisions?tenant_id=${TENANT}&decision=rejected&limit=5`);
  check('拒绝决定已落审计（含 price_version 与 running_total）',
    rejectedList.body.decisions.length > 0 &&
    rejectedList.body.decisions.every((d) => d.price_version === 'v2026.09' && d.running_total_micros !== null));

  // ---------- 阶段 6：账实相符 + 缓存一致 ----------
  console.log('\n[phase 6] 汇总校验');
  const all = await api(`/v1/decisions?tenant_id=${TENANT}&limit=1000`);
  const mine = all.body.decisions;
  const allowedSum = mine
    .filter((d) => d.decision === 'allowed')
    .reduce((acc, d) => acc + BigInt(d.cost_micros), 0n);
  const allowedCount = mine.filter((d) => d.decision === 'allowed').length;
  const final = await tenantTotal();
  check('审计中 allowed 决定之和 == 周期累计（账实相符）',
    allowedSum === final.cost - baseline.cost,
    `sum(allowed)=${allowedSum} == ${final.cost - baseline.cost}`);
  check('allowed 决定数 == event_count', BigInt(allowedCount) === final.events - baseline.events,
    `${allowedCount} == ${final.events - baseline.events}`);
  check('Redis 缓存与数据库一致（重启重建同源）', final.raw.cache_consistent === true,
    `cache=${JSON.stringify(final.raw.redis_cache)}`);
  const backfillRows = mine.filter((d) => d.is_backfill === true && d.decision === 'allowed');
  check('补发事件在审计中可筛出（is_backfill=true）', backfillRows.length >= 5,
    `${backfillRows.length} 行`);

  const failed = checks.filter((c) => !c.ok);
  console.log(`\n===== ${checks.length - failed.length}/${checks.length} 项通过 =====`);
  if (failed.length > 0) {
    console.error('失败项：', failed.map((f) => f.name).join('；'));
    process.exit(1);
  }
  console.log('三条链路（限速/拒绝/补发）均验证通过，全部决定可追溯。');
}

main().catch((e) => {
  console.error('load test 异常终止：', e);
  process.exit(1);
});
