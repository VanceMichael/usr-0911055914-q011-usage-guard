#!/usr/bin/env node
/**
 * 并发压测 + 三条链路可追溯验证：
 *   1. 限速：累计超过软阈值后，窗口内放行、窗口外 429（Retry-After）
 *   2. 拒绝：累计达到硬阈值后一律 429，且不计费
 *   3. 补发/乱序：同一批事件乱序 + 重复并发重发，总额不回退、只计费一次
 * 运行：BASE_URL=http://localhost:8080 node scripts/load-test.js
 */
const BASE = process.env.BASE_URL ?? 'http://localhost:8080';
const RUN = Date.now().toString(36);

let failures = 0;
function check(name, cond, detail = '') {
  const ok = !!cond;
  console.log(`${ok ? 'PASS' : 'FAIL'}  ${name}${ok ? '' : '  ' + detail}`);
  if (!ok) failures++;
}

async function post(path, body) {
  const res = await fetch(BASE + path, {
    method: 'POST',
    headers: { 'content-type': 'application/json' },
    body: JSON.stringify(body),
  });
  return { status: res.status, retryAfter: res.headers.get('retry-after'), body: await res.json() };
}
async function get(path) {
  const res = await fetch(BASE + path);
  return { status: res.status, body: await res.json() };
}

async function makeContract(tenant, { soft, hard, rpm, per1k }) {
  const r = await post('/admin/contracts', {
    tenant,
    valid_from: '2026-01-01T00:00:00Z',
    valid_to: '2027-01-01T00:00:00Z',
    soft_limit_micros: soft,
    hard_limit_micros: hard,
    throttle_rpm: rpm,
    model: 'demo',
    price_version: 'lt-v1',
    effective_from: '2026-01-01T00:00:00Z',
    tiers: [{ up_to: null, micros_per_1k: per1k }],
  });
  if (r.status !== 200) throw new Error('建合同失败: ' + JSON.stringify(r.body));
}

function ev(tenant, key, tokens, occurredAt) {
  return {
    tenant, caller: 'load-test', model: 'demo', tokens,
    idempotency_key: key,
    occurred_at: occurredAt ?? new Date().toISOString(),
  };
}

async function fire(events, concurrency = 25) {
  const results = [];
  for (let i = 0; i < events.length; i += concurrency) {
    const batch = events.slice(i, i + concurrency).map((e) => post('/v1/usage', e));
    results.push(...(await Promise.all(batch)));
  }
  return results;
}

async function state(tenant) {
  const r = await get(`/v1/tenants/${tenant}/state`);
  return r.body.periods[0] ?? { total_cost_micros: 0, total_tokens: 0 };
}

async function scenarioThrottle() {
  console.log('\n== 场景 1：软阈值限速 ==');
  const t = `lt-throttle-${RUN}`;
  await makeContract(t, { soft: 5000, hard: 10_000_000, rpm: 30, per1k: 100 });
  // 每个事件 1000 tokens = 100 micros；50 个后超过软阈值
  const results = await fire(Array.from({ length: 90 }, (_, i) => ev(t, `${t}-e${i}`, 1000)));
  const billed = results.filter((r) => r.status === 200);
  const throttledOut = results.filter((r) => r.status === 429 && r.body.decision === 'throttled');
  const markedThrottled = billed.filter((r) => r.body.decision === 'throttled');
  check('软阈值内有事件被放行', billed.length > 0, `billed=${billed.length}`);
  check('超软阈值后事件被标记 throttled 并计费', markedThrottled.length > 0);
  check('限速窗口外事件收到 429 + Retry-After',
    throttledOut.length > 0 && throttledOut.every((r) => r.retryAfter !== null),
    `429数=${throttledOut.length}`);
  check('所有决定都带价格版本', results.every((r) => r.body.price_version === 'lt-v1'));
  const st = await state(t);
  const expected = billed.reduce((s, r) => s + r.body.cost_micros, 0);
  check('账期总额 = 已计费事件成本之和', Number(st.total_cost_micros) === expected,
    `state=${st.total_cost_micros} expected=${expected}`);
}

async function scenarioReject() {
  console.log('\n== 场景 2：硬阈值拒绝 ==');
  const t = `lt-reject-${RUN}`;
  await makeContract(t, { soft: 1000, hard: 2000, rpm: 10_000, per1k: 100 });
  const results = await fire(Array.from({ length: 40 }, (_, i) => ev(t, `${t}-e${i}`, 1000)));
  const rejected = results.filter((r) => r.status === 429 && r.body.decision === 'rejected');
  check('达到硬阈值后出现 rejected', rejected.length > 0, `rejected=${rejected.length}`);
  const st = await state(t);
  check('拒绝事件不计费：总额不超过硬阈值+单事件成本',
    Number(st.total_cost_micros) <= 2000 + 100, `total=${st.total_cost_micros}`);
  const dec = await get(`/v1/tenants/${t}/decisions?limit=100`);
  check('拒绝决定已落审计（可追溯）',
    dec.body.decisions.some((d) => d.decision === 'rejected' && d.price_version === 'lt-v1'));
}

async function scenarioReplay() {
  console.log('\n== 场景 3：乱序 + 补发（幂等） ==');
  const t = `lt-replay-${RUN}`;
  await makeContract(t, { soft: 10_000_000, hard: 20_000_000, rpm: 10_000, per1k: 100 });
  // 乱序：occurred_at 在过去一小时内随机分布
  const unique = Array.from({ length: 50 }, (_, i) =>
    ev(t, `${t}-e${i}`, 1000, new Date(Date.now() - Math.floor(Math.random() * 3600_000)).toISOString()));
  const first = await fire(unique);
  check('首轮 50 个唯一事件全部计费',
    first.every((r) => r.status === 200 && r.body.billed === true),
    JSON.stringify(first.find((r) => r.status !== 200)?.body));
  const stAfterFirst = await state(t);
  // 并发补发同一批幂等键
  const replay = await fire(unique, 50);
  check('补发全部识别为 duplicate',
    replay.every((r) => r.status === 200 && r.body.decision === 'duplicate' && r.body.billed === false));
  const stAfterReplay = await state(t);
  const expectedTotal = 50 * 100; // 50 事件 × 1000 tokens × 100 micros/1k
  check(`补发后总额不回退也不膨胀（恰好 ${expectedTotal} micros）`,
    Number(stAfterReplay.total_cost_micros) === expectedTotal &&
    Number(stAfterFirst.total_cost_micros) === expectedTotal,
    `first=${stAfterFirst.total_cost_micros} replay=${stAfterReplay.total_cost_micros}`);
  const trace = await get(`/v1/decisions/${t}-e0`);
  check('单事件审计轨迹完整（allowed + duplicate）',
    trace.body.trail?.length === 2 &&
    trace.body.trail[0].decision === 'allowed' &&
    trace.body.trail[1].decision === 'duplicate',
    JSON.stringify(trace.body));
}

async function main() {
  const health = await get('/health');
  check('健康检查', health.status === 200 && health.body.checks?.postgres === 'ok' && health.body.checks?.redis === 'ok',
    JSON.stringify(health.body));
  await scenarioThrottle();
  await scenarioReject();
  await scenarioReplay();
  console.log(failures === 0 ? '\n全部通过 ✅' : `\n${failures} 项失败 ❌`);
  process.exit(failures === 0 ? 0 : 1);
}

main().catch((e) => { console.error(e); process.exit(1); });
