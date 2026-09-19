import { test } from 'node:test';
import assert from 'node:assert/strict';
import { computeCost } from '../dist/pricing.js';

const tiers = [
  { tierFromTokens: 0n, tierToTokens: 100000n, inputPriceMicrosPer1k: 3000n, outputPriceMicrosPer1k: 15000n },
  { tierFromTokens: 100000n, tierToTokens: null, inputPriceMicrosPer1k: 2400n, outputPriceMicrosPer1k: 12000n },
];

test('单档计价：1000 输入 + 500 输出，全在 tier0', () => {
  const r = computeCost(tiers, 0n, 1000, 500);
  // (1000*3000 + 500*15000) / 1000 = 3000 + 7500 = 10500
  assert.equal(r.costMicros, 10500n);
  assert.equal(r.segments.length, 1);
});

test('跨阶梯：事件跨越 100k 边界时输入/输出分段计价', () => {
  // 当前累计 99000，事件 1000 输入 + 500 输出
  // 输入占 [99000,100000) 全在 tier0；输出占 [100000,100500) 全在 tier1
  const r = computeCost(tiers, 99000n, 1000, 500);
  // 1000*3000/1000 + 500*12000/1000 = 3000 + 6000 = 9000
  assert.equal(r.costMicros, 9000n);
  assert.equal(r.segments.length, 2);
});

test('跨阶梯：输入本身跨档', () => {
  // 当前累计 99500，事件 1000 输入：500 在 tier0、500 在 tier1
  const r = computeCost(tiers, 99500n, 1000, 0);
  // (500*3000 + 500*2400)/1000 = 1500 + 1200 = 2700
  assert.equal(r.costMicros, 2700n);
});

test('大数精度：不走浮点', () => {
  const r = computeCost(tiers, 0n, 1, 0);
  assert.equal(r.costMicros, 3n); // 1 token * 3000/1k = 3 micros
});

test('阶梯未覆盖区间时报错', () => {
  assert.throws(() =>
    computeCost([{ tierFromTokens: 1000n, tierToTokens: null, inputPriceMicrosPer1k: 1n, outputPriceMicrosPer1k: 1n }], 0n, 100, 0),
  );
});
