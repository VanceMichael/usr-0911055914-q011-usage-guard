/**
 * 阶梯计价：纯函数、bigint 精确运算。
 *
 * 模型：事件占用的令牌区间为 [T, T+in+out)，其中 T 为周期内已累计的总令牌数。
 * 约定输入令牌先计价（占 [T, T+in)），输出令牌占 [T+in, T+in+out)，
 * 因此同一事件跨阶梯时会按比例落到不同价格档，结果确定且可重放。
 */

export interface PriceTier {
  tierFromTokens: bigint;
  tierToTokens: bigint | null; // null = 无上限
  inputPriceMicrosPer1k: bigint;
  outputPriceMicrosPer1k: bigint;
}

export interface CostBreakdownSegment {
  tierFromTokens: string;
  tierToTokens: string | null;
  inputTokens: string;
  outputTokens: string;
  inputPriceMicrosPer1k: string;
  outputPriceMicrosPer1k: string;
}

export interface CostResult {
  costMicros: bigint;
  segments: CostBreakdownSegment[];
}

function overlap(aFrom: bigint, aTo: bigint, bFrom: bigint, bTo: bigint): bigint {
  const from = aFrom > bFrom ? aFrom : bFrom;
  const to = aTo < bTo ? aTo : bTo;
  return to > from ? to - from : 0n;
}

const INF = BigInt('9223372036854775807'); // bigint 上界，代替 Infinity

export function computeCost(
  tiers: PriceTier[],
  currentTotalTokens: bigint,
  inputTokens: number,
  outputTokens: number,
): CostResult {
  if (tiers.length === 0) throw new Error('no price tiers configured');
  const sorted = [...tiers].sort((a, b) => (a.tierFromTokens < b.tierFromTokens ? -1 : 1));

  const inFrom = currentTotalTokens;
  const inTo = inFrom + BigInt(inputTokens);
  const outFrom = inTo;
  const outTo = outFrom + BigInt(outputTokens);

  let numerator = 0n; // Σ tokens * price_per_1k，最后统一除以 1000
  const segments: CostBreakdownSegment[] = [];

  for (const t of sorted) {
    const tFrom = t.tierFromTokens;
    const tTo = t.tierToTokens ?? INF;
    const inTk = overlap(inFrom, inTo, tFrom, tTo);
    const outTk = overlap(outFrom, outTo, tFrom, tTo);
    if (inTk === 0n && outTk === 0n) continue;
    numerator += inTk * t.inputPriceMicrosPer1k + outTk * t.outputPriceMicrosPer1k;
    segments.push({
      tierFromTokens: t.tierFromTokens.toString(),
      tierToTokens: t.tierToTokens === null ? null : t.tierToTokens.toString(),
      inputTokens: inTk.toString(),
      outputTokens: outTk.toString(),
      inputPriceMicrosPer1k: t.inputPriceMicrosPer1k.toString(),
      outputPriceMicrosPer1k: t.outputPriceMicrosPer1k.toString(),
    });
  }

  const covered = overlap(inFrom, outTo, sorted[0].tierFromTokens, sorted[sorted.length - 1].tierToTokens ?? INF);
  if (covered < outTo - inFrom) {
    throw new Error(`price tiers do not cover token range [${inFrom}, ${outTo})`);
  }

  return { costMicros: numerator / 1000n, segments };
}
