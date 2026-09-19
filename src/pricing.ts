export interface Tier {
  /** 累计 token 上界（不含），null 表示无界 */
  up_to: number | null;
  /** 每 1k token 的 micro-USD 单价 */
  micros_per_1k: number;
}

/**
 * 按账期内累计用量走阶梯：给定事件前的累计 token 数，
 * 计算本次 tokens 的边际成本（micro-USD，向上取整到 1 micro）。
 * 与事件到达顺序无关——成本只依赖“之前累计了多少”，
 * 因此乱序事件不会重复跨越阶梯边界导致总额回退。
 */
export function marginalCost(tiers: Tier[], tokensBefore: number, tokens: number): number {
  let cost = 0;
  let remaining = tokens;
  let pos = tokensBefore;
  for (const t of tiers) {
    if (remaining <= 0) break;
    const cap = t.up_to === null ? Number.POSITIVE_INFINITY : t.up_to;
    if (pos >= cap) continue;
    const take = Math.min(remaining, cap - pos);
    cost += Math.ceil((take * t.micros_per_1k) / 1000);
    pos += take;
    remaining -= take;
  }
  return cost;
}
