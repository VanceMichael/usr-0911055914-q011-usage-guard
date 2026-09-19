import Redis from 'ioredis';

export const redis = new Redis(process.env.REDIS_URL ?? 'redis://localhost:6379/0', {
  lazyConnect: true,
  maxRetriesPerRequest: 2,
});

/**
 * 短窗口限速计数：key 按 (租户, 分钟) 分片，60s 自然过期。
 * 服务重启后窗口计数最坏情况重置一分钟，长期限额状态由 PG 重建，不受影响。
 * 返回该窗口内当前请求序号（1 起）。
 */
export async function hitWindow(tenant: string): Promise<number> {
  const minute = Math.floor(Date.now() / 60000);
  const key = `guard:rate:${tenant}:${minute}`;
  const n = await redis.incr(key);
  if (n === 1) await redis.expire(key, 70);
  return n;
}
