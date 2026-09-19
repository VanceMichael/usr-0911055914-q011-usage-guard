import Redis from 'ioredis';

/**
 * REDIS_URL=mock:// 时使用 ioredis-mock（仅本地无 Docker 的沙箱验证，
 * 接口与 ioredis 一致；Compose/生产环境一律走真实 Redis）。
 */
function createClient(): Redis {
  const url = process.env.REDIS_URL ?? 'redis://localhost:6379/0';
  if (url.startsWith('mock://')) {
    // eslint-disable-next-line @typescript-eslint/no-var-requires
    const RedisMock = require('ioredis-mock');
    return new RedisMock() as Redis;
  }
  return new Redis(url, { lazyConnect: true, maxRetriesPerRequest: 2 });
}

export const redis: Redis = createClient();

export async function connectRedis(): Promise<void> {
  if (redis.status === 'wait') await redis.connect();
}

export async function closeRedis(): Promise<void> {
  redis.disconnect();
}
