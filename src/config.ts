export interface AppConfig {
  port: number;
  databaseUrl: string;
  redisUrl: string;
  /** occurred_at 早于接收时间超过该秒数即视为补发事件 */
  backfillSkewSeconds: number;
  /** Redis 中周期累计缓存的键前缀 */
  usageCachePrefix: string;
}

export function loadConfig(env: NodeJS.ProcessEnv = process.env): AppConfig {
  return {
    port: Number(env.PORT ?? 8080),
    databaseUrl: env.DATABASE_URL ?? 'postgres://guard:guard@localhost:5432/guard',
    redisUrl: env.REDIS_URL ?? 'redis://localhost:6379/0',
    backfillSkewSeconds: Number(env.BACKFILL_SKEW_SECONDS ?? 60),
    usageCachePrefix: env.USAGE_CACHE_PREFIX ?? 'guard:usage:',
  };
}
