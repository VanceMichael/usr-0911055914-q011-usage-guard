import { readFileSync } from 'fs';
import { join } from 'path';
import { loadConfig } from './config';
import { pool, closeDb } from './db';
import { connectRedis, closeRedis } from './redis';
import { rebuildUsageCache } from './guardrail';
import { createApp } from './app';

async function runMigrations(): Promise<void> {
  const sql = readFileSync(join(__dirname, '..', 'db', '001_init.sql'), 'utf8');
  await pool.query(sql);
}

async function main(): Promise<void> {
  const cfg = loadConfig();

  await connectRedis();
  await runMigrations();

  // 重启后从数据库重建 Redis 限额状态（数据库是唯一权威）
  const rebuilt = await rebuildUsageCache(cfg);
  console.log(`[boot] usage cache rebuilt from postgres: ${rebuilt} tenant period(s)`);

  const app = createApp(cfg);
  const server = app.listen(cfg.port, '0.0.0.0', () => {
    console.log(`[boot] cost guardrail listening on :${cfg.port}`);
  });

  const shutdown = async (signal: string) => {
    console.log(`[shutdown] ${signal} received, draining...`);
    server.close();
    await closeDb().catch(() => undefined);
    await closeRedis();
    process.exit(0);
  };
  process.on('SIGTERM', () => void shutdown('SIGTERM'));
  process.on('SIGINT', () => void shutdown('SIGINT'));
}

main().catch((err) => {
  console.error('[boot] fatal:', err);
  process.exit(1);
});
