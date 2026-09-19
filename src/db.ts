import { Pool, PoolClient } from 'pg';

export const pool = new Pool({
  connectionString: process.env.DATABASE_URL ?? 'postgres://guard:guard@localhost:5432/guard',
  max: Number(process.env.PG_POOL_MAX ?? 16),
});

/** pg 把 BIGINT 作为字符串返回；统一转 bigint 便于精确运算 */
export function toBigInt(v: string | number | bigint | null | undefined): bigint {
  if (v === null || v === undefined) return 0n;
  return BigInt(v);
}

export async function withTransaction<T>(fn: (client: PoolClient) => Promise<T>): Promise<T> {
  const client = await pool.connect();
  try {
    await client.query('BEGIN');
    const out = await fn(client);
    await client.query('COMMIT');
    return out;
  } catch (err) {
    await client.query('ROLLBACK').catch(() => undefined);
    throw err;
  } finally {
    client.release();
  }
}

export async function closeDb(): Promise<void> {
  await pool.end();
}
