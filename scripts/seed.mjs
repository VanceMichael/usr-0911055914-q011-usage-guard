#!/usr/bin/env node
/**
 * 种子数据：为演示租户（默认 tenant-apac-1）写入合同 + 阶梯价。
 * 阈值可用环境变量覆盖：SOFT_LIMIT_MICROS / HARD_LIMIT_MICROS / THROTTLE_RATE / THROTTLE_WINDOW_SECONDS
 */
import { seedTenant, DEFAULT_LIMITS } from './lib-seed.mjs';

const TENANT = process.env.TENANT ?? 'tenant-apac-1';
const { contractId } = await seedTenant(TENANT);
console.log(
  `seeded tenant=${TENANT} contract=${contractId} ` +
    `soft=${DEFAULT_LIMITS.softMicros} hard=${DEFAULT_LIMITS.hardMicros} micros ` +
    `throttle=${DEFAULT_LIMITS.throttleRate}/${DEFAULT_LIMITS.throttleWindowSeconds}s`,
);
