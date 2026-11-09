import { z } from 'zod';

export const SETTINGS = Symbol('settings');

const positiveInt = (fallback: number) =>
  z.coerce.number().int().positive().default(fallback);

const schema = z
  .object({
    MONGODB_URI: z
      .string()
      .regex(/^mongodb(?:\+srv)?:\/\//)
      .default(
        'mongodb://127.0.0.1:27018/?replicaSet=rs0&directConnection=true',
      ),
    MONGODB_DATABASE: z
      .string()
      .regex(/^[a-zA-Z0-9_-]+$/)
      .default('ingest'),
    PORT: positiveInt(3000),
    WORKER_CONCURRENCY: positiveInt(128).pipe(z.number().max(1024)),
    PROCESSING_DELAY_MS: z.coerce.number().int().nonnegative().default(5000),
    PROCESSING_TIMEOUT_MS: positiveInt(15000),
    LEASE_DURATION_MS: positiveInt(30000),
    LEASE_RENEW_MS: positiveInt(10000),
    POLL_INTERVAL_MS: positiveInt(250),
    MAX_ATTEMPTS: positiveInt(5),
    RETRY_BASE_MS: positiveInt(1000),
    RETRY_MAX_MS: positiveInt(30000),
    DB_TIMEOUT_MS: positiveInt(2000),
    SHUTDOWN_GRACE_MS: positiveInt(15000),
  })
  .refine((s) => s.LEASE_RENEW_MS * 2 < s.LEASE_DURATION_MS, {
    message: 'LEASE_DURATION_MS must exceed twice LEASE_RENEW_MS',
  })
  .refine((s) => s.RETRY_BASE_MS <= s.RETRY_MAX_MS, {
    message: 'RETRY_BASE_MS must not exceed RETRY_MAX_MS',
  });

export type Settings = z.infer<typeof schema>;

export function loadSettings(environment: Record<string, unknown>): Settings {
  return schema.parse(environment);
}
