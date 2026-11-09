import { loadSettings } from './settings.js';

it('uses the documented processing defaults', () => {
  expect(loadSettings({})).toMatchObject({
    WORKER_CONCURRENCY: 128,
    PROCESSING_DELAY_MS: 5000,
    MAX_ATTEMPTS: 5,
  });
});

it.each([
  { WORKER_CONCURRENCY: '0' },
  { WORKER_CONCURRENCY: 'lots' },
  { LEASE_RENEW_MS: 15000 },
  { RETRY_BASE_MS: 40000 },
])('fails startup for invalid configuration: %j', (environment) => {
  expect(() => loadSettings(environment)).toThrow();
});
