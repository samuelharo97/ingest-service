import { randomUUID, createHash } from 'node:crypto';
import { setTimeout as sleep } from 'node:timers/promises';
import { inject } from 'vitest';
import { Database } from '../src/core/database/database.js';
import { MongoEventStore } from '../src/modules/events/persistence/mongo-event.store.js';
import { EventWorker } from '../src/modules/events/event.worker.js';
import { eventSchema } from '../src/modules/events/event.schema.js';
import type { EventInput } from '../src/modules/events/event.js';
import { EventProcessor } from '../src/modules/events/event.processor.js';
import { loadSettings, type Settings } from '../src/core/config/settings.js';

export function input(
  number: number,
  patientId = 'patient-a',
  ts = new Date(Date.UTC(2026, 0, 1, 0, 0, number)).toISOString(),
): EventInput {
  return eventSchema.parse({
    eventId: `event-${number}`,
    patientId,
    type: 'observation',
    data: { value: number },
    ts,
  });
}

export async function waitFor<T>(
  read: () => Promise<T>,
  satisfied: (value: T) => boolean,
  timeout = 5000,
): Promise<T> {
  const deadline = Date.now() + timeout;
  do {
    const value = await read();
    if (satisfied(value)) return value;
    await sleep(10);
  } while (Date.now() < deadline);
  throw new Error('Condition did not become true before the deadline');
}

// Compute expected state independently so reducer bugs can fail the tests.
export function referenceState(events: EventInput[]) {
  const sha = (value: string) =>
    createHash('sha256').update(value).digest('hex');
  let digest = sha('');
  for (const event of [...events].sort((a, b) =>
    a.ts < b.ts
      ? -1
      : a.ts > b.ts
        ? 1
        : Buffer.compare(Buffer.from(a.eventId), Buffer.from(b.eventId)),
  )) {
    // This matches canonical JSON only for test inputs with one data property.
    const content = JSON.stringify({
      data: event.data,
      eventId: event.eventId,
      patientId: event.patientId,
      ts: event.ts,
      type: event.type,
    });
    digest = sha(`${digest}:${sha(content)}`);
  }
  return { appliedCount: events.length, digest };
}

export async function harness(overrides: Partial<Settings> = {}) {
  const settings = loadSettings({
    MONGODB_URI: inject('mongoUri'),
    WORKER_CONCURRENCY: 4,
    LEASE_DURATION_MS: 300,
    LEASE_RENEW_MS: 75,
    PROCESSING_DELAY_MS: 10,
    PROCESSING_TIMEOUT_MS: 1000,
    POLL_INTERVAL_MS: 5,
    MAX_ATTEMPTS: 3,
    RETRY_BASE_MS: 20,
    RETRY_MAX_MS: 40,
    DB_TIMEOUT_MS: 5000,
    SHUTDOWN_GRACE_MS: 30,
    ...overrides,
    MONGODB_DATABASE: `ingest_test_${randomUUID().replaceAll('-', '')}`,
  });
  const databases: Database[] = [];
  const workers: EventWorker[] = [];
  async function connect() {
    const database = new Database(settings);
    databases.push(database);
    await database.onModuleInit();
    const store = new MongoEventStore(database, settings);
    await store.onModuleInit();
    return { database, store };
  }
  const { database, store } = await connect();
  return {
    settings,
    database,
    store,
    connect,
    start(processor: EventProcessor, targetStore = store) {
      const worker = new EventWorker(targetStore, processor, settings);
      workers.push(worker);
      worker.onApplicationBootstrap();
      return worker;
    },
    async close() {
      await Promise.all(workers.map((worker) => worker.onModuleDestroy()));
      // The generated database name overrides caller settings, so cleanup is safe.
      await database.db.dropDatabase();
      await Promise.all(databases.map((db) => db.onApplicationShutdown()));
    },
  };
}

export async function processOne(store: MongoEventStore) {
  const claim = await store.claim(randomUUID());
  if (!claim) throw new Error('Expected claimable work');
  const attempt = await store.begin(claim);
  if (!attempt) throw new Error('Expected processable work');
  const result = { fingerprint: attempt.event.contentHash };
  await store.complete(attempt, result, new AbortController().signal);
  return attempt;
}
