import { Database } from '../../dist/core/database/database.js';
import { MongoEventStore } from '../../dist/modules/events/persistence/mongo-event.store.js';
import { EventWorker } from '../../dist/modules/events/event.worker.js';
import { SimulatedProcessor } from '../../dist/modules/events/event.processor.js';
import { loadSettings } from '../../dist/core/config/settings.js';

const settings = loadSettings({
  ...process.env,
  WORKER_CONCURRENCY: 1,
  PROCESSING_DELAY_MS: 10000,
  LEASE_DURATION_MS: 300,
  LEASE_RENEW_MS: 75,
  POLL_INTERVAL_MS: 5,
});
const database = new Database(settings);
await database.onModuleInit();
const store = new MongoEventStore(database, settings);
await store.onModuleInit();
const worker = new EventWorker(
  store,
  new SimulatedProcessor(settings),
  settings,
);
worker.onApplicationBootstrap();
process.send?.('ready');
