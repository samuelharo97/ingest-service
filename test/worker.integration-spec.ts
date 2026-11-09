import { EventStatus } from '../src/modules/events/event.js';
import { fork } from 'node:child_process';
import { once } from 'node:events';
import { setTimeout as sleep } from 'node:timers/promises';
import { MongoNetworkError } from 'mongodb';
import { SimulatedProcessor } from '../src/modules/events/event.processor.js';
import { harness, input, referenceState, waitFor } from './support.js';

describe('worker lifecycle across instances', () => {
  let h: Awaited<ReturnType<typeof harness>>;
  beforeEach(async () => {
    h = await harness();
  });
  afterEach(async () => {
    await h?.close();
  });

  it('never starts more attempts than the configured concurrency', async () => {
    const release = Promise.withResolvers<void>();
    let calls = 0;
    const events = Array.from({ length: 10 }, (_, i) =>
      input(i, `patient-${i}`),
    );
    await Promise.all(events.map((event) => h.store.accept(event)));
    const worker = h.start({
      async process(event) {
        calls++;
        await release.promise;
        return { fingerprint: event.contentHash };
      },
    });
    try {
      await waitFor(
        () => Promise.resolve(calls),
        (count) => count === h.settings.WORKER_CONCURRENCY,
      );
      await sleep(30);
      expect(calls).toBe(h.settings.WORKER_CONCURRENCY);
      expect(worker.activeCount).toBe(h.settings.WORKER_CONCURRENCY);
      release.resolve();
      for (const event of events) {
        await waitFor(
          () => h.store.getEvent(event.eventId),
          (value) => value.status === EventStatus.Processed,
        );
      }
      await waitFor(
        () => Promise.resolve(worker.activeCount),
        (count) => count === 0,
      );
      expect(worker.activity).toMatchObject({
        active: 0,
        processed: 10,
        failed: 0,
      });
    } finally {
      release.resolve();
    }
  });

  it('persists a processor timeout without allowing a late result to commit', async () => {
    await h.close();
    h = await harness({
      PROCESSING_TIMEOUT_MS: 30,
      PROCESSING_DELAY_MS: 200,
      MAX_ATTEMPTS: 1,
    });
    await h.store.accept(input(1));
    const worker = h.start(new SimulatedProcessor(h.settings));
    const event = await waitFor(
      () => h.store.getEvent('event-1'),
      (value) => value.status === EventStatus.Failed,
    );
    expect(event.lastError?.code).toBe('processor_timeout');
    await sleep(220);
    expect((await h.store.getEvent('event-1')).result).toBeNull();
    expect((await h.store.getPatient('patient-a')).state.appliedCount).toBe(0);
    expect(worker.activity).toMatchObject({ processed: 0, failed: 1 });
  });

  it('keeps a committed result when its completion acknowledgment is lost', async () => {
    await h.store.accept(input(1));
    const committed = Promise.withResolvers<void>();
    const complete = h.store.complete.bind(h.store);
    const fail = vi.spyOn(h.store, 'fail');
    vi.spyOn(h.store, 'complete').mockImplementationOnce(async (...args) => {
      await complete(...args);
      committed.resolve();
      throw new MongoNetworkError('Completion reply was lost');
    });
    const worker = h.start(new SimulatedProcessor(h.settings));
    await committed.promise;
    await waitFor(
      () => Promise.resolve(worker.activeCount),
      (count) => count === 0,
    );
    expect(fail).not.toHaveBeenCalled();
    expect(await h.store.getEvent('event-1')).toMatchObject({
      status: EventStatus.Processed,
      attempts: 1,
    });
    expect((await h.store.getPatient('patient-a')).state).toEqual(
      referenceState([input(1)]),
    );
    const replacement = await h.connect();
    expect(await replacement.store.claim('replacement')).toBeNull();
    expect((await replacement.store.accept(input(1))).duplicate).toBe(true);
  });

  it('serializes each patient across two workers while processing patients concurrently', async () => {
    const other = await h.connect();
    const active = new Set<string>();
    let peak = 0;
    let overlaps = 0;
    const processor = {
      async process(
        event: Parameters<SimulatedProcessor['process']>[0],
        signal: AbortSignal,
      ) {
        if (active.has(event.patientId)) overlaps++;
        active.add(event.patientId);
        peak = Math.max(peak, active.size);
        try {
          await sleep(40, undefined, { signal });
          return { fingerprint: event.contentHash };
        } finally {
          active.delete(event.patientId);
        }
      },
    };
    const events = Array.from({ length: 12 }, (_, i) =>
      input(i, `patient-${i % 3}`),
    );
    await Promise.all(events.map((event) => h.store.accept(event)));
    h.start(processor);
    h.start(processor, other.store);
    for (let p = 0; p < 3; p++) {
      const patientId = `patient-${p}`;
      const patient = await waitFor(
        () => h.store.getPatient(patientId),
        (value) => value.pendingCount === 0,
      );
      expect(patient.state).toEqual(
        referenceState(events.filter((event) => event.patientId === patientId)),
      );
    }
    expect(overlaps).toBe(0);
    expect(peak).toBeGreaterThan(1);
  });

  it('renews ownership during a processor call longer than the original lease', async () => {
    await h.store.accept(input(1));
    const worker = h.start(
      new SimulatedProcessor({ ...h.settings, PROCESSING_DELAY_MS: 700 }),
    );
    await waitFor(
      () => h.store.getEvent('event-1'),
      (event) => event.status === EventStatus.Processing,
    );
    await sleep(400);
    expect(await h.store.claim('competitor')).toBeNull();
    await waitFor(
      () => h.store.getEvent('event-1'),
      (event) => event.status === EventStatus.Processed,
    );
    expect((await h.store.getEvent('event-1')).attempts).toBe(1);
    await worker.onModuleDestroy();
  });

  it('recovers a real process killed during processing', async () => {
    await h.store.accept(input(1));
    const child = fork(
      new URL('./fixtures/crash-worker.mjs', import.meta.url),
      [],
      {
        env: {
          ...process.env,
          MONGODB_URI: h.settings.MONGODB_URI,
          MONGODB_DATABASE: h.settings.MONGODB_DATABASE,
        },
        stdio: ['ignore', 'ignore', 'pipe', 'ipc'],
      },
    );
    let stderr = '';
    child.stderr?.on('data', (data: Buffer) => {
      stderr += data.toString();
    });
    try {
      await Promise.race([
        once(child, 'message'),
        once(child, 'exit').then(() => {
          throw new Error(`Worker exited before startup: ${stderr}`);
        }),
        sleep(5000).then(() => {
          throw new Error('Child worker startup timeout');
        }),
      ]);
      await waitFor(
        () => h.store.getEvent('event-1'),
        (event) => event.status === EventStatus.Processing,
      );
      const exited = once(child, 'exit');
      child.kill('SIGKILL');
      await exited;
      h.start(new SimulatedProcessor(h.settings));
      await waitFor(
        () => h.store.getEvent('event-1'),
        (event) => event.status === EventStatus.Processed,
      );
      expect((await h.store.getEvent('event-1')).attempts).toBe(2);
      expect((await h.store.getPatient('patient-a')).state).toEqual(
        referenceState([input(1)]),
      );
    } finally {
      if (child.exitCode === null && child.signalCode === null) {
        const exited = once(child, 'exit');
        child.kill('SIGKILL');
        await exited;
      }
    }
  });

  it('leaves interrupted work recoverable after the graceful-shutdown deadline', async () => {
    await h.store.accept(input(1));
    const first = h.start(
      new SimulatedProcessor({ ...h.settings, PROCESSING_DELAY_MS: 5000 }),
    );
    await waitFor(
      () => h.store.getEvent('event-1'),
      (event) => event.status === EventStatus.Processing,
    );
    await first.onModuleDestroy();
    expect(first.ready).toBe(false);
    expect(first.activeCount).toBe(0);
    expect((await h.store.getPatient('patient-a')).pendingCount).toBe(1);
    h.start(new SimulatedProcessor(h.settings));
    await waitFor(
      () => h.store.getEvent('event-1'),
      (event) => event.status === EventStatus.Processed,
    );
    expect((await h.store.getPatient('patient-a')).state.appliedCount).toBe(1);
  });
});
