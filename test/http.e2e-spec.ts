import { EventStatus } from '../src/modules/events/event.js';
import { Test } from '@nestjs/testing';
import type { NestExpressApplication } from '@nestjs/platform-express';
import request from 'supertest';
import { AppModule } from '../src/app.module.js';
import { Database } from '../src/core/database/database.js';
import { configureApp } from '../src/configure-app.js';
import { EventWorker } from '../src/modules/events/event.worker.js';
import {
  EventProcessor,
  SimulatedProcessor,
} from '../src/modules/events/event.processor.js';
import { SETTINGS } from '../src/core/config/settings.js';
import { harness, input, referenceState, waitFor } from './support.js';

describe('HTTP ingestion and recovery', () => {
  let h: Awaited<ReturnType<typeof harness>>;
  let app: NestExpressApplication | undefined;

  beforeEach(async () => {
    h = await harness();
  });
  afterEach(async () => {
    await app?.close();
    app = undefined;
    await h?.close();
  });

  async function start(
    processor: EventProcessor = new SimulatedProcessor(h.settings),
  ) {
    const module = await Test.createTestingModule({ imports: [AppModule] })
      .overrideProvider(SETTINGS)
      .useValue(h.settings)
      .overrideProvider(EventProcessor)
      .useValue(processor)
      .compile();
    app = module.createNestApplication<NestExpressApplication>({
      logger: false,
    });
    configureApp(app);
    await app.init();
    return request(app.getHttpServer());
  }

  it('acknowledges durable acceptance before the processor can finish', async () => {
    const gate = Promise.withResolvers<void>();
    const api = await start({
      async process(event) {
        await gate.promise;
        return { fingerprint: event.contentHash };
      },
    });
    try {
      const accepted = await api.post('/events').send(input(1)).expect(202);
      expect(accepted.headers.location).toBe('/events/event-1');
      expect(accepted.body).toEqual({
        eventId: 'event-1',
        status: 'pending',
        duplicate: false,
      });
      expect((await h.store.getPatient('patient-a')).pendingCount).toBe(1);
      expect((await h.store.getEvent('event-1')).result).toBeNull();
      const duplicate = await api.post('/events').send(input(1)).expect(202);
      expect(duplicate.body.duplicate).toBe(true);
      await api
        .post('/events')
        .send({ ...input(1), data: { value: 2 } })
        .expect(409);
      gate.resolve();
      await waitFor(
        () => h.store.getEvent('event-1'),
        (event) => event.status === EventStatus.Processed,
      );
      const state = await api.get('/patients/patient-a').expect(200);
      expect(state.body).toMatchObject({
        status: 'current',
        pendingCount: 0,
        state: referenceState([input(1)]),
      });
      const outcome = await api.get('/events/event-1').expect(200);
      expect(outcome.body).not.toHaveProperty('data');
      await api.post('/events/event-1/retry').expect(409);
    } finally {
      gate.resolve();
    }
  });

  it('preserves IDs through acceptance, encoded URLs, processing, and retries', async () => {
    const api = await start();
    const event = {
      ...input(1),
      eventId: ` hospital/ação?${'x'.repeat(128)} `,
      patientId: ' paciente/ação #1 ',
    };
    const accepted = await api.post('/events').send(event).expect(202);
    expect(accepted.headers.location).toBe(
      `/events/${encodeURIComponent(event.eventId)}`,
    );
    const outcome = await api.get(accepted.headers.location).expect(200);
    expect(outcome.body.eventId).toBe(event.eventId);
    const duplicate = await api.post('/events').send(event).expect(202);
    expect(duplicate.body.duplicate).toBe(true);
    await waitFor(
      () => h.store.getEvent(event.eventId),
      (result) => result.status === EventStatus.Processed,
    );
    const patient = await api
      .get(`/patients/${encodeURIComponent(event.patientId)}`)
      .expect(200);
    expect(patient.body.patientId).toBe(event.patientId);
    expect(patient.body.state).toEqual(referenceState([event]));
    await api
      .post(`/events/${encodeURIComponent(event.eventId)}/retry`)
      .expect(409);
  });

  it('rejects malformed and oversized bodies before acceptance', async () => {
    const api = await start();
    await api
      .post('/events')
      .send({ ...input(1), eventId: undefined })
      .expect(400);
    await api
      .post('/events')
      .send({ ...input(1), data: [] })
      .expect(400);
    await api
      .post('/events')
      .send({ ...input(1), ts: 'not-a-date' })
      .expect(400);
    await api
      .post('/events')
      .send({ ...input(1), data: { big: 'x'.repeat(262144) } })
      .expect(413);
    expect(await h.database.db.collection('events').countDocuments({})).toBe(0);
    await api.get('/events/missing').expect(404);
    await api.get('/patients/missing').expect(404);
    await api.post('/events/missing/retry').expect(404);
  });

  it('rejects overflowing JSON numbers before storing or processing the event', async () => {
    const processEvent = vi.fn<EventProcessor['process']>();
    const api = await start({ process: processEvent });
    const body = JSON.stringify({ ...input(1), data: { value: 0 } }).replace(
      '"value":0',
      '"value":1e400',
    );
    await api
      .post('/events')
      .set('Content-Type', 'application/json')
      .send(body)
      .expect(400);
    expect(await h.database.db.collection('events').countDocuments({})).toBe(0);
    expect(processEvent).not.toHaveBeenCalled();
  });

  it('rebuilds state when an earlier event arrives during slow processing', async () => {
    const entered = Promise.withResolvers<void>();
    const release = Promise.withResolvers<void>();
    const calls: string[] = [];
    const api = await start({
      async process(event) {
        calls.push(event.eventId);
        if (event.eventId === 'event-2') {
          entered.resolve();
          await release.promise;
        }
        return { fingerprint: event.contentHash };
      },
    });
    try {
      await api.post('/events').send(input(2)).expect(202);
      await entered.promise;
      await api.post('/events').send(input(1)).expect(202);
      release.resolve();
      const patient = await waitFor(
        () => h.store.getPatient('patient-a'),
        (value) => value.pendingCount === 0,
      );
      expect(patient.state).toEqual(referenceState([input(1), input(2)]));
      expect(calls).toEqual(['event-2', 'event-1']);
    } finally {
      release.resolve();
    }
  });

  it('preserves arbitrary hospital payload keys through storage and processing', async () => {
    const received: unknown[] = [];
    const api = await start({
      async process(event) {
        received.push(event.data);
        return { fingerprint: event.contentHash };
      },
    });
    const payload = JSON.parse(
      '{"__proto__":{"reading":42},"constructor":null,"nested":[{"prototype":1,"bad\\u0000key":42}]}',
    );
    const event = { ...input(1), data: payload };
    await api
      .post('/events')
      .set('Content-Type', 'application/json')
      .send(JSON.stringify(event))
      .expect(202);
    await waitFor(
      () => h.store.getEvent(event.eventId),
      (value) => value.status === EventStatus.Processed,
    );
    expect(received).toEqual([payload]);
    const duplicate = await api
      .post('/events')
      .set('Content-Type', 'application/json')
      .send(JSON.stringify(event))
      .expect(202);
    expect(duplicate.body.duplicate).toBe(true);
    await api
      .post('/events')
      .send({ ...event, data: {} })
      .expect(409);
    expect((await h.store.getPatient(event.patientId)).state.appliedCount).toBe(
      1,
    );
  });

  it('exposes a failed event, blocks its patient, and recovers through explicit retry', async () => {
    let fail = true;
    const api = await start({
      async process(event) {
        if (fail && event.eventId === 'event-1')
          throw new Error('sensitive external detail');
        return { fingerprint: event.contentHash };
      },
    });
    await api.post('/events').send(input(1)).expect(202);
    await api.post('/events').send(input(2)).expect(202);
    await api.post('/events').send(input(3, 'patient-b')).expect(202);
    await waitFor(
      () => h.store.getEvent('event-1'),
      (event) => event.status === EventStatus.Failed,
    );
    await waitFor(
      () => h.store.getEvent('event-3'),
      (event) => event.status === EventStatus.Processed,
    );
    expect((await h.store.getEvent('event-2')).attempts).toBe(0);
    const blocked = await api.get('/patients/patient-a').expect(200);
    expect(blocked.body).toMatchObject({
      status: 'blocked',
      blockedByEventId: 'event-1',
      pendingCount: 2,
    });
    const failed = await api.get('/events/event-1').expect(200);
    expect(JSON.stringify(failed.body)).not.toContain('sensitive');
    fail = false;
    await api.post('/events/event-1/retry').expect(202);
    const patient = await waitFor(
      () => h.store.getPatient('patient-a'),
      (value) => value.pendingCount === 0,
    );
    expect(patient.state).toEqual(referenceState([input(1), input(2)]));
    expect((await h.store.getEvent('event-1')).attempts).toBe(4);
  });

  it('stops accepting and reports unready when the worker stops', async () => {
    const api = await start();
    await api.get('/health/ready').expect(200);
    if (!app) throw new Error('Application missing');
    await app.get(EventWorker).onModuleDestroy();
    await api.get('/health/ready').expect(503);
    await api.get('/health/live').expect(200);
    await api.post('/events').send(input(1)).expect(503);
    expect(
      await h.database.db
        .collection('events')
        .countDocuments({ kind: 'event' }),
    ).toBe(0);
  });

  it('returns a retryable error when storage is unavailable without accepting the event', async () => {
    const api = await start();
    if (!app) throw new Error('Application missing');
    await app.get(Database).client.close();
    const response = await api.post('/events').send(input(1)).expect(503);
    expect(response.headers['retry-after']).toBe('1');
    expect(
      await h.database.db
        .collection('events')
        .countDocuments({ kind: 'event' }),
    ).toBe(0);
    await api.get('/health/ready').expect(503);
  });
});
