import { EventStatus } from '../src/modules/events/event.js';
import { setTimeout as sleep } from 'node:timers/promises';
import {
  patientKey,
  type PatientDocument,
} from '../src/modules/events/persistence/store.documents.js';
import {
  ConflictError,
  LeaseLostError,
} from '../src/modules/events/events.errors.js';
import {
  harness,
  input,
  processOne,
  referenceState,
  waitFor,
} from './support.js';

describe('durable event store', () => {
  let h: Awaited<ReturnType<typeof harness>>;
  beforeEach(async () => {
    h = await harness();
  });
  afterEach(async () => {
    await h?.close();
  });

  it('processes existing records whose payload is stored as an object', async () => {
    const event = input(1);
    await h.store.accept(event);
    await h.database.db
      .collection('events')
      .updateOne(
        { kind: 'event', eventId: event.eventId },
        { $set: { data: event.data } },
      );
    const attempt = await processOne(h.store);
    expect(attempt.event.data).toEqual(event.data);
    expect((await h.store.getPatient(event.patientId)).state).toEqual(
      referenceState([event]),
    );
  });

  it('rolls back event acceptance if scheduling its patient cannot be persisted', async () => {
    await h.database.db.command({
      collMod: 'events',
      validator: { kind: 'event' },
    });
    await expect(h.store.accept(input(1))).rejects.toThrow();
    expect(await h.database.db.collection('events').countDocuments({})).toBe(0);
  });

  it('retains and blocks an event whose last allowed attempt was abandoned', async () => {
    await h.close();
    h = await harness({ MAX_ATTEMPTS: 1 });
    await h.store.accept(input(1));
    const first = await h.store.claim('crashed');
    if (!first) throw new Error('Missing claim');
    await h.store.begin(first);
    await sleep(h.settings.LEASE_DURATION_MS + 30);
    const recovered = await h.store.claim('replacement');
    if (!recovered) throw new Error('Missing recovery claim');
    expect(await h.store.begin(recovered)).toBeNull();
    expect(await h.store.getEvent('event-1')).toMatchObject({
      status: EventStatus.Failed,
      attempts: 1,
      lastError: { code: 'attempts_exhausted' },
    });
    expect(await h.store.getPatient('patient-a')).toMatchObject({
      pendingCount: 1,
      blockedByEventId: 'event-1',
    });
    await h.store.retry('event-1');
    await processOne(h.store);
    expect((await h.store.getEvent('event-1')).attempts).toBe(2);
  });

  it('deduplicates simultaneous submissions across independent connections', async () => {
    const second = await h.connect();
    const responses = await Promise.all(
      Array.from({ length: 20 }, (_, i) =>
        (i % 2 ? second.store : h.store).accept(input(1)),
      ),
    );
    expect(responses.filter((response) => !response.duplicate)).toHaveLength(1);
    expect((await h.store.getPatient('patient-a')).pendingCount).toBe(1);
    expect(
      await h.database.db
        .collection('events')
        .countDocuments({ kind: 'event' }),
    ).toBe(1);
    await expect(
      h.store.accept({ ...input(1), data: { value: 999 } }),
    ).rejects.toBeInstanceOf(ConflictError);
  });

  it('accepts distinct events concurrently when the patient does not yet exist', async () => {
    await Promise.all(
      Array.from({ length: 12 }, (_, i) => h.store.accept(input(i))),
    );
    expect((await h.store.getPatient('patient-a')).pendingCount).toBe(12);
    expect(
      await h.database.db
        .collection('events')
        .countDocuments({ kind: 'event' }),
    ).toBe(12);
    expect(
      await h.database.db
        .collection('events')
        .countDocuments({ kind: 'patient' }),
    ).toBe(1);
    const collections = await h.database.db
      .listCollections({}, { nameOnly: true })
      .toArray();
    expect(collections.map((collection) => collection.name)).toEqual([
      'events',
    ]);
  });

  it('reconstructs chronological state for events arriving after later events completed', async () => {
    for (const n of [2, 4, 1, 3, 5]) {
      await h.store.accept(input(n));
      await processOne(h.store);
    }
    const patient = await h.store.getPatient('patient-a');
    expect(patient.state).toEqual(
      referenceState([1, 2, 3, 4, 5].map((n) => input(n))),
    );
    expect(patient.pendingCount).toBe(0);
    expect((await h.store.accept(input(1))).duplicate).toBe(true);
    expect(await h.store.claim('duplicate-check')).toBeNull();
  });

  it('replays equal timestamps using the same lexical tie-break as MongoDB', async () => {
    const events = ['𐀀', '\uE000'].map((eventId) => ({
      ...input(1),
      eventId,
    }));
    for (const event of events) {
      await h.store.accept(event);
      await processOne(h.store);
    }
    const stored = await h.database.db
      .collection('events')
      .find({ kind: 'event' })
      .sort({ ts: 1, eventId: 1 })
      .toArray();
    expect(stored.map((event) => event.eventId)).toEqual(['\uE000', '𐀀']);
    expect((await h.store.getPatient('patient-a')).state).toEqual(
      referenceState(events),
    );
  });

  it('claims a patient once while allowing another patient to progress', async () => {
    await h.store.accept(input(1));
    await h.store.accept(input(2, 'patient-b'));
    const other = await h.connect();
    const claims = await Promise.all([
      h.store.claim('a'),
      other.store.claim('b'),
      h.store.claim('c'),
    ]);
    expect(claims.filter(Boolean)).toHaveLength(2);
    expect(
      new Set(
        claims
          .filter((claim) => claim !== null)
          .map((claim) => claim.patientId),
      ).size,
    ).toBe(2);
  });

  it('rejects a stale owner after expiry and takeover, including completion and failure', async () => {
    await h.store.accept(input(1));
    const stale = await h.store.claim('stale');
    if (!stale) throw new Error('Missing claim');
    const work = await h.store.begin(stale);
    if (!work) throw new Error('Missing work');
    const result = { fingerprint: work.event.contentHash };
    await sleep(h.settings.LEASE_DURATION_MS + 30);
    const other = await h.connect();
    await processOne(other.store);
    await expect(
      h.store.complete(work, result, new AbortController().signal),
    ).rejects.toBeInstanceOf(LeaseLostError);
    await expect(h.store.fail(work, 'processor_error')).rejects.toBeInstanceOf(
      LeaseLostError,
    );
    await expect(h.store.renew(stale)).rejects.toBeInstanceOf(LeaseLostError);
    expect((await h.store.getPatient('patient-a')).state.appliedCount).toBe(1);
    expect((await h.store.getEvent('event-1')).attempts).toBe(2);
  });

  it('cannot partially commit the event if updating patient state fails', async () => {
    await h.store.accept(input(1));
    await h.database.db.command({
      collMod: 'events',
      validator: {
        $or: [{ kind: 'event' }, { 'state.appliedCount': 0 }],
      },
    });
    await expect(processOne(h.store)).rejects.toThrow();
    expect((await h.store.getEvent('event-1')).result).toBeNull();
    expect((await h.store.getPatient('patient-a')).state.appliedCount).toBe(0);
    expect((await h.store.getPatient('patient-a')).pendingCount).toBe(1);
  });

  it('does not lose an arrival concurrent with completion or count completion twice', async () => {
    await h.store.accept(input(2));
    const claim = await h.store.claim('owner');
    if (!claim) throw new Error('Missing claim');
    const work = await h.store.begin(claim);
    if (!work) throw new Error('Missing work');
    const result = { fingerprint: work.event.contentHash };
    await Promise.all([
      h.store.accept(input(1)),
      h.store.complete(work, result, new AbortController().signal),
    ]);
    await expect(
      h.store.complete(work, result, new AbortController().signal),
    ).rejects.toBeInstanceOf(LeaseLostError);
    expect((await h.store.getPatient('patient-a')).pendingCount).toBe(1);
    await processOne(h.store);
    expect((await h.store.getPatient('patient-a')).state).toEqual(
      referenceState([input(1), input(2)]),
    );
  });

  it('keeps retries ahead of later events and blocks only the failed patient', async () => {
    await h.store.accept(input(1));
    await h.store.accept(input(2));
    for (let n = 0; n < h.settings.MAX_ATTEMPTS; n++) {
      const claim = await waitFor(
        () => h.store.claim('retry'),
        (value) => value !== null,
      );
      if (!claim) throw new Error('Missing claim');
      const work = await h.store.begin(claim);
      expect(work?.event.eventId).toBe('event-1');
      if (!work) throw new Error('Missing work');
      await h.store.fail(work, 'processor_error');
      const patient = await h.database.db
        .collection<PatientDocument>('events')
        .findOne({ _id: patientKey('patient-a') });
      if (!patient) throw new Error('Missing patient');
      const failed = await h.store.getEvent('event-1');
      expect(patient.nextRunAt.getTime()).toBeGreaterThan(
        failed.lastError?.at.getTime() ?? 0,
      );
    }
    expect((await h.store.getEvent('event-1')).status).toBe(EventStatus.Failed);
    expect((await h.store.getEvent('event-2')).attempts).toBe(0);
    await h.store.accept(input(3, 'patient-b'));
    expect((await processOne(h.store)).event.patientId).toBe('patient-b');
    await Promise.all([h.store.retry('event-1'), h.store.retry('event-1')]);
    await processOne(h.store);
    await processOne(h.store);
    expect((await h.store.getEvent('event-1')).attempts).toBe(
      h.settings.MAX_ATTEMPTS + 1,
    );
    expect((await h.store.getPatient('patient-a')).state).toEqual(
      referenceState([input(1), input(2)]),
    );
    await expect(h.store.retry('event-1')).rejects.toBeInstanceOf(
      ConflictError,
    );
  });
});
