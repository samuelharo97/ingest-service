import { Inject, Injectable, type OnModuleInit } from '@nestjs/common';
import { MongoServerError, type ClientSession, type Filter } from 'mongodb';
import { Database } from '../../../core/database/database.js';
import { SETTINGS, type Settings } from '../../../core/config/settings.js';
import {
  ConflictError,
  LeaseLostError,
  MissingRecordError,
} from '../events.errors.js';
import {
  EventStatus,
  fingerprint,
  type Acceptance,
  type EventInput,
  type EventOutcome,
  type EventReceipt,
  type EventResult,
  type Failure,
  type ProcessingEvent,
} from '../event.js';
import {
  initialState,
  PatientStatus,
  type PatientSnapshot,
} from '../patient-state.js';
import {
  EVENTS_COLLECTION,
  eventKey,
  patientKey,
  type EventDocument,
  type PatientDocument,
} from './store.documents.js';
import { projectResult } from './patient-replay.js';

const releasedLease = { 'lease.owner': null, 'lease.until': new Date(0) };

export interface PatientClaim {
  readonly patientId: string;
  readonly owner: string;
  readonly claimVersion: number;
}

export interface EventAttempt {
  readonly claim: PatientClaim;
  readonly event: ProcessingEvent;
  readonly number: number;
}

@Injectable()
export class MongoEventStore implements OnModuleInit {
  // Both types share one collection. Queries must use a kind or a prefixed ID.
  private readonly events;
  private readonly patients;

  constructor(
    @Inject(Database) private readonly database: Database,
    @Inject(SETTINGS) private readonly settings: Settings,
  ) {
    this.events = database.db.collection<EventDocument>(EVENTS_COLLECTION);
    this.patients = database.db.collection<PatientDocument>(EVENTS_COLLECTION);
  }

  async onModuleInit(): Promise<void> {
    await this.events.createIndexes([
      {
        name: 'patient_events',
        key: { patientId: 1, status: 1, ts: 1, eventId: 1 },
        partialFilterExpression: { kind: 'event' },
      },
      {
        name: 'runnable_patients',
        key: { nextRunAt: 1, 'lease.until': 1 },
        partialFilterExpression: {
          kind: 'patient',
          blockedByEventId: null,
          pendingCount: { $gt: 0 },
        },
      },
    ]);
  }

  async accept(input: EventInput): Promise<Acceptance> {
    const contentHash = fingerprint(input);
    const deadline = Date.now() + this.settings.DB_TIMEOUT_MS;
    // Concurrent first events can race to create the patient record.
    // Retry patient key conflicts. Only an existing event makes a request a duplicate.
    for (;;) {
      try {
        return await this.transaction(
          async (session) => {
            const existing = await this.events.findOne(
              { _id: eventKey(input.eventId) },
              { session },
            );
            if (existing) return this.duplicate(existing, contentHash);

            const event: EventDocument = {
              ...input,
              data: JSON.stringify(input.data),
              _id: eventKey(input.eventId),
              kind: 'event',
              ts: new Date(input.ts),
              contentHash,
              receivedAt: new Date(),
              status: EventStatus.Pending,
              attempts: 0,
              cycleAttempts: 0,
              lastError: null,
              result: null,
              completedAt: null,
            };
            await this.events.insertOne(event, { session });
            await this.patients.updateOne(
              { _id: patientKey(input.patientId) },
              {
                $inc: { pendingCount: 1 },
                $setOnInsert: {
                  kind: 'patient',
                  patientId: input.patientId,
                  state: initialState(),
                  lastAppliedKey: null,
                  activeEventId: null,
                  blockedByEventId: null,
                  nextRunAt: new Date(0),
                  lease: { owner: null, claimVersion: 0, until: new Date(0) },
                },
              },
              { session, upsert: true },
            );
            return {
              eventId: event.eventId,
              status: event.status,
              duplicate: false,
            };
          },
          Math.max(1, deadline - Date.now()),
        );
      } catch (error) {
        if (
          !(error instanceof MongoServerError) ||
          error.code !== 11000 ||
          Date.now() >= deadline
        )
          throw error;
      }
    }
  }

  async getEvent(eventId: string): Promise<EventOutcome> {
    const event = await this.events.findOne({ _id: eventKey(eventId) });
    if (!event) throw new MissingRecordError('Event not found');
    return {
      eventId: event.eventId,
      patientId: event.patientId,
      status: event.status,
      attempts: event.attempts,
      cycleAttempts: event.cycleAttempts,
      lastError: event.lastError,
      result: event.result,
      receivedAt: event.receivedAt,
      completedAt: event.completedAt,
    };
  }

  async getPatient(patientId: string): Promise<PatientSnapshot> {
    const patient = await this.patients.findOne({ _id: patientKey(patientId) });
    if (!patient) throw new MissingRecordError('Patient not found');
    return {
      patientId: patient.patientId,
      state: patient.state,
      pendingCount: patient.pendingCount,
      status: patient.blockedByEventId
        ? PatientStatus.Blocked
        : patient.pendingCount
          ? PatientStatus.CatchingUp
          : PatientStatus.Current,
      blockedByEventId: patient.blockedByEventId,
      lastAppliedKey: patient.lastAppliedKey,
    };
  }

  async claim(owner: string): Promise<PatientClaim | null> {
    const patient = await this.patients.findOneAndUpdate(
      {
        kind: 'patient',
        blockedByEventId: null,
        pendingCount: { $gt: 0 },
        $expr: {
          $and: [
            { $lte: ['$lease.until', '$$NOW'] },
            { $lte: ['$nextRunAt', '$$NOW'] },
          ],
        },
      },
      [
        {
          $set: {
            'lease.owner': { $literal: owner },
            'lease.claimVersion': { $add: ['$lease.claimVersion', 1] },
            'lease.until': this.after(this.settings.LEASE_DURATION_MS),
          },
        },
      ],
      { sort: { nextRunAt: 1 }, returnDocument: 'after' },
    );
    return patient
      ? {
          patientId: patient.patientId,
          owner,
          claimVersion: patient.lease.claimVersion,
        }
      : null;
  }

  async renew(claim: PatientClaim): Promise<void> {
    const result = await this.patients.updateOne(this.owned(claim), [
      { $set: { 'lease.until': this.after(this.settings.LEASE_DURATION_MS) } },
    ]);
    if (!result.matchedCount) throw new LeaseLostError();
  }

  async begin(claim: PatientClaim): Promise<EventAttempt | null> {
    return this.transaction(async (session) => {
      const patient = await this.fence(claim, session);
      const event = await this.events.findOne(
        patient.activeEventId
          ? { _id: eventKey(patient.activeEventId) }
          : {
              kind: 'event',
              patientId: claim.patientId,
              status: EventStatus.Pending,
            },
        { session, sort: { ts: 1, eventId: 1 } },
      );
      if (
        !event ||
        ![EventStatus.Pending, EventStatus.Processing].includes(event.status)
      ) {
        throw new Error('Pending patient has no processable active event');
      }
      // An abandoned attempt still counts, even if its process never recorded failure.
      if (event.cycleAttempts >= this.settings.MAX_ATTEMPTS) {
        await this.events.updateOne(
          { _id: event._id },
          {
            $set: {
              status: EventStatus.Failed,
              lastError: { code: 'attempts_exhausted', at: new Date() },
            },
          },
          { session },
        );
        await this.patients.updateOne(
          { _id: patient._id },
          {
            $set: {
              ...releasedLease,
              activeEventId: event.eventId,
              blockedByEventId: event.eventId,
            },
          },
          { session },
        );
        return null;
      }
      const started = await this.events.findOneAndUpdate(
        { _id: event._id },
        {
          $set: { status: EventStatus.Processing },
          $inc: { attempts: 1, cycleAttempts: 1 },
        },
        { session, returnDocument: 'after' },
      );
      if (!started) throw new Error('Active event disappeared');
      await this.patients.updateOne(
        { _id: patient._id },
        {
          $set: { activeEventId: event.eventId },
        },
        { session },
      );
      return {
        claim,
        number: started.attempts,
        event: {
          eventId: started.eventId,
          patientId: started.patientId,
          type: started.type,
          data:
            typeof started.data === 'string'
              ? JSON.parse(started.data)
              : started.data,
          ts: started.ts.toISOString(),
          contentHash: started.contentHash,
        },
      };
    });
  }

  async complete(
    attempt: EventAttempt,
    result: EventResult,
    signal: AbortSignal,
  ): Promise<{ replayed: boolean }> {
    const {
      claim,
      event: { eventId },
    } = attempt;
    // Compute the projection from stored state. The final transaction rejects
    // the write if another worker has taken ownership during replay.
    const patient = await this.patients.findOne(
      { ...this.owned(claim), activeEventId: eventId },
      { signal },
    );
    if (!patient) throw new LeaseLostError();
    const event = await this.events.findOne(
      {
        _id: eventKey(eventId),
        status: EventStatus.Processing,
        attempts: attempt.number,
      },
      { signal },
    );
    if (!event) throw new LeaseLostError();
    const projection = await projectResult(
      this.events,
      patient,
      event,
      result,
      signal,
    );
    signal.throwIfAborted();

    // Keep replay and external processing outside transactions that MongoDB may rerun.
    await this.transaction(async (session) => {
      const patient = await this.fence(claim, session);
      if (patient.activeEventId !== eventId) throw new LeaseLostError();
      const updated = await this.events.updateOne(
        {
          _id: eventKey(eventId),
          status: EventStatus.Processing,
          attempts: attempt.number,
        },
        {
          $set: {
            status: EventStatus.Processed,
            result,
            completedAt: new Date(),
            lastError: null,
          },
        },
        { session },
      );
      if (!updated.matchedCount) throw new LeaseLostError();
      await this.patients.updateOne(
        { _id: patient._id },
        [
          {
            $set: {
              ...releasedLease,
              activeEventId: null,
              state: { $literal: projection.state },
              lastAppliedKey: { $literal: projection.lastAppliedKey },
              pendingCount: { $subtract: ['$pendingCount', 1] },
              nextRunAt: '$$NOW',
            },
          },
        ],
        { session },
      );
    });
    return { replayed: projection.replayed };
  }

  async fail(attempt: EventAttempt, code: Failure['code']): Promise<void> {
    const {
      claim,
      event: { eventId },
    } = attempt;
    await this.transaction(async (session) => {
      const patient = await this.fence(claim, session);
      if (patient.activeEventId !== eventId) throw new LeaseLostError();
      const event = await this.events.findOne(
        {
          _id: eventKey(eventId),
          status: EventStatus.Processing,
          attempts: attempt.number,
        },
        { session },
      );
      if (!event) throw new LeaseLostError();
      const exhausted = event.cycleAttempts >= this.settings.MAX_ATTEMPTS;
      const cap = Math.min(
        this.settings.RETRY_MAX_MS,
        this.settings.RETRY_BASE_MS * 2 ** (event.cycleAttempts - 1),
      );
      const delay = Math.round(cap * (0.5 + Math.random() / 2));
      await this.events.updateOne(
        { _id: event._id },
        {
          $set: {
            status: exhausted ? EventStatus.Failed : EventStatus.Pending,
            lastError: { code, at: new Date() },
          },
        },
        { session },
      );
      await this.patients.updateOne(
        { _id: patient._id },
        [
          {
            $set: {
              ...releasedLease,
              nextRunAt: this.after(delay),
              blockedByEventId: { $literal: exhausted ? eventId : null },
            },
          },
        ],
        { session },
      );
    });
  }

  async retry(eventId: string): Promise<EventReceipt> {
    return this.transaction(async (session) => {
      const event = await this.events.findOne(
        { _id: eventKey(eventId) },
        { session },
      );
      if (!event) throw new MissingRecordError('Event not found');
      if (event.status === EventStatus.Processed)
        throw new ConflictError('Processed events cannot be retried');
      if (event.status !== EventStatus.Failed)
        return { eventId, status: event.status };
      const patient = await this.patients.updateOne(
        {
          _id: patientKey(event.patientId),
          blockedByEventId: eventId,
          'lease.owner': null,
        },
        [
          {
            $set: {
              ...releasedLease,
              blockedByEventId: null,
              nextRunAt: '$$NOW',
              'lease.claimVersion': { $add: ['$lease.claimVersion', 1] },
            },
          },
        ],
        { session },
      );
      if (!patient.matchedCount)
        throw new ConflictError('Patient is not blocked by this event');
      await this.events.updateOne(
        { _id: event._id },
        {
          $set: { status: EventStatus.Pending, cycleAttempts: 0 },
        },
        { session },
      );
      return { eventId, status: EventStatus.Pending };
    });
  }

  private duplicate(event: EventDocument, contentHash: string): Acceptance {
    if (event.contentHash !== contentHash)
      throw new ConflictError('eventId already belongs to different content');
    return { eventId: event.eventId, status: event.status, duplicate: true };
  }

  private owned(claim: PatientClaim): Filter<PatientDocument> {
    return {
      _id: patientKey(claim.patientId),
      'lease.owner': claim.owner,
      'lease.claimVersion': claim.claimVersion,
      $expr: { $gt: ['$lease.until', '$$NOW'] },
    };
  }

  private after(milliseconds: number) {
    return { $add: ['$$NOW', milliseconds] };
  }

  private async fence(
    claim: PatientClaim,
    session: ClientSession,
  ): Promise<PatientDocument> {
    // Write the lease so a concurrent takeover conflicts with this transaction.
    const patient = await this.patients.findOneAndUpdate(
      this.owned(claim),
      [
        {
          $set: {
            'lease.until': this.after(this.settings.LEASE_DURATION_MS),
          },
        },
      ],
      { session, returnDocument: 'after' },
    );
    if (!patient) throw new LeaseLostError();
    return patient;
  }

  private transaction<T>(
    work: (session: ClientSession) => Promise<T>,
    timeoutMS = this.settings.DB_TIMEOUT_MS,
  ): Promise<T> {
    return this.database.client.withSession((session) =>
      session.withTransaction(() => work(session), {
        readConcern: { level: 'snapshot' },
        writeConcern: { w: 'majority', j: true },
        timeoutMS,
        maxCommitTimeMS: timeoutMS,
      }),
    );
  }
}
