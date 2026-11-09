import type { Collection } from 'mongodb';
import { EventStatus, type EventResult } from '../event.js';
import {
  applyResult,
  compareKeys,
  initialState,
  type OrderKey,
  type PatientState,
} from '../patient-state.js';
import type { EventDocument, PatientDocument } from './store.documents.js';

export async function projectResult(
  events: Collection<EventDocument>,
  patient: PatientDocument,
  event: EventDocument,
  result: EventResult,
  signal: AbortSignal,
): Promise<{
  state: PatientState;
  lastAppliedKey: OrderKey;
  replayed: boolean;
}> {
  signal.throwIfAborted();
  const key = { ts: event.ts, eventId: event.eventId };
  if (!patient.lastAppliedKey || compareKeys(key, patient.lastAppliedKey) > 0) {
    return {
      state: applyResult(patient.state, result),
      lastAppliedKey: key,
      replayed: false,
    };
  }

  let state = initialState();
  let inserted = false;
  const cursor = events.find(
    {
      kind: 'event',
      patientId: event.patientId,
      status: EventStatus.Processed,
    },
    {
      sort: { ts: 1, eventId: 1 },
      projection: { ts: 1, eventId: 1, result: 1 },
      batchSize: 128,
      signal,
      timeoutMode: 'iteration',
    },
  );
  try {
    for await (const previous of cursor) {
      signal.throwIfAborted();
      if (!inserted && compareKeys(key, previous) < 0) {
        state = applyResult(state, result);
        inserted = true;
      }
      if (!previous.result) throw new Error('Processed event has no result');
      state = applyResult(state, previous.result);
    }
  } finally {
    await cursor.close();
  }
  if (!inserted) state = applyResult(state, result);
  return { state, lastAppliedKey: patient.lastAppliedKey, replayed: true };
}
