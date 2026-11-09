import { createHash } from 'node:crypto';
import type { EventResult } from './event.js';

export interface PatientState {
  appliedCount: number;
  digest: string;
}

export interface OrderKey {
  ts: Date;
  eventId: string;
}

export enum PatientStatus {
  Current = 'current',
  CatchingUp = 'catching_up',
  Blocked = 'blocked',
}

export interface PatientSnapshot {
  patientId: string;
  state: PatientState;
  lastAppliedKey: OrderKey | null;
  pendingCount: number;
  blockedByEventId: string | null;
  status: PatientStatus;
}

const hash = (value: string): string =>
  createHash('sha256').update(value).digest('hex');

export const initialState = (): PatientState => ({
  appliedCount: 0,
  digest: hash(''),
});

export function applyResult(
  state: PatientState,
  result: EventResult,
): PatientState {
  return {
    appliedCount: state.appliedCount + 1,
    digest: hash(`${state.digest}:${result.fingerprint}`),
  };
}

export function compareKeys(a: OrderKey, b: OrderKey): number {
  // Match MongoDB's binary string ordering for timestamp ties.
  return (
    a.ts.getTime() - b.ts.getTime() ||
    Buffer.compare(Buffer.from(a.eventId), Buffer.from(b.eventId))
  );
}
