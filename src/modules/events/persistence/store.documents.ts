import type {
  EventInput,
  EventResult,
  EventStatus,
  Failure,
} from '../event.js';
import type { OrderKey, PatientState } from '../patient-state.js';

export const EVENTS_COLLECTION = 'events';

export interface EventDocument extends Omit<EventInput, 'ts' | 'data'> {
  _id: string;
  kind: 'event';
  ts: Date;
  contentHash: string;
  // Store JSON text to preserve arbitrary keys. Older records may contain objects.
  data: string | EventInput['data'];
  receivedAt: Date;
  status: EventStatus;
  attempts: number;
  cycleAttempts: number;
  lastError: Failure | null;
  result: EventResult | null;
  completedAt: Date | null;
}

export interface PatientDocument {
  _id: string;
  kind: 'patient';
  patientId: string;
  state: PatientState;
  lastAppliedKey: OrderKey | null;
  pendingCount: number;
  activeEventId: string | null;
  blockedByEventId: string | null;
  nextRunAt: Date;
  lease: { owner: string | null; claimVersion: number; until: Date };
}

export const eventKey = (id: string): string => `event:${id}`;
export const patientKey = (id: string): string => `patient:${id}`;
