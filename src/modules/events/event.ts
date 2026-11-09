import { createHash } from 'node:crypto';

export type Json =
  | null
  | boolean
  | number
  | string
  | readonly Json[]
  | { readonly [key: string]: Json };

/** Timestamps must be normalized to UTC before acceptance. */
export type EventInput = {
  readonly eventId: string;
  readonly patientId: string;
  readonly type: string;
  readonly data: Readonly<Record<string, Json>>;
  readonly ts: string;
};

export type ProcessingEvent = EventInput & { readonly contentHash: string };

export interface EventResult {
  readonly fingerprint: string;
}

export enum EventStatus {
  Pending = 'pending',
  Processing = 'processing',
  Processed = 'processed',
  Failed = 'failed',
}

export interface Failure {
  code: 'processor_error' | 'processor_timeout' | 'attempts_exhausted';
  at: Date;
}

export interface EventReceipt {
  eventId: string;
  status: EventStatus;
}

export interface Acceptance extends EventReceipt {
  duplicate: boolean;
}

/** Excludes the clinical payload from event status responses. */
export interface EventOutcome extends EventReceipt {
  patientId: string;
  attempts: number;
  cycleAttempts: number;
  lastError: Failure | null;
  result: EventResult | null;
  receivedAt: Date;
  completedAt: Date | null;
}

export function canonicalJson(value: Json): string {
  if (value === null || typeof value !== 'object') return JSON.stringify(value);
  if (Array.isArray(value)) return `[${value.map(canonicalJson).join(',')}]`;
  return `{${Object.entries(value)
    .sort(([a], [b]) => (a < b ? -1 : a > b ? 1 : 0))
    .map(([key, entry]) => `${JSON.stringify(key)}:${canonicalJson(entry)}`)
    .join(',')}}`;
}

export function fingerprint(input: EventInput): string {
  return createHash('sha256').update(canonicalJson(input)).digest('hex');
}
