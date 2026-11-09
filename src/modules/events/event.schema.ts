import { z } from 'zod';
import type { EventInput } from './event.js';

const identifier = z.string().min(1);

const timestamp = z.iso
  .datetime({ offset: true })
  .refine(
    (value) => !/\.\d{4,}/.test(value),
    'Timestamp precision must be milliseconds or coarser',
  )
  .transform((value) => new Date(value).toISOString());

function hasFiniteNumbers(value: unknown): boolean {
  if (typeof value === 'number') return Number.isFinite(value);
  if (value === null || typeof value !== 'object') return true;
  return Object.values(value).every(hasFiniteNumbers);
}

// JSON.stringify converts non-finite numbers to null, so reject them here.
const data = z
  .custom<EventInput['data']>(
    (value) =>
      value !== null && typeof value === 'object' && !Array.isArray(value),
    'data must be a JSON object',
  )
  .refine(hasFiniteNumbers, 'data numbers must be finite');

export const eventSchema = z.strictObject({
  eventId: identifier,
  patientId: identifier,
  type: z.string().trim().min(1).max(128),
  data,
  ts: timestamp,
}) satisfies z.ZodType<EventInput>;
