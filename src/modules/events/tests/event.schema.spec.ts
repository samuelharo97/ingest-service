import { eventSchema } from '../event.schema.js';
import { canonicalJson, fingerprint } from '../event.js';

const input = {
  eventId: 'evt-1',
  patientId: 'patient-1',
  type: 'observation',
  data: { reading: 42, nested: { a: 1, b: [true, null] } },
  ts: '2026-01-01T00:00:00Z',
};

describe('event identity and validation', () => {
  it('normalizes equivalent timestamps and object key orders without changing array order', () => {
    const first = eventSchema.parse(input);
    const second = eventSchema.parse({
      ...input,
      ts: '2025-12-31T21:00:00-03:00',
      data: { nested: { b: [true, null], a: 1 }, reading: 42 },
    });
    expect(fingerprint(first)).toBe(fingerprint(second));
    expect(
      fingerprint(eventSchema.parse({ ...input, data: { reading: 43 } })),
    ).not.toBe(fingerprint(first));
    expect(canonicalJson([1, 2])).not.toBe(canonicalJson([2, 1]));
  });

  it('keeps identical clinical content with different identities distinct', () => {
    expect(fingerprint(eventSchema.parse(input))).not.toBe(
      fingerprint(eventSchema.parse({ ...input, eventId: 'evt-2' })),
    );
  });

  it.each([
    ' hospital/42 ',
    'paciente-ação',
    '!external?id=42',
    'x'.repeat(256),
  ])('preserves nonempty string IDs: %s', (id) => {
    const parsed = eventSchema.parse({ ...input, eventId: id, patientId: id });
    expect(parsed.eventId).toBe(id);
    expect(parsed.patientId).toBe(id);
  });

  it.each([
    { eventId: '' },
    { eventId: 42 },
    { patientId: 42 },
    { eventId: undefined },
    { patientId: '' },
    { data: null },
    { data: [] },
    { ts: '2026-02-30T00:00:00Z' },
    { ts: '2026-01-01T00:00:00' },
    { ts: '2026-01-01T00:00:00.1234Z' },
    { unexpected: true },
  ])('rejects invalid input: %j', (invalid) => {
    expect(eventSchema.safeParse({ ...input, ...invalid }).success).toBe(false);
  });

  it('accepts old events instead of silently imposing a lateness cutoff', () => {
    expect(
      eventSchema.safeParse({ ...input, ts: '1990-01-01T00:00:00Z' }).success,
    ).toBe(true);
  });

  it.each([
    '{"value":1e400}',
    '{"value":-1e400}',
    '{"nested":[{"value":1e400}]}',
    '{"__proto__":{"value":1e400}}',
  ])('rejects numbers that overflow during JSON parsing: %s', (json) => {
    expect(
      eventSchema.safeParse({ ...input, data: JSON.parse(json) }).success,
    ).toBe(false);
  });

  it.each([
    '{"__proto__":{"reading":42},"other":1}',
    '{"nested":[{"__proto__":{"reading":42}}]}',
    '{"constructor":null}',
    '{"nested":[{"prototype":{"reading":42}}]}',
    '{"bad\\u0000key":42}',
    '{"nested":[{"bad\\u0000key":42}]}',
  ])('preserves arbitrary JSON keys: %s', (json) => {
    expect(
      eventSchema.parse({ ...input, data: JSON.parse(json) }).data,
    ).toEqual(JSON.parse(json));
  });
});
