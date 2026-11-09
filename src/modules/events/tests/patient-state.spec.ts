import { applyResult, compareKeys, initialState } from '../patient-state.js';

describe('patient projection', () => {
  it('is order-sensitive and does not mutate earlier state', () => {
    const empty = initialState();
    const a = { fingerprint: 'a' };
    const b = { fingerprint: 'b' };
    const forward = applyResult(applyResult(empty, a), b);
    const reverse = applyResult(applyResult(empty, b), a);
    expect(forward.appliedCount).toBe(2);
    expect(reverse.appliedCount).toBe(2);
    expect(forward.digest).not.toBe(reverse.digest);
    expect(empty.appliedCount).toBe(0);
  });

  it('orders equal timestamps lexically, independently of locale', () => {
    const ts = new Date('2026-01-01T00:00:00Z');
    const keys = ['𐀀', '\uE000', 'z', 'a', 'A'].map((eventId) => ({
      eventId,
      ts,
    }));
    expect(keys.sort(compareKeys).map((key) => key.eventId)).toEqual([
      'A',
      'a',
      'z',
      '\uE000',
      '𐀀',
    ]);
  });
});
