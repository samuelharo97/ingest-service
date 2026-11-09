import assert from 'node:assert/strict';
import { createHash, randomUUID } from 'node:crypto';
import { setTimeout as sleep } from 'node:timers/promises';

const baseUrl = process.env.BASE_URL ?? 'http://127.0.0.1:3000';
const duration = Number(process.env.DURATION_SECONDS ?? 600);
const rate = Number(process.env.EVENTS_PER_MINUTE ?? 1000);
const patientCount = Number(process.env.PATIENTS ?? 256);
for (const [name, value] of Object.entries({ duration, rate, patientCount })) {
  assert(
    Number.isInteger(value) && value > 0,
    `${name} must be a positive integer`,
  );
}
assert(
  patientCount >= 256,
  'Use at least 256 independent patients for this capacity test',
);

const run = randomUUID();
const interval = 60000 / rate;
const total = Math.ceil((rate * duration) / 60);
const expected = new Map();
const latencies = [];
const samples = [];
const failures = [];
const inFlight = new Set();
let accepted = 0;
let duplicates = 0;

async function json(path, options = {}) {
  const response = await fetch(`${baseUrl}${path}`, {
    ...options,
    signal: AbortSignal.timeout(10000),
  });
  assert(response.ok, `${path}: HTTP ${response.status}`);
  return { status: response.status, body: await response.json() };
}

async function submit(event, duplicate) {
  const before = performance.now();
  const { status, body } = await json('/events', {
    method: 'POST',
    headers: { 'content-type': 'application/json' },
    body: JSON.stringify(event),
  });
  latencies.push(performance.now() - before);
  assert.equal(status, 202);
  assert.equal(body.eventId, event.eventId);
  assert.equal(body.duplicate, duplicate);
  if (duplicate) duplicates++;
  else {
    accepted++;
    const events = expected.get(event.patientId) ?? [];
    events.push(event);
    expected.set(event.patientId, events);
  }
}

async function inspect() {
  const ids = [...expected.keys()];
  const patients = [];
  for (let offset = 0; offset < ids.length; offset += 32) {
    patients.push(
      ...(await Promise.all(
        ids
          .slice(offset, offset + 32)
          .map(async (id) => (await json(`/patients/${id}`)).body),
      )),
    );
  }
  return {
    patients,
    pending: patients.reduce((sum, patient) => sum + patient.pendingCount, 0),
    processed: patients.reduce(
      (sum, patient) => sum + patient.state.appliedCount,
      0,
    ),
  };
}

const sha = (value) => createHash('sha256').update(value).digest('hex');
function reference(events) {
  let digest = sha('');
  events.sort((a, b) =>
    a.ts < b.ts ? -1 : a.ts > b.ts ? 1 : a.eventId < b.eventId ? -1 : 1,
  );
  for (const event of events) {
    // This matches canonical JSON only for generated inputs with one data property.
    const content = JSON.stringify({
      data: event.data,
      eventId: event.eventId,
      patientId: event.patientId,
      ts: event.ts,
      type: event.type,
    });
    digest = sha(`${digest}:${sha(content)}`);
  }
  return { appliedCount: events.length, digest };
}

function backlogSlope(points) {
  const meanTime =
    points.reduce((sum, p) => sum + p.seconds, 0) / points.length;
  const meanPending =
    points.reduce((sum, p) => sum + p.pending, 0) / points.length;
  const denominator = points.reduce(
    (sum, p) => sum + (p.seconds - meanTime) ** 2,
    0,
  );
  return (
    (60 *
      points.reduce(
        (sum, p) => sum + (p.seconds - meanTime) * (p.pending - meanPending),
        0,
      )) /
    denominator
  );
}

await json('/health/ready');
const started = performance.now();
const eventEpoch = Date.now();
const monitoring = new AbortController();
const monitor = (async () => {
  while (!monitoring.signal.aborted) {
    try {
      await sleep(15000, undefined, { signal: monitoring.signal });
    } catch {
      break;
    }
    try {
      const snapshot = await inspect();
      const sample = {
        seconds: (performance.now() - started) / 1000,
        accepted,
        processed: snapshot.processed,
        pending: snapshot.pending,
      };
      samples.push(sample);
      console.log(JSON.stringify({ event: 'load_sample', ...sample }));
    } catch (error) {
      failures.push(String(error));
    }
  }
})();

try {
  for (let i = 0; i < total; i++) {
    const delay = started + i * interval - performance.now();
    if (delay > 0) await sleep(delay);
    if (inFlight.size >= 256) await Promise.race(inFlight);
    const event = {
      eventId: `${run}-${String(i).padStart(10, '0')}`,
      patientId: `${run}-patient-${i % patientCount}`,
      type: 'observation',
      data: { value: i },
      // Backdate every twentieth event to exercise replay.
      ts: new Date(
        eventEpoch +
          Math.round((i - (i % 20 === 0 ? 2 * patientCount : 0)) * interval),
      ).toISOString(),
    };
    const task = (async () => {
      await submit(event, false);
      if (i % 10 === 0) await submit(event, true);
    })()
      .catch((error) => {
        failures.push(String(error));
      })
      .finally(() => {
        inFlight.delete(task);
      });
    inFlight.add(task);
  }
  await Promise.all(inFlight);
} finally {
  monitoring.abort();
  await monitor;
}

const offeredSeconds = (performance.now() - started) / 1000;
let final = await inspect();
const drainDeadline = Date.now() + 120000;
while (
  final.pending &&
  Date.now() < drainDeadline &&
  !final.patients.some((p) => p.status === 'blocked')
) {
  await sleep(500);
  final = await inspect();
}

for (const patient of final.patients) {
  assert.equal(
    patient.status,
    'current',
    `Patient ${patient.patientId} did not drain`,
  );
  assert.deepEqual(
    patient.state,
    reference(expected.get(patient.patientId)),
    'Chronological state mismatch',
  );
}

const completionLatencies = [];
for (const events of [...expected.values()].slice(0, 10)) {
  const { body } = await json(`/events/${events[0].eventId}`);
  completionLatencies.push(
    new Date(body.completedAt).getTime() - new Date(body.receivedAt).getTime(),
  );
}
latencies.sort((a, b) => a - b);
const steady = samples.filter((sample) => sample.seconds >= 60);
const slope = steady.length >= 4 ? backlogSlope(steady) : null;
const report = {
  event: 'load_result',
  run,
  requestedDurationSeconds: duration,
  offeredSeconds,
  targetEventsPerMinute: rate,
  offeredEventsPerMinute: (60 * accepted) / offeredSeconds,
  accepted,
  duplicates,
  processed: final.processed,
  pending: final.pending,
  acceptanceP50Ms: latencies[Math.floor(latencies.length * 0.5)],
  acceptanceP95Ms: latencies[Math.floor(latencies.length * 0.95)],
  minimumSampledCompletionMs: Math.min(...completionLatencies),
  steadyStateAssessed: slope !== null,
  backlogSlopeEventsPerMinute: slope,
  failures,
};
console.log(JSON.stringify(report, null, 2));
assert.equal(failures.length, 0, 'Requests or monitoring failed');
assert.equal(accepted, total);
assert.equal(final.processed, accepted);
assert.equal(final.pending, 0);
assert(
  report.offeredEventsPerMinute >= rate * 0.95,
  'Generator did not sustain offered load',
);
assert(
  report.minimumSampledCompletionMs >= 4900,
  'Use the real five-second processor for this benchmark',
);
if (slope !== null)
  assert(slope <= rate * 0.01, 'Backlog increased during steady traffic');
