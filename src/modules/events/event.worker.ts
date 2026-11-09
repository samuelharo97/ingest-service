import { randomUUID } from 'node:crypto';
import { setTimeout as sleep } from 'node:timers/promises';
import {
  Inject,
  Injectable,
  Logger,
  type OnApplicationBootstrap,
  type OnModuleDestroy,
} from '@nestjs/common';
import { SETTINGS, type Settings } from '../../core/config/settings.js';
import { LeaseLostError } from './events.errors.js';
import {
  MongoEventStore,
  type PatientClaim,
} from './persistence/mongo-event.store.js';
import { EventProcessor } from './event.processor.js';

function untilAborted<T>(work: Promise<T>, signal: AbortSignal): Promise<T> {
  let onAbort: () => void;
  const aborted = new Promise<never>((_resolve, reject) => {
    onAbort = () => reject(signal.reason);
    if (signal.aborted) onAbort();
    else signal.addEventListener('abort', onAbort, { once: true });
  });
  return Promise.race([work, aborted]).finally(() =>
    signal.removeEventListener('abort', onAbort),
  );
}

@Injectable()
export class EventWorker implements OnApplicationBootstrap, OnModuleDestroy {
  private readonly logger = new Logger(EventWorker.name);
  private readonly owner = randomUUID();
  private readonly stop = new AbortController();
  private readonly tasks = new Set<Promise<void>>();
  private readonly cancellations = new Set<AbortController>();
  private loop: Promise<void> | undefined;
  private running = false;
  private progressTimer: ReturnType<typeof setInterval> | undefined;
  private processed = 0;
  private failed = 0;

  get activity() {
    return {
      workerId: this.owner,
      active: this.tasks.size,
      concurrency: this.settings.WORKER_CONCURRENCY,
      processed: this.processed,
      failed: this.failed,
    };
  }

  constructor(
    @Inject(MongoEventStore) private readonly store: MongoEventStore,
    @Inject(EventProcessor) private readonly processor: EventProcessor,
    @Inject(SETTINGS) private readonly settings: Settings,
  ) {}

  get ready(): boolean {
    return this.running && !this.stop.signal.aborted;
  }
  get activeCount(): number {
    return this.tasks.size;
  }

  onApplicationBootstrap(): void {
    this.running = true;
    this.logger.log({ event: 'worker_started', ...this.activity });
    this.progressTimer = setInterval(() => {
      this.logger.log({ event: 'worker_progress', ...this.activity });
    }, 30_000);
    this.progressTimer.unref();
    this.loop = this.dispatch()
      .catch((error: unknown) => {
        this.logger.error({
          event: 'dispatcher_stopped',
          workerId: this.owner,
          error: error instanceof Error ? error.name : 'unknown',
        });
      })
      .finally(() => {
        this.running = false;
        clearInterval(this.progressTimer);
      });
  }

  async onModuleDestroy(): Promise<void> {
    clearInterval(this.progressTimer);
    if (!this.stop.signal.aborted)
      this.logger.log({ event: 'worker_stopping', ...this.activity });
    this.stop.abort();
    await this.loop;
    const deadline = new AbortController();
    try {
      await Promise.race([
        Promise.allSettled(this.tasks),
        sleep(this.settings.SHUTDOWN_GRACE_MS, undefined, {
          signal: deadline.signal,
        }),
      ]);
    } finally {
      deadline.abort();
      for (const cancellation of this.cancellations) cancellation.abort();
      await Promise.allSettled(this.tasks);
      this.logger.log({ event: 'worker_stopped', ...this.activity });
    }
  }

  private async dispatch(): Promise<void> {
    let databaseFailures = 0;
    while (!this.stop.signal.aborted) {
      if (this.tasks.size >= this.settings.WORKER_CONCURRENCY) {
        await untilAborted(Promise.race(this.tasks), this.stop.signal).catch(
          () => undefined,
        );
        continue;
      }
      try {
        const claim = await this.store.claim(this.owner);
        if (databaseFailures)
          this.logger.log({ event: 'claim_resumed', workerId: this.owner });
        databaseFailures = 0;
        if (this.stop.signal.aborted) break;
        if (!claim) {
          await this.pause(this.settings.POLL_INTERVAL_MS);
          continue;
        }
        const task = this.processClaim(claim).finally(() => {
          this.tasks.delete(task);
        });
        this.tasks.add(task);
      } catch (error) {
        if (this.stop.signal.aborted) break;
        if (databaseFailures++ === 0) {
          this.logger.warn({
            event: 'claim_unavailable',
            workerId: this.owner,
            error: error instanceof Error ? error.name : 'unknown',
          });
        }
        await this.pause(
          Math.min(5000, 250 * 2 ** Math.min(databaseFailures, 5)),
        );
      }
    }
  }

  private async pause(milliseconds: number): Promise<void> {
    await sleep(milliseconds, undefined, { signal: this.stop.signal }).catch(
      () => undefined,
    );
  }

  private async heartbeat(
    claim: PatientClaim,
    cancellation: AbortController,
  ): Promise<void> {
    while (!cancellation.signal.aborted) {
      try {
        await sleep(this.settings.LEASE_RENEW_MS, undefined, {
          signal: cancellation.signal,
        });
        await this.store.renew(claim);
      } catch (error) {
        if (!cancellation.signal.aborted) {
          this.logger.warn({
            event: 'lease_renewal_failed',
            workerId: this.owner,
            patientId: claim.patientId,
            error: error instanceof Error ? error.name : 'unknown',
          });
          cancellation.abort(error);
        }
      }
    }
  }

  private async processClaim(claim: PatientClaim): Promise<void> {
    const startedAt = performance.now();
    const cancellation = new AbortController();
    this.cancellations.add(cancellation);
    let renewals: Promise<void> | undefined;
    let eventId: string | undefined;
    try {
      const attempt = await this.store.begin(claim);
      if (!attempt) {
        this.logger.warn({
          event: 'patient_blocked',
          workerId: this.owner,
          patientId: claim.patientId,
        });
        return;
      }
      eventId = attempt.event.eventId;
      this.logger.debug({
        event: 'event_started',
        workerId: this.owner,
        eventId,
        attempt: attempt.number,
        claimVersion: claim.claimVersion,
      });
      renewals = this.heartbeat(claim, cancellation);
      const timeout = AbortSignal.timeout(this.settings.PROCESSING_TIMEOUT_MS);
      const signal = AbortSignal.any([cancellation.signal, timeout]);
      let result;
      try {
        result = await untilAborted(
          this.processor.process(attempt.event, signal),
          signal,
        );
      } catch {
        if (cancellation.signal.aborted) return;
        const code = timeout.aborted ? 'processor_timeout' : 'processor_error';
        await this.store.fail(attempt, code);
        this.failed++;
        this.logger.warn({
          event: 'attempt_failed',
          workerId: this.owner,
          durationMs: Math.round(performance.now() - startedAt),
          eventId,
          code,
          attempt: attempt.number,
        });
        return;
      }
      cancellation.signal.throwIfAborted();
      const completion = await this.store.complete(
        attempt,
        result,
        cancellation.signal,
      );
      this.processed++;
      this.logger.debug({
        event: 'event_processed',
        workerId: this.owner,
        durationMs: Math.round(performance.now() - startedAt),
        eventId,
        replayed: completion.replayed,
      });
    } catch (error) {
      // Completion may have committed despite the error. Leave recovery to the
      // next owner, which reads the stored status before resuming.
      if (!(error instanceof LeaseLostError) && !cancellation.signal.aborted) {
        this.logger.warn({
          event: 'attempt_interrupted',
          workerId: this.owner,
          eventId,
          error: error instanceof Error ? error.name : 'unknown',
        });
      }
    } finally {
      cancellation.abort();
      await renewals;
      this.cancellations.delete(cancellation);
    }
  }
}
