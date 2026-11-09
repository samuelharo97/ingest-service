import { setTimeout } from 'node:timers/promises';
import { Inject, Injectable } from '@nestjs/common';
import { SETTINGS, type Settings } from '../../core/config/settings.js';
import type { ProcessingEvent, EventResult } from './event.js';

/**
 * Results must depend only on the event so replay can reuse them.
 * Stop local work on abort. The worker cannot cancel detached background work.
 * Downstream services must deduplicate side effects by eventId.
 */
export abstract class EventProcessor {
  abstract process(
    event: ProcessingEvent,
    signal: AbortSignal,
  ): Promise<EventResult>;
}

@Injectable()
export class SimulatedProcessor extends EventProcessor {
  constructor(@Inject(SETTINGS) private readonly settings: Settings) {
    super();
  }

  async process(
    event: ProcessingEvent,
    signal: AbortSignal,
  ): Promise<EventResult> {
    await setTimeout(this.settings.PROCESSING_DELAY_MS, undefined, { signal });
    return { fingerprint: event.contentHash };
  }
}
