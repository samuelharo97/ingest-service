import {
  Body,
  BadRequestException,
  Controller,
  Get,
  HttpCode,
  Inject,
  Param,
  Post,
  Res,
  ServiceUnavailableException,
  UseFilters,
} from '@nestjs/common';
import type { Response } from 'express';
import { eventSchema } from './event.schema.js';
import { MongoEventStore } from './persistence/mongo-event.store.js';
import { EventWorker } from './event.worker.js';
import { StoreErrorFilter } from './store-error.filter.js';

@Controller()
@UseFilters(StoreErrorFilter)
export class EventsController {
  constructor(
    @Inject(MongoEventStore) private readonly store: MongoEventStore,
    @Inject(EventWorker) private readonly worker: EventWorker,
  ) {}

  @Post('events')
  @HttpCode(202)
  async accept(
    @Body() body: unknown,
    @Res({ passthrough: true }) response: Response,
  ) {
    this.assertRunning();
    const parsed = eventSchema.safeParse(body);
    if (!parsed.success) throw new BadRequestException('Invalid event payload');
    const acceptance = await this.store.accept(parsed.data);
    response.setHeader(
      'Location',
      `/events/${encodeURIComponent(acceptance.eventId)}`,
    );
    return acceptance;
  }

  @Get('events/:eventId')
  event(@Param('eventId') eventId: string) {
    return this.store.getEvent(eventId);
  }

  @Get('patients/:patientId')
  patient(@Param('patientId') patientId: string) {
    return this.store.getPatient(patientId);
  }

  @Post('events/:eventId/retry')
  @HttpCode(202)
  retry(@Param('eventId') eventId: string) {
    this.assertRunning();
    return this.store.retry(eventId);
  }

  private assertRunning(): void {
    if (!this.worker.ready)
      throw new ServiceUnavailableException(
        'Worker is stopping or unavailable',
      );
  }
}
