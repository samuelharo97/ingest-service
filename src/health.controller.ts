import {
  Controller,
  Get,
  Inject,
  ServiceUnavailableException,
} from '@nestjs/common';
import { Database } from './core/database/database.js';
import { EventWorker } from './modules/events/event.worker.js';

enum HealthStatus {
  Ok = 'ok',
  Ready = 'ready',
}

@Controller('health')
export class HealthController {
  constructor(
    @Inject(Database) private readonly database: Database,
    @Inject(EventWorker) private readonly worker: EventWorker,
  ) {}

  @Get('live')
  live() {
    return { status: HealthStatus.Ok };
  }

  @Get('ready')
  async ready() {
    if (!this.worker.ready)
      throw new ServiceUnavailableException('Worker unavailable');
    try {
      await this.database.ping();
    } catch {
      throw new ServiceUnavailableException('Database unavailable');
    }
    return { status: HealthStatus.Ready, ...this.worker.activity };
  }
}
