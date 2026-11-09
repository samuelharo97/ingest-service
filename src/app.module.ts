import { Module } from '@nestjs/common';
import { DatabaseModule } from './core/database/database.module.js';
import { EventsModule } from './modules/events/events.module.js';
import { HealthController } from './health.controller.js';

@Module({
  imports: [DatabaseModule, EventsModule],
  controllers: [HealthController],
})
export class AppModule {}
