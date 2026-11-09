import { Module } from '@nestjs/common';
import { ConfigModule } from '../../core/config/config.module.js';
import { DatabaseModule } from '../../core/database/database.module.js';
import { MongoEventStore } from './persistence/mongo-event.store.js';
import { EventWorker } from './event.worker.js';
import { EventsController } from './events.controller.js';
import { EventProcessor, SimulatedProcessor } from './event.processor.js';

@Module({
  imports: [ConfigModule, DatabaseModule],
  controllers: [EventsController],
  providers: [
    MongoEventStore,
    EventWorker,
    { provide: EventProcessor, useClass: SimulatedProcessor },
  ],
  exports: [EventWorker],
})
export class EventsModule {}
