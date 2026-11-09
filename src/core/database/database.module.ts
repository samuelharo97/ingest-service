import { Module } from '@nestjs/common';
import { ConfigModule } from '../config/config.module.js';
import { Database } from './database.js';

@Module({
  imports: [ConfigModule],
  providers: [Database],
  exports: [Database],
})
export class DatabaseModule {}
