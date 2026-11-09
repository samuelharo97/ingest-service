import { Module } from '@nestjs/common';
import { SETTINGS, loadSettings } from './settings.js';

@Module({
  providers: [
    { provide: SETTINGS, useFactory: () => loadSettings(process.env) },
  ],
  exports: [SETTINGS],
})
export class ConfigModule {}
