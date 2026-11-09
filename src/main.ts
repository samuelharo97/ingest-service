import { existsSync } from 'node:fs';
import { loadEnvFile } from 'node:process';
import { ConsoleLogger } from '@nestjs/common';
import { NestFactory } from '@nestjs/core';
import type { NestExpressApplication } from '@nestjs/platform-express';
import { AppModule } from './app.module.js';
import { configureApp } from './configure-app.js';
import { SETTINGS, type Settings } from './core/config/settings.js';

if (existsSync('.env')) loadEnvFile('.env');

const production = process.env.NODE_ENV === 'production';

const app = await NestFactory.create<NestExpressApplication>(AppModule, {
  logger: new ConsoleLogger({
    json: production,
    logLevels: production
      ? ['log', 'warn', 'error']
      : ['log', 'warn', 'error', 'debug'],
  }),
});
configureApp(app);
app.enableShutdownHooks();
await app.listen(app.get<Settings>(SETTINGS).PORT, '0.0.0.0');
