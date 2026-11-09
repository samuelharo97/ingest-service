import {
  Inject,
  Injectable,
  type OnApplicationShutdown,
  type OnModuleInit,
} from '@nestjs/common';
import { MongoClient } from 'mongodb';
import { SETTINGS, type Settings } from '../config/settings.js';

@Injectable()
export class Database implements OnModuleInit, OnApplicationShutdown {
  readonly client: MongoClient;
  readonly db;

  constructor(@Inject(SETTINGS) settings: Settings) {
    this.client = new MongoClient(settings.MONGODB_URI, {
      timeoutMS: settings.DB_TIMEOUT_MS,
      serverSelectionTimeoutMS: settings.DB_TIMEOUT_MS,
      maxPoolSize: 128,
      readPreference: 'primary',
      readConcern: { level: 'majority' },
      writeConcern: { w: 'majority', j: true },
    });
    this.db = this.client.db(settings.MONGODB_DATABASE);
  }

  async onModuleInit(): Promise<void> {
    await this.client.connect();
    const hello = await this.db.command({ hello: 1 });
    if (!hello.setName) {
      throw new Error(
        'MongoDB must run as a replica set: transactions are required',
      );
    }
  }

  async ping(): Promise<void> {
    await this.db.command({ ping: 1 });
  }

  async onApplicationShutdown(): Promise<void> {
    await this.client.close();
  }
}
