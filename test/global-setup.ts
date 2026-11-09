import { MongoMemoryReplSet } from 'mongodb-memory-server-core';
import type { TestProject } from 'vitest/node';

export default async function setup(project: TestProject) {
  if (process.env.TEST_MONGODB_URI) {
    project.provide('mongoUri', process.env.TEST_MONGODB_URI);
    return;
  }
  const mongo = await MongoMemoryReplSet.create({
    binary: { version: '7.0.24' },
    replSet: { count: 1, storageEngine: 'wiredTiger' },
  });
  project.provide('mongoUri', mongo.getUri());
  return () => mongo.stop().then(() => undefined);
}

declare module 'vitest' {
  export interface ProvidedContext {
    mongoUri: string;
  }
}
