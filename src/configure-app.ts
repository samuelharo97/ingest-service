import type { NestExpressApplication } from '@nestjs/platform-express';

export function configureApp(app: NestExpressApplication): void {
  app.useBodyParser('json', { limit: '256kb' });
}
