import {
  Catch,
  type ArgumentsHost,
  type ExceptionFilter,
} from '@nestjs/common';
import { MongoError } from 'mongodb';
import type { Response } from 'express';
import { ConflictError, MissingRecordError } from './events.errors.js';

@Catch(MongoError, ConflictError, MissingRecordError)
export class StoreErrorFilter implements ExceptionFilter {
  catch(
    error: MongoError | ConflictError | MissingRecordError,
    host: ArgumentsHost,
  ): void {
    const response = host.switchToHttp().getResponse<Response>();
    if (error instanceof ConflictError) {
      response.status(409).json({ statusCode: 409, message: error.message });
    } else if (error instanceof MissingRecordError) {
      response.status(404).json({ statusCode: 404, message: error.message });
    } else {
      response.setHeader('Retry-After', '1');
      response.status(503).json({
        statusCode: 503,
        message: 'Storage unavailable; retry with the same eventId',
      });
    }
  }
}
