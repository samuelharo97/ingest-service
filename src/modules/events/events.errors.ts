export class ConflictError extends Error {}
export class MissingRecordError extends Error {}
export class LeaseLostError extends Error {
  constructor() {
    super('Patient lease no longer belongs to this attempt');
  }
}
