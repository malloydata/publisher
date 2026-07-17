/**
 * A user-facing error: the message is printed as-is, with no stack trace, and the
 * process exits non-zero. Use it for every "you did something we can't do" case
 * (a bad name, a frozen config, a missing --data file) so the CLI stays quiet
 * about its own internals and loud about what the user can fix.
 */
export class ScaffoldError extends Error {
   constructor(message: string) {
      super(message);
      this.name = "ScaffoldError";
   }
}
