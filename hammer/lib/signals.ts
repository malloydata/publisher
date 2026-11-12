// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

// Ctrl-C and SIGTERM must still tear a run down: an interrupted run otherwise
// leaves its Postgres container behind under a name no later run will find.

type Signal = "SIGINT" | "SIGTERM";

/** What the handler needs from the process, injected in tests. */
export interface SignalHost {
   on(signal: Signal, listener: () => void): unknown;
   off(signal: Signal, listener: () => void): unknown;
   exit(code: number): void;
}

/** The exit code a shell reports for a run ended by `signal`. */
export const SIGNAL_EXIT_CODE: Record<Signal, number> = {
   SIGINT: 130,
   SIGTERM: 143,
};

/**
 * On the first SIGINT or SIGTERM, run `teardown` and exit with the signal's
 * code; a second signal while it runs exits at once (code 1), so a stuck
 * teardown never traps the user. Returns a function that removes the handlers,
 * for when the run finishes normally.
 */
export function installSignalTeardown(
   teardown: () => Promise<void>,
   host: SignalHost = {
      on: (s, l) => process.on(s, l),
      off: (s, l) => process.off(s, l),
      exit: (code) => process.exit(code),
   },
   say: (message: string) => void = (m) => console.error(m),
): () => void {
   let tearingDown = false;
   const handlers = new Map<Signal, () => void>();
   for (const signal of ["SIGINT", "SIGTERM"] as const) {
      const handler = (): void => {
         if (tearingDown) {
            say(`${signal} again: exiting without finishing teardown`);
            host.exit(1);
            return;
         }
         tearingDown = true;
         say(`${signal}: tearing down (send it again to exit at once)`);
         teardown()
            .catch((err: unknown) => {
               say(
                  `teardown failed: ${err instanceof Error ? err.message : String(err)}`,
               );
            })
            .finally(() => host.exit(SIGNAL_EXIT_CODE[signal]));
      };
      handlers.set(signal, handler);
      host.on(signal, handler);
   }
   return () => {
      for (const [signal, handler] of handlers) host.off(signal, handler);
   };
}
