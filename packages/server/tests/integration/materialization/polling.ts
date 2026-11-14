// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { pollUntil } from "../../harness/spawn_server";

/**
 * Waits shared by the materialization integration specs: each polls the
 * condition it is waiting for and fails loudly on timeout, rather than
 * sleeping and hoping, or falling through to a confusing assertion later.
 */

/** Wait until GET `url` answers 2xx (e.g. a package that finished loading). */
export async function waitUntilOk(
   url: string,
   what: string,
   timeoutMs = 30_000,
): Promise<void> {
   const ok = await pollUntil(
      async () => {
         try {
            return (await fetch(url)).ok;
         } catch {
            return false; // the server is still coming up
         }
      },
      timeoutMs,
      500,
   );
   if (!ok) throw new Error(`${what} did not become available in time`);
}

/**
 * Read `read()` until `done` holds for its value, and return that value;
 * throw `failure` if `timeoutMs` passes first.
 */
export async function pollFor<T>(
   read: () => Promise<T>,
   done: (value: T) => boolean,
   failure: string,
   timeoutMs: number,
   intervalMs = 250,
): Promise<T> {
   let last: T | undefined;
   const ok = await pollUntil(
      async () => {
         last = await read();
         return done(last);
      },
      timeoutMs,
      intervalMs,
   );
   if (!ok) throw new Error(failure);
   return last as T;
}

/** The statuses a materialization run ends in. */
export const TERMINAL_STATUSES = ["MANIFEST_FILE_READY", "FAILED", "CANCELLED"];

/** True when every row's status is terminal. */
export function allTerminal(rows: Array<Record<string, unknown>>): boolean {
   return rows.every((m) => TERMINAL_STATUSES.includes(m.status as string));
}
