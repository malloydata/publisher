// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * What the Console reports about the things it CHANGES.
 *
 * The dashboard surfaces already report themselves through `DashboardEvent`,
 * which the host wires per surface because each one is mounted with a
 * dashboard's name. The Console's writes are not like that: create, edit and
 * delete for environments, packages, connections and materializations happen
 * inside dialogs the host never mounts directly, and there is no seam to pass
 * a handler down through. So the sink is set once, by the host, at boot.
 *
 * Until now those writes reported to a six-second snackbar and nothing else.
 * There was no record that a package had been deleted, or that a delete had
 * failed — the operations whose outcome someone is most likely to be asked
 * about afterwards.
 */

/** The thing being written. */
export type ConsoleResource =
   | "environment"
   | "package"
   | "connection"
   | "materialization"
   | "schedule"
   | "scope";

export type ConsoleEvent = {
   type: "console.mutation";
   resource: ConsoleResource;
   /** What was done to it. */
   action: "create" | "update" | "delete";
   ok: boolean;
   durationMs: number;
   /** Present when `ok` is false: the message the operator was shown. */
   reason?: string;
};

export type ConsoleEventHandler = (event: ConsoleEvent) => void;

let handler: ConsoleEventHandler | undefined;

/**
 * Install the host's sink. Called once at boot; called again, it replaces.
 * Passing `undefined` removes it, which is what a test wants between cases.
 */
export function setConsoleEventHandler(
   next: ConsoleEventHandler | undefined,
): void {
   handler = next;
}

/**
 * Report one event, if anyone is listening.
 *
 * Never throws, and never lets a sink's own failure reach the caller: this is
 * called from the success and error paths of a write, and a telemetry bug that
 * turned a successful delete into a visible error would be worse than the
 * missing line it is reporting.
 */
export function reportConsoleEvent(event: ConsoleEvent): void {
   if (!handler) return;
   try {
      handler(event);
   } catch {
      // A sink that throws is the sink's problem, not the write's.
   }
}
