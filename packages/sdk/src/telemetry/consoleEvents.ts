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

/**
 * Wrap a mutation's function so it reports itself.
 *
 * The write that reports is the `mutationFn`, not the hook around it, because
 * the Console has two shapes of write and only one of them fits a shared hook.
 * The CRUD dialogs each own a mutation, a snackbar and a dialog to close, and
 * {@link useCrudMutation} holds all three. Connections and Materializations do
 * not: they `mutateAsync` from a caller that awaits the promise, and share one
 * snackbar across several mutations at the component level. Forcing those onto
 * the dialog hook would mean giving them a snackbar each and a dialog to close
 * that they do not have.
 *
 * Wrapping the function instead fits both, and reports the same event either
 * way: it times what the caller actually waits on, it sees the rejection
 * before any handler has turned it into a message, and it re-throws unchanged
 * so nothing downstream can tell it is there.
 */
export function reporting<F extends (variables: never) => Promise<unknown>>(
   resource: ConsoleResource,
   action: ConsoleEvent["action"],
   mutationFn: F,
): F {
   // Returns the SAME function type it was given, so the mutation hook infers
   // its variables from the wrapped function exactly as it did from the bare
   // one. Re-declaring the signature with fresh type parameters broke that
   // inference and collapsed the variables type to `void`.
   return (async (variables: Parameters<F>[0]) => {
      const startedAt = Date.now();
      try {
         const result = await mutationFn(variables);
         reportConsoleEvent({
            type: "console.mutation",
            resource,
            action,
            ok: true,
            durationMs: Date.now() - startedAt,
         });
         return result;
      } catch (error) {
         reportConsoleEvent({
            type: "console.mutation",
            resource,
            action,
            ok: false,
            durationMs: Date.now() - startedAt,
            reason:
               error instanceof Error
                  ? error.message
                  : "An unknown error occurred",
         });
         throw error;
      }
   }) as F;
}
