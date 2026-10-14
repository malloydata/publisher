// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * What the notebook's run effect should do about the state it just observed.
 *
 * Pulled out as a pure function because the inline version produced two defects
 * that no test could reach: it first lost the bound on concurrent runs
 * entirely, then, once a settle delay put the bound back, it let an abandoned
 * run fire. Both were reasoning errors about
 * three interacting pieces of state, which is exactly what a table of cases
 * pins and a component test does not.
 */
export interface RunPlanInput {
   /** Whether the notebook is loaded and runnable at all. */
   ready: boolean;
   /** Key of the run that has already been dispatched, or null before the first. */
   lastRunKey: string | null;
   /** Key of a run currently waiting out the settle delay, if any. */
   pendingKey: string | undefined;
   /** Key for the state the notebook is in now. */
   runKey: string;
   /** The `resourceUri|generation` prefix of {@link runKey}, without the parameters. */
   documentKey: string;
}

export interface RunPlan {
   /** Clear the pending timer, because what it would run is no longer current. */
   cancelPending: boolean;
   /** `now` skips the settle delay; `settle` waits it out; `none` does nothing. */
   dispatch: "now" | "settle" | "none";
}

export function planRun({
   ready,
   lastRunKey,
   pendingKey,
   runKey,
   documentKey,
}: RunPlanInput): RunPlan {
   // A notebook that is not loaded still has to disarm. This is the reason the
   // readiness check belongs here rather than as an early return in the effect:
   // as an early return it skipped the cancellation below, so navigating away
   // inside the settle window left a timer holding the PREVIOUS notebook's
   // closure, which then ran a full wave of cells against the notebook the
   // reader had just left and wrote them into the one now on screen.
   if (!ready) {
      return { cancelPending: pendingKey !== undefined, dispatch: "none" };
   }

   // A pending run for any other key is stale the moment the notebook moves on,
   // INCLUDING when it moves back to a key that has already run. That last case
   // is the one that bit: typing a value and deleting it again inside the
   // window returns to an already-run key, and leaving the timer armed meant the
   // abandoned value's results landed on screen with nothing to correct them.
   const cancelPending = pendingKey !== undefined && pendingKey !== runKey;

   if (lastRunKey === runKey) return { cancelPending, dispatch: "none" };
   if (pendingKey === runKey) return { cancelPending, dispatch: "none" };

   // Only a changed PARAMETER waits. A first run, a different notebook, and a
   // package reload all change the document rather than a value, and none of
   // them is a burst worth coalescing: delaying them is just a blank screen.
   const changedDocument =
      lastRunKey === null || !lastRunKey.startsWith(`${documentKey}|`);
   return { cancelPending, dispatch: changedDocument ? "now" : "settle" };
}
