import { useCallback, useEffect, useMemo, useRef, useState } from "react";
import { givensToParams, paramsToGivens } from "../components/given/paramCodec";
import type { GivenValue } from "./givenValue";

/**
 * The state behind a control row: what the controls show, what the queries run
 * with, and how a change gets from one to the other.
 *
 * Those are two different things whenever `autorun=false`. Editing three
 * controls should re-run the queries once, when Apply is pressed, not three
 * times on the way there, which matters most in a notebook, where one control
 * change re-runs every cell in the document.
 */
export interface UseGivensStateOptions {
   /** Declared type per given name; decides how a URL value is read back. */
   declaredTypes: ReadonlyMap<string, string | undefined>;
   /**
    * Starting values from `# artifact givens {…}`. Overridden by anything in
    * `params`, so a link always wins over the file's defaults: otherwise a
    * shared URL would not show what the sender was looking at.
    */
   startingValues?: Record<string, string>;
   /**
    * URL-carried values from the host. Read on mount and whenever they change
    * beneath us (a drill navigating to a new URL for the same dashboard).
    */
   params?: Record<string, string>;
   /**
    * Called with the applied values whenever they change, for the host to write
    * to the URL. It is the *applied* values, not the draft: a link should
    * reproduce what the sender was looking at, not controls they had not
    * committed yet.
    */
   onParamsChange?: (params: Record<string, string>) => void;
   /**
    * Identifies the document the edits belong to, so they are dropped when the
    * host swaps documents underneath this hook. Without it the edits are keyed
    * by their starting VALUES alone, and two documents whose starting values
    * coincide (the common case: both empty) look like one, so the previous
    * document's applied values keep filtering the new one.
    */
   documentKey?: string;
   /** False batches changes behind Apply. */
   autorun: boolean;
}

export interface UseGivensStateResult {
   /** What the controls show. */
   draft: Map<string, GivenValue>;
   /** What queries should run with. Equal to `draft` when autorun. */
   applied: Map<string, GivenValue>;
   setGiven: (name: string, value: GivenValue) => void;
   /**
    * Back to where the document says the controls start, committed.
    *
    * Committed unconditionally, `autorun` or not: Reset is an explicit action
    * like Apply, and leaving it pending makes the button look broken. And back
    * to the *starting values* rather than to empty, because empty is not a
    * state the document describes: clearing to it only to have the starting
    * values flow back in from the URL is what made Reset run the queries twice.
    */
   reset: () => void;
   /** Commit the draft. A no-op when autorun, where it is already committed. */
   apply: () => void;
   /** Whether the draft differs from what is applied. */
   pending: boolean;
}

/**
 * The user's edits, tagged with the starting point they were made against.
 *
 * The tag is what lets a new starting point take effect without an effect to
 * clear the old edits, because edits whose key no longer matches are simply not
 * read. A new starting point means a different dashboard, or the same one at a
 * URL a drill has just pushed.
 */
interface Edits {
   key: string;
   draft: Map<string, GivenValue>;
   applied: Map<string, GivenValue>;
}

/** Values for givens the model no longer declares, dropped. */
function prune(
   values: Map<string, GivenValue>,
   declaredTypes: ReadonlyMap<string, string | undefined>,
): Map<string, GivenValue> {
   const stale = Array.from(values.keys()).filter(
      (name) => !declaredTypes.has(name),
   );
   if (stale.length === 0) return values;
   const next = new Map(values);
   for (const name of stale) next.delete(name);
   return next;
}

/** Same entries, same values: enough for state whose values are primitives. */
function sameParams(
   a: Record<string, string>,
   b: Record<string, string>,
): boolean {
   const aKeys = Object.keys(a);
   if (aKeys.length !== Object.keys(b).length) return false;
   return aKeys.every((key) => a[key] === b[key]);
}

export function useGivensState({
   declaredTypes,
   startingValues,
   params,
   onParamsChange,
   documentKey,
   autorun,
}: UseGivensStateOptions): UseGivensStateResult {
   // Where the document says the controls start, with no URL in it. This is
   // what Reset goes back to, so it deliberately ignores `params`: the URL is
   // the thing being reset away from.
   const startingPoint = useMemo(
      () => paramsToGivens(startingValues ?? {}, declaredTypes),
      // eslint-disable-next-line react-hooks/exhaustive-deps
      [declaredTypes, JSON.stringify(startingValues)],
   );

   // The URL beats the file's starting values, per the precedence above.
   const initial = useMemo(
      () =>
         paramsToGivens(
            { ...(startingValues ?? {}), ...(params ?? {}) },
            declaredTypes,
         ),
      // eslint-disable-next-line react-hooks/exhaustive-deps
      [declaredTypes, JSON.stringify(startingValues), JSON.stringify(params)],
   );

   // Which starting point the edits below belong to. A change means a different
   // document, or the same one at a different URL after a drill.
   //
   // The document is part of the key, not just the values. Keyed by values
   // alone, navigating between two notebooks that both start empty produced
   // equal keys, so the first notebook's applied values stayed `active` and
   // filtered the second, while `lastReported` (equal to them already)
   // suppressed the report that would have put them in the URL: the address bar
   // and the running cells disagreed, and copying the URL reproduced neither.
   const initialKey = useMemo(
      () =>
         JSON.stringify([documentKey ?? null, Array.from(initial.entries())]),
      [documentKey, initial],
   );

   // KNOWN LIMITATION, latent today. A given that has a starting value cannot be
   // cleared: `setGiven(name, null)` drops the key, the report omits it, the host
   // removes it from the URL, `initial` recomputes from `startingValues` and
   // still carries it, `initialKey` therefore changes, the edits are discarded as
   // stale, and the control snaps back to the value the reader just cleared.
   //
   // Not reachable yet: no server populates `startingGivens`, and deliberately
   // not fixed here, because the fix has to tell our OWN echo of the URL (which
   // must not discard edits) from an externally pushed URL such as a drill
   // arriving (which must), and that is the one piece of this hook the drill path
   // depends on and is covered by live tests. Getting it wrong breaks something
   // that works to fix something nothing can reach.
   //
   // The intended shape, for whoever makes it reachable: compare the incoming
   // `params` against `lastReported` below, treat a match as our own echo, and
   // keep the edits (and their key) across it. An explicit `cleared` set on
   // `Edits` is the alternative and needs the same echo test to survive.
   //
   // Only the user's edits are state; `initial` is read through, not copied in.
   //
   // Copying it in on arrival cannot be made correct here. The starting values
   // are not known on mount: they arrive with the manifest, a render later,
   // and every way of adopting them afterwards is wrong in its own way. An
   // effect hands the surfaces the empty map first and the real values a commit
   // later, so a notebook link carrying parameters runs every cell bare and
   // then runs the whole document again. A render-phase update avoids the extra
   // commit but does not survive: React can restart a render attempt and drop
   // the queued updates, which left the controls blank while a stale draft won.
   //
   // Reading through has neither problem. The first render that sees the
   // manifest already reports the right values, and there is no second state to
   // fall out of step with the props.
   const [edits, setEdits] = useState<Edits | null>(null);
   const active = edits?.key === initialKey ? edits : null;

   // Drop values for givens that no longer exist, which is what a model reload
   // can do while the page is open. Sending one the model no longer declares
   // fails the query outright ("unknown given"), so a value outliving its
   // declaration is worse than losing the edit. Values for givens that survive
   // the reload are kept, since re-typing them would be the greater annoyance.
   //
   // `initial` needs none of this: `paramsToGivens` already keeps only declared
   // names, which is why this narrows the edits alone.
   const draft = useMemo(
      () => prune(active?.draft ?? initial, declaredTypes),
      [active, initial, declaredTypes],
   );
   const applied = useMemo(
      () => prune(active?.applied ?? initial, declaredTypes),
      [active, initial, declaredTypes],
   );

   // Edits are made against whatever is on screen now, so a first edit has to
   // start from `initial` rather than from an empty map: otherwise setting one
   // control would silently drop the starting values of the others.
   const base = useCallback(
      (prev: Edits | null): Edits =>
         prev?.key === initialKey
            ? prev
            : { key: initialKey, draft: initial, applied: initial },
      [initialKey, initial],
   );

   // Report applied values outward for the host to put in the URL. Skipped when
   // they already match, so this cannot loop against a host that feeds its URL
   // back in through `params`.
   //
   // Starting values are reported too, not just edits: the address bar is meant
   // to show what the reader is looking at, so a dashboard opened at its
   // declared defaults is already a link that reproduces them.
   //
   // The reference starts at the first render's values rather than empty. The
   // control row is empty until the manifest arrives and says which givens
   // exist, and an unseeded reference would report that emptiness on mount,
   // telling the host to clear a URL whose parameters had not been read yet,
   // wiping the state the link was carrying.
   //
   // ACCEPTED COST of that seeding: when the manifest is already cached, the
   // first render knows the types, so the seed equals the URL's values and no
   // initial report fires at all. A host that learns which names are its own
   // FROM the reports (as `NotebookPage` does) therefore never learns them, and
   // if a later model reload drops one of those givens, its parameter is left
   // behind in the address bar. Kept because the alternative is worse: an
   // unseeded reference wipes a shared link's parameters on mount for any
   // consumer that wires this callback before the manifest lands, and a
   // leftover parameter for an undeclared given is inert, since `paramsToGivens`
   // ignores names the model does not declare.
   const appliedParams = useMemo(
      () => givensToParams(applied, declaredTypes),
      [applied, declaredTypes],
   );
   const lastReported = useRef<Record<string, string>>(appliedParams);
   useEffect(() => {
      if (!onParamsChange) return;
      if (sameParams(lastReported.current, appliedParams)) return;
      lastReported.current = appliedParams;
      onParamsChange(appliedParams);
   }, [appliedParams, onParamsChange]);

   const setGiven = useCallback(
      (name: string, value: GivenValue) => {
         setEdits((prev) => {
            const from = base(prev);
            const next = new Map(from.draft);
            if (value === null) next.delete(name);
            else next.set(name, value);
            return {
               key: from.key,
               draft: next,
               applied: autorun ? next : from.applied,
            };
         });
      },
      [autorun, base],
   );

   const reset = useCallback(() => {
      setEdits((prev) => ({
         key: base(prev).key,
         draft: startingPoint,
         applied: startingPoint,
      }));
   }, [base, startingPoint]);

   const apply = useCallback(() => {
      setEdits((prev) => {
         const from = base(prev);
         return { key: from.key, draft: from.draft, applied: from.draft };
      });
   }, [base]);

   const pending = useMemo(() => {
      if (autorun) return false;
      if (draft.size !== applied.size) return true;
      for (const [name, value] of draft) {
         const other = applied.get(name);
         // Dates are the one non-primitive here, so compare them by instant.
         if (value instanceof Date && other instanceof Date) {
            if (value.getTime() !== other.getTime()) return true;
         } else if (Array.isArray(value) && Array.isArray(other)) {
            if (String(value) !== String(other)) return true;
         } else if (value !== other) {
            return true;
         }
      }
      return false;
   }, [autorun, draft, applied]);

   return { draft, applied, setGiven, reset, apply, pending };
}
