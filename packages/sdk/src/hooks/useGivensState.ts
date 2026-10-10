// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

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

/**
 * Entries in a fixed order, so the key below depends on the values and not on
 * the order a host happened to build its object in. `URLSearchParams` iterates
 * in the query string's own order, so `?B=2&A=1` and `?A=1&B=2` would otherwise
 * be different starting points and re-key the reader's edits for nothing.
 */
function stableEntries(source: Record<string, string> | undefined) {
   return Object.entries(source ?? {}).sort(([a], [b]) => (a < b ? -1 : 1));
}

/**
 * Same values for every DECLARED given, ignoring any other parameter a host
 * carries in its URL (a tab, a view option). This is the echo test: whether the
 * incoming URL says nothing about the controls that the last report did not.
 */
function sameDeclaredParams(
   incoming: Record<string, string>,
   reported: Record<string, string>,
   declaredTypes: ReadonlyMap<string, string | undefined>,
): boolean {
   for (const name of declaredTypes.keys()) {
      if (incoming[name] !== reported[name]) return false;
   }
   return true;
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

   // The applied values as last reported to the host, so an incoming `params`
   // can be recognised as this hook's own report coming back. Seeded on the
   // first render below, once `applied` exists; null until then, so nothing on
   // mount reads as an echo.
   const lastReported = useRef<Record<string, string> | null>(null);

   // The report BEFORE `lastReported`, kept only until the newer one is seen
   // coming back.
   //
   // A report does not reach `params` in the same tick: the host navigates, the
   // router commits, and `params` arrives a render or two later. Anything that
   // re-renders this hook in between — a cell finishing, the host's callback
   // changing identity with its location — sees the URL the report has not
   // replaced yet. That URL is STALE, not new, and the echo test below matched
   // only the newest report, so it read as an external push: the anchor moved,
   // the edits were discarded, and `initial` put the value the reader had just
   // cleared straight back into the control, and into the URL after it.
   //
   // Narrow by construction: it holds one report, only while that report is in
   // flight, and is dropped the moment the newer one is observed. The cost is
   // that an external push landing in that window whose values happen to equal
   // the report being replaced is taken for the echo — the same values we were
   // just showing, arriving in the few milliseconds before our own URL lands.
   const priorReported = useRef<Record<string, string> | null>(null);

   // The URL that defines the current starting point. This is `params` EXCEPT
   // when `params` is our own report arriving back through the host, which is
   // not a new starting point and must not be treated as one.
   //
   // The distinction is what lets a given with a starting value be cleared. A
   // clear drops the name from the report, the host removes it from the URL and
   // feeds that URL back in; read naively, that is a changed `params`, the
   // edits are re-keyed away, `initial` recomputes from `startingValues` and
   // still carries the value, and the control snaps back to what the reader
   // just cleared. An echo is recognised by comparing the DECLARED names in
   // `params` against the last report (a host may keep unrelated parameters of
   // its own beside ours), and leaves the anchor where it was. Anything else, a
   // drill landing on this document with new values, a Back button, a pasted
   // link, moves it, and the edits are re-keyed exactly as before.
   //
   // A ref written during render, deliberately: the anchor is memory across
   // renders, the update is idempotent, and putting it in state would cost a
   // second render on every URL change for no observable difference.
   //
   // An echo is only an echo for the SAME document: a navigation to another
   // document arrives with that document's URL, whatever the last report said,
   // so the anchor follows `documentKey` unconditionally.
   const anchor = useRef<{
      documentKey: string | undefined;
      params: Record<string, string> | undefined;
   }>({ documentKey, params });
   const echoes = (reported: Record<string, string> | null) =>
      reported !== null &&
      sameDeclaredParams(params ?? {}, reported, declaredTypes);
   // The newest report has arrived, so the one it replaced is no longer in
   // flight and stops counting as an echo.
   if (echoes(lastReported.current)) priorReported.current = null;
   if (
      anchor.current.documentKey !== documentKey ||
      (!sameParams(params ?? {}, anchor.current.params ?? {}) &&
         !echoes(lastReported.current) &&
         !echoes(priorReported.current))
   ) {
      anchor.current = { documentKey, params };
   }
   const anchorParams = anchor.current.params;
   const startingValuesKey = JSON.stringify(startingValues);
   const anchorParamsKey = JSON.stringify(anchorParams);

   // The URL beats the file's starting values, per the precedence above.
   const initial = useMemo(
      () =>
         paramsToGivens(
            { ...(startingValues ?? {}), ...(anchorParams ?? {}) },
            declaredTypes,
         ),
      // eslint-disable-next-line react-hooks/exhaustive-deps
      [declaredTypes, startingValuesKey, anchorParamsKey],
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
   //
   // Built from the INPUTS that define a starting point, not from `initial`,
   // which is those inputs already filtered by `declaredTypes`. Keying on the
   // filtered map made the set of declared givens part of the key, so a model
   // reload that merely dropped one given re-keyed everything and discarded the
   // reader's edits to the givens that SURVIVED, which is the opposite of what
   // `prune` below is written to do. Dropping stale values is `prune`'s job; a
   // declaration change is not a new starting point.
   const initialKey = useMemo(
      () =>
         JSON.stringify([
            documentKey ?? null,
            stableEntries(startingValues),
            stableEntries(anchorParams),
         ]),
      // eslint-disable-next-line react-hooks/exhaustive-deps
      [documentKey, startingValuesKey, anchorParamsKey],
   );

   // Keyed on the anchored URL rather than on `params` directly: see `anchor`
   // above. That is what makes a given with a starting value clearable, since
   // the host echoing the cleared URL back no longer reads as a new starting
   // point. It was a documented limitation while no server populated
   // `startingGivens`; dashboards and notebooks both do now.
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

   // Edits whose key no longer matches are DISCARDED, not merely ignored. The
   // line above stops reading them, which is enough until the key comes back:
   // the key space is small (document plus starting values plus params) and is
   // revisited routinely, so `a` -> `b` -> `a` from a clean link matched the
   // abandoned edits again and re-applied a filter the URL did not carry, to a
   // reader who had just opened the notebook fresh. Browser Back and a drill
   // pushing a URL equal to an earlier one do the same thing.
   //
   // In an effect rather than during render: `active` already ignores them, so
   // this cannot change what this render shows, and clearing is idempotent if
   // React runs it more than once.
   useEffect(() => {
      if (edits !== null && edits.key !== initialKey) setEdits(null);
   }, [edits, initialKey]);

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
   if (lastReported.current === null) lastReported.current = appliedParams;
   // Recorded only when the report is actually DELIVERED. A surface withholds
   // the callback while it is still loading — `Notebook` passes undefined until
   // the document has arrived, so that a report made in the window where it
   // does not yet know which givens exist cannot tell the host to clear
   // parameters belonging to the notebook then arriving — and a value recorded
   // in that window is remembered as reported without the host ever hearing it.
   //
   // That is not merely a missed report, because the echo test above reads this
   // same record: the host's URL still holds the old value, the record says the
   // new one, so the next `params` fails the echo test, the anchor moves, and
   // the edits are discarded as stale. Clearing a control while the cells were
   // re-running put the cleared value straight back in the box.
   //
   // The cost is the case this guard was briefly dropped for: a host that
   // writes the URL from `applied` directly without registering the callback
   // never echoes, so a given with a starting value is not clearable there.
   // Both Console pages register it, as does any host using `useGivenUrlParams`.
   useEffect(() => {
      if (!onParamsChange) return;
      if (sameParams(lastReported.current ?? {}, appliedParams)) return;
      priorReported.current = lastReported.current;
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
