// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { useCallback, useMemo } from "react";
import type { Given } from "../client";
import { useDrillSelf } from "../components/drill/useDrillSelf";
import type { GivensPanelProps } from "../components/given/GivensPanel";
import { useGivensState, type UseGivensStateResult } from "./useGivensState";
import { useSuggestOptions } from "./useSuggestOptions";

/**
 * Everything a document's control row needs, from what the document declares.
 *
 * A dashboard, a notebook and the builder's harness each wired this by hand:
 * the declared types read off the givens, the report to the host guarded on the
 * document having loaded, `useGivensState`, `useSuggestOptions` for the pickers'
 * options, `useDrillSelf` for `to=self`, and the same nine props into
 * `GivensPanel`. Sixty lines each, with the same reasoning in the comments of
 * each. This is that wiring once, so a control gains a behaviour on every
 * surface at the same time.
 */
export interface UseDocumentControlsOptions {
   /** The document's declared givens. Empty until it has loaded. */
   specs: Given[];
   /**
    * Whether the document has loaded. Reports to the host are WITHHELD until
    * it has: before then the declared set is empty, so a report would say "no
    * values, and I manage nothing", which a host reasonably reads as "clear
    * what you wrote" — and during an in-app move from one document to the next
    * that window sits between the two, so the parameters it would clear belong
    * to the document now arriving. A document that genuinely declares no givens
    * still reports, because by then the empty set is the answer rather than
    * the absence of one.
    */
   loaded: boolean;
   /** Where the controls start, from the file. A URL beats them. */
   startingValues?: Record<string, string>;
   /** Control values from the host, typically its URL query parameters. */
   params?: Record<string, string>;
   /**
    * Applied values, for a host that wants them in its URL, with every name
    * this document manages — `givens` alone says which parameters to write but
    * not which to remove.
    */
   onGivensChange?: (
      givens: Record<string, string>,
      managed: readonly string[],
   ) => void;
   /**
    * Which document the edits belong to. Without it the edits are keyed by
    * their starting VALUES alone, so two documents whose starting values
    * coincide (the common case: both empty) look like one, and the one you came
    * from keeps filtering the one you moved to. The version belongs in it too,
    * so a swap between versions drops the edits made to the other.
    */
   documentKey: string;
   /** False batches changes behind Apply. */
   autorun: boolean;
   /** Where `suggest` queries run: the document's own model. */
   environmentName: string;
   packageName: string;
   modelPath: string | undefined;
   versionId?: string;
   /** How the document names itself in a refused-drill warning. */
   documentName: string;
}

export interface DocumentControls extends UseGivensStateResult {
   /** Declared type per given name: what this document can set and how a value is encoded. */
   declaredTypes: ReadonlyMap<string, string | undefined>;
   options: Map<string, string[]>;
   optionsLoading: boolean;
   optionsFailed: ReadonlySet<string>;
   /** For `useDrill`: whether a `to=self` on this given can be honoured. */
   canSelf: (given: string) => boolean;
   /** For `useDrill`: set the given to the clicked value, encoded for its type. */
   onSelf: (given: string, rawValue: unknown) => void;
   /** Everything `GivensPanel` takes except its layout: spread it in. */
   panel: Omit<GivensPanelProps, "layout" | "title">;
}

export function useDocumentControls({
   specs,
   loaded,
   startingValues,
   params,
   onGivensChange,
   documentKey,
   autorun,
   environmentName,
   packageName,
   modelPath,
   versionId,
   documentName,
}: UseDocumentControlsOptions): DocumentControls {
   const declaredTypes = useMemo(
      () =>
         new Map(
            specs
               .filter((spec) => spec.name !== undefined)
               .map((spec) => [spec.name as string, spec.type]),
         ),
      [specs],
   );

   const report = useCallback(
      (next: Record<string, string>) => {
         if (!loaded) return;
         onGivensChange?.(next, Array.from(declaredTypes.keys()));
      },
      [loaded, onGivensChange, declaredTypes],
   );

   const state = useGivensState({
      declaredTypes,
      startingValues,
      params,
      // Withheld, not accepted and dropped: `useGivensState` records what it
      // last reported BEFORE calling out, so a report the callback threw away
      // would be remembered as delivered and never retried, leaving a parameter
      // stranded in the host's URL. Passing undefined skips the report and
      // leaves its record untouched.
      onParamsChange: loaded ? report : undefined,
      documentKey,
      autorun,
   });

   const {
      options,
      isLoading: optionsLoading,
      failed: optionsFailed,
   } = useSuggestOptions(
      environmentName,
      packageName,
      modelPath,
      specs,
      versionId,
      // So a suggest over a gated or scoped source carries the givens it needs.
      { values: state.applied, declaredTypes },
   );

   const { canSelf, onSelf } = useDrillSelf({
      declaredTypes,
      setGiven: state.setGiven,
      documentName,
   });

   return {
      ...state,
      declaredTypes,
      options,
      optionsLoading,
      optionsFailed,
      canSelf,
      onSelf,
      panel: {
         givens: specs,
         values: state.draft,
         onChange: state.setGiven,
         onReset: state.reset,
         options,
         optionsLoading,
         optionsFailed,
         // Absent means autorun; only an explicit `false` batches behind Apply.
         apply: autorun
            ? undefined
            : { onApply: state.apply, pending: state.pending },
      },
   };
}
