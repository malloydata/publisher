// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { useCallback, useMemo } from "react";
import type { Given } from "../../client";
import {
   useGivensState,
   type UseGivensStateResult,
} from "../../hooks/useGivensState";
import { useSuggestOptions } from "../../hooks/useSuggestOptions";
import type { GivensPanelProps } from "../given";

/**
 * A dashboard's control row, wired: the declared type of each given, the
 * draft and applied values, the suggested options, and the props a
 * `GivensPanel` takes to show it all. The viewer and the builder's live
 * surface both hold their controls this way, so they cannot drift.
 */
export function useDashboardControls({
   environmentName,
   packageName,
   versionId,
   modelPath,
   specs,
   startingValues,
   documentKey,
   autorun,
   params,
   onParamsChange,
}: {
   environmentName: string;
   packageName: string;
   versionId?: string;
   /** The model the suggest queries run against. */
   modelPath: string | undefined;
   /** The givens the row shows. */
   specs: Given[];
   startingValues?: Record<string, string>;
   /** Which document the edits belong to; see `useGivensState`. */
   documentKey: string;
   /** Whether a change applies at once, or waits for Apply. */
   autorun: boolean;
   /** Values from the host, typically its URL. */
   params?: Record<string, string>;
   /**
    * The applied values, with every given this dashboard manages, for a host
    * that keeps them in its URL. Pass it only once the manifest has loaded: an
    * empty declared set before then is the absence of an answer, not the
    * answer, and would clear the host's parameters.
    */
   onParamsChange?: (
      givens: Record<string, string>,
      managed: readonly string[],
   ) => void;
}): UseGivensStateResult & {
   declaredTypes: ReadonlyMap<string, string | undefined>;
   panel: GivensPanelProps;
} {
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
      (next: Record<string, string>) =>
         onParamsChange?.(next, Array.from(declaredTypes.keys())),
      [onParamsChange, declaredTypes],
   );
   const state = useGivensState({
      declaredTypes,
      startingValues,
      params,
      onParamsChange: onParamsChange ? report : undefined,
      documentKey,
      autorun,
   });
   const { options, isLoading, failed } = useSuggestOptions(
      environmentName,
      packageName,
      modelPath,
      specs,
      versionId,
      // So a suggest over a gated or scoped source carries the givens it needs.
      { values: state.applied, declaredTypes },
   );
   const panel: GivensPanelProps = {
      givens: specs,
      values: state.draft,
      onChange: state.setGiven,
      onReset: state.reset,
      layout: "bar",
      options,
      optionsLoading: isLoading,
      optionsFailed: failed,
      apply: autorun
         ? undefined
         : { onApply: state.apply, pending: state.pending },
   };
   return { ...state, declaredTypes, panel };
}
