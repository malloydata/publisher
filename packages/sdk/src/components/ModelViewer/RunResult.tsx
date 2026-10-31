// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { Box } from "@mui/material";
import { type ReactNode, useEffect, useMemo, useState } from "react";
import { Loading, LOADING_COPY } from "../Loading";
import { warmMalloyRenderer } from "../RenderedResult/loadRenderer";
import ResultContainer from "../RenderedResult/ResultContainer";
import { EmptyRow } from "./EmptyRow";
import { givensKeyFor, type GivenInfo } from "./modelGivens";
import { type RunGate, runGate } from "./runGate";
import { accessOf, filteredEmptyCaption, refusalMessage } from "./sourceAccess";
import { PANE_CARD_SX, PANE_INSET } from "./surface";
import { BodyText, CaptionText } from "./text";
import { PrimaryButton, QueryFailure } from "./ui";
import { givensOfRun, type RunContext, useRunQuery } from "./useRunQuery";

/** How tall a run's result may grow on the page before it scrolls inside its card. */
const RESULT_MAX_HEIGHT = 480;

/** A run's result as shown, with the given values it ran under. */
export interface ShownRun {
   result: string;
   /** The values it ran with, as `givensKeyFor` writes them: under the "values
    *  changed" banner these are not the panel's. */
   givensKey: string;
}

/** What the body shows: the run's result (Session), or the Malloy definition
 *  (Base). The two layer segments a view card switches between. */
export type RunResultMode = "result" | "code";

/**
 * The body of an open view's card (ViewRow): the result of running it, or the Malloy
 * that declares it, whichever the card's Result | Code switch picks. Body only —
 * the card above it carries the name and the switch.
 *
 * The run happens the first time the Result mode is shown, never in Code mode:
 * reading a definition must not bill a query. It renders through the SDK's own
 * result container, so a result here and in a report are one thing.
 */
export function RunResult({
   run,
   modelPath,
   query,
   sourceName,
   givens,
   mode = "result",
   renderCode,
   onShownChange,
}: {
   run: RunContext;
   modelPath: string;
   query: string;
   /** The source a view belongs to, for its required-filter check; a named
    *  query has none. */
   sourceName?: string;
   /** The givens the run reads, where its source can't say (a named query the
    *  model gives no source); else its source's. */
   givens?: ReadonlyArray<GivenInfo>;
   /** Which body to show. The switching is the card's job; this renders
    *  whichever mode it is told. */
   mode?: RunResultMode;
   /** Renders the Malloy body for the `definition` mode. A function, called
    *  only when that body is shown, so a result-mode stage does none of the
    *  code-stripping work. */
   renderCode?: () => ReactNode;
   /** Told the result this stage is showing, including one held under the "values
    *  changed" banner, so a host's download exports exactly what is on screen. */
   onShownChange?: (shown: ShownRun | undefined) => void;
}) {
   // A run is armed for one set of given values — the first time Result is
   // shown, at the values then in force — and stays armed for those, so
   // switching to Code and back shows the cached result rather than
   // running again. Committing a different value in the panel does NOT re-run
   // every card: a warehouse query is billed, so the card keeps its last
   // result, says the values changed, and re-runs on the reader's click.
   const givensKey = givensKeyFor(run.givenValues);
   const [armedKey, setArmedKey] = useState<string | null>(
      mode === "result" ? givensKey : null,
   );
   if (mode === "result" && armedKey === null) setArmedKey(givensKey);
   const valuesChanged = armedKey !== null && armedKey !== givensKey;

   // Can this run be honest about its inputs? A blocked run never fires.
   const requiredFilters = sourceName
      ? run.requiredFiltersBySource?.get(sourceName)
      : undefined;
   const gate: RunGate = useMemo(
      () =>
         runGate(
            givens ?? givensOfRun(run, sourceName),
            requiredFilters,
            run.givenValues,
         ),
      [run, sourceName, givens, requiredFilters],
   );
   const { result, isSuccess, isError, error, refetch } = useRunQuery({
      run,
      modelPath,
      query,
      enabled: armedKey === givensKey && gate.kind !== "blocked",
   });
   // A refusal says, in words, that the reader can't access the source — naming
   // a restricted source's rule.
   const sourceAccess = accessOf(run.accessBySource, sourceName);
   const forbiddenMessage = refusalMessage(sourceName, sourceAccess);
   const runAtCurrentValues = () =>
      valuesChanged ? setArmedKey(givensKey) : void refetch();
   // The last result this card showed, kept while the panel's values move on:
   // the query is keyed by the new values (and disabled until the reader
   // clicks), so its own `result` is empty then — but the run already paid for
   // stays on screen under the banner until the banner's Run swaps it.
   const [held, setHeld] = useState<{
      key: string;
      result: string;
      givensKey: string;
   } | null>(null);
   if (
      result !== undefined &&
      armedKey !== null &&
      (held?.key !== armedKey || held.result !== result)
   ) {
      setHeld({ key: armedKey, result, givensKey });
   }
   // `result` is the query keyed by the panel's values, so it ran with them. A blocked
   // card shows only why, so it reports no result: a download offers what is on screen.
   const shown: ShownRun | undefined =
      gate.kind === "blocked"
         ? undefined
         : result !== undefined
           ? { result, givensKey }
           : valuesChanged && held
             ? { result: held.result, givensKey: held.givensKey }
             : undefined;
   const shownResult = shown?.result;
   // Parses the result only on a row-filtered source, and once per result.
   const filteredCaption = useMemo(
      () => filteredEmptyCaption(sourceAccess, shownResult),
      [sourceAccess, shownResult],
   );
   // Reported by value, so the card re-renders only when what is shown changes.
   const shownGivensKey = shown?.givensKey ?? "";
   useEffect(() => {
      if (!onShownChange) return;
      if (shownResult === undefined) return onShownChange(undefined);
      onShownChange({ result: shownResult, givensKey: shownGivensKey });
   }, [onShownChange, shownResult, shownGivensKey]);
   const willShow = mode === "result" && gate.kind !== "blocked";
   useEffect(() => {
      if (willShow) warmMalloyRenderer();
   }, [willShow]);
   return (
      <Box data-testid="model-viewer-run-result" data-mode={mode}>
         {/* A run that finished at values the panel has since changed offers its
             re-run inline, in the stale banner below. */}
         {mode === "code" ? (
            renderCode?.()
         ) : gate.kind === "blocked" ? (
            // Refused, not failed: a bare 400 would say nothing about the
            // value the model wanted.
            <BodyText data-testid="model-viewer-run-blocked">
               {gate.reason}
            </BodyText>
         ) : isError && !valuesChanged ? (
            <QueryFailure
               query={query}
               error={error}
               onRetry={() => void refetch()}
               forbiddenMessage={forbiddenMessage}
            />
         ) : isSuccess && !result && !valuesChanged ? (
            // The run finished and the publisher sent no result body: say so,
            // rather than holding a skeleton that promises one.
            <EmptyRow message="The query returned no result." />
         ) : (
            // Skeleton and render share one box: the skeleton holds the
            // height, the render fades in over it, and the box is released
            // to the content's own height only once there is content. In the
            // same card as a code block (PANE_CARD_SX), so an opened view frames
            // its chart or table as a measure frames its code: a contained result,
            // like a dashboard's tile, not a table loose on the page. A table wider
            // than the card scrolls inside it.
            <Box
               data-testid="model-viewer-result-card"
               sx={{ ...PANE_CARD_SX, p: PANE_INSET, overflowX: "auto" }}
            >
               {valuesChanged && (
                  // The panel's values moved on since this ran. Re-running is
                  // the reader's call, not a side effect of a blur — and the
                  // result below is the one already paid for, kept until that click.
                  <Box
                     data-testid="model-viewer-run-stale"
                     sx={{
                        display: "flex",
                        alignItems: "center",
                        gap: 1.5,
                        flexWrap: "wrap",
                        mb: 1,
                     }}
                  >
                     <BodyText>
                        The given values changed since this ran.
                     </BodyText>
                     {/* One word on the face: the sentence beside it carries the
                         qualifier, which stays in the accessible name. */}
                     <PrimaryButton
                        size="small"
                        onClick={runAtCurrentValues}
                        aria-label="Run with these values"
                     >
                        Run
                     </PrimaryButton>
                  </Box>
               )}
               {gate.kind === "defaults" && shownResult && !valuesChanged && (
                  // What produced this result: a report shows its givens above
                  // the cells; the same view here must name the defaults it
                  // ran at, or it reads as a different answer.
                  <CaptionText
                     data-testid="model-viewer-run-defaults"
                     component="div"
                     sx={{ mb: 1 }}
                  >
                     {gate.note}
                  </CaptionText>
               )}
               {/* With values changed and nothing held (the run failed or hadn't
                   landed), nothing is running, so the banner stands alone rather
                   than over a spinner. */}
               {!(valuesChanged && !shownResult) &&
                  (shownResult === undefined ? (
                     <Loading text={LOADING_COPY.running} />
                  ) : (
                     <ResultContainer
                        result={shownResult}
                        maxHeight={RESULT_MAX_HEIGHT}
                        maxResultSize={run.maxResultSize}
                     />
                  ))}
               {filteredCaption && (
                  // An access filter that admits none of this reader's rows answers
                  // with an empty result, which would otherwise read as "no data".
                  <CaptionText
                     data-testid="model-viewer-run-filtered"
                     component="div"
                     sx={{ mt: 1 }}
                  >
                     {filteredCaption}
                  </CaptionText>
               )}
            </Box>
         )}
      </Box>
   );
}
