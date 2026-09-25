// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import "@malloydata/malloy-explorer/styles.css";
import { Stack, Typography } from "@mui/material";
import { useCallback, useEffect, useMemo, useRef, useState } from "react";
import { planRun } from "./runPlan";
import { RawNotebook } from "../../client";
import { GivenValue } from "../../hooks/givenValue";
import { useDocumentControls } from "../../hooks/useDocumentControls";
import { useModelGivens } from "../../hooks/useModelGivens";
import { useQueryWithApiError } from "../../hooks/useQueryWithApiError";
import { parseResourceUri } from "../../utils/formatting";
import { ApiErrorDisplay } from "../ApiErrorDisplay";
import type { NavigationClick } from "../click_helper";
import type { DrillNavigation } from "../drill";
import { GivensPanel } from "../given";
import { givensToParams, givensToRequest } from "../given/paramCodec";
import { Loading } from "../Loading";
import { useServer } from "../ServerProvider";
import { CleanNotebookContainer, CleanNotebookSection } from "../styles";
import { NotebookCell } from "./NotebookCell";
import { EnhancedNotebookCell } from "./types";

// Maximum number of concurrent cell executions to avoid overwhelming the server
const MAX_CONCURRENT = 4;

/**
 * How long a changed parameter has to stay changed before the notebook runs.
 *
 * Aborting the superseded run is not enough on its own: an abort cancels the
 * HTTP request, but the cells already dispatched have reached the server and go
 * on compiling and running against the customer's warehouse, and their answers
 * are then thrown away. So autorun on a text control put one wave of doomed
 * queries on that warehouse per keystroke. `origin/main` bounded the load by
 * refusing to start a second run at all, which dropped the newest values;
 * waiting for the value to settle bounds it without making that trade.
 */
const GIVEN_SETTLE_MS = 400;

/**
 * The server's own explanation of why a cell would not run.
 *
 * Publisher answers a refused query with `{code, message}`, and that message is
 * the useful half: "filter 'state' is required", say. Falls back through the
 * generic axios message to a fixed string, so a cell always has something to
 * show rather than rendering an empty error box.
 */
function cellErrorMessage(error: unknown): string {
   const body = (
      error as { response?: { data?: { message?: unknown } } } | undefined
   )?.response?.data?.message;
   if (typeof body === "string" && body !== "") return body;
   if (error instanceof Error && error.message !== "") return error.message;
   return "The server did not say why.";
}

interface NotebookProps {
   resourceUri: string;
   maxResultSize?: number;
   /**
    * Parameter values from the host, typically its URL query parameters: the
    * same contract `Dashboard` takes, so a link into a filtered notebook works
    * the way a link into a filtered dashboard does.
    */
   givens?: Record<string, string>;
   /**
    * Applied parameter values, for a host that wants them in its URL. Fires
    * with what the cells actually ran with, which is what makes the URL a link
    * that reproduces the view.
    *
    * That is every committed change. Under `## autorun=false` a change is
    * committed on Apply, so reports are batched; otherwise every change is
    * committed and typing in a text control reports per keystroke. It does NOT
    * run per keystroke: a changed value waits
    * `GIVEN_SETTLE_MS` before anything is dispatched, so a burst of typing
    * produces one run. A run that does start supersedes and aborts the one
    * before it. The REPORTS are still per keystroke, so a host doing something
    * expensive per call should debounce.
    *
    * `managed` names every given this notebook declares, set or not. A host
    * writing to a shared query string needs it: `givens` alone says which
    * parameters to write but not which to *remove*, and a host that guesses by
    * removing everything it did not just receive deletes the unrelated
    * parameters it has no business touching.
    */
   onGivensChange?: (
      givens: Record<string, string>,
      managed: readonly string[],
   ) => void;
   /**
    * Optional SPA navigation handler for links inside the notebook. When
    * omitted, in-notebook links fall back to plain absolute anchors, so no
    * react-router context is required to render a notebook.
    *
    * It does NOT carry drill. A `# drill { to=self }` filters this notebook
    * through `given:` state and needs no handler at all, and a drill naming a
    * dashboard goes through {@link onDrillNavigate}, which takes a destination
    * rather than a URL.
    */
   onNavigate?: (to: string, event?: NavigationClick) => void;
   /**
    * Opens a `# drill { to=<dashboard> }` from a cell, seeded with the clicked
    * value. Omitted on a host with no dashboard route, which leaves those
    * destinations inert AND unmarked rather than painting a dead link.
    */
   onDrillNavigate?: (target: DrillNavigation, event?: MouseEvent) => void;
}

// Requires PackageProvider
export default function Notebook({
   resourceUri,
   maxResultSize = 0,
   givens,
   onGivensChange,
   onNavigate,
   onDrillNavigate,
}: NotebookProps) {
   const { apiClients } = useServer();
   const {
      environmentName,
      packageName,
      versionId,
      modelPath: notebookPath,
   } = parseResourceUri(resourceUri);

   // Fetch the raw notebook cells
   const {
      data: notebook,
      isSuccess,
      isError,
      error,
   } = useQueryWithApiError<RawNotebook>({
      queryKey: [resourceUri],
      queryFn: async () => {
         const response = await apiClients.notebooks.getNotebook(
            environmentName,
            packageName,
            notebookPath,
            versionId,
         );
         return response.data;
      },
   });

   // State to store executed cells with results
   const [enhancedCells, setEnhancedCells] = useState<EnhancedNotebookCell[]>(
      [],
   );
   const [isExecuting, setIsExecuting] = useState(false);
   // A run is scheduled but its settle window has not elapsed. Separate from
   // `isExecuting`, which covers only a run actually in flight.
   const [runScheduled, setRunScheduled] = useState(false);
   // The document the last run belonged to, for abandoning its requests when
   // the reader moves to another notebook without this component unmounting.
   const lastDocumentRef = useRef<string | undefined>(undefined);
   const [executionError, setExecutionError] = useState<Error | null>(null);

   // Model-level `given:` declarations, and the state behind their controls:
   // the same hook the dashboard uses, so both surfaces get URL-addressable
   // parameters, Apply batching and `to=self` drill from one implementation.
   const declaredGivens = useModelGivens(notebook);
   // A file-level `## autorun=false` arrives as `RawNotebook.autorun`, the same
   // field with the same default a dashboard's `# artifact { autorun=false }`
   // produces. Batching matters more here than on a dashboard: one control
   // change re-runs every cell in the document.
   const autorun = notebook?.autorun !== false;
   const controls = useDocumentControls({
      specs: declaredGivens,
      loaded: isSuccess,
      // Where the controls start, from a file-level `## givens { … }`.
      startingValues: notebook?.startingGivens,
      params: givens,
      onGivensChange,
      documentKey: resourceUri,
      autorun,
      environmentName,
      packageName,
      // A notebook's model path is the notebook itself: `suggest` queries run
      // against the same model the cells do.
      modelPath: notebookPath,
      versionId,
      documentName: notebookPath,
   });
   const {
      applied,
      declaredTypes,
      canSelf: canDrillSelf,
      onSelf: onDrillSelf,
   } = controls;

   /**
    * The `givens` query param for the notebook-cell GET: the same map the
    * dashboard's POST body carries, JSON-encoded because this endpoint takes it
    * in the URL. Built by the shared codec so a given is encoded identically
    * whichever surface runs it.
    */
   const buildGivens = useCallback(
      (values: Map<string, GivenValue>): string | undefined => {
         const request = givensToRequest(values, declaredTypes);
         return Object.keys(request).length > 0
            ? JSON.stringify(request)
            : undefined;
      },
      [declaredTypes],
   );

   // For a cell's "Data Sources" dialog: the notebook's current values, so
   // exploring from a cell starts from what the reader is looking at rather
   // than the model's bare defaults.
   const cellStartingGivens = useMemo(
      () => givensToParams(applied, declaredTypes),
      [applied, declaredTypes],
   );

   /**
    * Run every code cell with one set of given values, up to
    * {@link MAX_CONCURRENT} at a time.
    *
    * Each run takes a number from `runIdRef`, and a run that is no longer the
    * current one drops its results on the floor. Cells resolve independently
    * and out of order, so without that a slow cell from the previous values
    * would land after the new run's and leave a stale number on screen under a
    * control row claiming otherwise.
    */
   const runIdRef = useRef(0);
   const inFlightRef = useRef<AbortController | null>(null);
   const lastRunRef = useRef<string | null>(null);
   /** The run waiting out {@link GIVEN_SETTLE_MS}, so a re-render does not restart its clock. */
   const pendingRunRef = useRef<
      { key: string; timer: ReturnType<typeof setTimeout> } | undefined
   >(undefined);

   // Cancel whatever is in flight when this component goes away, so navigating
   // off a twelve-cell notebook does not leave twelve requests to complete and be
   // thrown away.
   //
   // Its OWN effect, with an empty dependency list, and that is the whole point:
   // as the run effect's cleanup this ran on every dependency change instead. The
   // host echoes each reported parameter back, which gives `applied` a new
   // identity with identical content, so the cleanup fired and aborted the run
   // the control change had just started, while the run effect's `runKey` guard
   // saw an unchanged key and refused to start a replacement. Every cell was left
   // blank, with no result, no error and no spinner, until a reload. Measured on
   // `governed-analytics/orders.malloynb`: 4 rendered results before a control
   // change, 0 after.
   useEffect(
      () => () => {
         inFlightRef.current?.abort();
         // Forget which run was last started, so a component that comes back
         // runs again instead of trusting a key whose run was just cancelled.
         // The case it is reasoned from, and NOT one I could reproduce: React's
         // StrictMode mounts, cleans up, and mounts again in development, and
         // refs survive that. A first mount that already had the notebook
         // cached would start a run, this cleanup would abort it, and the
         // second mount would see an unchanged `runKey` and decline to start a
         // replacement. I could not get a browser into that state, so treat the
         // reset as belt-and-braces rather than a measured fix. It is right on
         // its own terms either way: a key that says "this run happened" must
         // not outlive the run being cancelled.
         lastRunRef.current = null;
         clearTimeout(pendingRunRef.current?.timer);
         pendingRunRef.current = undefined;
      },
      [],
   );
   const executeCells = useCallback(
      async (givensToApply: Map<string, GivenValue> = new Map()) => {
         if (!isSuccess || !notebook?.notebookCells) return;

         const runId = ++runIdRef.current;

         // Cancel the run this one supersedes rather than just ignoring its
         // results. Discarding results alone keeps the requests in flight, so
         // three quick control changes on a twelve-cell notebook put three full
         // runs on the server and use the answers from one. `origin/main`
         // refused to start a second run at all, which was the wrong trade,
         // it dropped the newest values, but it did bound the load, and
         // nothing replaced that bound.
         inFlightRef.current?.abort();
         const controller = new AbortController();
         inFlightRef.current = controller;

         // Rebuilt from the notebook being run, never carried over from the
         // previous render's cells. Reusing them kept the OLD document's
         // `text` and `type` while the new document's results were written in
         // on top, so navigating between two notebooks rendered one's prose
         // with the other's charts, and a shorter second notebook left the
         // first one's trailing cells on screen. Nothing worth preserving
         // survived that branch anyway: `result` and `error` were cleared, and
         // `newSources` is re-derived from the raw cell below.
         setEnhancedCells(notebook.notebookCells.map((cell) => ({ ...cell })));

         setIsExecuting(true);
         setExecutionError(null);

         const givensParam = buildGivens(givensToApply);

         try {
            // Build execution tasks for code cells
            const executionTasks: Array<() => Promise<void>> = [];

            for (let i = 0; i < notebook.notebookCells.length; i++) {
               const rawCell = notebook.notebookCells[i];

               // Markdown cells don't need execution
               if (rawCell.type === "markdown") continue;

               // Capture cell index for closure
               const cellIndex = i;

               const executeCell = async () => {
                  try {
                     // `filterParams` and `bypassFilters` go over as undefined:
                     // the `#(filter)` panel this component used to render is
                     // gone, replaced by `given:`. The server still enforces a
                     // `required` filter, so a model that has one now fails
                     // here: visibly, via `error` below, rather than as the
                     // blank cell it used to be. See RELEASE_NOTES.
                     const response =
                        await apiClients.notebooks.executeNotebookCell(
                           environmentName,
                           packageName,
                           notebookPath,
                           cellIndex,
                           versionId,
                           undefined,
                           undefined,
                           givensParam,
                           { signal: controller.signal },
                        );

                     if (runIdRef.current !== runId) return;

                     const executedCell = response.data;
                     const result = executedCell.result;
                     const newSources =
                        rawCell.newSources || executedCell.newSources;

                     // Update state incrementally
                     setEnhancedCells((prev) => {
                        const next = [...prev];
                        if (!next[cellIndex]) {
                           next[cellIndex] = { ...rawCell };
                        }
                        next[cellIndex] = {
                           ...next[cellIndex],
                           result,
                           newSources,
                        };
                        return next;
                     });
                  } catch (cellError) {
                     // A superseded or cancelled run is not a failure to
                     // report; its cells are about to be re-run.
                     if (
                        controller.signal.aborted ||
                        runIdRef.current !== runId
                     )
                        return;
                     console.error(
                        `Error executing cell ${cellIndex}:`,
                        cellError,
                     );
                     const message = cellErrorMessage(cellError);
                     setEnhancedCells((prev) => {
                        const next = [...prev];
                        if (!next[cellIndex]) {
                           next[cellIndex] = { ...rawCell };
                        }
                        next[cellIndex] = {
                           ...next[cellIndex],
                           error: message,
                        };
                        return next;
                     });
                  }
               };

               executionTasks.push(executeCell);
            }

            // Execute with limited concurrency (up to 4 parallel requests)
            const executing: Promise<void>[] = [];

            for (const task of executionTasks) {
               const promise = task().then(() => {
                  executing.splice(executing.indexOf(promise), 1);
               });
               executing.push(promise);

               if (executing.length >= MAX_CONCURRENT) {
                  await Promise.race(executing);
               }
            }

            // Wait for remaining tasks to complete
            await Promise.all(executing);
         } catch (error) {
            if (controller.signal.aborted || runIdRef.current !== runId) return;
            console.error("Error executing notebook cells:", error);
            setExecutionError(error as Error);
         } finally {
            if (runIdRef.current === runId) {
               setIsExecuting(false);
               inFlightRef.current = null;
            }
         }
      },
      [
         isSuccess,
         notebook,
         buildGivens,
         environmentName,
         packageName,
         notebookPath,
         versionId,
         apiClients.notebooks,
      ],
   );

   // Run the cells on load, and again whenever the applied parameters change.
   // Applied, not draft: under `## autorun=false` the cells wait for Apply.
   //
   // One effect covers both, keyed on exactly what would go over the wire plus
   // which notebook it is. Keying on the encoded givens means a change that
   // encodes identically does not re-run, and a link carrying parameters runs
   // once with them rather than running bare and running again when they land.
   // A run already in flight is superseded rather than waited on, which the
   // generation guard in `executeCells` makes safe.
   //
   // The notebook's own identity is part of that key. Its content can change
   // under an open tab: a package reload, and react-query hands over a new
   // object only when the content actually differs, so identity is exactly the
   // "has this document changed" signal. Keying on `resourceUri` alone silently
   // dropped that: a reloaded notebook kept rendering the previous run's
   // results forever, because the URI and the givens were unchanged.
   const notebookGenRef = useRef(0);
   const seenNotebookRef = useRef<RawNotebook | undefined>(undefined);
   if (seenNotebookRef.current !== notebook) {
      seenNotebookRef.current = notebook;
      notebookGenRef.current += 1;
   }
   const notebookGen = notebookGenRef.current;

   useEffect(() => {
      const documentKey = `${resourceUri}|${notebookGen}`;
      const runKey = `${documentKey}|${buildGivens(applied) ?? ""}`;

      // Abandon the previous notebook's requests when the document changes.
      // The abort-on-unmount effect does not cover this: navigating from one
      // notebook to another REUSES this component, so nothing unmounts and a
      // twelve-cell notebook left twelve requests running against a document
      // the reader had already left.
      if (
         lastDocumentRef.current !== undefined &&
         lastDocumentRef.current !== documentKey
      ) {
         inFlightRef.current?.abort();
         inFlightRef.current = null;
      }
      lastDocumentRef.current = documentKey;

      const plan = planRun({
         ready: isSuccess && !!notebook?.notebookCells,
         lastRunKey: lastRunRef.current,
         pendingKey: pendingRunRef.current?.key,
         runKey,
         documentKey,
      });

      if (plan.cancelPending) {
         clearTimeout(pendingRunRef.current?.timer);
         pendingRunRef.current = undefined;
         setRunScheduled(false);
      }
      if (plan.dispatch === "none") return;

      const start = () => {
         pendingRunRef.current = undefined;
         setRunScheduled(false);
         lastRunRef.current = runKey;
         void executeCells(applied);
      };

      if (plan.dispatch === "now") {
         start();
         return;
      }
      // Flagged while the settle timer runs, so the cells say a run is coming.
      // Without it the window was invisible: `isExecuting` is false until the
      // timer fires, so a changed control sat above the PREVIOUS values with
      // nothing on screen to say they were stale, and a reader who kept typing
      // kept extending the window.
      setRunScheduled(true);
      pendingRunRef.current = {
         key: runKey,
         timer: setTimeout(start, GIVEN_SETTLE_MS),
      };
   }, [
      isSuccess,
      notebook,
      notebookGen,
      resourceUri,
      applied,
      buildGivens,
      executeCells,
   ]);

   return (
      <CleanNotebookContainer>
         <CleanNotebookSection>
            <Stack spacing={3} component="section">
               {/* Parameters panel: the controls for `given:` declarations */}
               <GivensPanel {...controls.panel} />

               {/* Loading State */}
               {!isSuccess && !isError && (
                  <Loading text={"Fetching Notebook..."} />
               )}

               {/* Notebook Cells */}
               {isSuccess &&
                  (enhancedCells.length > 0
                     ? enhancedCells
                     : notebook?.notebookCells || []
                  ).map((cell, index) => (
                     <NotebookCell
                        cell={cell as EnhancedNotebookCell}
                        key={index}
                        index={index}
                        resourceUri={resourceUri}
                        maxResultSize={maxResultSize}
                        isExecuting={isExecuting}
                        givens={cellStartingGivens}
                        givenSpecs={declaredGivens}
                        // Distinct from `isExecuting`: the cell's spinner is
                        // gated on `!cell.result`, so feeding this through it
                        // showed nothing on a re-run, which is the only case
                        // the settle window has to signal.
                        pendingRerun={runScheduled}
                        onNavigate={onNavigate}
                        onDrillNavigate={onDrillNavigate}
                        onDrillSelf={
                           declaredGivens.length > 0 ? onDrillSelf : undefined
                        }
                        canDrillSelf={canDrillSelf}
                     />
                  ))}

               {/* Error States */}
               {isError && error.status === 404 && (
                  <Typography variant="body2" sx={{ color: "text.secondary" }}>
                     <code>{`${environmentName} > ${packageName} > ${notebookPath}`}</code>{" "}
                     not found.
                  </Typography>
               )}

               {isError && error.status !== 404 && (
                  <ApiErrorDisplay
                     error={error}
                     context={`${environmentName} > ${packageName} > ${notebookPath}`}
                  />
               )}

               {executionError && (
                  <ApiErrorDisplay
                     error={{
                        message: executionError.message,
                        status: 500,
                        name: "ExecutionError",
                     }}
                     context="Notebook Execution"
                  />
               )}
            </Stack>
         </CleanNotebookSection>
      </CleanNotebookContainer>
   );
}
