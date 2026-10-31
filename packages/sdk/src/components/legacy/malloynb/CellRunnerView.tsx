// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * @deprecated `.malloynb` only. The cell runner behind the deprecated
 * `Notebook`: it runs a `.malloynb` notebook's code cells through the
 * deprecated `…/notebooks/*` routes and draws them on the shared report page.
 * Nothing outside `legacy/malloynb/` imports it (eslint enforces this), and it
 * is deleted with the `.malloynb` format. A `.malloy` report is always tiles,
 * drawn by `DashboardView`.
 */

import { Typography } from "@mui/material";
import { useCallback, useEffect, useRef, useState } from "react";
import type { NotebookCell as RawCell, RawNotebook } from "../../../client";
import type { GivenValue } from "../../../hooks/givenValue";
import { useDocumentControls } from "../../../hooks/useDocumentControls";
import { useModelGivens } from "../../../hooks/useModelGivens";
import { GIVEN_SETTLE_MS } from "../../../hooks/useSettled";
import { parseResourceUri } from "../../../utils/formatting";
import { ApiErrorDisplay, type ApiError } from "../../ApiErrorDisplay";
import type { NavigationClick } from "../../click_helper";
import type { DrillNavigation } from "../../drill";
import { GivensPanel } from "../../given";
import { givensToRequest } from "../../given/paramCodec";
import { Loading, LOADING_COPY } from "../../Loading";
import { cellRuns } from "./cellKind";
import { NotebookCell } from "./NotebookCell";
import { planRun } from "./runPlan";
import type { EnhancedNotebookCell } from "./types";
import { failureCategory, useReportEvents } from "../../Report/telemetry";
import { Prose } from "../../Prose";
import {
   ReportHeader,
   ReportPage,
   StickyControls,
   TextTileFrame,
} from "../../Report/ReportPage";

// Maximum number of concurrent cell executions to avoid overwhelming the server
const MAX_CONCURRENT = 4;

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

/** What one cell's run is handed: the cell, its index, the values to run it with, and the signal that cancels it. */
interface CellRun {
   cell: RawCell;
   index: number;
   givens: ReadonlyMap<string, GivenValue>;
   declaredTypes: ReadonlyMap<string, string | undefined>;
   signal: AbortSignal;
}

/** Runs one cell. Stable across renders: a new runner re-runs every cell. */
export type CellRunner = (
   run: CellRun,
) => Promise<{ result?: string; newSources?: string[] }>;

export interface CellReportProps {
   /** `publisher://…/models/<path>`: the report, and the model its cells run against. */
   resourceUri: string;
   /** The report's title and description, drawn in the header every report has. */
   title?: string;
   description?: string;
   maxResultSize?: number;
   /**
    * Parameter values from the host, typically its URL query parameters: the
    * same contract `Report` takes, so a link into a filtered report works the
    * way a link into a filtered grid does.
    */
   givens?: Record<string, string>;
   /**
    * Applied parameter values, for a host that wants them in its URL. Fires
    * with what the cells actually ran with, which is what makes the URL a link
    * that reproduces the view.
    *
    * That is every committed change. Under `## autorun=false` a change is
    * committed on Apply, so reports are batched; otherwise every change is
    * committed. A changed value waits `GIVEN_SETTLE_MS` before anything is
    * dispatched, so a burst of changes produces one run. A run that
    * does start supersedes and aborts the one before it. A text, number, date
    * or slider control commits once it settles, so typed text runs after both
    * waits; the wait here still covers pickers, keyboard slider steps, drill
    * and Reset, which commit at once.
    *
    * `managed` names every given this report declares, set or not. A host
    * writing to a shared query string needs it: `givens` alone says which
    * parameters to write but not which to *remove*.
    */
   onGivensChange?: (
      givens: Record<string, string>,
      managed: readonly string[],
   ) => void;
   /**
    * Optional SPA navigation handler for links inside the cells. When omitted,
    * links fall back to plain absolute anchors, so no react-router context is
    * required to render one.
    *
    * It does NOT carry drill. A `# drill { to=self }` filters this report
    * through `given:` state and needs no handler at all, and a drill naming a
    * report goes through {@link onDrillNavigate}, which takes a destination
    * rather than a URL.
    */
   onNavigate?: (to: string, event?: NavigationClick) => void;
   /**
    * Opens a `# drill { to=<report> }` from a cell, seeded with the clicked
    * value. Omitted on a host with no report route, which leaves those
    * destinations inert AND unmarked rather than painting a dead link.
    */
   onDrillNavigate?: (target: DrillNavigation, event?: MouseEvent) => void;
}

/**
 * Cells run one by one, the engine behind a report written as `run:` cells.
 * Takes the cells (undefined while they load, `error` when they could not be
 * read) and the runner that executes one; what reads the cells and what runs
 * them is the caller's.
 */
export function CellRunnerView({
   resourceUri,
   title,
   description,
   maxResultSize = 0,
   givens,
   onGivensChange,
   onNavigate,
   onDrillNavigate,
   notebook,
   error,
   runCell,
}: CellReportProps & {
   notebook: RawNotebook | undefined;
   /** Why the cells could not be read; the view says so in place of them. */
   error?: ApiError | null;
   runCell: CellRunner;
}) {
   const {
      environmentName,
      packageName,
      versionId,
      modelPath: notebookPath,
   } = parseResourceUri(resourceUri);
   const isSuccess = notebook !== undefined;
   const isError = !isSuccess && !!error;

   // State to store executed cells with results
   const [enhancedCells, setEnhancedCells] = useState<EnhancedNotebookCell[]>(
      [],
   );
   const [isExecuting, setIsExecuting] = useState(false);
   // A run is scheduled but its settle window has not elapsed. Separate from
   // `isExecuting`, which covers only a run actually in flight.
   const [runScheduled, setRunScheduled] = useState(false);
   // The document the last run belonged to, for abandoning its requests when
   // the reader moves to another report without this component unmounting.
   const lastDocumentRef = useRef<string | undefined>(undefined);
   const [executionError, setExecutionError] = useState<Error | null>(null);

   // Model-level `given:` declarations, and the state behind their controls:
   // the same hook a grid uses, so both layouts get URL-addressable
   // parameters, Apply batching and `to=self` drill from one implementation.
   const declaredGivens = useModelGivens(notebook);
   // A file-level `## autorun=false` arrives as `RawNotebook.autorun`, the same
   // field with the same default a grid's `## artifact { autorun=false }`
   // produces. Batching matters more here than on a grid: one control change
   // re-runs every cell in the document.
   const autorun = notebook?.autorun !== false;
   // Set by `Report`; silent under the deprecated `Notebook`, which shares this view.
   const events = useReportEvents();
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
      // The report's model path is the report itself: `suggest` queries run
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

   /** The run key's encoding of a set of values: the request body the cells are sent, so values that encode identically do not re-run. */
   const encodeGivens = useCallback(
      (values: Map<string, GivenValue>): string | undefined => {
         const request = givensToRequest(values, declaredTypes);
         return Object.keys(request).length > 0
            ? JSON.stringify(request)
            : undefined;
      },
      [declaredTypes],
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
   // off a twelve-cell report does not leave twelve requests to complete and be
   // thrown away.
   //
   // Its OWN effect, with an empty dependency list, not the run effect's
   // cleanup: the host echoes each reported parameter back, giving `applied` a
   // new identity with identical content. A cleanup on that change would abort
   // the run the control change just started, while the `runKey` guard sees an
   // unchanged key and starts no replacement, leaving every cell blank.
   useEffect(
      () => () => {
         inFlightRef.current?.abort();
         // Forget which run was last started, so a component that comes back
         // runs again instead of trusting a key whose run was just cancelled
         // (React's StrictMode mounts, cleans up, and mounts again, and refs
         // survive that). A key that says "this run happened" must not outlive
         // the run being cancelled.
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
         // results: discarding results alone keeps the requests in flight, so
         // three quick control changes on a twelve-cell report put three full
         // runs on the server and use the answers from one.
         inFlightRef.current?.abort();
         const controller = new AbortController();
         inFlightRef.current = controller;

         // Rebuilt from the report being run, never carried over from the
         // previous render's cells, which would keep the OLD document's text
         // under the new document's results.
         setEnhancedCells(notebook.notebookCells.map((cell) => ({ ...cell })));

         setIsExecuting(true);
         setExecutionError(null);

         try {
            const executionTasks: Array<() => Promise<void>> = [];

            for (let i = 0; i < notebook.notebookCells.length; i++) {
               const rawCell = notebook.notebookCells[i];

               // Prose and definitions have nothing to run.
               if (!cellRuns(rawCell)) continue;

               const cellIndex = i;

               const executeCell = async () => {
                  try {
                     const executedCell = await runCell({
                        cell: rawCell,
                        index: cellIndex,
                        givens: givensToApply,
                        declaredTypes,
                        signal: controller.signal,
                     });

                     if (runIdRef.current !== runId) return;

                     const result = executedCell.result;
                     // A `.malloy` report's cells carry their sources on the
                     // manifest; a run adds none.
                     const newSources =
                        rawCell.newSources || executedCell.newSources;

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
                     // The cell shows its error; the host hears of it, or
                     // without one the console does, so it is never lost.
                     events.emit({
                        type: "report.query_failed",
                        tile: cellIndex,
                        category: failureCategory(cellError),
                     });
                     if (!events.listening)
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

            await Promise.all(executing);
         } catch (error) {
            if (controller.signal.aborted || runIdRef.current !== runId) return;
            events.emit({
               type: "report.query_failed",
               category: failureCategory(error),
            });
            if (!events.listening)
               console.error("Error executing notebook cells:", error);
            setExecutionError(error as Error);
         } finally {
            if (runIdRef.current === runId) {
               setIsExecuting(false);
               inFlightRef.current = null;
            }
         }
      },
      [isSuccess, notebook, declaredTypes, runCell, events],
   );

   // Run the cells on load, and again whenever the applied parameters change.
   // Applied, not draft: under `## autorun=false` the cells wait for Apply.
   //
   // One effect covers both, keyed on exactly what would go over the wire plus
   // which report it is. Keying on the encoded givens means a change that
   // encodes identically does not re-run, and a link carrying parameters runs
   // once with them rather than running bare and running again when they land.
   // A run already in flight is superseded rather than waited on, which the
   // generation guard in `executeCells` makes safe.
   //
   // The cells' own identity is part of that key: a package reload hands over
   // a new object only when the content actually differs, so identity is
   // exactly the "has this document changed" signal.
   const notebookGenRef = useRef(0);
   const seenNotebookRef = useRef<RawNotebook | undefined>(undefined);
   if (seenNotebookRef.current !== notebook) {
      seenNotebookRef.current = notebook;
      notebookGenRef.current += 1;
   }
   const notebookGen = notebookGenRef.current;

   useEffect(() => {
      const documentKey = `${resourceUri}|${notebookGen}`;
      const runKey = `${documentKey}|${encodeGivens(applied) ?? ""}`;

      // Abandon the previous report's requests when the document changes.
      // The abort-on-unmount effect does not cover this: navigating from one
      // report to another REUSES this component, so nothing unmounts.
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
      // Flagged while the settle timer runs, so the cells say a run is coming;
      // `isExecuting` is false until the timer fires.
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
      encodeGivens,
      executeCells,
   ]);

   const shownCells =
      enhancedCells.length > 0 ? enhancedCells : notebook?.notebookCells || [];
   // A .malloynb keeps the copy-link icon on its first cell.
   const copyLinkIndex = 0;

   return (
      // The page every report draws, whatever its format: the header, the
      // pinned row of filter buttons, then the cells as prose and tiles.
      <ReportPage
         // Cells read top to bottom, so a report of cells is linear.
         layout="linear"
         header={
            title || description ? (
               <ReportHeader
                  title={title ?? ""}
                  description={
                     description ? (
                        <TextTileFrame>
                           <Prose variant="document">{description}</Prose>
                        </TextTileFrame>
                     ) : undefined
                  }
               />
            ) : null
         }
         controls={
            controls.panel.givens.length > 0 ? (
               <StickyControls>
                  <GivensPanel {...controls.panel} layout="bar" />
               </StickyControls>
            ) : undefined
         }
      >
         {!isSuccess && !isError && (
            <Loading text={LOADING_COPY.opening("report")} />
         )}

         {isSuccess &&
            shownCells.map((cell, index) => (
               <NotebookCell
                  cell={cell as EnhancedNotebookCell}
                  key={index}
                  showCopyLink={index === copyLinkIndex}
                  resourceUri={resourceUri}
                  maxResultSize={maxResultSize}
                  isExecuting={isExecuting}
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

         {isError && error?.status === 404 && (
            <Typography variant="body2" sx={{ color: "text.secondary" }}>
               <code>{`${environmentName} > ${packageName} > ${notebookPath}`}</code>{" "}
               not found.
            </Typography>
         )}

         {isError && error?.status !== 404 && (
            <ApiErrorDisplay
               error={error as ApiError}
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
      </ReportPage>
   );
}
