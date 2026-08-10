import "@malloydata/malloy-explorer/styles.css";
import { Stack, Typography } from "@mui/material";
import { useCallback, useEffect, useMemo, useRef, useState } from "react";
import { planRun } from "./runPlan";
import { RawNotebook } from "../../client";
import { GivenValue } from "../../hooks/givenValue";
import { useGivensState } from "../../hooks/useGivensState";
import { useModelGivens } from "../../hooks/useModelGivens";
import { useQueryWithApiError } from "../../hooks/useQueryWithApiError";
import { useSuggestOptions } from "../../hooks/useSuggestOptions";
import { parseResourceUri } from "../../utils/formatting";
import { ApiErrorDisplay } from "../ApiErrorDisplay";
import type { NavigationClick } from "../click_helper";
import { encodeDrillValue } from "../drill";
import { GivensPanel } from "../given";
import { givensToRequest } from "../given/paramCodec";
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
    * That is every committed change, and today every change is committed: the
    * Apply batching `useGivensState` implements is not reachable here while
    * `autorun` is hardcoded true, so typing in a text control reports per
    * keystroke and starts a run per keystroke. Only the last completes: each
    * run aborts the one before it, but a host doing something expensive per
    * call should debounce.
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
    * through `given:` state and works with or without this handler, and a drill
    * naming a dashboard is not wired at all yet: see the note in
    * `NotebookCell` for why.
    */
   onNavigate?: (to: string, event?: NavigationClick) => void;
}

// Requires PackageProvider
export default function Notebook({
   resourceUri,
   maxResultSize = 0,
   givens,
   onGivensChange,
   onNavigate,
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
   const [executionError, setExecutionError] = useState<Error | null>(null);

   // Model-level `given:` declarations, and the state behind their controls.
   // The same hook the dashboard viewer uses, so both surfaces get URL-
   // addressable parameters, Apply batching, and `to=self` drill from one
   // implementation rather than two that drift.
   const declaredGivens = useModelGivens(notebook);
   const declaredTypes = useMemo(
      () =>
         new Map(
            declaredGivens
               .filter((given) => given.name !== undefined)
               .map((given) => [given.name as string, given.type]),
         ),
      [declaredGivens],
   );

   // Silent until the notebook has actually loaded. Which givens exist is not
   // known before then: `useModelGivens(undefined)` is empty, so a report in
   // that window says "no values, and I manage nothing", which a host reasonably
   // reads as "clear what you wrote". During an in-app navigation from one
   // notebook to another that window sits between the two, and the parameters it
   // would clear can belong to the notebook now ARRIVING. A notebook that
   // genuinely declares no givens still reports, because by then the empty set
   // is the answer rather than the absence of one.
   const reportGivens = useCallback(
      (next: Record<string, string>) => {
         if (!isSuccess) return;
         onGivensChange?.(next, Array.from(declaredTypes.keys()));
      },
      [isSuccess, onGivensChange, declaredTypes],
   );

   // Always on for now, and deliberately not read off the notebook. Batching
   // matters more here than on a dashboard: one control change re-runs every
   // cell in the document, and `useGivensState` implements it, but the
   // file-level `## autorun=false` that would turn it off has no reader on the
   // server yet, so `RawNotebook` carries no `autorun` field. Declaring one
   // nothing produces would be a spec that lies. When the reader lands, this
   // becomes `notebook?.autorun !== false` and the Apply button appears.
   const autorun = true;
   const { draft, applied, setGiven, reset, apply, pending } = useGivensState({
      declaredTypes,
      // Where the controls start, from a file-level `## givens { … }`. A URL
      // beats them, so a shared link still shows what the sender saw.
      startingValues: notebook?.startingGivens,
      params: givens,
      // Withheld until the notebook has loaded, rather than accepted and
      // dropped. `useGivensState` records what it last reported BEFORE calling
      // out, so a report the callback throws away is remembered as delivered
      // and never retried, leaving a parameter stranded in the host's URL.
      // Passing undefined makes the hook skip the report entirely, which leaves
      // its record untouched and the next real change still reportable.
      onParamsChange: isSuccess ? reportGivens : undefined,
      autorun,
   });

   // A notebook's model path is the notebook itself: `suggest` queries run
   // against the same model the cells do.
   const {
      options: givenOptions,
      isLoading: givenOptionsLoading,
      failed: givenOptionsFailed,
   } = useSuggestOptions(
      environmentName,
      packageName,
      notebookPath,
      declaredGivens,
   );

   // The declared names, indexed case-insensitively, so a drill tag resolves
   // whichever way the two were spelled.
   //
   // Neither convention can be assumed. `# drill` defaults the given name to
   // the DIMENSION's name, which is conventionally lower_snake, while givens
   // are conventionally SHOUTED (both in-repo examples declare `REGION` and
   // `MIN_AMOUNT`). An exact match therefore failed for the common case, and
   // upper-casing the dimension name, which is what this did first, only moved
   // the failure onto models that spell their givens in lower case. Folding
   // case resolves both instead of picking a side.
   const givenNamesByFold = useMemo(() => {
      const byFold = new Map<string, string>();
      for (const name of declaredTypes.keys()) {
         // First declaration wins, so a model with `REGION` and `region` keeps
         // the one it declared first rather than silently flipping.
         if (!byFold.has(name.toLowerCase()))
            byFold.set(name.toLowerCase(), name);
      }
      return byFold;
   }, [declaredTypes]);

   /** The declared given a drill tag's name refers to, or undefined. */
   const resolveGiven = useCallback(
      (given: string) =>
         declaredTypes.has(given)
            ? given
            : givenNamesByFold.get(given.toLowerCase()),
      [declaredTypes, givenNamesByFold],
   );

   // `to=self` filters in place, which only works for a given this notebook
   // actually declares: sending one it cannot bind would fail every cell. The
   // mismatch is reported to the author rather than issued: same rule, same
   // wording, as the dashboard viewer.
   // Asked before a cell is painted as drillable, so the affordance matches what
   // a click can actually do. The refusal below still stands as a backstop for a
   // caller that does not ask.
   const canDrillSelf = useCallback(
      (given: string) => resolveGiven(given) !== undefined,
      [resolveGiven],
   );

   const onDrillSelf = useCallback(
      (given: string, rawValue: unknown) => {
         const declared = resolveGiven(given);
         if (declared === undefined) {
            console.warn(
               `# drill { to=self } tried to set '${given}', which ` +
                  `'${notebookPath}' does not declare as a given. Name the ` +
                  `given with 'given=' on the drill tag.`,
            );
            return;
         }
         // Encoded against the declared type of the given being set, which is
         // knowable here and is not knowable at the click. Set under the name
         // the MODEL declares, not the one the tag spelled, so the value goes
         // into the URL and the request under the one name the server knows.
         const value = encodeDrillValue(rawValue, declaredTypes.get(declared));
         if (value === undefined) return;
         setGiven(declared, value);
      },
      [declaredTypes, notebookPath, resolveGiven, setGiven],
   );

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
      }
      if (plan.dispatch === "none") return;

      const start = () => {
         pendingRunRef.current = undefined;
         lastRunRef.current = runKey;
         void executeCells(applied);
      };

      if (plan.dispatch === "now") {
         start();
         return;
      }
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
               <GivensPanel
                  givens={declaredGivens}
                  values={draft}
                  onChange={setGiven}
                  onReset={reset}
                  options={givenOptions}
                  optionsLoading={givenOptionsLoading}
                  optionsFailed={givenOptionsFailed}
                  apply={autorun ? undefined : { onApply: apply, pending }}
               />

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
                        onNavigate={onNavigate}
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
