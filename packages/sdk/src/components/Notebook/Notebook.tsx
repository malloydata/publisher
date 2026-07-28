import "@malloydata/malloy-explorer/styles.css";
import { Stack, Typography } from "@mui/material";
import { useCallback, useEffect, useMemo, useRef, useState } from "react";
import { RawNotebook } from "../../client";
import { GivenValue } from "../../hooks/givenValue";
import { useGivensState } from "../../hooks/useGivensState";
import { useModelGivens } from "../../hooks/useModelGivens";
import { useQueryWithApiError } from "../../hooks/useQueryWithApiError";
import { useSuggestOptions } from "../../hooks/useSuggestOptions";
import { parseResourceUri } from "../../utils/formatting";
import { ApiErrorDisplay } from "../ApiErrorDisplay";
import type { NavigationClick } from "../click_helper";
import { GivensPanel } from "../given";
import { givensToRequest } from "../given/paramCodec";
import { Loading } from "../Loading";
import { useServer } from "../ServerProvider";
import { CleanNotebookContainer, CleanNotebookSection } from "../styles";
import { NotebookCell } from "./NotebookCell";
import { EnhancedNotebookCell } from "./types";

// Maximum number of concurrent cell executions to avoid overwhelming the server
const MAX_CONCURRENT = 4;

interface NotebookProps {
   resourceUri: string;
   maxResultSize?: number;
   /**
    * Parameter values from the host, typically its URL query parameters — the
    * same contract `Dashboard` takes, so a link into a filtered notebook works
    * the way a link into a filtered dashboard does.
    */
   givens?: Record<string, string>;
   /**
    * Applied parameter values, for a host that wants them in its URL. Fires
    * with what the cells reflect, not with every keystroke.
    */
   onGivensChange?: (givens: Record<string, string>) => void;
   /** Optional SPA navigation handler for links inside the notebook, and for a
    * `# drill` click leaving a cell for a dashboard. When omitted, in-notebook
    * links fall back to plain absolute anchors, so no react-router context is
    * required to render a notebook, and drill is inert. */
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

   // Batching matters more here than on a dashboard: one control change re-runs
   // every cell in the document, so an author with expensive cells can ask for
   // an Apply button with a file-level `## autorun=false`.
   const autorun = notebook?.autorun !== false;
   const { draft, applied, setGiven, clearAll, apply, pending } =
      useGivensState({
         declaredTypes,
         // Where the controls start, from a file-level `## givens { … }`. A URL
         // beats them, so a shared link still shows what the sender saw.
         startingValues: notebook?.startingGivens,
         params: givens,
         onParamsChange: onGivensChange,
         autorun,
      });

   // A notebook's model path is the notebook itself: `suggest` queries run
   // against the same model the cells do.
   const { options: givenOptions, isLoading: givenOptionsLoading } =
      useSuggestOptions(
         environmentName,
         packageName,
         notebookPath,
         declaredGivens,
      );

   // `to=self` filters in place, which only works for a given this notebook
   // actually declares: sending one it cannot bind would fail every cell. The
   // mismatch is reported to the author rather than issued — same rule, same
   // wording, as the dashboard viewer.
   const onDrillSelf = useCallback(
      (given: string, value: string) => {
         if (!declaredTypes.has(given)) {
            console.warn(
               `# drill { to=self } tried to set '${given}', which ` +
                  `'${notebookPath}' does not declare as a given. Name the ` +
                  `given with 'given=' on the drill tag.`,
            );
            return;
         }
         setGiven(given, value);
      },
      [declaredTypes, notebookPath, setGiven],
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
   const executeCells = useCallback(
      async (givensToApply: Map<string, GivenValue> = new Map()) => {
         if (!isSuccess || !notebook?.notebookCells) return;

         const runId = ++runIdRef.current;

         // Initialize or reset cells
         setEnhancedCells((prev) => {
            if (prev.length === 0) {
               return notebook.notebookCells.map((cell) => ({ ...cell }));
            }
            return prev.map((cell) => ({
               ...cell,
               result: undefined,
            }));
         });

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
                     // Use notebook cell execution API with optional filter_params and givens
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
                     console.error(
                        `Error executing cell ${cellIndex}:`,
                        cellError,
                     );
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
            if (runIdRef.current !== runId) return;
            console.error("Error executing notebook cells:", error);
            setExecutionError(error as Error);
         } finally {
            if (runIdRef.current === runId) setIsExecuting(false);
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
   const lastRunRef = useRef<string | null>(null);
   useEffect(() => {
      if (!isSuccess || !notebook?.notebookCells) return;
      const runKey = `${resourceUri}|${buildGivens(applied) ?? ""}`;
      if (lastRunRef.current === runKey) return;
      lastRunRef.current = runKey;
      void executeCells(applied);
   }, [isSuccess, notebook, resourceUri, applied, buildGivens, executeCells]);

   return (
      <CleanNotebookContainer>
         <CleanNotebookSection>
            <Stack spacing={3} component="section">
               {/* Parameters panel — the controls for `given:` declarations */}
               <GivensPanel
                  givens={declaredGivens}
                  values={draft}
                  onChange={setGiven}
                  onClearAll={clearAll}
                  options={givenOptions}
                  optionsLoading={givenOptionsLoading}
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
