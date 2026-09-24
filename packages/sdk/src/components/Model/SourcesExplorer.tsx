// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import * as Malloy from "@malloydata/malloy-interfaces";
import type { Message } from "@malloydata/malloy-explorer";
import { Box, Stack } from "@mui/system";
import {
   StyledCardMedia,
   StyledExplorerContent,
   StyledExplorerPage,
} from "../styles";

import React, { useEffect, useState } from "react";
import { useMutationWithApiError } from "../../hooks/useQueryWithApiError";
import { parseResourceUri } from "../../utils/formatting";
// import { ApiErrorDisplay } from "../ApiErrorDisplay";
import { useServer } from "../ServerProvider";
import type { RunGate } from "./runGate";

type ExplorerComponents = typeof import("@malloydata/malloy-explorer");
type QueryBuilder = typeof import("@malloydata/malloy-query-builder");

export interface SourceAndPath {
   modelPath: string;
   sourceInfo: Malloy.SourceInfo;
}

export interface SourceExplorerProps {
   sourceAndPaths: SourceAndPath[];
   selectedSourceIndex: number;
   existingQuery?: QueryExplorerResult;
   onQueryChange?: (query: QueryExplorerResult) => void;
   onSourceChange?: (index: number) => void;
   resourceUri: string;
   /** The control row's current values, sent with every Run. */
   givens?: Record<string, unknown>;
   /** Whether Run can proceed given what the control row currently holds. */
   gate?: RunGate;
}

/**
 * Component for Exploring a set of sources.
 * Sources are provided as a list of SourceAndPath objects where each entry
 * Maps from a model path to a source info object.
 * It is expected that multiple sourceInfo entries will correspond to the same
 * model path.
 */
export function SourcesExplorer({
   sourceAndPaths,
   selectedSourceIndex,
   existingQuery,
   onQueryChange,
   onSourceChange,
   resourceUri,
   givens,
   gate,
}: SourceExplorerProps) {
   // Notify parent component when selected source changes
   React.useEffect(() => {
      if (onSourceChange) {
         onSourceChange(selectedSourceIndex);
      }
   }, [selectedSourceIndex, onSourceChange]);

   return (
      <StyledCardMedia>
         <Stack spacing={2} component="section">
            <SourceExplorerComponent
               sourceAndPath={sourceAndPaths[selectedSourceIndex]}
               existingQuery={existingQuery}
               onChange={(query) => {
                  if (onQueryChange) {
                     onQueryChange(query);
                  }
               }}
               resourceUri={resourceUri}
               givens={givens}
               gate={gate}
            />
            <Box height="5px" />
         </Stack>
      </StyledCardMedia>
   );
}

interface SourceExplorerComponentProps {
   sourceAndPath: SourceAndPath;
   existingQuery?: QueryExplorerResult;
   onChange?: (query: QueryExplorerResult) => void;
   resourceUri: string;
   givens?: Record<string, unknown>;
   gate?: RunGate;
}

export interface QueryExplorerResult {
   query: string | undefined;
   malloyQuery: Malloy.Query | string | undefined;
   malloyResult: Malloy.Result | undefined;
}

export function emptyQueryExplorerResult(): QueryExplorerResult {
   return {
      query: undefined,
      malloyQuery: undefined,
      malloyResult: undefined,
   };
}
function SourceExplorerComponentInner({
   sourceAndPath,
   onChange,
   existingQuery,
   explorerComponents,
   QueryBuilder,
   resourceUri,
   givens,
   gate,
}: SourceExplorerComponentProps & {
   explorerComponents: ExplorerComponents;
   QueryBuilder: QueryBuilder;
   resourceUri: string;
}) {
   const [query, setQuery] = React.useState<QueryExplorerResult>(
      existingQuery || emptyQueryExplorerResult(),
   );
   const [submittedQuery, setSubmittedQuery] = React.useState<
      | {
           executionState: "running" | "finished";
           response: {
              result?: Malloy.Result;
              messages?: Message[];
           };
           query: Malloy.Query | string;
           queryResolutionStartMillis: number;
           onCancel: () => void;
        }
      | undefined
   >(undefined);

   // Update query when existingQuery changes
   React.useEffect(() => {
      if (existingQuery) {
         setQuery(existingQuery);
      }
   }, [existingQuery]);
   const [focusedNestViewPath, setFocusedNestViewPath] = React.useState<
      string[]
   >([]);

   const {
      MalloyExplorerProvider,
      QueryPanel,
      ResizableCollapsiblePanel,
      ResultPanel,
      SourcePanel,
   } = explorerComponents;

   React.useEffect(() => {
      if (onChange) {
         onChange(query);
      }
   }, [onChange, query]);
   const {
      environmentName: environmentName,
      packageName: packageName,
      versionId: versionId,
   } = parseResourceUri(resourceUri);
   const { apiClients } = useServer();

   // Captured at Run, so the defaults note describes the run that produced the result.
   const gateAtRunRef = React.useRef<RunGate | undefined>(undefined);

   const mutation = useMutationWithApiError({
      mutationFn: () => {
         gateAtRunRef.current = gate;
         // If malloyQuery is a string, we can use it directly, otherwise convert to Malloy
         const malloy =
            typeof query?.malloyQuery === "string"
               ? query.malloyQuery
               : new QueryBuilder.ASTQuery({
                    source: sourceAndPath.sourceInfo,
                    query: query?.malloyQuery,
                 }).toMalloy();

         // Set submitted query when execution starts
         setSubmittedQuery({
            executionState: "running",
            query: query?.malloyQuery,
            queryResolutionStartMillis: Date.now(),
            onCancel: () => {
               mutation.reset();
               setSubmittedQuery(undefined);
            },
            response: {},
         });

         setQuery({
            ...query,
            query: malloy,
         });
         return apiClients.models.executeQueryModel(
            environmentName,
            packageName,
            sourceAndPath.modelPath,
            {
               query: malloy,
               sourceName: undefined,
               queryName: undefined,
               versionId: versionId,
               // Omitted when empty, so a model with no givens sends the same body as before.
               ...(givens && Object.keys(givens).length > 0 ? { givens } : {}),
            },
         );
      },
      onSuccess: (data) => {
         if (data) {
            const parsedResult = JSON.parse(data.data.result);
            setQuery({
               ...query,
               malloyResult: parsedResult as Malloy.Result,
            });
            const ranGate = gateAtRunRef.current;
            // Update submitted query with results
            setSubmittedQuery((prev) =>
               prev
                  ? {
                       ...prev,
                       executionState: "finished",
                       response: {
                          result: parsedResult as Malloy.Result,
                          ...(ranGate?.kind === "defaults"
                             ? {
                                  messages: [
                                     { severity: "INFO", title: ranGate.note },
                                  ],
                               }
                             : {}),
                       },
                    }
                  : undefined,
            );
         }
      },
      onError: (error) => {
         console.error("Query execution error:", error);
         const message =
            (error as { data?: { message?: string } } | undefined)?.data
               ?.message ??
            (error as Error | undefined)?.message ??
            "The query could not be run.";
         // Shown in the results pane rather than cleared, so the server's reason is visible.
         setSubmittedQuery((prev) => ({
            executionState: "finished",
            query: prev?.query ?? query?.malloyQuery,
            queryResolutionStartMillis:
               prev?.queryResolutionStartMillis ?? Date.now(),
            onCancel:
               prev?.onCancel ??
               (() => {
                  mutation.reset();
                  setSubmittedQuery(undefined);
               }),
            response: { messages: [{ severity: "ERROR", title: message }] },
         }));
      },
   });

   const [oldSourceInfo, setOldSourceInfo] = React.useState(
      sourceAndPath.sourceInfo.name,
   );

   // This hack is needed since sourceInfo is updated before
   // query is reset, which results in the query not being found
   // because it does not exist on the new source.
   React.useEffect(() => {
      if (oldSourceInfo !== sourceAndPath.sourceInfo.name) {
         setOldSourceInfo(sourceAndPath.sourceInfo.name);
         setQuery(emptyQueryExplorerResult());
         setSubmittedQuery(undefined);
      }
   }, [sourceAndPath, oldSourceInfo]);

   const onQueryChange = React.useCallback(
      (malloyQuery: Malloy.Query | string | undefined) => {
         setQuery({ ...query, malloyQuery, malloyResult: undefined });
      },
      [query],
   );

   if (oldSourceInfo !== sourceAndPath.sourceInfo.name) {
      return <div>Loading...</div>;
   }
   return (
      <StyledExplorerContent
         key={sourceAndPath.sourceInfo.name}
         sx={{
            border: "1px solid #e0e0e0",
            borderRadius: "8px",
            overflow: "hidden",
         }}
      >
         <MalloyExplorerProvider
            source={sourceAndPath.sourceInfo}
            query={query?.malloyQuery}
            topValues={[]}
            onFocusedNestViewPathChange={setFocusedNestViewPath}
            focusedNestViewPath={focusedNestViewPath}
            onQueryChange={onQueryChange}
         >
            <div
               style={{
                  display: "flex",
                  height: "100%",
                  overflowY: "auto",
               }}
            >
               <ResizableCollapsiblePanel
                  isInitiallyExpanded={true}
                  initialWidth={180}
                  minWidth={180}
                  icon="database"
                  title={sourceAndPath.sourceInfo.name}
               >
                  <SourcePanel
                     onRefresh={() => setQuery(emptyQueryExplorerResult())}
                  />
               </ResizableCollapsiblePanel>
               <ResizableCollapsiblePanel
                  isInitiallyExpanded={true}
                  initialWidth={280}
                  minWidth={280}
                  icon="filterSliders"
                  title="Query"
               >
                  <QueryPanel
                     runQuery={() => {
                        console.log(
                           `running query with:  ${query?.malloyQuery}`,
                        );
                        if (gate?.kind === "blocked") {
                           // Say which given is missing instead of sending a request that can only fail.
                           setSubmittedQuery({
                              executionState: "finished",
                              query: query?.malloyQuery,
                              queryResolutionStartMillis: Date.now(),
                              onCancel: () => setSubmittedQuery(undefined),
                              response: {
                                 messages: [
                                    { severity: "WARN", title: gate.reason },
                                 ],
                              },
                           });
                           return;
                        }
                        try {
                           mutation.mutate();
                        } catch (error) {
                           console.error("Error running query:", error);
                        }
                     }}
                  />
               </ResizableCollapsiblePanel>
               <ResultPanel
                  source={sourceAndPath.sourceInfo}
                  draftQuery={query?.malloyQuery}
                  setDraftQuery={(malloyQuery) =>
                     setQuery({ ...query, malloyQuery: malloyQuery })
                  }
                  submittedQuery={submittedQuery}
                  options={{ showRawQuery: true }}
               />
            </div>
         </MalloyExplorerProvider>
      </StyledExplorerContent>
   );
}

// Lazy-loaded wrapper component
export function SourceExplorerComponent(props: SourceExplorerComponentProps) {
   const [explorerComponents, setExplorerComponents] =
      useState<ExplorerComponents | null>(null);
   const [QueryBuilder, setQueryBuilder] = useState<QueryBuilder | null>(null);
   const [loading, setLoading] = useState(true);

   useEffect(() => {
      let isMounted = true;

      Promise.all([
         import("@malloydata/malloy-explorer"),
         import("@malloydata/malloy-query-builder"),
      ])
         .then(([explorerComponents, queryBuilder]) => {
            if (isMounted) {
               setExplorerComponents(explorerComponents);
               setQueryBuilder(queryBuilder);
               setLoading(false);
            }
         })
         .catch((error) => {
            console.error("Failed to load Malloy components:", error);
            if (isMounted) {
               setLoading(false);
            }
         });

      return () => {
         isMounted = false;
      };
   }, []);

   if (loading || !explorerComponents || !QueryBuilder) {
      return (
         <StyledExplorerPage>
            <StyledExplorerContent>
               <Box
                  sx={{
                     alignItems: "center",
                     justifyContent: "center",
                     height: "200px",
                     color: "text.secondary",
                  }}
               >
                  Loading explorer...
               </Box>
            </StyledExplorerContent>
         </StyledExplorerPage>
      );
   }

   return (
      <SourceExplorerComponentInner
         {...props}
         explorerComponents={explorerComponents}
         QueryBuilder={QueryBuilder}
         resourceUri={props.resourceUri}
      />
   );
}
