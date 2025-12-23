import * as Malloy from "@malloydata/malloy-interfaces";
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
              result: Malloy.Result;
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
      projectName: projectName,
      packageName: packageName,
      versionId: versionId,
   } = parseResourceUri(resourceUri);
   const { apiClients } = useServer();

   const mutation = useMutationWithApiError({
      mutationFn: () => {
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
            response: {
               result: {} as Malloy.Result, // placeholder
            },
         });

         setQuery({
            ...query,
            query: malloy,
         });
         return apiClients.models.executeQueryModel(
            projectName,
            packageName,
            sourceAndPath.modelPath,
            {
               query: malloy,
               sourceName: undefined,
               queryName: undefined,
               versionId: versionId,
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
            // Update submitted query with results
            setSubmittedQuery((prev) =>
               prev
                  ? {
                       ...prev,
                       executionState: "finished",
                       response: {
                          result: parsedResult as Malloy.Result,
                       },
                    }
                  : undefined,
            );
         }
      },
      onError: (error) => {
         setSubmittedQuery(undefined);
         console.error("Query execution error:", error);
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
               <div
                  style={{
                     alignItems: "center",
                     justifyContent: "center",
                     height: "200px",
                     color: "#666",
                  }}
               >
                  Loading explorer...
               </div>
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
