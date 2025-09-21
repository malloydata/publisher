import { Box, Button, Stack } from "@mui/material";
import { styled } from "@mui/material/styles";
import React from "react";
import { CompiledModel } from "../../client";
import { parseResourceUri } from "../../utils/formatting";
import { ApiErrorDisplay } from "../ApiErrorDisplay";
import { Loading } from "../Loading";
import { StyledCard, StyledCardContent, StyledCardMedia } from "../styles";
import { QueryExplorerResult, SourcesExplorer } from "./SourcesExplorer";
import { useModelData } from "./useModelData";

// Add a styled component for the multi-row tab bar
const MultiRowTabBar = styled(Box)(({ theme }) => ({
   display: "flex",
   flexWrap: "wrap",
   gap: theme.spacing(1),
   borderBottom: "1px solid #f0f0f0",
   minHeight: 48,
   paddingBottom: "8px",
}));

const MultiRowTab = styled(Button)<{ selected?: boolean }>(
   ({ theme, selected }) => ({
      minHeight: 32,
      padding: theme.spacing(0.75, 2),
      borderRadius: "6px",
      background: selected ? "#f8f9fa" : "transparent",
      color: selected ? "#495057" : "#666666",
      fontWeight: selected ? 600 : 500,
      border: selected ? "1px solid #e9ecef" : "1px solid transparent",
      boxShadow: "none",
      textTransform: "none",
      fontSize: "15px",
      "&:hover": {
         background: selected ? "#f8f9fa" : "#fafafa",
         border: selected ? "1px solid #e9ecef" : "1px solid #f0f0f0",
      },
   }),
);

export interface ModelExplorerProps {
   data?: CompiledModel;
   /** Callback when the explorer changes (e.g. when a query is selected). */
   onChange?: (query: QueryExplorerResult) => void;
   /** Existing query to initialize the explorer with */
   existingQuery?: QueryExplorerResult;
   /** Initial selected source index */
   initialSelectedSourceIndex?: number;
   /** Callback when source selection changes */
   onSourceChange?: (index: number) => void;
   resourceUri: string;
}

/**
 * ModelExplorer renders the main explorer UI for a Malloy model. It shows the
 * selected source (via `SourceExplorerComponent`) along with the list of named
 * queries for that model. This logic was originally embedded inside `Model.tsx`
 * but has been extracted for easier reuse.
 */
export function ModelExplorer({
   data,
   onChange,
   existingQuery,
   initialSelectedSourceIndex = 0,
   onSourceChange,
   resourceUri,
}: ModelExplorerProps) {
   const [selectedTab, setSelectedTab] = React.useState(
      initialSelectedSourceIndex,
   );

   // Update selectedTab when initialSelectedSourceIndex changes
   React.useEffect(() => {
      setSelectedTab(initialSelectedSourceIndex);
   }, [initialSelectedSourceIndex]);

   // If data is not provided, fetch it internally
   const {
      data: fetchedData,
      isError,
      isLoading,
      error,
   } = useModelData(resourceUri, !data); // we shld only fetch when data is not provided
   const { modelPath } = parseResourceUri(resourceUri);

   const effectiveData = data || fetchedData;

   if (isLoading && !data) {
      return <Loading text="Fetching Model..." />;
   }

   if (isError && !data) {
      console.log("error", error);
      return (
         <ApiErrorDisplay
            error={error}
            context={`ModelExplorer > ${modelPath}`}
         />
      );
   }

   if (!effectiveData) {
      return <Loading text="Loading..." />;
   }

   return (
      <StyledCard variant="outlined">
         <StyledCardContent>
            <Stack
               sx={{
                  flexDirection: "row",
                  justifyContent: "space-between",
               }}
            >
               {/* Render the tabs for source selection */}
               {Array.isArray(effectiveData.sourceInfos) &&
                  effectiveData.sourceInfos.length > 0 && (
                     <MultiRowTabBar>
                        {effectiveData.sourceInfos.map((source, idx) => {
                           let sourceInfo;
                           try {
                              sourceInfo = JSON.parse(source);
                           } catch {
                              sourceInfo = { name: String(idx) };
                           }
                           return (
                              <MultiRowTab
                                 key={sourceInfo.name || idx}
                                 selected={selectedTab === idx}
                                 onClick={() => {
                                    setSelectedTab(idx);
                                    if (onSourceChange) {
                                       onSourceChange(idx);
                                    }
                                 }}
                              >
                                 {sourceInfo.name || `Source ${idx + 1}`}
                              </MultiRowTab>
                           );
                        })}
                     </MultiRowTabBar>
                  )}
            </Stack>
         </StyledCardContent>
         <StyledCardMedia>
            <Stack spacing={2} component="section">
               {/* Render the selected source info */}
               {Array.isArray(effectiveData.sourceInfos) &&
                  effectiveData.sourceInfos.length > 0 && (
                     <SourcesExplorer
                        sourceAndPaths={effectiveData.sourceInfos.map(
                           (source) => {
                              const sourceInfo = JSON.parse(source);
                              return {
                                 sourceInfo: sourceInfo,
                                 modelPath: modelPath,
                              };
                           },
                        )}
                        selectedSourceIndex={selectedTab}
                        existingQuery={existingQuery}
                        onQueryChange={onChange}
                        resourceUri={resourceUri}
                     />
                  )}

               <Box height="5px" />
            </Stack>
         </StyledCardMedia>
      </StyledCard>
   );
}
