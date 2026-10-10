// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import PlayArrowIcon from "@mui/icons-material/PlayArrow";
import SearchIcon from "@mui/icons-material/Search";
import { Box, Button, Typography } from "@mui/material";
import React, { useEffect } from "react";
import { useQueryResult } from "../../hooks/useQueryResult";
import { usePublisherTheme } from "../../theme/ThemeContext";
import { parseResourceUri } from "../../utils/formatting";
import { FloatingIconButton } from "../FloatingIconButton";
import { highlight } from "../highlighter";
import { ResultPanel } from "../RenderedResult/ResultPanel";
import { MODEL_CELL_MAX_HEIGHT } from "../RenderedResult/resultSizing";
import ResultsDialog from "../ResultsDialog";
import { CleanMetricCard } from "../styles";

interface ModelCellProps {
   sourceName?: string;
   queryName: string;
   noView?: boolean;
   annotations?: string[];
   resourceUri: string;
   runOnDemand?: boolean;
   maxResultSize?: number;
}

export function ModelCell({
   queryName,
   annotations,
   resourceUri,
   runOnDemand = false,
   maxResultSize = 0,
}: ModelCellProps) {
   const [highlightedAnnotations, setHighlightedAnnotations] =
      React.useState<string>();
   const [resultsDialogOpen, setResultsDialogOpen] = React.useState(false);
   const [hasRun, setHasRun] = React.useState(false);

   const { packageName, environmentName, versionId, modelPath } =
      parseResourceUri(resourceUri);
   // Run on demand or always; a query not yet asked for is not fetched.
   const shouldRun = !runOnDemand || hasRun;
   const state = useQueryResult(
      { environmentName, packageName, modelPath, versionId, queryName },
      { enabled: shouldRun },
   );
   const queryData = state.data;

   const { mode } = usePublisherTheme();
   useEffect(() => {
      if (annotations && annotations.length > 0) {
         const code = annotations
            .map((annotation) => `// ${annotation}`)
            .join("\n");
         highlight(code, "typescript", mode).then((highlightedCode) => {
            setHighlightedAnnotations(highlightedCode);
         });
      }
   }, [annotations, mode]);

   return (
      <Box>
         {highlightedAnnotations && (
            <Box sx={{ marginBottom: "16px" }}>
               <Typography
                  fontSize="12px"
                  sx={{
                     fontSize: "12px",
                     "& .line": { textWrap: "wrap" },
                  }}
               >
                  <div
                     className="content"
                     dangerouslySetInnerHTML={{
                        __html: highlightedAnnotations,
                     }}
                  />
               </Typography>
            </Box>
         )}

         {/* Query name and magnifying glass - styled like explorer tabs */}
         <Box
            sx={{
               display: "flex",
               justifyContent: "space-between",
               alignItems: "center",
               marginBottom: "8px",
            }}
         >
            <Typography
               variant="body2"
               sx={{
                  fontSize: "15px",
                  fontWeight: 600,
                  color: "text.primary",
                  px: 2,
                  py: 1,
                  bgcolor: "action.hover",
                  borderRadius: 1.5,
                  border: 1,
                  borderColor: "divider",
               }}
            >
               {queryName}
            </Typography>
            <FloatingIconButton
               aria-label="Expand results"
               onClick={() => setResultsDialogOpen(true)}
            >
               <SearchIcon />
            </FloatingIconButton>
         </Box>

         <CleanMetricCard
            sx={{
               position: "relative",
            }}
         >
            {runOnDemand && !hasRun && (
               <Box
                  sx={{
                     padding: "40px 20px",
                     textAlign: "center",
                     display: "flex",
                     flexDirection: "column",
                     alignItems: "center",
                     gap: "16px",
                  }}
               >
                  <Typography variant="body2" color="text.secondary">
                     Click Run to execute the query
                  </Typography>
                  <Button
                     variant="contained"
                     startIcon={<PlayArrowIcon />}
                     onClick={() => setHasRun(true)}
                  >
                     Run Query
                  </Button>
               </Box>
            )}
            {shouldRun && (
               <ResultPanel
                  state={state}
                  context={queryName}
                  maxHeight={MODEL_CELL_MAX_HEIGHT}
                  maxResultSize={maxResultSize}
               />
            )}
         </CleanMetricCard>

         {/* Results Dialog */}
         <ResultsDialog
            open={resultsDialogOpen}
            onClose={() => setResultsDialogOpen(false)}
            result={queryData?.data?.result || ""}
            renderLogs={queryData?.data?.renderLogs}
            title={`Query: ${queryName}`}
         />
      </Box>
   );
}
