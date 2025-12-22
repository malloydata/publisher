import PlayArrowIcon from "@mui/icons-material/PlayArrow";
import SearchIcon from "@mui/icons-material/Search";
import { Box, Button, IconButton, Typography } from "@mui/material";
import React, { useEffect } from "react";
import { useQueryWithApiError } from "../../hooks/useQueryWithApiError";
import { parseResourceUri } from "../../utils/formatting";
import { highlight } from "../highlighter";
import ResultContainer from "../RenderedResult/ResultContainer";
import ResultsDialog from "../ResultsDialog";
import { useServer } from "../ServerProvider";
import { CleanMetricCard, CleanNotebookCell } from "../styles";

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

   const { packageName, projectName, versionId, modelPath } =
      parseResourceUri(resourceUri);
   const { apiClients } = useServer();

   const {
      data: queryData,
      isSuccess,
      isLoading,
   } = useQueryWithApiError({
      queryKey: ["namedQueryResult", resourceUri, queryName],
      queryFn: () =>
         apiClients.models.executeQueryModel(
            projectName,
            packageName,
            modelPath,
            {
               query: undefined,
               sourceName: undefined,
               queryName: queryName,
               versionId: versionId,
            },
         ),
      enabled: runOnDemand ? hasRun : true, // Execute on demand or always
   });

   useEffect(() => {
      if (annotations && annotations.length > 0) {
         const code = annotations
            .map((annotation) => `// ${annotation}`)
            .join("\n");
         highlight(code, "typescript").then((highlightedCode) => {
            setHighlightedAnnotations(highlightedCode);
         });
      }
   }, [annotations]);

   return (
      <CleanNotebookCell>
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
                  fontWeight: "600",
                  color: "#495057",
                  padding: "8px 16px",
                  backgroundColor: "#f8f9fa",
                  borderRadius: "6px",
                  border: "1px solid #e9ecef",
               }}
            >
               {queryName}
            </Typography>
            <IconButton
               sx={{
                  backgroundColor: "rgba(255, 255, 255, 0.9)",
                  "&:hover": {
                     backgroundColor: "rgba(255, 255, 255, 1)",
                  },
                  width: "32px",
                  height: "32px",
               }}
               onClick={() => setResultsDialogOpen(true)}
            >
               <SearchIcon sx={{ fontSize: "18px", color: "#666666" }} />
            </IconButton>
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
                     sx={{
                        backgroundColor: "#1976d2",
                        "&:hover": {
                           backgroundColor: "#1565c0",
                        },
                        textTransform: "none",
                        fontSize: "14px",
                        fontWeight: 600,
                        padding: "8px 24px",
                     }}
                  >
                     Run Query
                  </Button>
               </Box>
            )}
            {(!runOnDemand || hasRun) && isLoading && (
               <Box sx={{ padding: "20px", textAlign: "center" }}>
                  <Typography>Loading results...</Typography>
               </Box>
            )}
            {(!runOnDemand || hasRun) &&
               isSuccess &&
               queryData?.data?.result && (
                  <ResultContainer
                     result={queryData.data.result}
                     maxHeight={600}
                     maxResultSize={maxResultSize}
                  />
               )}
         </CleanMetricCard>

         {/* Results Dialog */}
         <ResultsDialog
            open={resultsDialogOpen}
            onClose={() => setResultsDialogOpen(false)}
            result={queryData?.data?.result || ""}
            title={`Query: ${queryName}`}
         />
      </CleanNotebookCell>
   );
}
