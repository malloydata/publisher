import CloseIcon from "@mui/icons-material/Close";
import CodeIcon from "@mui/icons-material/Code";
import ContentCopyIcon from "@mui/icons-material/ContentCopy";
import LinkOutlinedIcon from "@mui/icons-material/LinkOutlined";
import SearchIcon from "@mui/icons-material/Search";
import {
   Box,
   CircularProgress,
   Dialog,
   DialogContent,
   DialogTitle,
   IconButton,
   Snackbar,
   Stack,
   Tooltip,
   Typography,
} from "@mui/material";
import Markdown from "markdown-to-jsx";
import React, { useEffect, useState } from "react";
import { highlight } from "../highlighter";
import { ModelExplorerDialog } from "../Model/ModelExplorerDialog";
import { createEmbeddedQueryResult } from "../QueryResult/QueryResult";
import ResultContainer from "../RenderedResult/ResultContainer";
import ResultsDialog from "../ResultsDialog";
import { CleanMetricCard, CleanNotebookCell } from "../styles";
import { EnhancedNotebookCell } from "./types";

interface NotebookCellProps {
   cell: EnhancedNotebookCell;
   expandCodeCell?: boolean;
   hideCodeCellIcon?: boolean;
   expandEmbedding?: boolean;
   hideEmbeddingIcon?: boolean;
   resourceUri: string;
   index: number;
   maxResultSize?: number;
   isExecuting?: boolean;
}

export function NotebookCell({
   cell,
   hideCodeCellIcon,
   hideEmbeddingIcon,
   resourceUri,
   index,
   maxResultSize,
   isExecuting,
}: NotebookCellProps) {
   const [codeDialogOpen, setCodeDialogOpen] = React.useState<boolean>(false);
   const [embeddingDialogOpen, setEmbeddingDialogOpen] =
      React.useState<boolean>(false);
   const [resultsDialogOpen, setResultsDialogOpen] =
      React.useState<boolean>(false);
   const [highlightedMalloyCode, setHighlightedMalloyCode] =
      React.useState<string>();
   const [highlightedEmbedCode, setHighlightedEmbedCode] =
      React.useState<string>();
   const [sourcesDialogOpen, setSourcesDialogOpen] =
      React.useState<boolean>(false);

   const [copyMessage, setCopyMessage] = useState("");

   // Regex to extract imported names from import statements
   const IMPORT_NAMES_REGEX = /import\s*\{([^}]+)\}\s*from\s*['"`][^'"`]+['"`]/;

   // Regex to extract model path from import statements
   const IMPORT_MODEL_PATH_REGEX =
      /import\s*(?:\{[^}]*\}\s*from\s*)?['"`]([^'"`]+)['"`]/;

   const hasValidImport =
      !!cell.text &&
      (IMPORT_NAMES_REGEX.test(cell.text) ||
         IMPORT_MODEL_PATH_REGEX.test(cell.text));
   const getInitialSourceIndex = () => {
      if (!cell.newSources || cell.newSources.length === 0) return 0;

      let importNames = [];
      let importPath = "";

      if (cell.text) {
         const namesMatch = cell.text.match(IMPORT_NAMES_REGEX);
         if (namesMatch) {
            importNames = namesMatch[1].split(",").map((name) => name.trim());
         }

         const pathMatch = cell.text.match(IMPORT_MODEL_PATH_REGEX);
         if (pathMatch) {
            importPath = pathMatch[1].trim();
         }
      }

      for (let i = 0; i < cell.newSources.length; i++) {
         try {
            const sourceInfo = JSON.parse(cell.newSources[i]);

            // Match either by imported name or by path
            if (
               (importNames.length > 0 &&
                  importNames.includes(sourceInfo.name)) ||
               (importPath && importPath === sourceInfo.path)
            ) {
               return i;
            }
         } catch (_e) {
            continue; // Skip invalid JSON
         }
      }

      return 0; // Default to the first source
   };

   const modelDataFromNewSources =
      cell.newSources && cell.newSources.length > 0
         ? {
              sourceInfos: cell.newSources,
              resource: resourceUri,
           }
         : undefined;

   const queryResultCodeSnippet = createEmbeddedQueryResult({
      query: cell.text,
      resourceUri: resourceUri,
   });

   useEffect(() => {
      if (cell.type === "code")
         highlight(cell.text, "malloy").then((code) => {
            setHighlightedMalloyCode(code);
         });
   }, [cell]);

   useEffect(() => {
      highlight(queryResultCodeSnippet, "typescript").then((code) => {
         setHighlightedEmbedCode(code);
      });
   }, [queryResultCodeSnippet]);

   const copyToClipboard = () => {
      const url = window.location.href;
      navigator.clipboard
         .writeText(url)
         .then(() => setCopyMessage("URL copied to clipboard!"))
         .catch(() => setCopyMessage("Failed to copy URL"));
   };

   return (
      (cell.type === "markdown" && (
         <CleanNotebookCell>
            <Box
               sx={{
                  "& h1, & h2, & h3, & h4, & h5, & h6": {
                     fontWeight: "600",
                     color: "#1a1a1a",
                     marginBottom: "8px",
                     marginTop: "16px",
                  },
                  "& h1": { fontSize: "28px" },
                  "& h2": { fontSize: "24px" },
                  "& h3": { fontSize: "20px" },
                  "& p": {
                     color: "#333333",
                     lineHeight: "1.7",
                     marginBottom: "8px",
                     fontSize: "16px",
                  },
                  "& ul, & ol": {
                     color: "#333333",
                     lineHeight: "1.7",
                     marginBottom: "8px",
                     fontSize: "16px",
                  },
                  "& li": {
                     marginBottom: "4px",
                  },
               }}
            >
               {index === 0 ? (
                  <Stack
                     direction="row"
                     alignItems="flex-start"
                     justifyContent="space-between"
                  >
                     <Markdown>{cell.text}</Markdown>
                     <Tooltip title="Click to copy link">
                        <LinkOutlinedIcon
                           sx={{
                              fontSize: "24px",
                              color: "#666666",
                              cursor: "pointer",
                              marginTop: "26px",
                           }}
                           onClick={copyToClipboard}
                        />
                     </Tooltip>
                  </Stack>
               ) : (
                  <Markdown>{cell.text}</Markdown>
               )}
               <Snackbar
                  open={copyMessage !== ""}
                  autoHideDuration={6000}
                  onClose={() => setCopyMessage("")}
                  message={copyMessage}
               />
            </Box>
         </CleanNotebookCell>
      )) ||
      (cell.type === "code" && (
         <CleanNotebookCell>
            {(!hideCodeCellIcon ||
               (!hideEmbeddingIcon && cell.result) ||
               (cell.newSources && cell.newSources.length > 0)) && (
               <Stack
                  sx={{
                     flexDirection: "column",
                     gap: "8px",
                     marginBottom: "16px",
                  }}
               >
                  {cell.newSources && cell.newSources.length > 0 && (
                     <CleanMetricCard
                        sx={{
                           position: "relative",
                           padding: "0",
                        }}
                     >
                        <Box
                           sx={{
                              display: "flex",
                              alignItems: "center",
                              justifyContent: "space-between",
                              paddingLeft: "24px",
                              paddingRight: "8px",
                           }}
                        >
                           {/* This shouldn't be needed but there's a compiler bug */}
                           {highlightedMalloyCode && (
                              <span
                                 dangerouslySetInnerHTML={{
                                    __html: highlightedMalloyCode,
                                 }}
                                 style={{
                                    fontFamily: "monospace",
                                    fontSize: "14px",
                                    flex: 1,
                                    marginRight: "8px",
                                 }}
                              />
                           )}
                           {hasValidImport && (
                              <IconButton
                                 sx={{
                                    backgroundColor: "rgba(255, 255, 255, 0.9)",
                                    "&:hover": {
                                       backgroundColor:
                                          "rgba(255, 255, 255, 1)",
                                    },
                                    width: "32px",
                                    height: "32px",
                                    flexShrink: 0,
                                 }}
                                 onClick={() => setSourcesDialogOpen(true)}
                              >
                                 <SearchIcon
                                    sx={{ fontSize: "18px", color: "#666666" }}
                                 />
                              </IconButton>
                           )}
                        </Box>
                     </CleanMetricCard>
                  )}
               </Stack>
            )}

            {/* Data Sources Dialog */}
            <ModelExplorerDialog
               open={sourcesDialogOpen}
               onClose={() => setSourcesDialogOpen(false)}
               title="Data Sources"
               hasValidImport={hasValidImport}
               resourceUri={resourceUri}
               data={modelDataFromNewSources}
               initialSelectedSourceIndex={getInitialSourceIndex()}
            />

            {/* Code Dialog */}
            <Dialog
               open={codeDialogOpen}
               onClose={() => setCodeDialogOpen(false)}
               maxWidth="lg"
               fullWidth
            >
               <DialogTitle
                  sx={{
                     display: "flex",
                     justifyContent: "space-between",
                     alignItems: "center",
                  }}
               >
                  Malloy Code
                  <IconButton
                     onClick={() => setCodeDialogOpen(false)}
                     sx={{ color: "#666666" }}
                  >
                     <CloseIcon />
                  </IconButton>
               </DialogTitle>
               <DialogContent>
                  <Box
                     sx={{
                        border: "1px solid #e0e0e0",
                        borderRadius: "8px",
                        padding: "16px",
                        fontFamily: "monospace",
                        fontSize: "14px",
                        lineHeight: "1.5",
                        overflow: "auto",
                        maxHeight: "70vh",
                        backgroundColor: "#ffffff",
                     }}
                  >
                     <pre
                        className="code-display"
                        style={{
                           margin: 0,
                        }}
                        dangerouslySetInnerHTML={{
                           __html: highlightedMalloyCode,
                        }}
                     />
                  </Box>
               </DialogContent>
            </Dialog>

            {/* Embedding Dialog */}
            <Dialog
               open={embeddingDialogOpen}
               onClose={() => setEmbeddingDialogOpen(false)}
               maxWidth="lg"
               fullWidth
            >
               <DialogTitle
                  sx={{
                     display: "flex",
                     justifyContent: "space-between",
                     alignItems: "center",
                  }}
               >
                  Embeddable Code
                  <IconButton
                     onClick={() => setEmbeddingDialogOpen(false)}
                     sx={{ color: "#666666" }}
                  >
                     <CloseIcon />
                  </IconButton>
               </DialogTitle>
               <DialogContent>
                  <Stack
                     sx={{
                        flexDirection: "row",
                        justifyContent: "space-between",
                        alignItems: "flex-start",
                     }}
                  >
                     <Typography
                        component="div"
                        sx={{
                           fontSize: "12px",
                           fontFamily: "monospace",
                           "& .line": { textWrap: "wrap" },
                           flex: 1,
                        }}
                        dangerouslySetInnerHTML={{
                           __html: highlightedEmbedCode,
                        }}
                     />
                     <Tooltip title="Copy Embeddable Code">
                        <IconButton
                           sx={{
                              width: "24px",
                              height: "24px",
                              marginLeft: "8px",
                              color: "#666666",
                           }}
                           onClick={() => {
                              navigator.clipboard.writeText(
                                 queryResultCodeSnippet,
                              );
                           }}
                        >
                           <ContentCopyIcon />
                        </IconButton>
                     </Tooltip>
                  </Stack>
               </DialogContent>
            </Dialog>

            {/* Results Dialog */}
            <ResultsDialog
               open={resultsDialogOpen}
               onClose={() => setResultsDialogOpen(false)}
               result={cell.result || ""}
               title="Results"
            />

            {/* Loading state for executing code cells (not import cells) */}
            {isExecuting &&
               !cell.result &&
               !hasValidImport &&
               !(cell.newSources && cell.newSources.length > 0) && (
                  <CleanMetricCard
                     sx={{
                        display: "flex",
                        justifyContent: "center",
                        alignItems: "center",
                        minHeight: 200,
                     }}
                  >
                     <CircularProgress size={32} />
                  </CleanMetricCard>
               )}

            {cell.result && (
               <CleanMetricCard
                  sx={{
                     position: "relative",
                  }}
               >
                  <Box
                     sx={{
                        paddingTop: "24px",
                     }}
                  >
                     <ResultContainer
                        result={cell.result}
                        minHeight={300}
                        maxHeight={1000}
                        maxResultSize={maxResultSize}
                     />
                  </Box>

                  {/* Fade effect at bottom to indicate more content */}
                  <Box
                     sx={{
                        position: "absolute",
                        bottom: 0,
                        left: 0,
                        right: 0,
                        height: "40px",
                        background:
                           "linear-gradient(transparent, rgba(255, 255, 255, 0.9))",
                        pointerEvents: "none",
                        zIndex: 1,
                     }}
                  />

                  {/* Top right corner controls */}
                  <Stack
                     sx={{
                        position: "absolute",
                        top: "8px",
                        right: "8px",
                        flexDirection: "row",
                        gap: "8px",
                        alignItems: "center",
                        zIndex: 2,
                     }}
                  >
                     {!hideCodeCellIcon && (
                        <IconButton
                           sx={{
                              backgroundColor: "rgba(255, 255, 255, 0.9)",
                              "&:hover": {
                                 backgroundColor: "rgba(255, 255, 255, 1)",
                              },
                              width: "32px",
                              height: "32px",
                           }}
                           onClick={(e) => {
                              e.stopPropagation();
                              setCodeDialogOpen(true);
                           }}
                        >
                           <CodeIcon
                              sx={{ fontSize: "18px", color: "#666666" }}
                           />
                        </IconButton>
                     )}
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
                        <SearchIcon
                           sx={{ fontSize: "18px", color: "#666666" }}
                        />
                     </IconButton>
                  </Stack>
               </CleanMetricCard>
            )}
         </CleanNotebookCell>
      ))
   );
}
