import React from "react";
import { Suspense, lazy } from "react";
import {
   Stack,
   Collapse,
   CardActions,
   CardContent,
   IconButton,
   Tooltip,
} from "@mui/material";
import { StyledCard, StyledCardContent } from "../styles";
import Markdown from "markdown-to-jsx";
import { NotebookCell as ClientNotebookCell } from "../../client";
import { Typography } from "@mui/material";
import { Divider } from "@mui/material";
import ContentCopyIcon from "@mui/icons-material/ContentCopy";
import { highlight } from "../highlighter";
import { useEffect } from "react";
import CodeIcon from "@mui/icons-material/Code";
import LinkOutlinedIcon from "@mui/icons-material/LinkOutlined";

const RenderedResult = lazy(() => import("../RenderedResult/RenderedResult"));

interface NotebookCellProps {
   cell: ClientNotebookCell;
   modelInfo: string;
   queryResultCodeSnippet: string;
   expandCodeCell?: boolean;
   hideCodeCellIcon?: boolean;
   expandEmbedding?: boolean;
   hideEmbeddingIcon?: boolean;
}

export function NotebookCell({
   cell,
   queryResultCodeSnippet,
   expandCodeCell,
   hideCodeCellIcon,
   expandEmbedding,
   hideEmbeddingIcon,
}: NotebookCellProps) {
   const [codeExpanded, setCodeExpanded] =
      React.useState<boolean>(expandCodeCell);
   const [embeddingExpanded, setEmbeddingExpanded] =
      React.useState<boolean>(expandEmbedding);
   const [highlightedMalloyCode, setHighlightedMalloyCode] =
      React.useState<string>();
   const [highlightedEmbedCode, setHighlightedEmbedCode] =
      React.useState<string>();
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

   return (
      (cell.type === "markdown" && (
         <StyledCard variant="outlined" sx={{ border: 0 }}>
            <StyledCardContent>
               <Markdown>{cell.text}</Markdown>
            </StyledCardContent>
         </StyledCard>
      )) ||
      (cell.type === "code" && (
         <StyledCard variant="outlined">
            {(!hideCodeCellIcon || (!hideEmbeddingIcon && cell.result)) && (
               <Stack sx={{ flexDirection: "row", justifyContent: "right" }}>
                  <CardActions
                     sx={{
                        padding: "0px 10px 0px 10px",
                        mb: "auto",
                        mt: "auto",
                     }}
                  >
                     {!hideCodeCellIcon && (
                        <Tooltip
                           title={codeExpanded ? "Hide Code" : "View Code"}
                        >
                           <IconButton
                              size="small"
                              onClick={() => {
                                 setCodeExpanded(!codeExpanded);
                              }}
                           >
                              <CodeIcon />
                           </IconButton>
                        </Tooltip>
                     )}
                     {!hideEmbeddingIcon && cell.result && (
                        <Tooltip
                           title={
                              embeddingExpanded
                                 ? "Hide Embedding"
                                 : "View Embedding"
                           }
                        >
                           <IconButton
                              size="small"
                              onClick={() => {
                                 setEmbeddingExpanded(!embeddingExpanded);
                              }}
                           >
                              <LinkOutlinedIcon />
                           </IconButton>
                        </Tooltip>
                     )}
                  </CardActions>
               </Stack>
            )}
            <Collapse in={embeddingExpanded} timeout="auto" unmountOnExit>
               <Divider />
               <Stack
                  sx={{
                     p: "10px",
                     borderRadius: 0,
                     flexDirection: "row",
                     justifyContent: "space-between",
                  }}
               >
                  <Typography
                     component="div"
                     sx={{
                        fontSize: "12px",
                        "& .line": { textWrap: "wrap" },
                     }}
                     dangerouslySetInnerHTML={{
                        __html: highlightedEmbedCode,
                     }}
                  />
                  <Tooltip title="Copy Embeddable Code">
                     <IconButton
                        sx={{ width: "24px", height: "24px" }}
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
            </Collapse>
            <Collapse in={codeExpanded} timeout="auto" unmountOnExit>
               <Divider />
               <Stack
                  sx={{
                     p: "10px",
                     borderRadius: 0,
                     flexDirection: "row",
                     justifyContent: "space-between",
                  }}
               >
                  <Typography
                     component="div"
                     className="content"
                     sx={{ fontSize: "12px", "& .line": { textWrap: "wrap" } }}
                     dangerouslySetInnerHTML={{
                        __html: highlightedMalloyCode,
                     }}
                  />
               </Stack>
            </Collapse>
            {cell.result && (
               <>
                  <Divider sx={{ mb: "10px" }} />
                  <CardContent sx={{ maxHeight: "400px", overflow: "auto" }}>
                     <Suspense fallback="Loading malloy...">
                        <RenderedResult result={cell.result} />
                     </Suspense>
                  </CardContent>
               </>
            )}
         </StyledCard>
      ))
   );
}
