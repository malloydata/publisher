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
import ShareIcon from "@mui/icons-material/ShareOutlined";

const RenderedResult = lazy(() => import("../RenderedResult/RenderedResult"));

interface NotebookCellProps {
   cell: ClientNotebookCell;
   modelDef: string;
   dataStyles: string;
   queryResultCodeSnippet: string;
}

export function NotebookCell({
   cell,
   modelDef,
   dataStyles,
   queryResultCodeSnippet,
}: NotebookCellProps) {
   const [codeExpanded, setCodeExpanded] = React.useState<boolean>(false);
   const [sharingExpanded, setSharingExpanded] = React.useState<boolean>(false);
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
            <Stack
               sx={{ flexDirection: "row", justifyContent: "space-between" }}
            >
               <Typography variant="overline" sx={{ ml: "10px" }}>
                  Code {cell.queryResult && "+ Results"} Cell
               </Typography>
               <Stack>
                  <CardActions
                     sx={{
                        padding: "0px 10px 0px 10px",
                        mb: "auto",
                        mt: "auto",
                     }}
                  >
                     <Tooltip title={codeExpanded ? "Hide Code" : "View Code"}>
                        <IconButton
                           size="small"
                           onClick={() => {
                              setCodeExpanded(!codeExpanded);
                           }}
                        >
                           <CodeIcon />
                        </IconButton>
                     </Tooltip>
                     {cell.queryResult && (
                        <Tooltip
                           title={
                              sharingExpanded ? "Hide Sharing" : "View Sharing"
                           }
                        >
                           <IconButton
                              size="small"
                              onClick={() => {
                                 setSharingExpanded(!sharingExpanded);
                              }}
                           >
                              <ShareIcon />
                           </IconButton>
                        </Tooltip>
                     )}
                  </CardActions>
               </Stack>
            </Stack>
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
                     sx={{ fontSize: "12px", "& .line": { textWrap: "wrap" } }}
                  >
                     <div
                        className="content"
                        dangerouslySetInnerHTML={{
                           __html: highlightedMalloyCode,
                        }}
                     />
                  </Typography>
                  <Tooltip title="View Code">
                     <IconButton
                        sx={{ width: "24px", height: "24px" }}
                        onClick={() => {
                           navigator.clipboard.writeText(cell.text);
                        }}
                     >
                        <ContentCopyIcon />
                     </IconButton>
                  </Tooltip>
               </Stack>
            </Collapse>
            <Collapse in={sharingExpanded} timeout="auto" unmountOnExit>
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
                     sx={{
                        fontSize: "12px",
                        "& .line": { textWrap: "wrap" },
                     }}
                  >
                     <div
                        dangerouslySetInnerHTML={{
                           __html: highlightedEmbedCode,
                        }}
                     />
                  </Typography>
                  <Tooltip title="View Code">
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
            {cell.queryResult && (
               <>
                  <Divider sx={{ mb: "10px" }} />
                  <CardContent>
                     <Suspense fallback="Loading malloy...">
                        <RenderedResult
                           modelDef={modelDef}
                           queryResult={cell.queryResult}
                           dataStyles={dataStyles}
                        />
                     </Suspense>
                  </CardContent>
               </>
            )}
         </StyledCard>
      ))
   );
}
