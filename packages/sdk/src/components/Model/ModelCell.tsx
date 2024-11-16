import React from "react";
import {
   styled,
   Collapse,
   CardActions,
   Card,
   CardContent,
   Divider,
   Stack,
   Typography,
   Tooltip,
   IconButton,
} from "@mui/material";
import { QueryResult } from "../QueryResult";
import AnalyticsOutlinedIcon from "@mui/icons-material/AnalyticsOutlined";
import ShareOutlinedIcon from "@mui/icons-material/ShareOutlined";
import ContentCopyIcon from "@mui/icons-material/ContentCopy";
import { useEffect } from "react";
import { highlight } from "../highlighter";

const StyledCard = styled(Card)({
   display: "flex",
   flexDirection: "column",
   height: "100%",
});

interface ModelCellProps {
   server: string;
   packageName: string;
   modelPath: string;
   sourceName?: string;
   queryName: string;
}

export function ModelCell({
   server,
   packageName,
   modelPath,
   sourceName,
   queryName,
}: ModelCellProps) {
   const [resultsExpanded, setResultsExpanded] = React.useState(false);
   const [sharingExpanded, setSharingExpanded] = React.useState(false);
   const [highlightedEmbedCode, setHighlightedEmbedCode] =
      React.useState<string>();
   useEffect(() => {
      highlight(
         getQueryResultCodeSnippet(
            server,
            packageName,
            modelPath,
            sourceName,
            queryName,
         ),
         "typescript",
      ).then((code) => {
         setHighlightedEmbedCode(code);
      });
   }, [server, packageName, modelPath, sourceName, queryName]);

   return (
      <>
         <StyledCard
            variant="outlined"
            sx={{
               padding: "0px 10px 0px 10px",
               borderRadius: "0px",
            }}
         >
            <Stack
               sx={{
                  display: "flex",
                  flexDirection: "row",
                  justifyContent: "space-between",
                  width: "100%",
               }}
            >
               <Typography
                  variant="subtitle2"
                  sx={{ mt: "auto", mb: "auto" }}
               >{`View > ${queryName}`}</Typography>
               <CardActions sx={{ padding: "0px" }}>
                  <Tooltip
                     title={resultsExpanded ? "Hide Results" : "Show Results"}
                  >
                     <IconButton
                        size="small"
                        onClick={() => {
                           setResultsExpanded(!resultsExpanded);
                        }}
                     >
                        <AnalyticsOutlinedIcon />
                     </IconButton>
                  </Tooltip>
                  <Tooltip
                     title={sharingExpanded ? "Hide Sharing" : "Show Sharing"}
                  >
                     <IconButton
                        size="small"
                        onClick={() => {
                           setSharingExpanded(!sharingExpanded);
                        }}
                     >
                        <ShareOutlinedIcon />
                     </IconButton>
                  </Tooltip>
               </CardActions>
            </Stack>
            <Collapse in={sharingExpanded} timeout="auto" unmountOnExit>
               <Divider sx={{ mb: "10px" }} />
               <Stack
                  sx={{
                     p: "10px",
                     borderRadius: 0,
                     flexDirection: "row",
                     justifyContent: "space-between",
                  }}
               >
                  <Typography
                     fontSize="12px"
                     sx={{ fontSize: "12px", "& .line": { textWrap: "wrap" } }}
                  >
                     <div
                        className="content"
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
                              getQueryResultCodeSnippet(
                                 server,
                                 packageName,
                                 modelPath,
                                 sourceName,
                                 queryName,
                              ),
                           );
                        }}
                     >
                        <ContentCopyIcon />
                     </IconButton>
                  </Tooltip>
               </Stack>
            </Collapse>
            <Collapse in={resultsExpanded} timeout="auto" unmountOnExit>
               <Divider sx={{ mb: "10px" }} />
               <CardContent>
                  <QueryResult
                     server={server}
                     packageName={packageName}
                     modelPath={modelPath}
                     sourceName={sourceName}
                     queryName={queryName}
                  />
               </CardContent>
            </Collapse>
         </StyledCard>
      </>
   );
}

function getQueryResultCodeSnippet(
   server: string,
   packageName: string,
   modelPath: string,
   sourceName: string,
   queryName: string,
): string {
   return `<QueryResult server="${server}" packageName="${packageName}" modelPath="${modelPath}" sourceName="${sourceName}" queryName="${queryName}"/>`;
}