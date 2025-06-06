import ContentCopyIcon from "@mui/icons-material/ContentCopy";
import LinkOutlinedIcon from "@mui/icons-material/LinkOutlined";
import {
   CardActions,
   Collapse,
   Divider,
   IconButton,
   Tooltip,
   Typography,
} from "@mui/material";
import Stack from "@mui/material/Stack";
import { QueryClient, useQuery } from "@tanstack/react-query";
import React, { useEffect } from "react";
import { Configuration, NotebooksApi } from "../../client";
import { highlight } from "../highlighter";
import { usePublisherPackage } from "../Package";
import { StyledCard, StyledCardContent, StyledCardMedia } from "../styles";
import { NotebookCell } from "./NotebookCell";

const notebooksApi = new NotebooksApi(new Configuration());
const queryClient = new QueryClient();

interface NotebookProps {
   notebookPath: string;
   expandCodeCells?: boolean;
   hideCodeCellIcons?: boolean;
   expandEmbeddings?: boolean;
   hideEmbeddingIcons?: boolean;
}
// Requires PublisherPackageProvider
export default function Notebook({
   notebookPath,
   expandCodeCells,
   hideCodeCellIcons,
   expandEmbeddings,
   hideEmbeddingIcons,
}: NotebookProps) {
   const [embeddingExpanded, setEmbeddingExpanded] =
      React.useState<boolean>(false);
   const [highlightedEmbedCode, setHighlightedEmbedCode] =
      React.useState<string>();
   const { server, projectName, packageName, accessToken, versionId } =
      usePublisherPackage();
   const notebookCodeSnippet = getNotebookCodeSnippet(
      server,
      packageName,
      notebookPath,
      true,
   );

   useEffect(() => {
      highlight(notebookCodeSnippet, "typescript").then((code) => {
         setHighlightedEmbedCode(code);
      });
   }, [embeddingExpanded, notebookCodeSnippet]);

   const {
      data: notebook,
      isSuccess,
      isError,
      error,
   } = useQuery(
      {
         queryKey: [
            "notebook",
            server,
            projectName,
            packageName,
            notebookPath,
            versionId,
         ],
         queryFn: () =>
            notebooksApi.getNotebook(
               projectName,
               packageName,
               notebookPath,
               versionId,
               {
                  baseURL: server,
                  withCredentials: !accessToken,
                  headers: {
                     Authorization: accessToken && `Bearer ${accessToken}`,
                  },
               },
            ),
         retry: false,
      },
      queryClient,
   );

   return (
      <StyledCard variant="outlined">
         <StyledCardContent>
            <Stack
               sx={{
                  flexDirection: "row",
                  justifyContent: "space-between",
               }}
            >
               <Typography variant="overline" fontWeight="bold">
                  Notebook
               </Typography>
               <CardActions
                  sx={{
                     padding: "0px 10px 0px 10px",
                     mb: "auto",
                     mt: "auto",
                  }}
               >
                  <Tooltip
                     title={
                        embeddingExpanded ? "Hide Embedding" : "View Embedding"
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
               </CardActions>
            </Stack>
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
                  <Tooltip title="Copy Embeddable Code">
                     <IconButton
                        sx={{ width: "24px", height: "24px" }}
                        onClick={() => {
                           navigator.clipboard.writeText(notebookCodeSnippet);
                        }}
                     >
                        <ContentCopyIcon />
                     </IconButton>
                  </Tooltip>
               </Stack>
            </Collapse>
            <Divider />
         </StyledCardContent>
         <StyledCardMedia>
            <Stack spacing={1} component="section">
               {!isSuccess && !isError && (
                  <Typography variant="body2" sx={{ p: "10px", m: "auto" }}>
                     Fetching Notebook...
                  </Typography>
               )}
               {isSuccess &&
                  notebook.data.notebookCells?.map((cell, index) => (
                     <NotebookCell
                        cell={cell}
                        notebookPath={notebookPath}
                        queryResultCodeSnippet={getQueryResultCodeSnippet(
                           server,
                           projectName,
                           packageName,
                           notebookPath,
                           cell.text,
                        )}
                        expandCodeCell={expandCodeCells}
                        hideCodeCellIcon={hideCodeCellIcons}
                        expandEmbedding={expandEmbeddings}
                        hideEmbeddingIcon={hideEmbeddingIcons}
                        key={index}
                     />
                  ))}
               {isError && (
                  <Typography variant="body2" sx={{ p: "10px", m: "auto" }}>
                     {(error.message.includes("404") &&
                        `${notebookPath} does not exist`) ||
                        `${projectName} > ${packageName} > ${notebookPath} > ${versionId} - ${error.message}`}
                  </Typography>
               )}
            </Stack>
         </StyledCardMedia>
      </StyledCard>
   );
}

function getQueryResultCodeSnippet(
   server: string,
   projectName: string,
   packageName: string,
   modelPath: string,
   query: string,
): string {
   return `<QueryResult
   server="${server}"
   accessToken={accessToken}
   projectName="${projectName}"
   packageName="${packageName}"
   modelPath="${modelPath}"
   query="
      ${query}
   "
/>`;
}

function getNotebookCodeSnippet(
   server: string,
   packageName: string,
   notebookPath: string,
   expandedCodeCells: boolean,
): string {
   return `<Notebook
   server="${server}"
   packageName="${packageName}"
   notebookPath="${notebookPath}"
   versionId={versionId}
   accessToken={accessToken}
   expandCodeCells={${expandedCodeCells}}
/>`;
}
