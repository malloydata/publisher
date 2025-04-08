import React, { useEffect } from "react";
import {
   Box,
   Typography,
   Stack,
   CardActions,
   Tooltip,
   IconButton,
   Collapse,
   Divider,
} from "@mui/material";
import { QueryClient, useQuery } from "@tanstack/react-query";
import LinkOutlinedIcon from "@mui/icons-material/LinkOutlined";
import ContentCopyIcon from "@mui/icons-material/ContentCopy";
import axios from "axios";
import { Configuration, ModelsApi } from "../../client";
import { ModelCell } from "./ModelCell";
import { StyledCard, StyledCardContent, StyledCardMedia } from "../styles";
import { highlight } from "../highlighter";

axios.defaults.baseURL = "http://localhost:4000";
const modelsApi = new ModelsApi(new Configuration());
const queryClient = new QueryClient();

interface ModelProps {
   server?: string;
   projectName: string;
   packageName: string;
   modelPath: string;
   versionId?: string;
   expandResults?: boolean;
   hideResultIcons?: boolean;
   expandEmbeddings?: boolean;
   hideEmbeddingIcons?: boolean;
   accessToken?: string;
}

export default function Model({
   server,
   projectName,
   packageName,
   modelPath,
   versionId,
   expandResults,
   hideResultIcons,
   expandEmbeddings,
   hideEmbeddingIcons,
   accessToken,
}: ModelProps) {
   const [embeddingExpanded, setEmbeddingExpanded] =
      React.useState<boolean>(false);
   const [highlightedEmbedCode, setHighlightedEmbedCode] =
      React.useState<string>();

   const modelCodeSnippet = getModelCodeSnippet(server, packageName, modelPath);

   useEffect(() => {
      highlight(modelCodeSnippet, "typescript").then((code) => {
         setHighlightedEmbedCode(code);
      });
   }, [embeddingExpanded, modelCodeSnippet]);

   const { data, isError, isLoading, error } = useQuery(
      {
         queryKey: [
            "package",
            server,
            projectName,
            packageName,
            modelPath,
            versionId,
         ],
         queryFn: () =>
            modelsApi.getModel(projectName, packageName, modelPath, versionId, {
               baseURL: server,
               withCredentials: !accessToken,
               headers: {
                  Authorization: accessToken && `Bearer ${accessToken}`,
               },
            }),
      },
      queryClient,
   );

   if (isLoading) {
      return (
         <Typography sx={{ p: "20px", m: "auto" }}>
            Fetching Model...
         </Typography>
      );
   }

   if (isError) {
      return (
         <Typography variant="body2" sx={{ p: "10px", m: "auto" }}>
            {`${packageName} > ${modelPath} > ${versionId} - ${error.message}`}
         </Typography>
      );
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
               <Typography variant="overline" fontWeight="bold">
                  Model
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
                           navigator.clipboard.writeText(modelCodeSnippet);
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
            <Stack spacing={2} component="section">
               {data.data.sources?.map((source) => (
                  <StyledCard
                     key={source.name}
                     variant="outlined"
                     sx={{ padding: "0px 10px 0px 10px" }}
                  >
                     <StyledCardContent sx={{ p: "10px" }}>
                        <Typography variant="subtitle1">
                           {"Source > " + source.name}
                        </Typography>
                     </StyledCardContent>
                     <Stack spacing={1} component="section">
                        {source.views &&
                           source.views.length > 0 &&
                           source.views.map((view) => (
                              <ModelCell
                                 key={`${source.name}-${view.name}`}
                                 server={server}
                                 projectName={projectName}
                                 packageName={packageName}
                                 modelPath={modelPath}
                                 versionId={versionId}
                                 sourceName={source.name}
                                 queryName={view.name}
                                 expandResult={expandResults}
                                 hideResultIcon={hideResultIcons}
                                 expandEmbedding={expandEmbeddings}
                                 hideEmbeddingIcon={hideEmbeddingIcons}
                                 accessToken={accessToken}
                                 annotations={view.annotations}
                              />
                           ))}
                     </Stack>
                     <Box height="10px" />
                  </StyledCard>
               ))}
               {data.data.queries?.length > 0 && (
                  <StyledCard
                     variant="outlined"
                     sx={{ padding: "0px 10px 0px 10px" }}
                  >
                     <StyledCardContent sx={{ p: "10px" }}>
                        <Typography variant="subtitle1">
                           Named Queries
                        </Typography>
                     </StyledCardContent>
                     <Stack spacing={1} component="section">
                        {data.data.queries.map((query) => (
                           <ModelCell
                              key={query.name}
                              server={server}
                              projectName={projectName}
                              packageName={packageName}
                              modelPath={modelPath}
                              versionId={versionId}
                              queryName={query.name}
                              expandResult={expandResults}
                              hideResultIcon={hideResultIcons}
                              expandEmbedding={expandEmbeddings}
                              hideEmbeddingIcon={hideEmbeddingIcons}
                              accessToken={accessToken}
                              noView={true}
                              annotations={query.annotations}
                           />
                        ))}
                     </Stack>
                     <Box height="10px" />
                  </StyledCard>
               )}
               <Box height="5px" />
            </Stack>
         </StyledCardMedia>
      </StyledCard>
   );
}

function getModelCodeSnippet(
   server: string,
   packageName: string,
   modelPath: string,
): string {
   return `<Model
   server="${server}"
   packageName="${packageName}"
   modelPath="${modelPath}"
   accessToken={accessToken}
/>`;
}
