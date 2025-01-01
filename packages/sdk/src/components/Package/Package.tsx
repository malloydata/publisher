import React, { useEffect } from "react";
import {
   Divider,
   Grid2,
   Typography,
   Stack,
   CardActions,
   IconButton,
   Tooltip,
   Collapse,
} from "@mui/material";
import LinkOutlinedIcon from "@mui/icons-material/LinkOutlined";
import ContentCopyIcon from "@mui/icons-material/ContentCopy";
import { Notebook } from "../Notebook";
import { StyledCard, StyledCardContent, StyledCardMedia } from "../styles";
import Config from "./Config";
import Models from "./Models";
import Databases from "./Databases";
import Schedules from "./Schedules";
import { highlight } from "../highlighter";

const README_NOTEBOOK = "README.malloynb";

interface PackageProps {
   server?: string;
   packageName: string;
   versionId?: string;
   navigate?: (to: string) => void;
   accessToken?: string;
}

export default function Package({
   server,
   packageName,
   versionId,
   navigate,
   accessToken,
}: PackageProps) {
   const [embeddingExpanded, setEmbeddingExpanded] =
      React.useState<boolean>(false);
   const [highlightedEmbedCode, setHighlightedEmbedCode] =
      React.useState<string>();

   const packageCodeSnippet = getNotebookCodeSnippet(
      server,
      packageName,
      README_NOTEBOOK,
      true,
   );

   useEffect(() => {
      highlight(packageCodeSnippet, "typescript").then((code) => {
         setHighlightedEmbedCode(code);
      });
   }, [embeddingExpanded]);

   if (!navigate) {
      navigate = (to: string) => {
         window.location.href = to;
      };
   }

   return (
      <Grid2
         container
         spacing={2}
         columns={12}
         sx={{ mb: (theme) => theme.spacing(2) }}
      >
         <Grid2 size={{ md: 12, lg: 4 }}>
            <Config
               server={server}
               packageName={packageName}
               versionId={versionId}
               accessToken={accessToken}
            />
         </Grid2>
         <Grid2 size={{ md: 12, lg: 4 }}>
            <Models
               server={server}
               packageName={packageName}
               versionId={versionId}
               navigate={navigate}
               accessToken={accessToken}
            />
         </Grid2>
         <Grid2 size={{ md: 12, lg: 4 }}>
            <Databases
               server={server}
               packageName={packageName}
               versionId={versionId}
               accessToken={accessToken}
            />
         </Grid2>
         <Grid2 size={{ md: 12 }}>
            <Schedules
               server={server}
               packageName={packageName}
               versionId={versionId}
               accessToken={accessToken}
            />
         </Grid2>
         <Grid2 size={{ md: 12 }}>
            <StyledCard variant="outlined">
               <StyledCardContent>
                  <Stack
                     sx={{
                        flexDirection: "row",
                        justifyContent: "space-between",
                     }}
                  >
                     <Typography variant="overline" fontWeight="bold">
                        Readme
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
                                 navigator.clipboard.writeText(
                                    packageCodeSnippet,
                                 );
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
                  <Notebook
                     server={server}
                     packageName={packageName}
                     notebookPath={README_NOTEBOOK}
                     versionId={versionId}
                     expandCodeCells={true}
                     accessToken={accessToken}
                  />
               </StyledCardMedia>
            </StyledCard>
         </Grid2>
      </Grid2>
   );
}

function getNotebookCodeSnippet(
   server: string,
   packageName: string,
   notebookPath: string,
   expandedCodeCells: boolean,
): string {
   return `
<Notebook
   server="${server}"
   packageName="${packageName}"
   notebookPath="${notebookPath}"
   versionId={versionId}
   accessToken={accessToken}
   expandCodeCells={${expandedCodeCells}}
/>`;
}
