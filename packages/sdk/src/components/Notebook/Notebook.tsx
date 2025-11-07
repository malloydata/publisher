import "@malloydata/malloy-explorer/styles.css";
import { Stack, Typography } from "@mui/material";
import { CompiledNotebook } from "../../client";
import { useQueryWithApiError } from "../../hooks/useQueryWithApiError";
import { ApiErrorDisplay } from "../ApiErrorDisplay";

import { parseResourceUri } from "../../utils/formatting";
import { Loading } from "../Loading";
import { useServer } from "../ServerProvider";
import { CleanNotebookContainer, CleanNotebookSection } from "../styles";
import { NotebookCell } from "./NotebookCell";

interface NotebookProps {
   resourceUri: string;
   maxResultSize?: number;
}

// Requires PackageProvider
export default function Notebook({
   resourceUri,
   maxResultSize = 0,
}: NotebookProps) {
   const { apiClients } = useServer();
   const {
      projectName,
      packageName,
      versionId,
      modelPath: notebookPath,
   } = parseResourceUri(resourceUri);
   const {
      data: notebook,
      isSuccess,
      isError,
      error,
   } = useQueryWithApiError<CompiledNotebook>({
      queryKey: [resourceUri],
      queryFn: async () => {
         const response = await apiClients.notebooks.getNotebook(
            projectName,
            packageName,
            notebookPath,
            versionId,
         );
         return response.data;
      },
   });

   return (
      <CleanNotebookContainer>
         <CleanNotebookSection>
            <Stack spacing={3} component="section">
               {!isSuccess && !isError && (
                  <Loading text="Fetching Notebook..." />
               )}
               {isSuccess &&
                  notebook.notebookCells?.map((cell, index) => (
                     <NotebookCell
                        cell={cell}
                        key={index}
                        index={index}
                        resourceUri={resourceUri}
                        maxResultSize={maxResultSize}
                     />
                  ))}
               {isError && error.status === 404 && (
                  <Typography variant="body2" sx={{ color: "#666666" }}>
                     <code>{`${projectName} > ${packageName} > ${notebookPath}`}</code>{" "}
                     not found.
                  </Typography>
               )}

               {isError && error.status !== 404 && (
                  <ApiErrorDisplay
                     error={error}
                     context={`${projectName} > ${packageName} > ${notebookPath}`}
                  />
               )}
            </Stack>
         </CleanNotebookSection>
      </CleanNotebookContainer>
   );
}
