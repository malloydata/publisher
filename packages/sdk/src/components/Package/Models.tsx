import { Box, Divider, Typography } from "@mui/material";
import { QueryClient, useQuery } from "@tanstack/react-query";
import axios from "axios";
import { Configuration, ListModelsFilterEnum, ModelsApi } from "../../client";
import { StyledCard, StyledCardContent } from "../styles";
import { FileTreeView } from "./FileTreeView";

axios.defaults.baseURL = "http://localhost:4000";
const modelsApi = new ModelsApi(new Configuration());
const queryClient = new QueryClient();

const DEFAULT_EXPANDED_FOLDERS = ["notebooks/", "models/"];

interface ModelsProps {
   server?: string;
   projectName: string;
   packageName: string;
   versionId?: string;
   navigate: (to: string) => void;
   accessToken?: string;
   filter?: ListModelsFilterEnum;
}

export default function Models({
   server,
   projectName,
   packageName,
   versionId,
   navigate,
   accessToken,
   filter,
}: ModelsProps) {
   const { data, isSuccess, isError, error } = useQuery(
      {
         queryKey: [
            "models",
            server,
            projectName,
            packageName,
            versionId,
            filter,
         ],
         queryFn: () =>
            modelsApi.listModels(projectName, packageName, versionId, filter, {
               baseURL: server,
               withCredentials: !accessToken,
               headers: {
                  Authorization: accessToken && `Bearer ${accessToken}`,
               },
            }),
         retry: false,
      },
      queryClient,
   );

   return (
      <StyledCard variant="outlined" sx={{ padding: "10px", width: "100%" }}>
         <StyledCardContent>
            <Typography variant="overline" fontWeight="bold">
               {filter === ListModelsFilterEnum.Source ? "Models" : "Notebooks"}
            </Typography>
            <Divider />
            <Box
               sx={{
                  mt: "10px",
                  maxHeight: "200px",
                  overflowY: "auto",
               }}
            >
               {!isSuccess && !isError && (
                  <Typography variant="body2" sx={{ p: "20px", m: "auto" }}>
                     Fetching&nbsp;
                     {filter === ListModelsFilterEnum.Source
                        ? "Models"
                        : "Notebooks"}
                  </Typography>
               )}
               {isSuccess && data.data.length > 0 && (
                  <FileTreeView
                     items={data.data.sort((a, b) => {
                        return a.path.localeCompare(b.path);
                     })}
                     defaultExpandedItems={DEFAULT_EXPANDED_FOLDERS}
                     navigate={navigate}
                  />
               )}
               {isError && (
                  <Typography variant="body2" sx={{ p: "10px", m: "auto" }}>
                     {`${projectName} > ${packageName} > ${versionId} - ${error.message}`}
                  </Typography>
               )}
            </Box>
         </StyledCardContent>
      </StyledCard>
   );
}
