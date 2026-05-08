import { Box, Typography } from "@mui/material";
import { useQueryWithApiError } from "../../hooks/useQueryWithApiError";
import { ApiErrorDisplay } from "../ApiErrorDisplay";
import { Loading } from "../Loading";
import {
   PackageCard,
   PackageCardContent,
   PackageSectionTitle,
} from "../styles";
import { FileTreeView } from "./FileTreeView";
import { parseResourceUri } from "../../utils/formatting";
import { useServer } from "../ServerProvider";

const DEFAULT_EXPANDED_FOLDERS = ["notebooks/", "models/"];

interface ModelsProps {
   onClickModelFile: (to: string, event?: React.MouseEvent) => void;
   resourceUri: string;
}

export default function Models({ onClickModelFile, resourceUri }: ModelsProps) {
   const {
      environmentName: environmentName,
      packageName: packageName,
      versionId: versionId,
   } = parseResourceUri(resourceUri);
   const { apiClients } = useServer();

   const { data, isError, error, isSuccess } = useQueryWithApiError({
      queryKey: ["models", environmentName, packageName, versionId],
      queryFn: () =>
         apiClients.models.listModels(environmentName, packageName, versionId),
   });

   return (
      <PackageCard>
         <PackageCardContent>
            <PackageSectionTitle>Semantic Models</PackageSectionTitle>
            <Box
               sx={{
                  maxHeight: "200px",
                  overflowY: "auto",
               }}
            >
               {!isSuccess && !isError && <Loading text="Fetching Models..." />}
               {isError && (
                  <ApiErrorDisplay
                     error={error}
                     context={`${environmentName} > ${packageName} > Models`}
                  />
               )}
               {isSuccess && data.data.length > 0 && (
                  <FileTreeView
                     items={data.data.sort((a, b) => {
                        return a.path.localeCompare(b.path);
                     })}
                     onClickTreeNode={onClickModelFile}
                     defaultExpandedItems={DEFAULT_EXPANDED_FOLDERS}
                  />
               )}
               {isSuccess && data.data.length === 0 && (
                  <Typography variant="body2">No models found</Typography>
               )}
            </Box>
         </PackageCardContent>
      </PackageCard>
   );
}
