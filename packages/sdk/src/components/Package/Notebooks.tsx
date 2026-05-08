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

const DEFAULT_EXPANDED_FOLDERS = ["notebooks/"];

interface NotebooksProps {
   onClickNotebookFile: (to: string, event?: React.MouseEvent) => void;
   resourceUri: string;
}

export default function Notebooks({
   onClickNotebookFile,
   resourceUri,
}: NotebooksProps) {
   const { apiClients } = useServer();
   const {
      environmentName: environmentName,
      packageName: packageName,
      versionId: versionId,
   } = parseResourceUri(resourceUri);

   const { data, isError, error, isSuccess } = useQueryWithApiError({
      queryKey: ["notebooks", environmentName, packageName, versionId],
      queryFn: () =>
         apiClients.notebooks.listNotebooks(
            environmentName,
            packageName,
            versionId,
         ),
   });

   return (
      <PackageCard>
         <PackageCardContent>
            <PackageSectionTitle>Notebooks</PackageSectionTitle>
            <Box
               sx={{
                  maxHeight: "200px",
                  overflowY: "auto",
               }}
            >
               {!isSuccess && !isError && (
                  <Loading text="Fetching Notebooks..." />
               )}
               {isError && (
                  <ApiErrorDisplay
                     error={error}
                     context={`${environmentName} > ${packageName} > Notebooks`}
                  />
               )}
               {isSuccess && data.data.length > 0 && (
                  <FileTreeView
                     items={data.data.sort((a, b) => {
                        return a.path.localeCompare(b.path);
                     })}
                     defaultExpandedItems={DEFAULT_EXPANDED_FOLDERS}
                     onClickTreeNode={onClickNotebookFile}
                  />
               )}
               {isSuccess && data.data.length === 0 && (
                  <Typography variant="body2">No notebooks found</Typography>
               )}
            </Box>
         </PackageCardContent>
      </PackageCard>
   );
}
