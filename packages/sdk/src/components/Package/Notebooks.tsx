import { Box, Typography } from "@mui/material";
import React, { useCallback, useRef } from "react";
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
   const { projectName, packageName, versionId } =
      parseResourceUri(resourceUri);

   const { data, isError, error, isSuccess } = useQueryWithApiError({
      queryKey: ["notebooks", projectName, packageName, versionId],
      queryFn: () =>
         apiClients.notebooks.listNotebooks(
            projectName,
            packageName,
            versionId,
         ),
   });
   const clickedSet = useRef<Set<string>>(new Set());
   const normalizePath = (path: string) =>
      path?.replace(/\/+$/, "").split("/").pop() || path;
   const handleClickNotebookFile = useCallback(
      async (to: string, event?: React.MouseEvent) => {
         const key = normalizePath(to);
         if (clickedSet.current.has(key)) return;
         clickedSet.current.add(key);
         try {
            await onClickNotebookFile(to, event);
         } catch (err) {
            console.error("Notebook click error:", err);
            clickedSet.current.delete(key);
         }
      },
      [onClickNotebookFile],
   );

   return (
      <PackageCard>
         <PackageCardContent>
            <PackageSectionTitle>Notebooks</PackageSectionTitle>
            <Box sx={{ maxHeight: 200, overflowY: "auto" }}>
               {!isSuccess && !isError && (
                  <Loading text="Fetching Notebooks..." />
               )}
               {isError && (
                  <ApiErrorDisplay
                     error={error}
                     context={`${projectName} > ${packageName} > Notebooks`}
                  />
               )}
               {isSuccess && data.data.length > 0 && (
                  <FileTreeView
                     items={data.data.sort((a, b) =>
                        a.path.localeCompare(b.path),
                     )}
                     defaultExpandedItems={DEFAULT_EXPANDED_FOLDERS}
                     onClickTreeNode={handleClickNotebookFile}
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
