import React, { useCallback, useRef, useState } from "react";
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
   onClickModelFile: (path: string, event?: React.MouseEvent) => void;
   resourceUri: string;
}

export default function Models({ onClickModelFile, resourceUri }: ModelsProps) {
   const { projectName, packageName, versionId } =
      parseResourceUri(resourceUri);
   const { apiClients } = useServer();

   const { data, isError, error, isSuccess } = useQueryWithApiError({
      queryKey: ["models", projectName, packageName, versionId],
      queryFn: () =>
         apiClients.models.listModels(projectName, packageName, versionId),
   });

   const [isLoadingClick, setIsLoadingClick] = useState(false);
   const clickedSet = useRef<Set<string>>(new Set());
   const normalizePath = (path: string) =>
      path?.replace(/\/+$/, "").split("/").pop() || path;

   const handleClick = useCallback(
      async (path: string, event?: React.MouseEvent) => {
         const key = normalizePath(path);
         if (clickedSet.current.has(key) || isLoadingClick) return;

         setIsLoadingClick(true);
         try {
            clickedSet.current.add(key);
            await onClickModelFile(path, event);
         } catch (err) {
            clickedSet.current.delete(key);
            console.error("Model click error:", err);
         } finally {
            setIsLoadingClick(false);
         }
      },
      [isLoadingClick, onClickModelFile],
   );

   return (
      <PackageCard>
         <PackageCardContent>
            <PackageSectionTitle>Semantic Models</PackageSectionTitle>
            <Box sx={{ maxHeight: 200, overflowY: "auto" }}>
               {!isSuccess && !isError && <Loading text="Fetching Models..." />}
               {isError && (
                  <ApiErrorDisplay
                     error={error}
                     context={`${projectName} > ${packageName} > Models`}
                  />
               )}
               {isSuccess && data.data.length > 0 && (
                  <FileTreeView
                     items={[...data.data].sort((a, b) =>
                        a.path.localeCompare(b.path),
                     )}
                     onClickTreeNode={handleClick}
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
