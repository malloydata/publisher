// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import {
   Box,
   Divider,
   List,
   ListItem,
   ListItemText,
   Typography,
} from "@mui/material";
import React from "react";
import { useWorkbookStorage } from "./WorkbookStorageProvider";
import { WorkbookLocator } from "./WorkbookStorage";

interface WorkbookListProps {
   onWorkbookClick: (
      workbook: WorkbookLocator,
      event: React.MouseEvent,
   ) => void;
}

export function WorkbookList({ onWorkbookClick }: WorkbookListProps) {
   const { workbookStorage } = useWorkbookStorage();
   const [workbooks, setWorkbooks] = React.useState<WorkbookLocator[]>([]);
   const [lastError, setLastError] = React.useState<string | undefined>(
      undefined,
   );

   React.useEffect(() => {
      if (workbookStorage) {
         workbookStorage.listWorkspaces(false).then((workspaces) => {
            const allWorkbooks: WorkbookLocator[] = [];
            Promise.all(
               workspaces.map(async (workspace) => {
                  await workbookStorage
                     .listWorkbooks(workspace)
                     .then((newWorkbooks) => {
                        allWorkbooks.push(...newWorkbooks);
                     })
                     .catch((error) => {
                        setLastError(
                           `Error listing workbooks: ${error.message}`,
                        );
                     });
               }),
            ).then(() => {
               setWorkbooks(allWorkbooks);
               setLastError(undefined);
            });
         });
      }
   }, [workbookStorage]);

   return (
      <>
         {lastError && (
            <Box sx={{ mb: 2 }}>
               <Typography color="error" variant="body2">
                  {lastError}
               </Typography>
            </Box>
         )}
         <Divider />
         <Box
            sx={{
               maxHeight: "300px",
               overflow: "auto",
               "&::-webkit-scrollbar": {
                  width: "8px",
               },
               "&::-webkit-scrollbar-track": {
                  background: "transparent",
               },
               "&::-webkit-scrollbar-thumb": {
                  background: "rgba(0,0,0,0.2)",
                  borderRadius: "4px",
               },
            }}
         >
            <List dense>
               {workbooks.length === 0 && (
                  <ListItem>
                     <ListItemText
                        primary="No workbooks found."
                        sx={{ textAlign: "center" }}
                     />
                  </ListItem>
               )}
               {workbooks.map((workbook) => (
                  <ListItem
                     key={workbook.path}
                     onClick={(event: React.MouseEvent) =>
                        onWorkbookClick(workbook, event)
                     }
                     sx={{
                        cursor: "pointer",
                        "&:hover": {
                           backgroundColor: "action.hover",
                        },
                     }}
                  >
                     <ListItemText primary={workbook.path} />
                  </ListItem>
               ))}
            </List>
         </Box>
      </>
   );
}
