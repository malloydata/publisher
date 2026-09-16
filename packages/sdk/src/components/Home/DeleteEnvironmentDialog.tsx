// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import React, { useState } from "react";
import Button from "@mui/material/Button";
import Typography from "@mui/material/Typography";
import { ListItemIcon, ListItemText, MenuItem, Snackbar } from "@mui/material";
import { Delete } from "@mui/icons-material";
import { Environment } from "../../client";
import { useMutationWithApiError } from "../../hooks/useQueryWithApiError";
import { useServer } from "../ServerProvider";
import { useQueryClient } from "@tanstack/react-query";
import { AppDialog } from "../AppDialog";

export default function DeleteEnvironmentDialog({
   environment,
   onCloseDialog,
}: {
   environment: Environment;
   onCloseDialog: () => void;
}) {
   const [open, setOpen] = useState(false);
   const { apiClients } = useServer();
   const queryClient = useQueryClient();
   const [notificationMessage, setNotificationMessage] = useState("");
   const handleClickOpen = () => {
      setOpen(true);
   };
   const handleClose = () => {
      setOpen(false);
      onCloseDialog();
   };

   const deleteEnvironment = useMutationWithApiError({
      mutationFn: () =>
         apiClients.environments.deleteEnvironment(environment.name),
      onSuccess() {
         handleClose();
         queryClient.invalidateQueries({ queryKey: ["environments"] });
         setNotificationMessage("Environment deleted successfully");
      },
      onError(error) {
         setNotificationMessage(
            error instanceof Error
               ? error.message
               : "An unknown error occurred",
         );
      },
   });

   return (
      <React.Fragment>
         <MenuItem onClick={handleClickOpen}>
            <ListItemIcon>
               <Delete fontSize="small" />
            </ListItemIcon>
            <ListItemText>Delete</ListItemText>
         </MenuItem>
         <AppDialog
            open={open}
            onClose={handleClose}
            title="Delete environment"
            actions={
               <>
                  <Button onClick={handleClose}>Cancel</Button>
                  <Button
                     variant="contained"
                     color="error"
                     autoFocus
                     onClick={() => deleteEnvironment.mutate()}
                     loading={deleteEnvironment.isPending}
                  >
                     Delete environment
                  </Button>
               </>
            }
         >
            <Typography variant="body2">
               Delete <strong>{environment.name}</strong>? Its packages stop
               being served and this cannot be undone.
            </Typography>
         </AppDialog>
         <Snackbar
            open={notificationMessage !== ""}
            autoHideDuration={6000}
            onClose={() => setNotificationMessage("")}
            message={notificationMessage}
         />
      </React.Fragment>
   );
}
