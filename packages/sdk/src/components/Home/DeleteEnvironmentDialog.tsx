// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import React, { useState } from "react";
import Button from "@mui/material/Button";
import Typography from "@mui/material/Typography";
import { ListItemIcon, ListItemText, MenuItem } from "@mui/material";
import { Delete } from "@mui/icons-material";
import { Environment } from "../../client";
import { useCrudMutation } from "../../hooks/useCrudMutation";
import { useServer } from "../ServerProvider";
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
   const handleClickOpen = () => {
      setOpen(true);
   };
   const handleClose = () => {
      setOpen(false);
      onCloseDialog();
   };

   const deleteEnvironment = useCrudMutation({
      mutationFn: () =>
         apiClients.environments.deleteEnvironment(environment.name),
      success: "Environment deleted",
      invalidates: [["environments"]],
      closeDialog: handleClose,
      resource: "environment",
      action: "delete",
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
         {deleteEnvironment.notice}
      </React.Fragment>
   );
}
