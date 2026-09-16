// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import React, { useState } from "react";
import Button from "@mui/material/Button";
import Typography from "@mui/material/Typography";
import { ListItemIcon, ListItemText, MenuItem } from "@mui/material";
import { Delete } from "@mui/icons-material";
import { useCrudMutation } from "../../hooks/useCrudMutation";
import { useServer } from "../ServerProvider";
import { parseResourceUri } from "../../utils/formatting";
import { AppDialog } from "../AppDialog";

export default function DeletePackageDialog({
   resourceUri,
   onCloseDialog,
}: {
   resourceUri: string;
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
   const { environmentName, packageName } = parseResourceUri(resourceUri);

   const deletePackage = useCrudMutation({
      mutationFn: () =>
         apiClients.packages.deletePackage(environmentName, packageName),
      success: "Package deleted",
      invalidates: [["packages", environmentName]],
      onSettled: handleClose,
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
            title="Delete package"
            actions={
               <>
                  <Button onClick={handleClose}>Cancel</Button>
                  <Button
                     variant="contained"
                     color="error"
                     autoFocus
                     onClick={() => deletePackage.mutate()}
                     loading={deletePackage.isPending}
                  >
                     Delete package
                  </Button>
               </>
            }
         >
            <Typography variant="body2">
               Delete <strong>{packageName}</strong>? The package stops being
               served and this cannot be undone.
            </Typography>
         </AppDialog>
         {deletePackage.notice}
      </React.Fragment>
   );
}
