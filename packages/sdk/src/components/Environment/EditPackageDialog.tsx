// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { Edit } from "@mui/icons-material";
import { ListItemIcon, ListItemText, MenuItem, Snackbar } from "@mui/material";
import Button from "@mui/material/Button";
import TextField from "@mui/material/TextField";
import { useQueryClient } from "@tanstack/react-query";
import React, { useState } from "react";
import { Package } from "../../client";
import { useMutationWithApiError } from "../../hooks/useQueryWithApiError";
import { parseResourceUri } from "../../utils/formatting";
import { useServer } from "../ServerProvider";
import { Stack } from "@mui/material";
import { AppDialog } from "../AppDialog";

interface EditPackageDialogProps {
   package: Package;
   resourceUri: string;
   onCloseDialog: () => void;
}

export default function EditPackageDialog({
   package: _package,
   resourceUri,
   onCloseDialog,
}: EditPackageDialogProps) {
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

   const { packageName, environmentName } = parseResourceUri(resourceUri);
   const editPackage = useMutationWithApiError({
      async mutationFn(variables: { description: string }) {
         return apiClients.packages.updatePackage(
            environmentName,
            packageName,
            {
               name: packageName,
               description: variables.description,
            },
         );
      },
      onSuccess() {
         handleClose();
         setNotificationMessage("Package updated successfully");
         queryClient.invalidateQueries({
            queryKey: ["packages", environmentName],
         });
      },
      onError(error) {
         setNotificationMessage(
            error instanceof Error
               ? error.message
               : "An unknown error occurred",
         );
      },
   });

   const handleSubmit = (event: React.FormEvent<HTMLFormElement>) => {
      event.preventDefault();
      const formData = new FormData(event.currentTarget);
      const description = formData.get("description")?.toString();
      editPackage.mutate({ description });
   };

   return (
      <React.Fragment>
         <MenuItem onClick={handleClickOpen}>
            <ListItemIcon>
               <Edit fontSize="small" />
            </ListItemIcon>
            <ListItemText>Edit</ListItemText>
         </MenuItem>

         <AppDialog
            open={open}
            onClose={handleClose}
            title="Edit package"
            description={`What ${_package.name} is, for the people who open it.`}
            actions={
               <>
                  <Button
                     disabled={editPackage.isPending}
                     onClick={handleClose}
                  >
                     Cancel
                  </Button>
                  <Button
                     type="submit"
                     form="package-form"
                     variant="contained"
                     loading={editPackage.isPending}
                  >
                     Save changes
                  </Button>
               </>
            }
         >
            <form onSubmit={handleSubmit} id="package-form">
               <Stack sx={{ gap: 2 }}>
                  <TextField
                     autoFocus
                     required
                     id="name"
                     name="name"
                     label="Name"
                     disabled
                     type="text"
                     fullWidth
                     size="small"
                     defaultValue={_package.name}
                     InputLabelProps={{ shrink: true }}
                  />
                  <TextField
                     id="description"
                     name="description"
                     label="Description"
                     multiline
                     fullWidth
                     rows={4}
                     size="small"
                     defaultValue={_package.description}
                     InputLabelProps={{ shrink: true }}
                  />
               </Stack>
            </form>
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
