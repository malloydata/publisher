// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import React from "react";
import Button from "@mui/material/Button";
import TextField from "@mui/material/TextField";
import { useState } from "react";
import { Edit } from "@mui/icons-material";
import { MenuItem, ListItemIcon, ListItemText } from "@mui/material";
import { Environment } from "../../client";
import {
   generateEnvironmentReadme,
   getEnvironmentDescription,
} from "../../utils/parsing";
import { useCrudMutation } from "../../hooks/useCrudMutation";
import { useServer } from "../ServerProvider";
import Stack from "@mui/material/Stack";
import { AppDialog } from "../AppDialog";

interface EditEnvironmentModalProps {
   environment: Environment;
   onCloseDialog: () => void;
}

export default function EditEnvironmentDialog({
   environment,
   onCloseDialog,
}: EditEnvironmentModalProps) {
   const [open, setOpen] = useState(false);
   const { apiClients } = useServer();

   const handleClickOpen = () => {
      setOpen(true);
   };

   const handleClose = () => {
      setOpen(false);
      onCloseDialog();
   };

   const editEnvironment = useCrudMutation({
      mutationFn(variables: { description: string }) {
         return apiClients.environments.updateEnvironment(environment.name, {
            name: environment.name,
            readme: generateEnvironmentReadme(
               {
                  name: environment.name,
                  readme: environment.readme,
               },
               variables.description,
            ),
         });
      },
      success: "Environment updated",
      invalidates: [["environments"]],
      closeDialog: handleClose,
      resource: "environment",
      action: "update",
   });

   const handleSubmit = (event: React.FormEvent<HTMLFormElement>) => {
      event.preventDefault();
      const formData = new FormData(event.currentTarget);
      const description = formData.get("description")?.toString();
      editEnvironment.mutate({ description });
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
            title="Edit environment"
            description="What this environment is, for the people who open it."
            actions={
               <>
                  <Button
                     disabled={editEnvironment.isPending}
                     onClick={handleClose}
                  >
                     Cancel
                  </Button>
                  <Button
                     type="submit"
                     form="environment-form"
                     variant="contained"
                     loading={editEnvironment.isPending}
                  >
                     Save changes
                  </Button>
               </>
            }
         >
            <form onSubmit={handleSubmit} id="environment-form">
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
                     defaultValue={environment.name}
                     InputLabelProps={{ shrink: true }}
                  />
                  <TextField
                     id="description"
                     name="description"
                     label="Description"
                     type="text"
                     fullWidth
                     size="small"
                     defaultValue={getEnvironmentDescription(
                        environment.readme,
                     )}
                     InputLabelProps={{ shrink: true }}
                  />
               </Stack>
            </form>
         </AppDialog>
         {editEnvironment.notice}
      </React.Fragment>
   );
}
