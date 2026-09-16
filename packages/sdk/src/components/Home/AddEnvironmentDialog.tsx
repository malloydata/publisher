// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import Button from "@mui/material/Button";
import TextField from "@mui/material/TextField";
import React, { useState } from "react";
import { useCrudMutation } from "../../hooks/useCrudMutation";
import { generateEnvironmentReadme } from "../../utils/parsing";
import { useServer } from "../ServerProvider";
import { AddButton } from "../buttons";
import Stack from "@mui/material/Stack";
import { AppDialog } from "../AppDialog";

export default function AddEnvironmentDialog() {
   const [open, setOpen] = useState(false);
   const { apiClients } = useServer();
   const handleClickOpen = () => {
      setOpen(true);
   };

   const handleClose = () => {
      setOpen(false);
   };
   const addEnvironment = useCrudMutation({
      mutationFn(variables: { name: string; description: string }) {
         return apiClients.environments.createEnvironment({
            name: variables.name,
            readme: generateEnvironmentReadme(
               {
                  name: variables.name,
                  readme: "",
               },
               variables.description,
            ),
         });
      },
      success: "Environment created",
      invalidates: [["environments"]],
      onSettled: handleClose,
   });

   const handleSubmit = async (event: React.FormEvent<HTMLFormElement>) => {
      event.preventDefault();

      const formData = new FormData(event.currentTarget);
      const name = formData.get("name")?.toString();
      const description = formData.get("description")?.toString();
      if (!name) {
         throw new Error("Name is required");
      }
      addEnvironment.mutate({ name, description });
   };

   return (
      <React.Fragment>
         <AddButton label="Environment" onClick={handleClickOpen} />
         <AppDialog
            open={open}
            onClose={handleClose}
            title="New environment"
            description="An environment holds packages and the connections they query through."
            actions={
               <>
                  <Button
                     disabled={addEnvironment.isPending}
                     onClick={handleClose}
                  >
                     Cancel
                  </Button>
                  <Button
                     type="submit"
                     form="environment-form"
                     variant="contained"
                     loading={addEnvironment.isPending}
                  >
                     Create environment
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
                     type="text"
                     fullWidth
                     size="small"
                     InputLabelProps={{ shrink: true }}
                  />
                  <TextField
                     id="description"
                     name="description"
                     label="Description"
                     placeholder="Explore semantic models, run queries, and build dashboards"
                     type="text"
                     fullWidth
                     size="small"
                     InputLabelProps={{ shrink: true }}
                  />
               </Stack>
            </form>
         </AppDialog>
         {addEnvironment.notice}
      </React.Fragment>
   );
}
