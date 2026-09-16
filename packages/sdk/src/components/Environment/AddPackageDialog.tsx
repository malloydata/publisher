// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { Box, Button, Stack, TextField } from "@mui/material";
import React, { useState } from "react";
import { Package } from "../../client";
import { useCrudMutation } from "../../hooks/useCrudMutation";
import { parseResourceUri } from "../../utils/formatting";
import { DOC_LINKS } from "../../constants/docLinks";
import { useServer } from "../ServerProvider";
import { AppDialog } from "../AppDialog";
import { AddButton } from "../buttons";

interface AddPackageDialogProps {
   resourceUri: string;
}

export default function AddPackageDialog({
   resourceUri,
}: AddPackageDialogProps) {
   const [open, setOpen] = useState(false);
   const { apiClients } = useServer();

   const { environmentName } = parseResourceUri(resourceUri);

   const addPackage = useCrudMutation({
      mutationFn(variables: Package) {
         return apiClients.packages.createPackage(environmentName, {
            name: variables.name,
            description: variables.description,
            location: variables.location,
         });
      },
      success: "Package created",
      invalidates: [["packages", environmentName]],
      onSettled: () => setOpen(false),
   });

   const handleSubmit = (event: React.FormEvent<HTMLFormElement>) => {
      event.preventDefault();
      const formData = new FormData(event.currentTarget);
      const name = formData.get("name")?.toString();
      const description = formData.get("description")?.toString();
      const location = formData.get("location")?.toString();
      addPackage.mutate({ name, description, location });
   };

   return (
      <>
         <AddButton label="Package" onClick={() => setOpen(true)} />

         <AppDialog
            open={open}
            onClose={() => setOpen(false)}
            title="New package"
            description={
               <>
                  A package is a directory of Malloy models and their data. Give
                  its location as a GitHub, S3 or GCS URL, or an absolute path
                  the server can read; see the{" "}
                  <Box
                     component="a"
                     href={DOC_LINKS.publishing}
                     target="_blank"
                     rel="noopener noreferrer"
                     sx={{ color: "text.primary" }}
                  >
                     package format
                  </Box>
                  .
               </>
            }
            actions={
               <>
                  <Button
                     disabled={addPackage.isPending}
                     onClick={() => setOpen(false)}
                  >
                     Cancel
                  </Button>
                  <Button
                     type="submit"
                     form="package-form"
                     variant="contained"
                     loading={addPackage.isPending}
                  >
                     Create package
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
                     type="text"
                     fullWidth
                     size="small"
                     InputLabelProps={{ shrink: true }}
                  />
                  <TextField
                     id="description"
                     name="description"
                     label="Description"
                     multiline
                     rows={3}
                     fullWidth
                     size="small"
                     InputLabelProps={{ shrink: true }}
                  />
                  <TextField
                     id="location"
                     name="location"
                     label="Location"
                     type="text"
                     placeholder="e.g. s3://my-bucket/my-package.zip"
                     fullWidth
                     size="small"
                     InputLabelProps={{ shrink: true }}
                  />
               </Stack>
            </form>
         </AppDialog>
         {addPackage.notice}
      </>
   );
}
