import React from "react";
import Button from "@mui/material/Button";
import TextField from "@mui/material/TextField";
import Dialog from "@mui/material/Dialog";
import DialogActions from "@mui/material/DialogActions";
import DialogContent from "@mui/material/DialogContent";
import DialogContentText from "@mui/material/DialogContentText";
import DialogTitle from "@mui/material/DialogTitle";
import { useState } from "react";
import { Add } from "@mui/icons-material";
import { Snackbar } from "@mui/material";
import { Package } from "../../client";
import { useQueryClient } from "@tanstack/react-query";
import { useMutationWithApiError } from "../../hooks/useQueryWithApiError";
import { useServer } from "../ServerProvider";
import { parseResourceUri } from "../../utils/formatting";

interface AddPackageDialogProps {
   resourceUri: string;
}

export default function AddPackageDialog({
   resourceUri,
}: AddPackageDialogProps) {
   const [open, setOpen] = useState(false);
   const { apiClients } = useServer();
   const queryClient = useQueryClient();
   const [notificationMessage, setNotificationMessage] = useState("");

   const handleClickOpen = () => {
      setOpen(true);
   };

   const handleClose = () => {
      setOpen(false);
   };

   const { environmentName } = parseResourceUri(resourceUri);
   const addPackage = useMutationWithApiError({
      async mutationFn(variables: Package) {
         return apiClients.packages.createPackage(environmentName, {
            name: variables.name,
            description: variables.description,
            location: variables.location,
         });
      },
      onSuccess() {
         handleClose();
         setNotificationMessage("Package created successfully");
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
      const name = formData.get("name")?.toString();
      const description = formData.get("description")?.toString();
      const location = formData.get("location")?.toString();
      addPackage.mutate({ name, description, location });
   };

   return (
      <React.Fragment>
         <Button
            onClick={handleClickOpen}
            variant="contained"
            color="primary"
            startIcon={<Add />}
            sx={{
               color: "white",
            }}
         >
            Add Package
         </Button>

         <Dialog open={open} onClose={handleClose}>
            <DialogTitle>Create New Package</DialogTitle>
            <DialogContent>
               <DialogContentText>
                  Create a new malloy package to start exploring your data.
                  <br />
                  <br />
                  The location can be a GitHub/S3/GCP URL containing a package
                  (Zipped or unzipped), or an absolute path to a directory that
                  the publisher server has access to.
                  <br />
                  <br />
                  Make sure to conform the{" "}
                  <a
                     href="https://github.com/malloydata/publisher/blob/main/README.md#architecture-overview"
                     target="_blank"
                     rel="noopener noreferrer"
                  >
                     Malloy Package Format
                  </a>
                  .
               </DialogContentText>
               <form onSubmit={handleSubmit} id="package-form">
                  <TextField
                     autoFocus
                     required
                     margin="dense"
                     id="name"
                     name="name"
                     label="Package Name"
                     type="text"
                     fullWidth
                     variant="standard"
                  />
                  <TextField
                     id="description"
                     name="description"
                     label="Description"
                     multiline
                     fullWidth
                     rows={4}
                     variant="standard"
                  />
                  <TextField
                     id="location"
                     name="location"
                     label="Location"
                     type="text"
                     placeholder="E.g. s3://my-bucket/my-package.zip"
                     fullWidth
                     variant="standard"
                  />
               </form>
            </DialogContent>
            <DialogActions>
               <Button disabled={addPackage.isPending} onClick={handleClose}>
                  Cancel
               </Button>
               <Button
                  type="submit"
                  form="package-form"
                  loading={addPackage.isPending}
               >
                  Save Changes
               </Button>
            </DialogActions>
         </Dialog>
         <Snackbar
            open={notificationMessage !== ""}
            autoHideDuration={6000}
            onClose={() => setNotificationMessage("")}
            message={notificationMessage}
         />
      </React.Fragment>
   );
}
