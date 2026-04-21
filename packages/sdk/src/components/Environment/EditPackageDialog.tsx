import { Edit } from "@mui/icons-material";
import { ListItemIcon, ListItemText, MenuItem, Snackbar } from "@mui/material";
import Button from "@mui/material/Button";
import Dialog from "@mui/material/Dialog";
import DialogActions from "@mui/material/DialogActions";
import DialogContent from "@mui/material/DialogContent";
import DialogContentText from "@mui/material/DialogContentText";
import DialogTitle from "@mui/material/DialogTitle";
import TextField from "@mui/material/TextField";
import { useQueryClient } from "@tanstack/react-query";
import React, { useState } from "react";
import { Package } from "../../client";
import { useMutationWithApiError } from "../../hooks/useQueryWithApiError";
import { parseResourceUri } from "../../utils/formatting";
import { useServer } from "../ServerProvider";

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
         return apiClients.packages.updatePackage(environmentName, packageName, {
            name: packageName,
            description: variables.description,
         });
      },
      onSuccess() {
         handleClose();
         setNotificationMessage("Package updated successfully");
         queryClient.invalidateQueries({ queryKey: ["packages", environmentName] });
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

         <Dialog open={open} onClose={handleClose}>
            <DialogTitle>Edit Package</DialogTitle>
            <DialogContent>
               <DialogContentText>
                  Update the details for &quot;{_package.name}&quot;.
               </DialogContentText>
               <form onSubmit={handleSubmit} id="package-form">
                  <TextField
                     autoFocus
                     required
                     margin="dense"
                     id="name"
                     name="name"
                     label="Package Name"
                     disabled
                     type="text"
                     fullWidth
                     variant="standard"
                     defaultValue={_package.name}
                  />
                  <TextField
                     id="description"
                     name="description"
                     label="Description"
                     multiline
                     fullWidth
                     rows={4}
                     defaultValue={_package.description}
                     variant="standard"
                  />
               </form>
            </DialogContent>
            <DialogActions>
               <Button disabled={editPackage.isPending} onClick={handleClose}>
                  Cancel
               </Button>
               <Button
                  type="submit"
                  form="package-form"
                  loading={editPackage.isPending}
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
