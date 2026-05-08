import React from "react";
import Button from "@mui/material/Button";
import TextField from "@mui/material/TextField";
import Dialog from "@mui/material/Dialog";
import DialogActions from "@mui/material/DialogActions";
import DialogContent from "@mui/material/DialogContent";
import DialogContentText from "@mui/material/DialogContentText";
import DialogTitle from "@mui/material/DialogTitle";
import { useState } from "react";
import { Edit } from "@mui/icons-material";
import { MenuItem, ListItemIcon, ListItemText, Snackbar } from "@mui/material";
import { Environment } from "../../client";
import {
   generateEnvironmentReadme,
   getEnvironmentDescription,
} from "../../utils/parsing";
import { useQueryClient } from "@tanstack/react-query";
import { useMutationWithApiError } from "../../hooks/useQueryWithApiError";
import { useServer } from "../ServerProvider";

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
   const queryClient = useQueryClient();
   const [notificationMessage, setNotificationMessage] = useState("");

   const handleClickOpen = () => {
      setOpen(true);
   };

   const handleClose = () => {
      setOpen(false);
      onCloseDialog();
   };

   const editEnvironment = useMutationWithApiError({
      async mutationFn(variables: { description: string }) {
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
      onSuccess() {
         handleClose();
         queryClient.invalidateQueries({ queryKey: ["environments"] });
         setNotificationMessage("Environment updated successfully");
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

         <Dialog open={open} onClose={handleClose}>
            <DialogTitle>Edit Environment</DialogTitle>
            <DialogContent>
               <DialogContentText>
                  Edit this environment&apos;s description.
               </DialogContentText>
               <form onSubmit={handleSubmit} id="environment-form">
                  <TextField
                     autoFocus
                     required
                     margin="dense"
                     id="name"
                     name="name"
                     label="Environment Name"
                     disabled
                     type="text"
                     fullWidth
                     variant="standard"
                     defaultValue={environment.name}
                  />
                  <TextField
                     margin="dense"
                     id="description"
                     name="description"
                     label="Environment Description"
                     type="text"
                     fullWidth
                     variant="standard"
                     defaultValue={getEnvironmentDescription(
                        environment.readme,
                     )}
                  />
               </form>
            </DialogContent>
            <DialogActions>
               <Button
                  disabled={editEnvironment.isPending}
                  onClick={handleClose}
               >
                  Cancel
               </Button>
               <Button
                  type="submit"
                  form="environment-form"
                  loading={editEnvironment.isPending}
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
