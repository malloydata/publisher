import { AddCircleRounded } from "@mui/icons-material";
import Button from "@mui/material/Button";
import Dialog from "@mui/material/Dialog";
import DialogActions from "@mui/material/DialogActions";
import DialogContent from "@mui/material/DialogContent";
import DialogContentText from "@mui/material/DialogContentText";
import DialogTitle from "@mui/material/DialogTitle";
import Snackbar from "@mui/material/Snackbar";
import TextField from "@mui/material/TextField";
import { useQueryClient } from "@tanstack/react-query";
import React, { useState } from "react";
import { useMutationWithApiError } from "../../hooks/useQueryWithApiError";
import { generateEnvironmentReadme } from "../../utils/parsing";
import { useServer } from "../ServerProvider";

export default function AddEnvironmentDialog() {
   const [open, setOpen] = useState(false);
   const { apiClients } = useServer();
   const [notificationMessage, setNotificationMessage] = useState("");
   const handleClickOpen = () => {
      setOpen(true);
   };

   const handleClose = () => {
      setOpen(false);
   };
   const queryClient = useQueryClient();
   const addEnvironment = useMutationWithApiError({
      async mutationFn(variables: { name: string; description: string }) {
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
      onSuccess() {
         handleClose();
         queryClient.invalidateQueries({ queryKey: ["environments"] });
         setNotificationMessage("Environment created successfully");
      },
      onError(error) {
         setNotificationMessage(
            error instanceof Error
               ? error.message
               : "An unknown error occurred",
         );
      },
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
         <Button
            variant="contained"
            onClick={handleClickOpen}
            startIcon={<AddCircleRounded />}
            sx={{ mt: 2, color: "white" }}
         >
            Create New Environment
         </Button>
         <Dialog open={open} onClose={handleClose}>
            <DialogTitle>Create New Environment</DialogTitle>
            <DialogContent>
               <DialogContentText>
                  Add a new environment to start exploring semantic models and
                  analyzing data.
               </DialogContentText>
               <form onSubmit={handleSubmit} id="environment-form">
                  <TextField
                     autoFocus
                     required
                     margin="dense"
                     id="name"
                     name="name"
                     label="Environment Name"
                     type="text"
                     fullWidth
                     variant="standard"
                  />
                  <TextField
                     margin="dense"
                     id="description"
                     name="description"
                     label="Environment Description"
                     placeholder="Explore semantic models, run queries, and build dashboards"
                     type="text"
                     fullWidth
                     variant="standard"
                  />
               </form>
            </DialogContent>
            <DialogActions>
               <Button
                  disabled={addEnvironment.isPending}
                  onClick={handleClose}
               >
                  Cancel
               </Button>
               <Button
                  type="submit"
                  form="environment-form"
                  loading={addEnvironment.isPending}
               >
                  Create Environment
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
