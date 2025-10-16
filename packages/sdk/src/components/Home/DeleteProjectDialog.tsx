import React, { useState } from "react";
import Button from "@mui/material/Button";
import Dialog from "@mui/material/Dialog";
import DialogTitle from "@mui/material/DialogTitle";
import DialogContent from "@mui/material/DialogContent";
import DialogActions from "@mui/material/DialogActions";
import IconButton from "@mui/material/IconButton";
import CloseIcon from "@mui/icons-material/Close";
import Typography from "@mui/material/Typography";
import { ListItemIcon, ListItemText, MenuItem, Snackbar } from "@mui/material";
import { Delete } from "@mui/icons-material";
import { Project } from "../../client";
import { useMutationWithApiError } from "../../hooks/useQueryWithApiError";
import { useServer } from "../ServerProvider";
import { useQueryClient } from "@tanstack/react-query";

export default function DeleteProjectDialog({
   project,
   onCloseDialog,
}: {
   project: Project;
   onCloseDialog: () => void;
}) {
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

   const deleteProject = useMutationWithApiError({
      mutationFn: () => apiClients.projects.deleteProject(project.name),
      onSuccess() {
         handleClose();
         queryClient.invalidateQueries({ queryKey: ["projects"] });
         setNotificationMessage("Project deleted successfully");
      },
      onError(error) {
         setNotificationMessage(
            error instanceof Error
               ? error.message
               : "An unknown error occurred",
         );
      },
   });

   return (
      <React.Fragment>
         <MenuItem onClick={handleClickOpen}>
            <ListItemIcon>
               <Delete fontSize="small" />
            </ListItemIcon>
            <ListItemText>Delete</ListItemText>
         </MenuItem>
         <Dialog
            onClose={handleClose}
            aria-labelledby="customized-dialog-title"
            open={open}
         >
            <DialogTitle sx={{ m: 0, p: 2 }} id="customized-dialog-title">
               Delete Project
            </DialogTitle>
            <IconButton
               aria-label="close"
               onClick={handleClose}
               sx={(theme) => ({
                  position: "absolute",
                  right: 8,
                  top: 8,
                  color: theme.palette.grey[500],
               })}
            >
               <CloseIcon />
            </IconButton>
            <DialogContent dividers>
               <Typography gutterBottom>
                  Are you sure you want to delete &quot;{project.name}&quot;?
                  This action cannot be undone.
               </Typography>
            </DialogContent>
            <DialogActions>
               <Button
                  variant="outlined"
                  onClick={handleClose}
                  style={{
                     borderColor: "#14b3cb",
                     color: "#14b3cb",
                  }}
               >
                  Cancel
               </Button>
               <Button
                  loading={deleteProject.isPending}
                  variant="contained"
                  autoFocus
                  onClick={() => deleteProject.mutate()}
                  color="error"
               >
                  Delete
               </Button>
            </DialogActions>
            <Snackbar
               open={notificationMessage !== ""}
               autoHideDuration={6000}
               onClose={() => setNotificationMessage("")}
               message={notificationMessage}
            />
         </Dialog>
      </React.Fragment>
   );
}
