import { Delete } from "@mui/icons-material";
import CloseIcon from "@mui/icons-material/Close";
import { Snackbar } from "@mui/material";
import Button from "@mui/material/Button";
import Dialog from "@mui/material/Dialog";
import DialogActions from "@mui/material/DialogActions";
import DialogContent from "@mui/material/DialogContent";
import DialogTitle from "@mui/material/DialogTitle";
import IconButton from "@mui/material/IconButton";
import Typography from "@mui/material/Typography";
import React, { useState } from "react";
import { Connection } from "../../client";

export default function DeleteConnectionDialog({
   connection,
   onCloseDialog,
   isMutating,
   onDelete,
}: {
   connection: Connection;
   onCloseDialog: () => void;
   isMutating: boolean;
   onDelete: () => void;
}) {
   const [open, setOpen] = useState(false);
   const [notificationMessage, setNotificationMessage] = useState("");
   const handleClickOpen = () => {
      setOpen(true);
   };
   const handleClose = () => {
      setOpen(false);
      onCloseDialog();
   };

   return (
      <React.Fragment>
         <IconButton
            aria-label={`Delete connection ${connection?.name ?? ""}`.trim()}
            onClick={(event) => {
               event.stopPropagation();
               handleClickOpen();
            }}
         >
            <Delete />
         </IconButton>

         <Dialog
            onClose={handleClose}
            aria-labelledby="customized-dialog-title"
            open={open}
         >
            <DialogTitle sx={{ m: 0, p: 2 }} id="customized-dialog-title">
               Delete Connection
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
                  Are you sure you want to delete &quot;{connection.name}&quot;?
                  This action cannot be undone.
               </Typography>
            </DialogContent>
            <DialogActions>
               <Button
                  loading={isMutating}
                  variant="contained"
                  autoFocus
                  onClick={() => onDelete()}
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
