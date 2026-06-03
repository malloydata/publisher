import { Delete } from "@mui/icons-material";
import CloseIcon from "@mui/icons-material/Close";
import Button from "@mui/material/Button";
import Dialog from "@mui/material/Dialog";
import DialogActions from "@mui/material/DialogActions";
import DialogContent from "@mui/material/DialogContent";
import DialogTitle from "@mui/material/DialogTitle";
import IconButton from "@mui/material/IconButton";
import ListItemIcon from "@mui/material/ListItemIcon";
import ListItemText from "@mui/material/ListItemText";
import MenuItem from "@mui/material/MenuItem";
import Typography from "@mui/material/Typography";
import React, { useState } from "react";
import { Materialization } from "../../client";

export default function DeleteMaterializationDialog({
   materialization,
   onCloseDialog,
   isMutating,
   onDelete,
}: {
   materialization: Materialization;
   onCloseDialog: () => void;
   isMutating: boolean;
   onDelete: () => void;
}) {
   const [open, setOpen] = useState(false);

   const handleClickOpen = () => setOpen(true);
   const handleClose = () => {
      setOpen(false);
      onCloseDialog();
   };

   return (
      <React.Fragment>
         <MenuItem
            aria-label={`Delete materialization ${materialization?.id ?? ""}`.trim()}
            onClick={(event) => {
               event.stopPropagation();
               handleClickOpen();
            }}
            sx={{ color: "error.main" }}
         >
            <ListItemIcon sx={{ color: "inherit" }}>
               <Delete fontSize="small" />
            </ListItemIcon>
            <ListItemText>Delete</ListItemText>
         </MenuItem>

         <Dialog
            onClose={handleClose}
            open={open}
            aria-labelledby="delete-materialization-title"
         >
            <DialogTitle sx={{ m: 0, p: 2 }} id="delete-materialization-title">
               Delete Materialization
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
                  Are you sure you want to delete this materialization record?
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
         </Dialog>
      </React.Fragment>
   );
}
