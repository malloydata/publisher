// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { Delete } from "@mui/icons-material";
import { Snackbar } from "@mui/material";
import Button from "@mui/material/Button";
import ListItemIcon from "@mui/material/ListItemIcon";
import ListItemText from "@mui/material/ListItemText";
import MenuItem from "@mui/material/MenuItem";
import Typography from "@mui/material/Typography";
import React, { useState } from "react";
import { Connection } from "../../client";
import { AppDialog } from "../AppDialog";

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
         <MenuItem
            aria-label={`Delete connection ${connection?.name ?? ""}`.trim()}
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

         <AppDialog
            open={open}
            onClose={handleClose}
            title="Delete connection"
            actions={
               <>
                  <Button onClick={handleClose}>Cancel</Button>
                  <Button
                     variant="contained"
                     color="error"
                     autoFocus
                     onClick={() => onDelete()}
                     loading={isMutating}
                  >
                     Delete connection
                  </Button>
               </>
            }
         >
            <Typography variant="body2">
               Delete <strong>{connection.name}</strong>? Packages that query
               through it stop working, and this cannot be undone.
            </Typography>
         </AppDialog>
         <Snackbar
            open={notificationMessage !== ""}
            autoHideDuration={6000}
            onClose={() => setNotificationMessage("")}
            message={notificationMessage}
         />
      </React.Fragment>
   );
}
