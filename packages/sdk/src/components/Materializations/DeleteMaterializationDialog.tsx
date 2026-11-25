// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { Delete } from "@mui/icons-material";
import Button from "@mui/material/Button";
import ListItemIcon from "@mui/material/ListItemIcon";
import ListItemText from "@mui/material/ListItemText";
import MenuItem from "@mui/material/MenuItem";
import Typography from "@mui/material/Typography";
import React, { useState } from "react";
import { Materialization } from "../../client";
import { AppDialog } from "../AppDialog";

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

         <AppDialog
            open={open}
            onClose={handleClose}
            title="Delete materialization"
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
                     Delete run
                  </Button>
               </>
            }
         >
            <Typography variant="body2">
               Delete this run&apos;s record? This cannot be undone. The tables
               it built stay in place: drop any you no longer need through their
               connection.
            </Typography>
         </AppDialog>
      </React.Fragment>
   );
}
