// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { Delete } from "@mui/icons-material";
import Button from "@mui/material/Button";
import Checkbox from "@mui/material/Checkbox";
import FormControlLabel from "@mui/material/FormControlLabel";
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
   onDelete: (dropTables: boolean) => void;
}) {
   const [open, setOpen] = useState(false);
   const [dropTables, setDropTables] = useState(false);

   const handleClickOpen = () => setOpen(true);
   const handleClose = () => {
      setOpen(false);
      setDropTables(false);
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
                     onClick={() => onDelete(dropTables)}
                     loading={isMutating}
                  >
                     Delete run
                  </Button>
               </>
            }
         >
            <Typography variant="body2">
               Delete this run&apos;s record? This cannot be undone.
            </Typography>
            <FormControlLabel
               control={
                  <Checkbox
                     checked={dropTables}
                     onChange={(event) => setDropTables(event.target.checked)}
                  />
               }
               label="Also drop the materialized tables this run produced"
            />
         </AppDialog>
      </React.Fragment>
   );
}
