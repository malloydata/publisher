// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import {
   Button,
   FormControlLabel,
   FormGroup,
   Switch,
   Tooltip,
} from "@mui/material";
import { useState } from "react";
import { AppDialog } from "../AppDialog";
import { AddButton } from "../buttons";

type CreateMaterializationDialogProps = {
   onSubmit: (opts: { forceRefresh: boolean }) => Promise<unknown>;
   isSubmitting: boolean;
   disabled?: boolean;
   disabledReason?: string;
};

export default function CreateMaterializationDialog({
   onSubmit,
   isSubmitting,
   disabled,
   disabledReason,
}: CreateMaterializationDialogProps) {
   const [open, setOpen] = useState(false);
   const [forceRefresh, setForceRefresh] = useState(false);

   const handleClose = () => setOpen(false);

   const handleRun = async () => {
      try {
         await onSubmit({ forceRefresh });
         setOpen(false);
      } catch {
         // The mutation surfaces the error through the caller's Snackbar;
         // keep the dialog open so the user can retry or cancel.
      }
   };

   const button = (
      <span>
         <AddButton
            label="Materialization"
            onClick={() => setOpen(true)}
            disabled={disabled}
         />
      </span>
   );

   return (
      <>
         {disabled && disabledReason ? (
            <Tooltip title={disabledReason}>{button}</Tooltip>
         ) : (
            button
         )}

         <AppDialog
            open={open}
            onClose={handleClose}
            title="New materialization"
            description="Compile the package, build a table for every persist source, and load them so queries serve from the tables."
            actions={
               <>
                  <Button onClick={handleClose}>Cancel</Button>
                  <Button
                     variant="contained"
                     loading={isSubmitting}
                     onClick={handleRun}
                  >
                     Materialize
                  </Button>
               </>
            }
         >
            <FormGroup>
               <FormControlLabel
                  control={
                     <Switch
                        checked={forceRefresh}
                        onChange={(event) =>
                           setForceRefresh(event.target.checked)
                        }
                     />
                  }
                  label="Force refresh (rebuild even if unchanged)"
               />
            </FormGroup>
         </AppDialog>
      </>
   );
}
