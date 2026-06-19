import AddIcon from "@mui/icons-material/Add";
import {
   Button,
   Dialog,
   DialogActions,
   DialogContent,
   DialogContentText,
   DialogTitle,
   FormControlLabel,
   FormGroup,
   Switch,
   Tooltip,
} from "@mui/material";
import { useState } from "react";

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
         <Button
            variant="contained"
            startIcon={<AddIcon />}
            onClick={() => setOpen(true)}
            disabled={disabled}
            aria-label="New materialization"
         >
            New materialization
         </Button>
      </span>
   );

   return (
      <>
         {disabled && disabledReason ? (
            <Tooltip title={disabledReason}>{button}</Tooltip>
         ) : (
            button
         )}

         <Dialog
            open={open}
            onClose={handleClose}
            maxWidth="xs"
            fullWidth
            aria-labelledby="create-materialization-title"
         >
            <DialogTitle id="create-materialization-title">
               New materialization
            </DialogTitle>
            <DialogContent>
               <DialogContentText sx={{ mb: 2 }}>
                  Compile this package and produce a build plan for every
                  persist source. The control plane assigns table identities and
                  drives the build from the plan.
               </DialogContentText>
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
            </DialogContent>
            <DialogActions>
               <Button onClick={handleClose}>Cancel</Button>
               <Button
                  variant="contained"
                  loading={isSubmitting}
                  onClick={handleRun}
               >
                  Create plan
               </Button>
            </DialogActions>
         </Dialog>
      </>
   );
}
