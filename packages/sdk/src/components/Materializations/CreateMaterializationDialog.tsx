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
   onSubmit: (opts: {
      forceRefresh: boolean;
      pauseBetweenPhases: boolean;
   }) => Promise<unknown>;
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
   const [pauseBetweenPhases, setPauseBetweenPhases] = useState(false);

   const handleClose = () => setOpen(false);

   const handleRun = async () => {
      try {
         await onSubmit({ forceRefresh, pauseBetweenPhases });
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
                  Materialize every persist source in this package: compile,
                  build the tables, and load them so queries serve from the
                  materialized tables. Enable &ldquo;Pause between phases&rdquo;
                  to stop at the build plan for a control plane to drive the
                  build instead.
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
                  <FormControlLabel
                     control={
                        <Switch
                           checked={pauseBetweenPhases}
                           onChange={(event) =>
                              setPauseBetweenPhases(event.target.checked)
                           }
                        />
                     }
                     label="Pause between phases (control-plane two-round flow)"
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
                  {pauseBetweenPhases ? "Create plan" : "Materialize"}
               </Button>
            </DialogActions>
         </Dialog>
      </>
   );
}
