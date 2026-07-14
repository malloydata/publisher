import ScheduleIcon from "@mui/icons-material/Schedule";
import {
   Alert,
   Box,
   Button,
   Dialog,
   DialogActions,
   DialogContent,
   DialogContentText,
   DialogTitle,
   TextField,
   Tooltip,
   Typography,
} from "@mui/material";
import { useState } from "react";
import { MONO_FONT_FAMILY } from "../styles";
import { describeCron, formatNextRun } from "./cron";

type SetScheduleDialogProps = {
   /** The current cron, or null when no schedule is set. */
   currentSchedule: string | null;
   isSubmitting: boolean;
   disabled?: boolean;
   disabledReason?: string;
   /** Persist a new cron, or null to clear the schedule. */
   onSubmit: (schedule: string | null) => Promise<unknown>;
};

const DEFAULT_CRON = "0 6 * * *";

export default function SetScheduleDialog({
   currentSchedule,
   isSubmitting,
   disabled,
   disabledReason,
   onSubmit,
}: SetScheduleDialogProps) {
   const [open, setOpen] = useState(false);
   const [expr, setExpr] = useState(currentSchedule ?? DEFAULT_CRON);

   const handleOpen = () => {
      setExpr(currentSchedule ?? DEFAULT_CRON);
      setOpen(true);
   };
   const handleClose = () => setOpen(false);

   const info = describeCron(expr);

   const submit = async (schedule: string | null) => {
      try {
         await onSubmit(schedule);
         setOpen(false);
      } catch {
         // The caller surfaces the error via its Snackbar; keep the dialog
         // open so the user can correct the cron and retry.
      }
   };

   const button = (
      <span>
         <Button
            variant="outlined"
            size="small"
            startIcon={<ScheduleIcon />}
            onClick={handleOpen}
            disabled={disabled}
            aria-label={currentSchedule ? "Edit schedule" : "Set schedule"}
         >
            {currentSchedule ? "Edit schedule" : "Set schedule"}
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
            aria-labelledby="set-schedule-title"
         >
            <DialogTitle id="set-schedule-title">
               {currentSchedule ? "Edit schedule" : "Set schedule"}
            </DialogTitle>
            <DialogContent>
               <DialogContentText sx={{ mb: 2 }}>
                  The publisher rebuilds the materializations in this package on
                  this cadence. Enter a 5-field UNIX cron, evaluated in UTC.
               </DialogContentText>
               <TextField
                  autoFocus
                  fullWidth
                  label="Cron expression"
                  value={expr}
                  onChange={(event) => setExpr(event.target.value)}
                  error={expr.trim() !== "" && !info.valid}
                  slotProps={{
                     htmlInput: { style: { fontFamily: MONO_FONT_FAMILY } },
                  }}
               />
               <Box sx={{ mt: 1.5, minHeight: 48 }}>
                  {info.valid ? (
                     <>
                        <Typography variant="body2">
                           {info.description}
                        </Typography>
                        <Typography variant="body2" color="text.secondary">
                           Next run: {formatNextRun(info.nextRun)}
                        </Typography>
                     </>
                  ) : (
                     <Typography variant="body2" color="error">
                        {info.error}
                     </Typography>
                  )}
               </Box>
               <Alert severity="info" sx={{ mt: 2 }}>
                  A schedule runs only on version-scoped packages, so saving one
                  sets <code>scope: version</code> for this package.
               </Alert>
            </DialogContent>
            <DialogActions sx={{ justifyContent: "space-between", px: 3 }}>
               <Box>
                  {currentSchedule && (
                     <Button
                        color="error"
                        loading={isSubmitting}
                        onClick={() => submit(null)}
                     >
                        Clear schedule
                     </Button>
                  )}
               </Box>
               <Box>
                  <Button onClick={handleClose}>Cancel</Button>
                  <Button
                     variant="contained"
                     loading={isSubmitting}
                     disabled={!info.valid}
                     onClick={() => submit(expr.trim())}
                     sx={{ ml: 1 }}
                  >
                     Save
                  </Button>
               </Box>
            </DialogActions>
         </Dialog>
      </>
   );
}
