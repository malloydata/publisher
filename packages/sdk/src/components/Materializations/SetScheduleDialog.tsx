// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import ScheduleIcon from "@mui/icons-material/Schedule";
import { Alert, Button, TextField } from "@mui/material";
import { useState } from "react";
import { MONO_FONT_FAMILY } from "../styles";
import { AppDialog } from "../AppDialog";
import { SecondaryButton } from "../buttons";
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

   return (
      <>
         <SecondaryButton
            label="Schedule"
            icon={<ScheduleIcon />}
            onClick={handleOpen}
            disabled={disabled}
            {...(disabledReason ? { disabledReason } : {})}
            ariaHasPopup="dialog"
            // The label is the noun, like every other control on this row; the
            // accessible name keeps the verb, and says which verb it is.
            ariaLabel={currentSchedule ? "Edit schedule" : "Set schedule"}
         />

         <AppDialog
            open={open}
            onClose={handleClose}
            title={currentSchedule ? "Edit schedule" : "Set schedule"}
            description="The publisher rebuilds this package's materializations on this cadence: a 5-field UNIX cron, in UTC. In a hosted deployment the change takes effect on your next publish."
            actions={
               <>
                  {currentSchedule && (
                     <Button
                        color="error"
                        loading={isSubmitting}
                        onClick={() => submit(null)}
                        // The destructive option sits away from the pair that
                        // confirms and cancels.
                        sx={{ mr: "auto" }}
                     >
                        Clear schedule
                     </Button>
                  )}
                  <Button onClick={handleClose}>Cancel</Button>
                  <Button
                     variant="contained"
                     loading={isSubmitting}
                     disabled={!info.valid}
                     onClick={() => submit(expr.trim())}
                  >
                     Save schedule
                  </Button>
               </>
            }
         >
            <TextField
               autoFocus
               fullWidth
               size="small"
               label="Cron expression"
               value={expr}
               onChange={(event) => setExpr(event.target.value)}
               error={expr.trim() !== "" && !info.valid}
               helperText={
                  info.valid
                     ? `${info.description} · next run ${formatNextRun(info.nextRun)}`
                     : info.error
               }
               slotProps={{
                  htmlInput: { style: { fontFamily: MONO_FONT_FAMILY } },
               }}
            />
            <Alert severity="info">
               A schedule runs only on version-scoped packages, so saving one
               sets <code>scope: version</code> for this package.
            </Alert>
         </AppDialog>
      </>
   );
}
