import ScheduleIcon from "@mui/icons-material/Schedule";
import TouchAppIcon from "@mui/icons-material/TouchApp";
import { Chip, Tooltip } from "@mui/material";
import { MaterializationMetadata, triggerLabel } from "./utils";

/**
 * Chip showing how a run was initiated — Scheduled (the standalone scheduler
 * fired the package's cron) vs Manual (an API/UI create) — with a tooltip. One
 * source of truth for the color/icon/label so the runs list and the detail
 * dialog can never drift.
 */
export default function TriggerChip({
   meta,
}: {
   meta: MaterializationMetadata;
}) {
   const scheduled = meta.trigger === "SCHEDULER";
   return (
      <Tooltip
         title={
            scheduled
               ? "Fired by the materialization schedule"
               : "Triggered manually"
         }
      >
         <Chip
            size="small"
            variant="outlined"
            color={scheduled ? "info" : "default"}
            icon={
               scheduled ? (
                  <ScheduleIcon fontSize="small" />
               ) : (
                  <TouchAppIcon fontSize="small" />
               )
            }
            label={triggerLabel(meta)}
         />
      </Tooltip>
   );
}
