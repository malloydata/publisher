// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { Tooltip, Typography } from "@mui/material";
import { MaterializationMetadata, triggerLabel } from "./utils";

/**
 * Chip showing how a run was initiated — Scheduled (the standalone scheduler
 * fired the package's cron) vs Manual (an API/UI create) — with a tooltip. One
 * source of truth for the label so the runs list and the detail dialog can
 * never drift.
 *
 * Word only, like the status chip beside it: the glyphs this carried (a hand
 * pressing a button for Manual) were the only pictograms in a table of words,
 * and a two-value distinction needs no icon to be read.
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
         {/* The word, like every other cell in the row. A bordered pill here
             made a value look like something to click. */}
         <Typography variant="body2" color="text.secondary">
            {triggerLabel(meta)}
         </Typography>
      </Tooltip>
   );
}
