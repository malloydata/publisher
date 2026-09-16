// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { Chip, Tooltip } from "@mui/material";
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
         <Chip
            size="small"
            variant="outlined"
            label={triggerLabel(meta)}
            // Quiet, whichever value it is. Colour in this table means STATUS —
            // the chip beside it is green for done and red for failed — so a
            // trigger painted blue for one value and near-black for the other
            // read as two kinds of thing in a column that holds one.
            sx={{ color: "text.secondary", borderColor: "divider" }}
         />
      </Tooltip>
   );
}
