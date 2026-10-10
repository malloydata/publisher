// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { Tooltip, Typography } from "@mui/material";
import { MaterializationMetadata, triggerLabel } from "./utils";

/**
 * How a run was initiated — Scheduled (the standalone scheduler fired the
 * package's cron) or Manual (an API or UI create) — with the tooltip that says
 * which. One source of truth for the label, so the runs list and the detail
 * dialog cannot drift.
 *
 * A word, like every other cell in the row. A two-value distinction needs no
 * pictogram, and a bordered pill here made a value look like something to
 * click.
 */
export default function TriggerLabel({
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
         <Typography variant="body2" color="text.secondary">
            {triggerLabel(meta)}
         </Typography>
      </Tooltip>
   );
}
