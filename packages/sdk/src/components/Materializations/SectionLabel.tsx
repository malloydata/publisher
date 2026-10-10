// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { Typography } from "@mui/material";
import { ReactNode } from "react";

/**
 * A sub-heading INSIDE a materialization view — the detail dialog and the
 * manifest view — where `PackageSection` does not apply: those are sections of
 * a dialog, not of a page. Small and uppercase to sit a level under the
 * dialog's own title.
 *
 * Renders as an `<h6>`, so it is a real heading to anything navigating by
 * them; the caption styling is purely visual.
 */
export default function SectionLabel({ children }: { children: ReactNode }) {
   return (
      <Typography
         variant="caption"
         component="h6"
         sx={{
            display: "block",
            textTransform: "uppercase",
            letterSpacing: "0.06em",
            fontWeight: 600,
            color: "text.secondary",
            mb: 1,
         }}
      >
         {children}
      </Typography>
   );
}
