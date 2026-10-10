// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { Button, Stack, Typography } from "@mui/material";
import { useState, type ReactNode } from "react";
import { isNarrowScreen } from "../hooks/useNarrowScreen";

/**
 * Wraps an editor: on a screen under 600px it shows a notice with "Edit anyway" first.
 * Decided once at mount, so rotating a phone or resizing a window never swaps the editor out from under an edit.
 */
export function NarrowEditGate({ children }: { children: ReactNode }) {
   const [narrow] = useState(isNarrowScreen);
   const [proceed, setProceed] = useState(false);
   if (!narrow || proceed) return <>{children}</>;
   return (
      <Stack sx={{ alignItems: "flex-start", gap: 1.5, py: 3 }}>
         <Typography variant="h6">
            Editing works best on a larger screen
         </Typography>
         <Typography variant="body2" color="text.secondary">
            The editor is built for a wider window. You can still try it here.
         </Typography>
         <Button variant="outlined" onClick={() => setProceed(true)}>
            Edit anyway
         </Button>
      </Stack>
   );
}
