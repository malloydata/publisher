// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { Box } from "@mui/material";
import type { ReactNode } from "react";
import { BodyText } from "./text";

/**
 * Quiet empty-state row for a section list: a single dashed row carrying a
 * message and a right-aligned action slot. Matches the width and rhythm of a
 * populated list so an empty section reads as the same surface, not a hero
 * card.
 *
 * An empty state carries a one-line explanation of what the section will hold,
 * plus the action that puts the first thing in it where there is one.
 *
 * `secondary` is a quieter follow-on line under the message — the "or, do it
 * this other way" alternative that shouldn't compete with the primary action.
 *
 * Where a section has no action that would fill it (a read-only artifact, a
 * container whose contents arrive from elsewhere), pass `message` alone: the
 * explanation is still worth more than the bare line, and an invented CTA is
 * worse than none.
 */
export function EmptyRow({
   message,
   secondary,
   actions,
}: {
   message: string;
   secondary?: ReactNode;
   actions?: ReactNode;
}) {
   return (
      <Box
         // So a caller's test can assert the SHAPE, not just the words.
         data-testid="empty-row"
         sx={{
            display: "flex",
            alignItems: "center",
            gap: 2,
            flexWrap: "wrap",
            p: 2,
            border: "1px dashed",
            borderColor: "divider",
            borderRadius: 2,
         }}
      >
         <Box sx={{ flex: 1, minWidth: 200 }}>
            <BodyText>{message}</BodyText>
            {secondary && (
               <BodyText sx={{ color: "text.secondary", mt: 0.5 }}>
                  {secondary}
               </BodyText>
            )}
         </Box>
         {actions && (
            <Box
               sx={{
                  display: "flex",
                  gap: 1,
                  flexShrink: 0,
                  alignItems: "center",
               }}
            >
               {actions}
            </Box>
         )}
      </Box>
   );
}
