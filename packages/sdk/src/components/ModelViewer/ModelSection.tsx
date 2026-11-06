// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { Box } from "@mui/material";
import type { ReactNode } from "react";
import { fontSizes, fontWeights } from "./theme";
import { ink, ROW_META_SX } from "./tokens";

/**
 * One kind of thing a source holds (Views, Queries, Measures, Calculations,
 * Dimensions, Joins)
 * as a page section that is always open: the page's own section header (an h2),
 * its Expand all under the title where it has one, then everything in it. No count: every
 * row is on the page, and the line is the section's control's. A source page is read like a
 * notebook, top to bottom, so nothing on it is folded away.
 *
 * <p>`level="h3"` is the same section one step down, inside a join: a joined
 * source's Measures and Dimensions, titled as the page's own, a size smaller.
 */
export function ModelSection({
   label,
   count,
   level = "h2",
   testId,
   action,
   children,
}: {
   label: string;
   count: number;
   level?: "h2" | "h3";
   testId?: string;
   /** A control on the line under the title (Expand all), on an h2 section. */
   action?: ReactNode;
   children: ReactNode;
}) {
   if (count === 0) return null;
   if (level === "h3") {
      // The h2's shape one step down: the title, then the rows.
      return (
         <Box
            component="section"
            aria-label={label}
            data-testid={testId}
            sx={{ mb: 2 }}
         >
            <Box
               component="h3"
               sx={{
                  m: 0,
                  mb: 1,
                  fontSize: fontSizes.bodyLarge,
                  fontWeight: fontWeights.strong,
                  color: ink,
               }}
            >
               {label}
            </Box>
            {children}
         </Box>
      );
   }
   return (
      <Box
         component="section"
         aria-label={label}
         data-testid={testId}
         sx={{ mb: 3 }}
      >
         <Box sx={{ mb: 2 }}>
            <Box
               component="h2"
               sx={{
                  m: 0,
                  fontSize: "1.25rem",
                  fontWeight: fontWeights.strong,
                  color: ink,
               }}
            >
               {label}
            </Box>
            {/* The section's Expand all, under the title rather than beside it. */}
            {action && (
               <Box
                  sx={{
                     ...ROW_META_SX,
                     mt: 0.25,
                     display: "flex",
                     alignItems: "center",
                  }}
               >
                  {action}
               </Box>
            )}
         </Box>
         {children}
      </Box>
   );
}
