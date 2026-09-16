// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { Box, Stack, Typography } from "@mui/material";
import * as React from "react";
import { useId } from "react";

/**
 * One titled section of a page: a heading, how many things are in it, what they
 * are, and the control that adds one.
 *
 * Every section on every page is this component, so moving between a package
 * and the environment above it does not restate the same heading at a different
 * size, indent or gap.
 *
 * The section is a landmark named by its own heading, which is what makes
 * "Dashboards" addressable — to a screen reader moving by region, and to a test
 * that wants the dashboards list rather than every row on the page that happens
 * to share a word with it.
 */
export function PackageSection({
   title,
   count,
   description,
   action,
   children,
}: {
   title: string;
   count?: number;
   /** What the section holds, in a line, under the heading. */
   description?: string;
   /** A control on the heading's row, hard right. */
   action?: React.ReactNode;
   children: React.ReactNode;
}) {
   const titleId = useId();
   return (
      <Box component="section" aria-labelledby={titleId} sx={{ mb: 4 }}>
         <Stack
            direction="row"
            alignItems="center"
            justifyContent="space-between"
            // Tall enough for the section's add button whether or not it has
            // one, so a section with nothing to add does not sit tighter than
            // its neighbours. The action is pushed right by the layout rather
            // than by a margin on itself: `Stack`'s own spacing rule outranks
            // an `ml: auto` on a child, which is how the button ended up
            // beside the heading instead of at the edge.
            sx={{ mb: description ? 0 : 1, minHeight: 40 }}
         >
            <Stack direction="row" alignItems="baseline" spacing={1}>
               <Typography
                  id={titleId}
                  variant="h6"
                  sx={{
                     fontWeight: 600,
                     letterSpacing: "-0.025em",
                     // The heading's box is centred against the buttons, but
                     // its ink is not: at the theme's 1.6 line height the
                     // leading sits mostly under a word with no descenders, so
                     // the title reads as riding high. Trim the box to the
                     // glyphs and the two line up by eye as well as by
                     // measurement.
                     lineHeight: 1.2,
                  }}
               >
                  {title}
               </Typography>
               {count !== undefined && (
                  <Typography variant="caption" color="text.secondary">
                     ({count})
                  </Typography>
               )}
            </Stack>
            {action}
         </Stack>
         {description && (
            <Typography variant="body2" color="text.secondary" sx={{ mb: 1.5 }}>
               {description}
            </Typography>
         )}
         <Box>{children}</Box>
      </Box>
   );
}
