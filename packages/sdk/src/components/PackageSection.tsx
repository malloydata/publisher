// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { Box, Stack, Typography } from "@mui/material";
import * as React from "react";

/**
 * One titled section of a package's page: a heading, how many things are in it,
 * and the control that adds one. Shared so a section written elsewhere — the
 * materializations section is one — sits in the same rhythm as the rest.
 */
export function PackageSection({
   title,
   count,
   action,
   children,
}: {
   title: string;
   count?: number;
   /** A control on the heading's row, hard right. */
   action?: React.ReactNode;
   children: React.ReactNode;
}) {
   return (
      <Box sx={{ mb: 4 }}>
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
            sx={{ mb: 1, minHeight: 40 }}
         >
            <Stack direction="row" alignItems="baseline" spacing={1}>
               <Typography
                  variant="h6"
                  sx={{ fontWeight: 600, letterSpacing: "-0.025em" }}
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
         <Box>{children}</Box>
      </Box>
   );
}

/**
 * A row names its content type once. The glyph and the color behind it are two
 * halves of one signal, so the row derives both rather than letting a caller
 * pair a dashboard's icon with a model's color.
 *
 * That is not hypothetical tidying: with the two passed separately, four of the
 * six rows on this page had been handed the same teal, so color told a reader
 * nothing about four of the kinds it was there to distinguish. A rule each call
 * site has to remember is a rule some call sites will forget.
 */
