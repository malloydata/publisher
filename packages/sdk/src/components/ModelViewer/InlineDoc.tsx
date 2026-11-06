// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { Box, type SxProps, type Theme } from "@mui/material";
import type { MouseEvent } from "react";
import { fontSizes, TEXT_TERTIARY } from "./theme";

/** Whether a one-line description hides part of `doc`: clipped by its ellipsis,
 *  or showing only the first of several lines. */
export function isDocCut(el: HTMLElement, doc: string): boolean {
   return doc.includes("\n") || el.scrollWidth > el.clientWidth;
}

/**
 * A row's description, printed on the row: its first line, cut to the space it
 * has, with the whole text as a plain title only when some of it is cut off.
 */
export function InlineDoc({
   doc,
   sx,
   testId,
}: {
   doc: string;
   sx?: SxProps<Theme>;
   testId?: string;
}) {
   return (
      <Box
         component="span"
         data-testid={testId}
         onMouseEnter={(e: MouseEvent<HTMLElement>) => {
            const el = e.currentTarget;
            el.title = isDocCut(el, doc) ? doc : "";
         }}
         sx={[
            {
               minWidth: 0,
               fontSize: fontSizes.small,
               color: TEXT_TERTIARY,
               whiteSpace: "nowrap",
               overflow: "hidden",
               textOverflow: "ellipsis",
            },
            ...(Array.isArray(sx) ? sx : [sx]),
         ]}
      >
         {doc.split("\n")[0]}
      </Box>
   );
}
