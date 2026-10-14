// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import type { Theme } from "@mui/material";
import type { SystemStyleObject } from "@mui/system";
import { MOTION_FAST, reducedMotionSx } from "../../theme/motion";
import type { ResolvedTheme } from "../../theme/types";

/** What kind of text is edited: a line (a title, a subtitle) or a block of prose. */
export type PencilAt = "line" | "prose";

/**
 * How text that is edited where it is read says so: a faint wash behind it
 * under the pointer, in the page's hover colour, and nothing at rest, so the
 * page reads as the report until the pointer reaches something editable. The
 * wash is padded out into the text's own margin (an equal negative margin
 * takes the padding back), so it never moves the text or its neighbours. The
 * keyboard gets a focus ring, since a wash does not say where focus is.
 *
 * A line gets a tight wash; a block of prose a roomier one, the size of the
 * paragraph it covers.
 */
export const editableSx = (
   theme: ResolvedTheme,
   at: PencilAt = "line",
): SystemStyleObject<Theme> => {
   const [x, y] = at === "line" ? ["6px", "2px"] : ["10px", "6px"];
   return {
      cursor: "text",
      borderRadius: at === "line" ? "4px" : "6px",
      mx: `-${x}`,
      px: x,
      my: `-${y}`,
      py: y,
      transition: `background-color ${MOTION_FAST}`,
      ...reducedMotionSx,
      "&:hover": { bgcolor: "action.hover" },
      "&:focus-visible": {
         outline: `2px solid ${theme.accent}`,
         outlineOffset: 2,
      },
   };
};
