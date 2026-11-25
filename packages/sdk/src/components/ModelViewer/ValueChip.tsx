// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { Box } from "@mui/material";
import type { ReactNode } from "react";
import { MONO_FONT_FAMILY, fontSizes, roles } from "./theme";

/**
 * A single data VALUE — a sample from a dimension's index — as a **grey,
 * outlined, gently-rounded rectangle**, deliberately unlike every other chip on
 * these surfaces (the borderless, fully-rounded, semantically- or
 * neutrally-toned pills the Base/Declarations/Platform layers use). A sample
 * value is neither a layer status nor an annotation; it is raw data pulled from
 * the warehouse, so it gets its own bordered-box vocabulary rather than reading
 * as one more layer pill.
 *
 * <p>Mono, because a value is a literal that exists verbatim in the data (like
 * the identifiers elsewhere here). On theme tokens (`action.hover` / `divider` /
 * `text.primary`), no raw hex.
 */
export function ValueChip({ children }: { children: ReactNode }) {
   return (
      <Box
         component="span"
         sx={{
            fontFamily: MONO_FONT_FAMILY,
            fontSize: fontSizes.caption,
            color: "text.primary",
            bgcolor: roles.background.muted,
            border: "1px solid",
            borderColor: "divider",
            borderRadius: 1,
            padding: "2px 8px",
         }}
      >
         {children}
      </Box>
   );
}
