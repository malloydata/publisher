// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { Box } from "@mui/material";
import type { ReactNode } from "react";
import { fontSizes, MONO_FONT_FAMILY, TEXT_TERTIARY } from "./theme";

/** The facts grid: labels share a column sized to the longest, values line up. */
export const FACTS_GRID_SX = {
   display: "grid",
   gridTemplateColumns: "max-content 1fr",
   columnGap: "16px",
   rowGap: "6px",
   alignItems: "baseline",
} as const;

/** One label/value pair in the facts grid. Contributes both grid cells. */
export function Fact({
   label,
   value,
   mono,
}: {
   label: string;
   value: ReactNode;
   mono?: boolean;
}) {
   return (
      <>
         <Box
            component="span"
            sx={{ color: TEXT_TERTIARY, fontSize: fontSizes.caption }}
         >
            {label}
         </Box>
         <Box
            component="span"
            sx={{
               fontFamily: mono ? MONO_FONT_FAMILY : undefined,
               wordBreak: "break-all",
            }}
         >
            {value}
         </Box>
      </>
   );
}
