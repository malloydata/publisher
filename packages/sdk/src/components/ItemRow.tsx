// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { Box, Typography } from "@mui/material";
import * as React from "react";
import { MONO_FONT_FAMILY } from "./styles";

/**
 * One thing in a list, anywhere in the Console: a colored icon plate, the
 * thing's name, what it is, and whatever acts on it at the right edge. One per
 * row.
 *
 * The rule is that a list is a list: everything a reader scans down is drawn
 * this way, so the same reading task looks the same on every page. A grid of
 * cards says "these are tiles to arrange", which is not what any of these
 * lists are; a row also lets the description be a description rather than two
 * clamped lines.
 *
 * The color is passed rather than derived here: the package page keys it off
 * content type (see `CONTENT_TINT`), and the environment-scoped lists off what
 * kind of thing the list holds (see `SURFACE_TINT`). Both are exhaustive maps,
 * so the color stays a property of the kind and not of the call site.
 */
export function ItemRow({
   icon,
   tint,
   label,
   mono,
   description,
   rightLabel,
   trailingAction,
   onClick,
   ariaLabel,
}: {
   /** The glyph, drawn white on the tinted plate. */
   icon: React.ReactNode;
   tint: string;
   label: string;
   /** Monospace label, for a name that is a file path. */
   mono?: boolean;
   /** What this one is, in the author's words. One line, then ellipsis. */
   description?: string;
   /** A fact about the row, right-aligned before any action: "1.0 K rows". */
   rightLabel?: string;
   /**
    * Rendered at the end of the row: a menu, an open-in-new-tab button. Clicks
    * on it should `event.stopPropagation()` so the row's own click does not
    * also fire.
    */
   trailingAction?: React.ReactNode;
   onClick?: (event: React.MouseEvent) => void;
   /** Overrides the accessible name, which is otherwise the label. */
   ariaLabel?: string;
}) {
   const interactive = !!onClick;
   const handleKeyDown = (event: React.KeyboardEvent<HTMLDivElement>) => {
      if (!onClick) return;
      if (event.key === "Enter" || event.key === " ") {
         event.preventDefault();
         onClick(event as unknown as React.MouseEvent);
      }
   };
   return (
      <Box
         onClick={onClick}
         onKeyDown={interactive ? handleKeyDown : undefined}
         role={interactive ? "button" : undefined}
         tabIndex={interactive ? 0 : undefined}
         aria-label={ariaLabel}
         sx={(theme) => ({
            display: "flex",
            alignItems: "center",
            gap: 1.5,
            py: 1,
            px: 1,
            mx: -1,
            cursor: interactive ? "pointer" : "default",
            borderRadius: 1.5,
            transition: "background-color 0.1s",
            "&:hover": interactive
               ? {
                    backgroundColor:
                       theme.palette.mode === "dark"
                          ? "rgba(255, 255, 255, 0.08)"
                          : "grey.100",
                 }
               : undefined,
            "&:focus-visible": interactive
               ? {
                    outline: "2px solid",
                    outlineColor: "primary.main",
                    outlineOffset: 2,
                 }
               : undefined,
         })}
      >
         <Box
            sx={{
               width: 32,
               height: 32,
               borderRadius: 1,
               bgcolor: tint,
               color: "#FFFFFF",
               display: "flex",
               alignItems: "center",
               justifyContent: "center",
               flexShrink: 0,
            }}
         >
            {icon}
         </Box>
         <Box
            sx={{
               flex: 1,
               minWidth: 0,
               display: "flex",
               alignItems: "baseline",
               gap: 1.5,
            }}
         >
            <Typography
               variant="body2"
               sx={{
                  ...(mono ? { fontFamily: MONO_FONT_FAMILY } : {}),
                  flexShrink: 0,
                  maxWidth: "100%",
                  overflow: "hidden",
                  textOverflow: "ellipsis",
                  whiteSpace: "nowrap",
               }}
            >
               {label}
            </Typography>
            {description && (
               <Typography
                  variant="body2"
                  color="text.secondary"
                  sx={{
                     minWidth: 0,
                     overflow: "hidden",
                     textOverflow: "ellipsis",
                     whiteSpace: "nowrap",
                  }}
               >
                  {description}
               </Typography>
            )}
         </Box>
         {rightLabel && (
            <Typography
               variant="caption"
               color="text.secondary"
               sx={{ flexShrink: 0 }}
            >
               {rightLabel}
            </Typography>
         )}
         {trailingAction && (
            <Box sx={{ flexShrink: 0, display: "flex", alignItems: "center" }}>
               {trailingAction}
            </Box>
         )}
      </Box>
   );
}
