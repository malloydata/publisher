// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { Box, Tooltip } from "@mui/material";
import { createContext, useContext, type ReactNode } from "react";
import { focusRingSx } from "./clickableBox";
import type { RowLayer } from "./rowLayers";

/**
 * The source header's facts card, as the chips on its status row see it: which
 * layer it is open on (none: shut), and how a chip opens or shuts it. Provided
 * by HeaderBar; absent anywhere else, where a chip is only a tooltip.
 */
export interface HeaderCard {
   openLayer: RowLayer | undefined;
   /** The layers the card has facts for; a chip for any other stays a tooltip. */
   layers: ReadonlyArray<RowLayer>;
   toggle: (layer: RowLayer) => void;
   cardId: string;
}

export const HeaderCardContext = createContext<HeaderCard | undefined>(
   undefined,
);

/**
 * A status-row chip that is also the way into the facts behind it: the chip is a
 * button, `aria-expanded` saying whether its card is open; the pointer and a
 * shade on hover say it opens, and it stays shaded while open. A chip whose
 * layer the card has no facts for (or outside the header) is the plain focusable
 * chip it always was: a tab stop whose name is its meaning. So is a chip with no layer at all
 * (a file's Public / Private), which has no facts to open, only its why.
 */
export function HeaderChip({
   layer,
   title,
   children,
   testId,
}: {
   /** The card layer it opens; none for a chip that only says what it means. */
   layer?: RowLayer;
   /** The tooltip and accessible name: what the chip means. */
   title: string;
   children: ReactNode;
   testId?: string;
}) {
   const card = useContext(HeaderCardContext);
   if (!card || !layer || !card.layers.includes(layer)) {
      return (
         <Tooltip title={title}>
            <Box
               component="span"
               tabIndex={0}
               role="img"
               aria-label={title}
               data-testid={testId}
               sx={{ display: "inline-flex", borderRadius: 1, ...focusRingSx }}
            >
               {children}
            </Box>
         </Tooltip>
      );
   }
   const open = card.openLayer === layer;
   return (
      <Tooltip title={title}>
         <Box
            component="button"
            type="button"
            onClick={() => card.toggle(layer)}
            aria-expanded={open}
            aria-controls={card.cardId}
            aria-label={`${title} ${open ? "Hide" : "Show"} details.`}
            data-testid={testId}
            data-header-chip={layer}
            sx={{
               appearance: "none",
               border: "none",
               background: "none",
               font: "inherit",
               p: 0,
               cursor: "pointer",
               display: "inline-flex",
               alignItems: "center",
               borderRadius: 1,
               // The pointer and a shade on hover say it opens; it holds the shade
               // while its card is open.
               "& [data-pill-tone]": {
                  transition: "filter 120ms ease",
                  ...(open ? { filter: "brightness(0.9)" } : {}),
               },
               "&:hover [data-pill-tone]": {
                  filter: open ? "brightness(0.9)" : "brightness(0.94)",
               },
               ...focusRingSx,
            }}
         >
            {children}
         </Box>
      </Tooltip>
   );
}
