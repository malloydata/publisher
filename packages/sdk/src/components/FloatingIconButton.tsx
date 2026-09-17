// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { IconButton, type IconButtonProps } from "@mui/material";
import { usePublisherTheme } from "../theme/ThemeContext";

/**
 * A small round button that sits OVER content — a rendered result, an
 * explorer, a chart — to open something about it: the code, the full result,
 * a render warning.
 *
 * On the instance theme's tile colour, with its border, so it reads as a chip
 * of the same card it floats on in both modes. Six of these were drawn by hand
 * before, every one on a hardcoded white plate with a hardcoded dark icon,
 * and the notebook's comment said why that was the only way: a white plate
 * needs a dark icon whatever the mode. The plate follows the theme now, so
 * the icon can too.
 *
 * Positioning is the caller's: it passes `sx` for where the button floats.
 */
export function FloatingIconButton({ sx, ...props }: IconButtonProps) {
   const { theme } = usePublisherTheme();
   return (
      <IconButton
         size="small"
         {...props}
         sx={[
            {
               width: 32,
               height: 32,
               bgcolor: theme.tile,
               border: theme.border,
               color: theme.tileTitle,
               "&:hover": { bgcolor: theme.tile, color: theme.drillLink },
               "& .MuiSvgIcon-root": { fontSize: 18 },
            },
            ...(Array.isArray(sx) ? sx : [sx]),
         ]}
      />
   );
}
