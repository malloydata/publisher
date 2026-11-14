// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import CloseIcon from "@mui/icons-material/Close";
import { IconButton, Tooltip } from "@mui/material";
import { MOTION_FAST, reducedMotionSx } from "../theme/motion";
import { usePublisherTheme } from "../theme/ThemeContext";
import { edgeColor, hoverEdgeColor } from "./controlTokens";

/**
 * The class on a remove badge. Its container shows it while hovered or
 * focused: `"& .remove-badge": { opacity: 0 }` and
 * `"&:hover .remove-badge, &:focus-within .remove-badge": { opacity: 1 }`,
 * as {@link removeBadgeRevealSx} writes.
 */
export const REMOVE_BADGE_CLASS = "remove-badge";

/** The container rules that reveal its remove badge on hover and focus, and always on touch. */
export const removeBadgeRevealSx = {
   [`& .${REMOVE_BADGE_CLASS}`]: { opacity: 0 },
   [`&:hover .${REMOVE_BADGE_CLASS}, &:focus-within .${REMOVE_BADGE_CLASS}`]: {
      opacity: 1,
   },
   "@media (hover: none)": {
      [`& .${REMOVE_BADGE_CLASS}`]: { opacity: 1 },
   },
} as const;

/**
 * Take something off the page: a small round × on its top-right corner. The
 * one removal control in the builder, for a filter and a tile alike, so
 * removing looks and works the same wherever it is. The container must be
 * `position: relative` and spread {@link removeBadgeRevealSx}.
 *
 * With `disabledReason`, the badge stays focusable (aria-disabled, not
 * disabled) and says why on hover and to a screen reader, rather than
 * vanishing or greying out without explanation.
 */
export function RemoveBadge({
   label,
   onRemove,
   disabledReason,
   /** How far outside the container's edge the badge's centre sits, in px. */
   inset = 0,
}: {
   /** The accessible name: "Remove filter BRAND", "Remove Revenue by month". */
   label: string;
   onRemove: () => void;
   disabledReason?: string;
   inset?: number;
}) {
   const { theme } = usePublisherTheme();
   const disabled = disabledReason !== undefined;
   const badge = (
      <IconButton
         className={REMOVE_BADGE_CLASS}
         aria-label={label}
         aria-disabled={disabled || undefined}
         {...(disabled ? { "aria-description": disabledReason } : {})}
         onClick={(event) => {
            // Never also the tile's or filter's own press.
            event.stopPropagation();
            if (!disabled) onRemove();
         }}
         onPointerDown={(event) => event.stopPropagation()}
         sx={(mui) => ({
            position: "absolute",
            top: -8 - inset,
            right: -8 - inset,
            zIndex: 3,
            width: 18,
            height: 18,
            p: 0,
            borderRadius: "50%",
            border: `1px solid ${edgeColor(theme.cardBorder)}`,
            bgcolor: mui.palette.background.paper,
            color: mui.palette.text.secondary,
            cursor: disabled ? "default" : "pointer",
            transition: `opacity ${MOTION_FAST}`,
            ...reducedMotionSx,
            "&:hover": {
               bgcolor: mui.palette.background.paper,
               color: disabled
                  ? mui.palette.text.secondary
                  : mui.palette.text.primary,
               borderColor: hoverEdgeColor(theme.cardBorder, theme.tileTitle),
            },
         })}
      >
         <CloseIcon sx={{ fontSize: 12 }} />
      </IconButton>
   );
   return disabled ? <Tooltip title={disabledReason}>{badge}</Tooltip> : badge;
}
