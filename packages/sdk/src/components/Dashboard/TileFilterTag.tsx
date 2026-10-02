// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import FilterAltOutlinedIcon from "@mui/icons-material/FilterAltOutlined";
import { Box } from "@mui/material";
import type { Given } from "../../client";
import { usePublisherTheme } from "../../theme/ThemeContext";

/**
 * Display labels of the controls a tile answers to.
 *
 * `givenNames` undefined means discovery could not resolve the tile, so the
 * whole control row applies, as it does when the tile runs.
 */
export function tileFilterLabels(
   givenNames: readonly string[] | undefined,
   declared: readonly Given[],
): string[] {
   const byName = new Map(
      declared.flatMap((given) =>
         given.name ? [[given.name, given.label ?? given.name] as const] : [],
      ),
   );
   if (givenNames === undefined) return [...byName.values()];
   return givenNames.map((name) => byName.get(name) ?? name);
}

/** A small tag naming the filters that apply to a tile; nothing when none do. */
export function TileFilterTag({ labels }: { labels: readonly string[] }) {
   const { theme } = usePublisherTheme();
   if (labels.length === 0) return null;
   return (
      <Box
         data-testid="tile-filter-tag"
         title={`Filtered by ${labels.join(", ")}`}
         sx={{
            display: "inline-flex",
            alignItems: "center",
            gap: 0.5,
            maxWidth: "100%",
            mt: 1,
            px: 0.75,
            py: 0.125,
            border: theme.cardBorder,
            borderRadius: 1,
            fontSize: 11,
            lineHeight: 1.6,
            color: theme.tileTitle,
            fontFamily: theme.font.family,
            opacity: 0.8,
         }}
      >
         <FilterAltOutlinedIcon sx={{ fontSize: 12 }} />
         <Box
            component="span"
            sx={{
               overflow: "hidden",
               textOverflow: "ellipsis",
               whiteSpace: "nowrap",
            }}
         >
            {labels.join(", ")}
         </Box>
      </Box>
   );
}
