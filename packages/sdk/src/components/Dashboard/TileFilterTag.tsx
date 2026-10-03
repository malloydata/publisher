// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import FilterAltOffOutlinedIcon from "@mui/icons-material/FilterAltOffOutlined";
import { Box } from "@mui/material";
import type { Given } from "../../client";
import { usePublisherTheme } from "../../theme/ThemeContext";

/** Past this many, the chip counts the filters instead of naming them. */
const NAMED_LIMIT = 3;

/**
 * Display labels of the controls on the page that a tile does NOT read.
 *
 * `givenNames` undefined means discovery could not resolve the tile, so the
 * whole control row applies, as it does when the tile runs: nothing to warn of.
 */
export function tileIgnoredFilterLabels(
   givenNames: readonly string[] | undefined,
   declared: readonly Given[],
): string[] {
   if (givenNames === undefined) return [];
   const reads = new Set(givenNames);
   return declared.flatMap((given) =>
      given.name && !reads.has(given.name) ? [given.label ?? given.name] : [],
   );
}

function joinOr(labels: readonly string[]): string {
   return labels.length < 2
      ? labels.join("")
      : `${labels.slice(0, -1).join(", ")} or ${labels[labels.length - 1]}`;
}

/** A small amber chip naming the page's filters a tile ignores; nothing when it reads them all. */
export function TileFilterTag({ ignored }: { ignored: readonly string[] }) {
   const { theme } = usePublisherTheme();
   if (ignored.length === 0) return null;
   const text =
      ignored.length > NAMED_LIMIT
         ? `Doesn't respond to ${ignored.length} filters`
         : `Doesn't respond to ${ignored.join(", ")}`;
   return (
      <Box
         data-testid="tile-filter-tag"
         title={`This tile's query never reads ${joinOr(ignored)}, so changing ${ignored.length === 1 ? "it" : "them"} won't change this tile`}
         sx={{
            display: "inline-flex",
            alignItems: "center",
            gap: 0.5,
            maxWidth: "100%",
            mb: 1,
            alignSelf: "flex-start",
            px: 0.75,
            py: 0.125,
            border: 1,
            borderColor: "warning.main",
            borderRadius: 1,
            fontSize: 11,
            lineHeight: 1.6,
            color: "warning.main",
            fontFamily: theme.font.family,
         }}
      >
         <FilterAltOffOutlinedIcon sx={{ fontSize: 12 }} />
         <Box
            component="span"
            sx={{
               overflow: "hidden",
               textOverflow: "ellipsis",
               whiteSpace: "nowrap",
            }}
         >
            {text}
         </Box>
      </Box>
   );
}
