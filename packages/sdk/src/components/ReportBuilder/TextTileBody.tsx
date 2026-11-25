// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { Box } from "@mui/material";
import { TextTileFrame } from "../Report/ReportPage";
import { InlineMarkdown } from "./InlineMarkdown";
import type { TextTile } from "./document";

export const TEXT_TILE_PLACEHOLDER =
   "Write markdown: # heading, **bold**, - list";

/**
 * A text tile as the builder draws it: the reader's prose frame, its markdown edited in place.
 *
 * `sized`: the tile has a set height (a `rowspan`, or a placed report's rows), so prose taller
 * than it scrolls inside, as the reader scrolls it, rather than running on under the tiles
 * below. Left to flow, the frame is as tall as the prose and nothing clips.
 */
export function TextTileBody({
   tile,
   sized = false,
   onChange,
}: {
   tile: TextTile;
   sized?: boolean;
   onChange: (markdown: string) => void;
}) {
   const body = (
      // Overflow shown on the frame itself: the open editor's ring and hint stand a few
      // pixels outside the prose, which a clipping frame cut off.
      <TextTileFrame sx={{ overflow: "visible" }}>
         <InlineMarkdown
            markdown={tile.markdown}
            placeholder={TEXT_TILE_PLACEHOLDER}
            onCommit={onChange}
         />
      </TextTileFrame>
   );
   if (!sized) return body;
   return (
      // The scroller sits outside the frame, with the ring's few pixels of room folded in,
      // so the ring still shows and the prose still stops at the tile's edge.
      <Box
         data-testid="text-tile-scroller"
         sx={{ minHeight: 0, overflow: "auto", p: "4px", m: "-4px" }}
      >
         {body}
      </Box>
   );
}
