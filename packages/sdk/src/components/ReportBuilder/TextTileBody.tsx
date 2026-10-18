// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { TextTileFrame } from "../Report/ReportPage";
import { InlineMarkdown } from "./InlineMarkdown";
import type { TextTile } from "./document";

export const TEXT_TILE_PLACEHOLDER =
   "Write markdown: # heading, **bold**, - list";

/** A text tile as the builder draws it: the reader's prose frame, its markdown edited in place. */
export function TextTileBody({
   tile,
   onChange,
}: {
   tile: TextTile;
   onChange: (markdown: string) => void;
}) {
   return (
      // Overflow shown: the open editor's ring and hint stand a few pixels
      // outside the prose, which a clipping frame cut off.
      <TextTileFrame sx={{ overflow: "visible" }}>
         <InlineMarkdown
            markdown={tile.markdown}
            placeholder={TEXT_TILE_PLACEHOLDER}
            onCommit={onChange}
         />
      </TextTileFrame>
   );
}
