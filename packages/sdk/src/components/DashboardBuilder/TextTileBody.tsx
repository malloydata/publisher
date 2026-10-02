// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { TileCard } from "../Dashboard/TileCard";
import { InlineMarkdown } from "./InlineMarkdown";
import type { TextTile } from "./document";

export const TEXT_TILE_PLACEHOLDER =
   "Write markdown: # heading, **bold**, - list";

/** A text tile as the builder draws it: its markdown, edited in place. */
export function TextTileBody({
   tile,
   onChange,
}: {
   tile: TextTile;
   onChange: (markdown: string) => void;
}) {
   return (
      <TileCard sx={{ minHeight: 72 }}>
         <InlineMarkdown
            markdown={tile.markdown}
            placeholder={TEXT_TILE_PLACEHOLDER}
            onCommit={onChange}
         />
      </TileCard>
   );
}
