// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { TileCard, type TileChrome } from "../Dashboard/TileCard";
import { InlineMarkdown } from "./InlineMarkdown";
import type { TextTile } from "./document";

export const TEXT_TILE_PLACEHOLDER =
   "Write markdown: # heading, **bold**, - list";

/** A text tile as the builder draws it: its markdown, edited in place. */
export function TextTileBody({
   tile,
   onChange,
   chrome = "card",
}: {
   tile: TextTile;
   onChange: (markdown: string) => void;
   /** The document's tile chrome: a card on a dashboard, bare on a notebook, as the reader draws it. */
   chrome?: TileChrome;
}) {
   return (
      <TileCard chrome={chrome} sx={chrome === "card" ? { minHeight: 72 } : {}}>
         <InlineMarkdown
            markdown={tile.markdown}
            placeholder={TEXT_TILE_PLACEHOLDER}
            onCommit={onChange}
         />
      </TileCard>
   );
}
