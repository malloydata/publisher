// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { Box, Typography } from "@mui/material";
import { usePublisherTheme } from "../../theme/ThemeContext";
import { TileCard } from "../Dashboard/TileCard";
import { Prose } from "../Prose";
import type { TextTile } from "./document";

export const TEXT_TILE_PLACEHOLDER =
   "Write markdown: # heading, **bold**, - list";

/** A text tile as the builder draws it: its markdown, or a hint while it is empty. */
export function TextTileBody({ tile }: { tile: TextTile }) {
   const { theme } = usePublisherTheme();
   const empty = tile.markdown.trim() === "";
   return (
      <TileCard sx={{ minHeight: 72 }}>
         {empty ? (
            <Typography
               variant="body2"
               sx={{ color: theme.tileTitle, opacity: 0.6 }}
            >
               {TEXT_TILE_PLACEHOLDER}
            </Typography>
         ) : (
            <Box
               // A link in a tile being arranged is not a way out of the builder.
               onClick={(event) => {
                  if ((event.target as HTMLElement).closest("a"))
                     event.preventDefault();
               }}
            >
               <Prose variant="document">{tile.markdown}</Prose>
            </Box>
         )}
      </TileCard>
   );
}
