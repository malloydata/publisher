// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { Popover, Stack, TextField, Typography } from "@mui/material";
import { useEffect, useState } from "react";
import { usePublisherTheme } from "../../theme/ThemeContext";
import type { DashboardTile } from "./document";

/**
 * A tile's own settings, on the tile.
 *
 * The properties panel this replaces sat under the page, so editing a tile
 * meant scrolling away from it. This is a popover off the tile's menu button:
 * its title and subtitle. Its row is set by dragging it — a drop into the empty
 * end of a row is the whole of what "start a new row" meant — and its card is
 * the reader's to decide, so neither is a toggle here. Which controls it
 * answers to is NOT here either: filters are configured in one place, the strip
 * under the header, whose window maps a control across tiles.
 *
 * Edits are committed ONCE, on close, as a single history entry. Per-keystroke
 * commits would put a document in the undo stack for every letter typed into a
 * title, and undo would then walk back through the word.
 */
export interface TileMenuProps {
   anchor: HTMLElement | null;
   tile: DashboardTile | undefined;
   onClose: () => void;
   /** Apply the edited tile. Called once, on close, only when something changed. */
   onCommit: (next: DashboardTile) => void;
}

export function TileMenu({ anchor, tile, onClose, onCommit }: TileMenuProps) {
   const { theme } = usePublisherTheme();
   const [draft, setDraft] = useState<DashboardTile | undefined>(undefined);

   useEffect(() => {
      if (anchor && tile) setDraft(structuredClone(tile));
   }, [anchor, tile]);

   const close = () => {
      if (draft && tile && JSON.stringify(draft) !== JSON.stringify(tile))
         onCommit(draft);
      onClose();
   };

   const patch = (change: (t: DashboardTile) => void) =>
      setDraft((previous) => {
         if (!previous) return previous;
         const next = structuredClone(previous);
         change(next);
         return next;
      });

   const editable =
      draft !== undefined && draft.declaration.kind !== "inherited";

   return (
      <Popover
         open={anchor !== null && draft !== undefined}
         anchorEl={anchor}
         onClose={close}
         anchorOrigin={{ vertical: "bottom", horizontal: "right" }}
         transformOrigin={{ vertical: "top", horizontal: "right" }}
         slotProps={{ paper: { sx: { width: 320, p: 2 } } }}
      >
         {draft && (
            <Stack sx={{ gap: 1.5 }}>
               <Typography
                  variant="overline"
                  sx={{ color: theme.tileTitle, lineHeight: 1.5 }}
               >
                  {draft.source} → {draft.name}
               </Typography>

               {editable ? (
                  <>
                     <TextField
                        size="small"
                        label="Title"
                        value={draft.label ?? ""}
                        autoFocus
                        inputProps={{ "aria-label": "Tile title" }}
                        onChange={(event) =>
                           patch((t) => {
                              const v = event.target.value;
                              if (v === "") delete t.label;
                              else t.label = v;
                           })
                        }
                     />
                     <TextField
                        size="small"
                        label="Subtitle"
                        value={draft.subtitle ?? ""}
                        inputProps={{ "aria-label": "Tile subtitle" }}
                        onChange={(event) =>
                           patch((t) => {
                              const v = event.target.value;
                              if (v === "") delete t.subtitle;
                              else t.subtitle = v;
                           })
                        }
                     />
                  </>
               ) : (
                  <Typography variant="body2" sx={{ color: theme.tileTitle }}>
                     Declared on its source, so its title and layout are set in
                     the model. It can still be moved.
                  </Typography>
               )}
            </Stack>
         )}
      </Popover>
   );
}
