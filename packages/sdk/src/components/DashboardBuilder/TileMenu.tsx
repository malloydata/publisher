// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import {
   Button,
   Divider,
   Popover,
   Stack,
   TextField,
   Typography,
} from "@mui/material";
import { useDraft } from "./useDraft";
import { usePublisherTheme } from "../../theme/ThemeContext";
import type { DashboardTile } from "./document";

/**
 * A tile's own settings, on the tile: a popover off its menu button, so
 * editing it never means scrolling away from it. Its title, subtitle, width
 * presets, clickable cells and removal. Its row is set by dragging it — a drop
 * into the empty end of a row is what "start a new row" means — and its card
 * is the reader's to decide, so neither is a toggle here. Which controls it
 * answers to is not here either: filters are configured in one place, the
 * strip under the header. Edits commit on close (`useDraft`).
 */
export interface TileMenuProps {
   anchor: HTMLElement | null;
   tile: DashboardTile | undefined;
   onClose: () => void;
   /** Apply the edited tile. Called once, on close, only when something changed. */
   onCommit: (next: DashboardTile) => void;
   /** Take the tile off the dashboard. Offered on every tile: order is this file's. */
   onRemove: () => void;
   /** The grid's width, which the width presets are fractions of. */
   columns: number;
   /** Open the clickable-cells window for this tile's source. */
   onDrills: () => void;
}

export function TileMenu({
   anchor,
   tile,
   onClose,
   onCommit,
   onRemove,
   columns,
   onDrills,
}: TileMenuProps) {
   const { theme } = usePublisherTheme();
   const { draft, patch, close, discard } = useDraft(
      tile,
      anchor !== null,
      onCommit,
      onClose,
   );

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
                     {/* Width presets, as fractions of this grid. A
                         tile's width is otherwise a drag, and a drag cannot
                         say "a third". */}
                     <Stack
                        direction="row"
                        sx={{ gap: 0.5, alignItems: "center" }}
                     >
                        <Typography
                           variant="caption"
                           sx={{ color: theme.tileTitle, mr: 0.5 }}
                        >
                           Width
                        </Typography>
                        {(
                           [
                              ["Full", 1],
                              ["½", 2],
                              ["⅓", 3],
                              ["¼", 4],
                           ] as const
                        ).map(([label, share]) => {
                           const span = Math.max(
                              1,
                              Math.round(columns / share),
                           );
                           const active = (draft.colspan ?? 1) === span;
                           return (
                              <Button
                                 key={label}
                                 size="small"
                                 variant={active ? "contained" : "outlined"}
                                 aria-label={`Width ${label}`}
                                 aria-pressed={active}
                                 onClick={() =>
                                    patch((t) => {
                                       t.colspan = span;
                                    })
                                 }
                                 sx={{ minWidth: 40, px: 1 }}
                              >
                                 {label}
                              </Button>
                           );
                        })}
                     </Stack>
                  </>
               ) : (
                  <Typography variant="body2" sx={{ color: theme.tileTitle }}>
                     Declared on its source, so its title and layout are set in
                     the model. It can still be moved.
                  </Typography>
               )}
               {draft?.declaration.kind === "opaque" && (
                  <Typography variant="body2" sx={{ color: theme.tileTitle }}>
                     Its body is {draft.declaration.why}, so a filter has no
                     single place to go. Everything else here is editable.
                  </Typography>
               )}
               <Divider />
               <Stack direction="row" sx={{ justifyContent: "space-between" }}>
                  {editable ? (
                     <Button
                        size="small"
                        onClick={() => {
                           close();
                           onDrills();
                        }}
                     >
                        Clickable cells…
                     </Button>
                  ) : (
                     <span />
                  )}
                  <Button
                     color="error"
                     size="small"
                     onClick={() => {
                        discard();
                        onRemove();
                     }}
                  >
                     Remove tile
                  </Button>
               </Stack>
            </Stack>
         )}
      </Popover>
   );
}
