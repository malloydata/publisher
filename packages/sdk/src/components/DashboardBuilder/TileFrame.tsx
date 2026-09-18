// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import DragIndicatorIcon from "@mui/icons-material/DragIndicator";
import MoreVertIcon from "@mui/icons-material/MoreVert";
import { Box, IconButton, Typography } from "@mui/material";
import type { PointerEvent, ReactNode } from "react";
import { usePublisherTheme } from "../../theme/ThemeContext";
import { GRID_GAP_PX } from "../Dashboard/DashboardGrid";
import { TileCard, TileHeading } from "../Dashboard/TileCard";
import { tileKey, type DashboardTile } from "./document";
import { gapId } from "./layout";
import { GapDroppable, TileSortable } from "./sortable";

/**
 * Everything the builder draws AROUND a tile: the selection outline, the grip,
 * the menu button, the resize badge and the handle at the right edge. The tile
 * itself is `children` — the host's real `DashboardTile`, or a placeholder
 * saying what the tile will run.
 */
export function TileFrame({
   tile,
   index,
   selected,
   menuOpen,
   resizeSpan,
   columns,
   onSelect,
   onOpenMenu,
   onResizeStart,
   onResizeMove,
   onResizeEnd,
   children,
}: {
   tile: DashboardTile;
   index: number;
   selected: boolean;
   /** Whether this tile's menu is open, which keeps its button showing. */
   menuOpen: boolean;
   /** The width being previewed while THIS tile is resized. */
   resizeSpan: number | undefined;
   columns: number;
   onSelect: () => void;
   onOpenMenu: (anchor: HTMLElement) => void;
   onResizeStart: (event: PointerEvent<HTMLDivElement>) => void;
   onResizeMove: (event: PointerEvent<HTMLDivElement>) => void;
   onResizeEnd: (event: PointerEvent<HTMLDivElement>) => void;
   children: ReactNode;
}) {
   const { theme } = usePublisherTheme();
   return (
      <TileSortable id={tileKey(tile)} index={index}>
         {({ ref, handleRef, isDragSource }) => (
            <Box
               ref={ref}
               onClick={onSelect}
               // Selected on the press, so a tile reads as
               // held the moment it is. The drag itself is
               // the sensor's — see `sortable.tsx`.
               onPointerDown={(event) => {
                  if (event.button === 0) onSelect();
               }}
               aria-label={`Tile ${tile.name}`}
               aria-current={selected}
               sx={{
                  // Anchors the resize and drag handles to
                  // this tile.
                  position: "relative",
                  // Same again: this wrapper sits BETWEEN
                  // the grid item and the tile, so it has to
                  // pass the height on rather than shrink to
                  // the tile.
                  display: "grid",
                  // And the WIDTH the other way: it must
                  // follow the grid item down when a tile is
                  // narrowed, not stay as wide as the chart
                  // it holds. See the `minWidth: 0` note in
                  // `DashboardGrid`; this is the next link in
                  // that chain.
                  minWidth: 0,
                  // Says the card can be picked up. Content
                  // with a cursor of its own — a drill link,
                  // a scrollbar — still wins over its own
                  // pixels.
                  cursor: "grab",
                  borderRadius: 1,
                  // An outline rather than a border, and
                  // outside the tile rather than on it: the
                  // tile already has an edge of its own, and
                  // an outline neither doubles that edge nor
                  // takes up space, so selecting a tile
                  // cannot shift the layout being arranged.
                  outline: selected
                     ? `2px solid ${theme.drillLink}`
                     : `2px solid transparent`,
                  outlineOffset: 2,
                  transition:
                     "outline-color 120ms, opacity 120ms, box-shadow 120ms",
                  // The handles, grip and menu are invisible
                  // until wanted, and wanted is: the pointer
                  // over the tile, or the tile selected. A
                  // hover rule on the WRAPPER, so all three
                  // appear together rather than as the
                  // pointer finds tile.
                  // The library marks the tile in hand
                  // `data-dnd-dragging` and the copy it leaves
                  // in the flow `data-dnd-placeholder`, and
                  // mirrors every class and style of the one
                  // onto the other — so styling driven by
                  // React state landed on BOTH, and the tile
                  // under the pointer was as faded and dashed
                  // as the slot it was leaving. Styled by the
                  // attributes instead: the tile in hand is
                  // solid and lifted; the slot it will land
                  // in is the faded, dashed one — the pair every
                  // drag-and-drop grid draws. Doubled so they outrank
                  // the hover rule below on the tile in hand.
                  "&&[data-dnd-dragging]": {
                     opacity: 1,
                     outline: "none",
                     boxShadow: "0 12px 32px rgba(0, 0, 0, 0.22)",
                  },
                  "&&[data-dnd-placeholder]": {
                     opacity: 0.45,
                     outline: `2px dashed ${theme.drillLink}`,
                     boxShadow: "none",
                  },
                  "&:hover .builder-affordance, &:focus-within .builder-affordance":
                     { opacity: 1 },
                  // A hovered tile lifts, as a card does in any
                  // builder's edit mode: the one card that
                  // will respond to the pointer, told apart
                  // from the ones that will not.
                  "&:hover": {
                     outlineColor: selected
                        ? theme.drillLink
                        : theme.cardBorder,
                     boxShadow: "0 2px 10px rgba(0, 0, 0, 0.10)",
                  },
               }}
            >
               {children}

               {/* The grip. On every tile, including an
                inherited one — order is this file's
                `tiles=[…]` array, not anything on the view.
                The whole card starts a pointer drag; the grip
                is the sign of it and the library's HANDLE,
                where keyboard focus and the screen-reader
                instructions land: Space picks the tile up,
                the arrows move it, Escape puts it back. */}
               <Box
                  ref={handleRef}
                  className="builder-affordance"
                  aria-label={`Move ${tile.label ?? tile.name}`}
                  sx={{
                     position: "absolute",
                     top: "2px",
                     left: "2px",
                     display: "grid",
                     placeItems: "center",
                     width: "22px",
                     height: "22px",
                     borderRadius: "4px",
                     cursor: "grab",
                     touchAction: "none",
                     zIndex: 2,
                     color: theme.tileTitle,
                     bgcolor: theme.tile,
                     opacity: isDragSource || selected ? 0.9 : 0,
                     transition: "opacity 120ms",
                     "&:hover": { opacity: 1 },
                     "&:active": { cursor: "grabbing" },
                     "&:focus-visible": {
                        opacity: 1,
                        outline: `2px solid ${theme.drillLink}`,
                        outlineOffset: 1,
                     },
                  }}
               >
                  <DragIndicatorIcon sx={{ fontSize: 16 }} />
               </Box>

               {/* The tile's menu: its title and subtitle.
                Hidden while a resize badge sits in the same
                corner. A press here is never the start of a
                drag: the sensor refuses to activate from a
                button. */}
               {resizeSpan === undefined && (
                  <IconButton
                     className="builder-affordance"
                     size="small"
                     aria-label={`Settings for ${tile.label ?? tile.name}`}
                     onClick={(event) => {
                        event.stopPropagation();
                        onOpenMenu(event.currentTarget);
                     }}
                     sx={{
                        position: "absolute",
                        top: "2px",
                        right: "2px",
                        width: 22,
                        height: 22,
                        zIndex: 2,
                        color: theme.tileTitle,
                        bgcolor: theme.tile,
                        opacity: selected || menuOpen ? 0.9 : 0,
                        transition: "opacity 120ms",
                        "&:hover": {
                           opacity: 1,
                           bgcolor: theme.tile,
                        },
                     }}
                  >
                     <MoreVertIcon sx={{ fontSize: 16 }} />
                  </IconButton>
               )}

               {/* A tile's width and share of the dashboard,
                reported while you drag it, as the best builders do.
                Height is not ours to show, but the span and
                its share are exactly what a flow grid leaves
                you guessing at. */}
               {resizeSpan !== undefined && (
                  <Box
                     aria-hidden
                     sx={{
                        position: "absolute",
                        top: "6px",
                        right: "6px",
                        px: 0.75,
                        py: 0.25,
                        borderRadius: "4px",
                        bgcolor: theme.drillLink,
                        color: theme.tile,
                        fontSize: 11,
                        fontVariantNumeric: "tabular-nums",
                        zIndex: 4,
                        pointerEvents: "none",
                     }}
                  >
                     {resizeSpan} of {columns} ·{" "}
                     {Math.round((resizeSpan / columns) * 100)}%
                  </Box>
               )}

               {/* The right edge, draggable — but only on a
                tile whose tags this file owns. An inherited
                tile's tags live on the model's view, which
                the builder does not write, so a drag here
                could not be saved. Its own pointer handling
                stops the press reaching the sortable, and
                the sensor refuses a separator regardless. */}
               {tile.declaration.kind !== "inherited" && (
                  <Box
                     className="builder-affordance"
                     role="separator"
                     aria-orientation="vertical"
                     aria-label={`Resize ${tile.label ?? tile.name}`}
                     onPointerDown={onResizeStart}
                     onPointerMove={onResizeMove}
                     onPointerUp={onResizeEnd}
                     onPointerCancel={onResizeEnd}
                     sx={{
                        position: "absolute",
                        top: 0,
                        bottom: 0,
                        // Straddles the edge, so the target
                        // is a usable width without eating
                        // into the tile's content.
                        right: "-5px",
                        width: "10px",
                        cursor: "col-resize",
                        touchAction: "none",
                        zIndex: 1,
                        // Invisible until wanted: a rule down
                        // every tile edge would read as a
                        // table, and the tile already draws
                        // an edge of its own.
                        opacity: resizeSpan !== undefined || selected ? 1 : 0,
                        transition: "opacity 120ms",
                        "&:hover": { opacity: 1 },
                        "&::after": {
                           content: '""',
                           position: "absolute",
                           top: "50%",
                           left: "50%",
                           transform: "translate(-50%, -50%)",
                           width: "4px",
                           height: "28px",
                           borderRadius: "2px",
                           bgcolor: theme.drillLink,
                        },
                     }}
                  />
               )}
            </Box>
         )}
      </TileSortable>
   );
}

/**
 * No host-supplied tile: say what this one will run, so the surface is still
 * legible without a server.
 */
export function TilePlaceholder({ tile }: { tile: DashboardTile }) {
   const { theme } = usePublisherTheme();
   return (
      <TileCard sx={{ minHeight: 140 }}>
         <TileHeading
            title={tile.label ?? tile.name}
            subtitle={tile.subtitle}
         />
         <Typography
            variant="caption"
            sx={{ display: "block", color: theme.tileTitle, opacity: 0.7 }}
         >
            {tile.source} → {tile.name}
         </Typography>
      </TileCard>
   );
}

/** The empty end of a row, made a drop target while a drag is live. */
export function GapTarget({ after }: { after: string }) {
   const { theme } = usePublisherTheme();
   return (
      <GapDroppable id={gapId(after)} after={after}>
         {({ ref, isDropTarget }) => (
            <Box
               ref={ref}
               aria-hidden
               sx={{
                  minHeight: 48,
                  borderRadius: 1,
                  border: `1px dashed ${theme.drillLink}`,
                  // Tinted with the same hue as the dashed edge, so the fill
                  // and the border read as one affordance lighting up. It was
                  // `theme.tile`, the CARD colour, which is the one value
                  // guaranteed to match whatever this sits on: a near-no-op
                  // before the card went white, and an exact one after.
                  bgcolor: isDropTarget
                     ? `color-mix(in srgb, ${theme.drillLink} 12%, transparent)`
                     : "transparent",
                  opacity: isDropTarget ? 0.95 : 0.4,
                  transition: "opacity 120ms, background-color 120ms",
               }}
            />
         )}
      </GapDroppable>
   );
}

/**
 * Column guides, drawn only during a gesture: a flow grid is invisible until
 * you are trying to land on it.
 */
export function GridGuides({ columns }: { columns: number }) {
   const { theme } = usePublisherTheme();
   return (
      <Box
         aria-hidden
         sx={{
            position: "absolute",
            inset: 0,
            pointerEvents: "none",
            zIndex: 3,
            display: "grid",
            gridTemplateColumns: `repeat(${columns}, minmax(0, 1fr))`,
            gap: `${GRID_GAP_PX}px`,
         }}
      >
         {Array.from({ length: columns }, (_, column) => (
            <Box
               key={column}
               sx={{
                  borderLeft: `1px dashed ${theme.drillLink}`,
                  borderRight:
                     column === columns - 1
                        ? `1px dashed ${theme.drillLink}`
                        : "none",
                  opacity: 0.35,
               }}
            />
         ))}
      </Box>
   );
}
