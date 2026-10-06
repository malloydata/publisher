// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import AddIcon from "@mui/icons-material/Add";
import { Box, IconButton, SvgIcon, Typography } from "@mui/material";
import type { PointerEvent, ReactNode } from "react";
import { usePublisherTheme } from "../../theme/ThemeContext";
import { GRID_GAP_PX } from "../Dashboard/DashboardGrid";
import {
   TileCard,
   TileHeading,
   type TileHeadingSlots,
} from "../Dashboard/TileCard";
import {
   tileKey,
   tileLabel,
   type DashboardTile,
   type QueryTile,
} from "./document";
import { gapId } from "./layout";
import { GapDroppable, TileSortable } from "./sortable";

/**
 * Everything the builder draws AROUND a tile: the selection outline, the menu
 * button, the handle at the right edge and, in a notebook, the insert button. The tile
 * itself is `children` — the host's real `DashboardTile`, or a placeholder
 * saying what the tile will run.
 */
export function TileFrame({
   tile,
   index,
   selected,
   flash = false,
   menuOpen,
   onInsertAfter,
   onSelect,
   onOpenMenu,
   resizing = false,
   onResizeStart,
   onResizeMove,
   onResizeEnd,
   children,
}: {
   tile: DashboardTile;
   index: number;
   selected: boolean;
   /** Briefly lit: the tile an undo or redo just changed. */
   flash?: boolean;
   /** Whether this tile's menu is open, which keeps its button showing. */
   menuOpen: boolean;
   /** Offers a "+" on the bottom edge that adds a tile after this one; set where tiles stack in one column. */
   onInsertAfter?: () => void;
   onSelect: () => void;
   onOpenMenu: (anchor: HTMLElement) => void;
   /** This tile's edge is being dragged. */
   resizing?: boolean;
   /** Dragging the right edge sets the width; absent, the tile has no edge handle (one column, or tags the file does not own). */
   onResizeStart?: (event: PointerEvent<HTMLDivElement>) => void;
   onResizeMove?: (event: PointerEvent<HTMLDivElement>) => void;
   onResizeEnd?: (event: PointerEvent<HTMLDivElement>) => void;
   children: ReactNode;
}) {
   const { theme } = usePublisherTheme();
   return (
      <TileSortable id={tileKey(tile)} index={index}>
         {({ ref, handleRef }) => (
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
               data-tile-key={tileKey(tile)}
               data-flash={flash || undefined}
               sx={{
                  // Anchors the menu, edge and insert buttons to
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
                     ? `2px solid ${theme.accent}`
                     : `2px solid transparent`,
                  outlineOffset: 2,
                  ...(flash && {
                     outlineColor: theme.accent,
                     boxShadow: `0 0 0 6px color-mix(in srgb, ${theme.accent} 25%, transparent)`,
                  }),
                  transition:
                     "outline-color 120ms, opacity 120ms, box-shadow 120ms",
                  // The edge handle and menu are invisible
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
                     outline: `2px dashed ${theme.accent}`,
                     boxShadow: "none",
                  },
                  "&:hover .builder-affordance, &:focus-within .builder-affordance":
                     { opacity: 1 },
                  // A hovered tile lifts, as a card does in any
                  // builder's edit mode: the one card that
                  // will respond to the pointer, told apart
                  // from the ones that will not.
                  "&:hover": {
                     ...TILE_HOVER(theme),
                     ...(selected && { outlineColor: theme.accent }),
                  },
               }}
            >
               {children}

               {/* The library's HANDLE, where keyboard focus and the
                screen-reader instructions land: Space picks the tile up, the
                arrows move it, Escape puts it back. Visually hidden: a pointer
                drags the whole card, and the open hand over it says so. */}
               <Box
                  ref={handleRef}
                  aria-label={`Move ${tileLabel(tile)}`}
                  sx={{
                     position: "absolute",
                     width: "1px",
                     height: "1px",
                     overflow: "hidden",
                     clipPath: "inset(50%)",
                     whiteSpace: "nowrap",
                  }}
               />

               {/* The tile's menu: its chart and drill-through.
                A press here is never the start of a drag: the
                sensor refuses to activate from a button. */}
               <IconButton
                  className="builder-affordance"
                  size="small"
                  aria-label={`Settings for ${tileLabel(tile)}`}
                  onClick={(event) => {
                     event.stopPropagation();
                     onOpenMenu(event.currentTarget);
                  }}
                  sx={{
                     position: "absolute",
                     top: "2px",
                     right: "2px",
                     width: 28,
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
                  <SpreadDotsIcon />
               </IconButton>

               {/* The right edge, draggable to set the width. Its own
                pointer handling stops the press reaching the sortable, and
                the sensor refuses a separator regardless. */}
               {onResizeStart && (
                  <Box
                     className="builder-affordance"
                     role="separator"
                     aria-orientation="vertical"
                     aria-label={`Resize ${tileLabel(tile)}`}
                     onPointerDown={onResizeStart}
                     onPointerMove={onResizeMove}
                     onPointerUp={onResizeEnd}
                     onPointerCancel={onResizeEnd}
                     onClick={(event) => event.stopPropagation()}
                     sx={{
                        position: "absolute",
                        top: 0,
                        bottom: 0,
                        // Straddles the edge, so the target is a usable width
                        // without eating into the tile's content.
                        right: "-5px",
                        width: "10px",
                        cursor: "col-resize",
                        touchAction: "none",
                        zIndex: 2,
                        // Invisible until wanted: a rule down every tile edge
                        // would read as a table.
                        opacity: resizing || selected ? 1 : 0,
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
                           bgcolor: theme.accent,
                        },
                     }}
                  />
               )}

               {onInsertAfter && (
                  <IconButton
                     className="builder-affordance"
                     size="small"
                     aria-label={`Insert tile after ${tileLabel(tile)}`}
                     onClick={(event) => {
                        event.stopPropagation();
                        onInsertAfter();
                     }}
                     sx={{
                        position: "absolute",
                        bottom: "-18px",
                        left: "50%",
                        transform: "translateX(-50%)",
                        width: 20,
                        height: 20,
                        zIndex: 3,
                        color: theme.tileTitle,
                        bgcolor: theme.tile,
                        border: theme.cardBorder,
                        opacity: 0,
                        transition: "opacity 120ms",
                        "&:hover, &:focus-visible": {
                           opacity: 1,
                           bgcolor: theme.tile,
                        },
                     }}
                  >
                     <AddIcon sx={{ fontSize: 14 }} />
                  </IconButton>
               )}
            </Box>
         )}
      </TileSortable>
   );
}

/**
 * How anything in the builder that responds to the pointer says so on hover:
 * a lift and an edge, the way a tile does. Shared, so the page's description
 * highlights exactly as a tile or a text block beside it does.
 */
export const TILE_HOVER = (theme: { cardBorder: string }) => ({
   outlineColor: theme.cardBorder,
   boxShadow: "0 2px 10px rgba(0, 0, 0, 0.10)",
});

/**
 * No host-supplied tile: say what this one will run, so the surface is still
 * legible without a server.
 */
export function TilePlaceholder({
   tile,
   heading,
   note,
}: {
   tile: QueryTile;
   heading?: TileHeadingSlots;
   /** Why there is no preview, in place of an empty body. */
   note?: string;
}) {
   const { theme } = usePublisherTheme();
   return (
      <TileCard sx={{ minHeight: 140 }}>
         <TileHeading
            title={heading?.title ?? tile.label ?? tile.name}
            subtitle={heading ? heading.subtitle : tile.subtitle}
         />
         {note && (
            <Typography variant="body2" sx={{ color: theme.tileTitle, py: 2 }}>
               {note}
            </Typography>
         )}
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
                  border: `1px dashed ${theme.accent}`,
                  // Tinted with the same hue as the dashed edge, so the fill
                  // and the border read as one affordance lighting up. It was
                  // `theme.tile`, the CARD colour, which is the one value
                  // guaranteed to match whatever this sits on: a near-no-op
                  // before the card went white, and an exact one after.
                  bgcolor: isDropTarget
                     ? `color-mix(in srgb, ${theme.accent} 12%, transparent)`
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
                  borderLeft: `1px dashed ${theme.accent}`,
                  borderRight:
                     column === columns - 1
                        ? `1px dashed ${theme.accent}`
                        : "none",
                  opacity: 0.35,
               }}
            />
         ))}
      </Box>
   );
}

/**
 * The tile menu's ⋯, with its dots spread wider than the stock icon's so it
 * reads as three dots rather than a dash at this size.
 */
function SpreadDotsIcon() {
   return (
      <SvgIcon sx={{ fontSize: 18 }} viewBox="0 0 24 24">
         <circle cx="3.5" cy="12" r="2" />
         <circle cx="12" cy="12" r="2" />
         <circle cx="20.5" cy="12" r="2" />
      </SvgIcon>
   );
}
