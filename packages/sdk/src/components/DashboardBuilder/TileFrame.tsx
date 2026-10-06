// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import AddIcon from "@mui/icons-material/Add";
import { Box, IconButton, SvgIcon, Typography } from "@mui/material";
import type { PointerEvent, ReactNode } from "react";
import type { Theme } from "@mui/material";
import type { SystemStyleObject } from "@mui/system";
import { MOTION_FAST, reducedMotionSx } from "../../theme/motion";
import { usePublisherTheme } from "../../theme/ThemeContext";
import type { ResolvedTheme } from "../../theme/types";
import {
   BARE_RING_OFFSET_PX,
   GRID_GAP_PX,
   NOTEBOOK_GAP_PX,
} from "../Dashboard/DashboardGrid";
import {
   TileCard,
   TileHeading,
   type TileChrome,
   type TileHeadingSlots,
} from "../Dashboard/TileCard";
import { tileDisplayTitle } from "./tileDisplayTitle";
import {
   tileKey,
   tileLabel,
   type DashboardTile,
   type QueryTile,
} from "./document";
import { gapId } from "./layout";
import { GapDroppable, NO_DRAG, TileSortable } from "./sortable";

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
   bare = false,
   onInsertAfter,
   onSelect,
   onOpenMenu,
   resizing = false,
   width,
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
   /**
    * The tile has no card (a notebook's): its ring is drawn clear of the
    * content and its menu sits on the ring's top edge, so neither touches the
    * text, and the tile reads where the reader draws it.
    */
   bare?: boolean;
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
   /** The tile's width and the grid's, and a step of one column: the edge's keyboard route. */
   width?: {
      span: number;
      columns: number;
      onStep: (to: number) => void;
   };
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
               // A named group, so the name is announced: a label on a plain
               // element is not.
               role="group"
               aria-label={`Tile ${tile.name}`}
               aria-current={selected}
               // Keyboard focus anywhere in the tile selects it, as a press
               // does, so the arrow-key nudge and the menu follow the keyboard.
               onFocus={onSelect}
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
                  ...selectionSx(theme, { selected, flash, bare }),
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
                     boxShadow: theme.shadow.drag,
                  },
                  "&&[data-dnd-placeholder]": {
                     opacity: 0.45,
                     outline: `2px dashed ${theme.accent}`,
                     boxShadow: "none",
                  },
                  "&:hover .builder-affordance, &:focus-within .builder-affordance":
                     { opacity: 1 },
                  // No hover on a touch screen: the affordances stand.
                  "@media (hover: none)": {
                     "& .builder-affordance": { opacity: 1 },
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
                     // Inside a card's corner, clear of its edge. A bare tile
                     // has no corner to sit in, so the menu straddles the top
                     // of its ring, clear of the text under it.
                     top: bare ? `${-(BARE_RING_OFFSET_PX + 13)}px` : "8px",
                     right: bare ? 0 : "8px",
                     ...(bare && {
                        border: theme.cardBorder,
                        borderRadius: 1,
                     }),
                     // A 24px hit target at least, whatever the glyph.
                     width: 28,
                     height: 24,
                     zIndex: 2,
                     color: theme.tileTitle,
                     bgcolor: theme.tile,
                     opacity: selected || menuOpen ? 0.9 : 0,
                     transition: `opacity ${MOTION_FAST}`,
                     ...reducedMotionSx,
                     "&:hover": {
                        opacity: 1,
                        bgcolor: theme.tile,
                     },
                  }}
               >
                  <SpreadDotsIcon />
               </IconButton>

               {/* The right edge, which sets the width: dragged, or focused and
                stepped with the arrow keys a column at a time (Home and End go
                to one column and the full grid). Marked so the move sensor
                never takes a press on it. */}
               {onResizeStart && (
                  <Box
                     className="builder-affordance"
                     {...{ [NO_DRAG]: "" }}
                     role="separator"
                     tabIndex={width ? 0 : -1}
                     aria-orientation="vertical"
                     aria-label={`Width of ${tileLabel(tile)}`}
                     {...(width
                        ? {
                             "aria-valuenow": width.span,
                             "aria-valuemin": 1,
                             "aria-valuemax": width.columns,
                             "aria-valuetext": `${width.span} of ${width.columns} columns`,
                          }
                        : {})}
                     onKeyDown={(event) => {
                        if (!width) return;
                        const to =
                           event.key === "ArrowRight"
                              ? width.span + 1
                              : event.key === "ArrowLeft"
                                ? width.span - 1
                                : event.key === "Home"
                                  ? 1
                                  : event.key === "End"
                                    ? width.columns
                                    : undefined;
                        if (to === undefined) return;
                        // The builder's own arrow-key nudge must not step it again.
                        event.preventDefault();
                        event.stopPropagation();
                        width.onStep(Math.min(Math.max(to, 1), width.columns));
                     }}
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
                        transition: `opacity ${MOTION_FAST}`,
                        ...reducedMotionSx,
                        "&:hover": { opacity: 1 },
                        "&:focus-visible": {
                           opacity: 1,
                           outline: `2px solid ${theme.accent}`,
                           outlineOffset: 1,
                           borderRadius: "3px",
                        },
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
                        // Centred in the gap below the tile, not on its edge: on
                        // a one-line notebook text tile, an edge-centred button
                        // covered the middle of the text it sits under.
                        bottom: `-${(bare ? NOTEBOOK_GAP_PX : GRID_GAP_PX) / 2 + 12}px`,
                        left: "50%",
                        transform: "translateX(-50%)",
                        width: 24,
                        height: 24,
                        zIndex: 3,
                        color: theme.tileTitle,
                        bgcolor: theme.tile,
                        border: theme.cardBorder,
                        opacity: 0,
                        transition: `opacity ${MOTION_FAST}`,
                        ...reducedMotionSx,
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

/** How far the selection ring stands outside what it selects: its 2px offset plus its 2px width. */
export const SELECTION_RING_PX = 4;

/**
 * The builder's selection look, for anything a person can select — a tile,
 * the description: an outline outside the element (so selecting moves no
 * layout), solid in the accent when selected, lifting on hover the way a card
 * does in any builder's edit mode, and a brief halo when an undo or redo just
 * changed it. One definition, so the description selects exactly as a tile.
 */
export const selectionSx = (
   theme: ResolvedTheme,
   {
      selected,
      flash = false,
      bare = false,
   }: { selected: boolean; flash?: boolean; bare?: boolean },
): SystemStyleObject<Theme> => {
   const ring = selected || flash ? theme.accent : "transparent";
   const hoverRing = selected
      ? theme.accent
      : theme.cardBorder.replace(/^1px solid /, "");
   const transition = `outline-color ${MOTION_FAST}, border-color ${MOTION_FAST}, opacity ${MOTION_FAST}, box-shadow ${MOTION_FAST}`;
   if (bare)
      // Bare content has no padding of its own, so the ring stands off it by
      // the clear space a card's text has, without moving the text from where
      // the reader draws it. Drawn as its own layer rather than an offset
      // outline: an outline's corners grow by the offset, and the ring would
      // read rounder than every card on the page. No lift: there is no
      // surface to lift, only the ring.
      return {
         borderRadius: 1,
         "&::before": {
            content: '""',
            position: "absolute",
            inset: `${-BARE_RING_OFFSET_PX}px`,
            borderRadius: 1,
            border: `2px solid ${ring}`,
            pointerEvents: "none",
            transition,
            ...reducedMotionSx,
         },
         "&:hover::before": { borderColor: hoverRing },
      };
   // A card carries its own padding, so its ring hugs its edge.
   return {
      borderRadius: 1,
      outline: `2px solid ${ring}`,
      outlineOffset: 2,
      ...(flash && {
         boxShadow: `0 0 0 6px color-mix(in srgb, ${theme.accent} 25%, transparent)`,
      }),
      transition,
      ...reducedMotionSx,
      "&:hover": {
         outlineColor: hoverRing,
         boxShadow: theme.shadow.lift,
      },
   };
};

/**
 * No host-supplied tile: say what this one will run, so the surface is still
 * legible without a server.
 */
export function TilePlaceholder({
   tile,
   heading,
   note,
   chrome = "card",
}: {
   tile: QueryTile;
   heading?: TileHeadingSlots;
   /** Why there is no preview, in place of an empty body. */
   note?: string;
   /** The document's tile chrome, as the reader draws it. */
   chrome?: TileChrome;
}) {
   const { theme } = usePublisherTheme();
   return (
      <TileCard chrome={chrome}>
         <TileHeading
            title={heading?.title ?? tileDisplayTitle(tile)}
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
                  transition: `opacity ${MOTION_FAST}, background-color ${MOTION_FAST}`,
                  ...reducedMotionSx,
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
