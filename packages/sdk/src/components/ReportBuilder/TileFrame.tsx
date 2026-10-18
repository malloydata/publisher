// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { reportClasses } from "../../theme/classes";
import AddIcon from "@mui/icons-material/Add";
import { Box, IconButton, SvgIcon, Typography } from "@mui/material";
import type { PointerEvent, ReactNode } from "react";
import type { Theme } from "@mui/material";
import type { SystemStyleObject } from "@mui/system";
import { MOTION_FAST, reducedMotionSx } from "../../theme/motion";
import { edgeColor, hoverEdgeColor } from "../controlTokens";
import {
   REMOVE_BADGE_CLASS,
   RemoveBadge,
   removeBadgeRevealSx,
} from "../RemoveBadge";
import { usePublisherTheme } from "../../theme/ThemeContext";
import type { ResolvedTheme } from "../../theme/types";
import {
   BARE_RING_OFFSET_PX,
   GRID_GAP_PX,
   MAX_TILE_ROWS,
   TILE_ROW_PX,
} from "../Dashboard/DashboardGrid";
import {
   TILE_HEADING_CLASS,
   TileCard,
   TileHeading,
   type TileChrome,
   type TileHeadingSlots,
} from "../Dashboard/TileCard";
import { tileDisplayTitle } from "./tileDisplayTitle";
import {
   isTextTile,
   tileExpression,
   tileKey,
   tileLabel,
   type DashboardTile,
   type QueryTile,
} from "./document";
import { gapId } from "./layout";
import { GapDroppable, NO_DRAG, TileSortable } from "./sortable";
import { widthLabel } from "./widthLabel";

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
   onRemove,
   removeBlocked,
   resizing = false,
   width,
   onResizeStart,
   onResizeMove,
   onResizeEnd,
   heightEdge,
   fixed = false,
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
   /** Take the tile off the report, from the × on its corner. */
   onRemove: () => void;
   /** Why it cannot be removed (a saved report keeps one tile); the × says so instead. */
   removeBlocked?: string;
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
      /** The widest it may go, when that is less than the grid: a placed tile stops at the right edge. */
      max?: number;
      onStep: (to: number) => void;
   };
   /**
    * The bottom edge, which sets the height in grid rows: dragged, or focused
    * and stepped with the arrow keys. Delete or a double-click puts the tile
    * back to fitting its content. Absent, the tile has no bottom handle.
    */
   /** The tile cannot be moved now (the grid is drawn as one column). */
   fixed?: boolean;
   heightEdge?: {
      /** Its rows: the set height, or the drag's preview; absent on auto, as tall as its row. */
      rows: number | undefined;
      /** This edge is being dragged. */
      dragging: boolean;
      /** The fewest rows offered: a chart's floor, or prose's. */
      min: number;
      onStart: (event: PointerEvent<HTMLDivElement>) => void;
      onMove: (event: PointerEvent<HTMLDivElement>) => void;
      onEnd: (event: PointerEvent<HTMLDivElement>) => void;
      /** A keyboard step, or `undefined` to fit the content again. */
      onStep: (to: number | undefined) => void;
   };
   children: ReactNode;
}) {
   const { theme } = usePublisherTheme();
   const ui = theme.ui;
   return (
      <TileSortable id={tileKey(tile)} index={index} disabled={fixed}>
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
               className={reportClasses.builderTile}
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
                  // A set height holds the row too, so the tile
                  // shrinks to it rather than the row growing to
                  // the tile's content.
                  ...((tile.rowspan !== undefined ||
                     tile.row !== undefined) && {
                     gridTemplateRows: "minmax(0, 1fr)",
                     minHeight: 0,
                  }),
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
                  // Room at the end of a card's heading for the menu
                  // button, so a long title wraps before it rather than
                  // running under it.
                  ...(!bare && {
                     [`& .${TILE_HEADING_CLASS}`]: { pr: "36px" },
                  }),
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
                  // The remove ×, as a filter shows it: on hover and focus,
                  // and while the tile is selected.
                  ...removeBadgeRevealSx,
                  ...(selected && {
                     [`& .${REMOVE_BADGE_CLASS}`]: { opacity: 1 },
                  }),
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

               {/* Removing the tile: the × on its corner, the one removal
                control in the builder (a filter's is the same). On the
                selection ring's corner for bare prose, the card's otherwise. */}
               <RemoveBadge
                  label={`Remove ${tileLabel(tile)}`}
                  onRemove={onRemove}
                  {...(removeBlocked ? { disabledReason: removeBlocked } : {})}
                  inset={bare ? BARE_RING_OFFSET_PX : 0}
               />

               {/* The tile's menu: its chart and drill-through.
                A press here is never the start of a drag: the
                sensor refuses to activate from a button. */}
               {/* A text tile has no menu: it is edited in place and removed
                from its ×. */}
               {!isTextTile(tile) && (
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
                        // Inside the tile, top right: on a card, in its heading
                        // row level with the title (the heading leaves room for
                        // it: see `TILE_HEADING_CLASS` below); on prose, in its
                        // top corner, which a readable line leaves empty. Not
                        // astride the edge, half outside the tile it belongs to.
                        top: bare ? 0 : `${MENU_INSET_PX}px`,
                        right: bare ? 0 : `${MENU_INSET_PX}px`,
                        borderRadius: ui.radius.control,
                        width: 28,
                        height: 28,
                        zIndex: 2,
                        color: "text.secondary",
                        bgcolor: "action.hover",
                        opacity: selected || menuOpen ? 1 : 0,
                        transition: `opacity ${MOTION_FAST}`,
                        ...reducedMotionSx,
                        "&:hover": {
                           opacity: 1,
                           bgcolor: "action.selected",
                        },
                     }}
                  >
                     <SpreadDotsIcon />
                  </IconButton>
               )}

               {/* The right edge, which sets the width: dragged, or focused and
                stepped with the arrow keys a column at a time (Home and End go
                to one column and the full grid). Marked so the move sensor
                never takes a press on it. */}
               {onResizeStart && (
                  <Box
                     className={`builder-affordance ${reportClasses.widthHandle}`}
                     {...{ [NO_DRAG]: "" }}
                     role="separator"
                     tabIndex={width ? 0 : -1}
                     aria-orientation="vertical"
                     aria-label={`Width of ${tileLabel(tile)}`}
                     {...(width
                        ? {
                             "aria-valuenow": width.span,
                             "aria-valuemin": 1,
                             "aria-valuemax": width.max ?? width.columns,
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
                                    ? (width.max ?? width.columns)
                                    : undefined;
                        if (to === undefined) return;
                        // The builder's own arrow-key nudge must not step it again.
                        event.preventDefault();
                        event.stopPropagation();
                        width.onStep(
                           Math.min(
                              Math.max(to, 1),
                              width.max ?? width.columns,
                           ),
                        );
                     }}
                     onPointerDown={onResizeStart}
                     onPointerMove={onResizeMove}
                     onPointerUp={onResizeEnd}
                     onPointerCancel={onResizeEnd}
                     onClick={(event) => event.stopPropagation()}
                     sx={(mui) => ({
                        position: "absolute",
                        top: 0,
                        bottom: 0,
                        // Straddles the edge, so the target is a usable width
                        // without eating into the tile's content; wide enough
                        // for the knob at its middle.
                        right: "-8px",
                        width: "16px",
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
                        // A grip tab at the middle of the edge: the shape a
                        // splitter's handle has, so it reads as "drag sideways"
                        // without a word. The whole edge strip still takes the
                        // drag; the tab is where the eye goes.
                        "&::before": {
                           content: '""',
                           position: "absolute",
                           top: "50%",
                           left: "50%",
                           transform: "translate(-50%, -50%)",
                           width: "12px",
                           height: "40px",
                           boxSizing: "border-box",
                           borderRadius: ui.radius.control,
                           border: `1px solid ${
                              resizing
                                 ? theme.accent
                                 : edgeColor(theme.cardBorder)
                           }`,
                           bgcolor: mui.palette.background.paper,
                           boxShadow:
                              "0 1px 2px rgba(16, 24, 40, 0.10), 0 2px 6px rgba(16, 24, 40, 0.08)",
                           transition: `border-color ${MOTION_FAST}`,
                        },
                        // Its two grip lines.
                        "&::after": {
                           content: '""',
                           position: "absolute",
                           top: "50%",
                           left: "50%",
                           transform: "translate(-50%, -50%)",
                           width: "5px",
                           height: "16px",
                           borderLeft: `1.5px solid ${
                              resizing
                                 ? theme.accent
                                 : mui.palette.text.secondary
                           }`,
                           borderRight: `1.5px solid ${
                              resizing
                                 ? theme.accent
                                 : mui.palette.text.secondary
                           }`,
                           boxSizing: "border-box",
                           opacity: resizing ? 1 : 0.7,
                        },
                        "&:hover::before": { borderColor: theme.accent },
                        "&:hover::after": {
                           borderColor: theme.accent,
                           opacity: 1,
                        },
                     })}
                  >
                     {/* While dragging, the width it will snap to, in words. The
                      separator's own value text says the same to a screen reader. */}
                     {resizing && width && (
                        <Box
                           aria-hidden
                           sx={(mui) => ({
                              position: "absolute",
                              top: "50%",
                              // Set against the edge, inside the tile: centred
                              // on it, half the label hung past the grid's right
                              // edge for a tile that reaches it, where a
                              // scrolling host cuts it off.
                              right: "50%",
                              transform: "translateY(calc(-100% - 28px))",
                              px: 1,
                              py: 0.5,
                              borderRadius: ui.radius.control,
                              bgcolor: mui.palette.text.primary,
                              color: mui.palette.background.paper,
                              ...ui.type.label,
                              whiteSpace: "nowrap",
                              pointerEvents: "none",
                           })}
                        >
                           {widthLabel(width.span, width.columns)}
                        </Box>
                     )}
                  </Box>
               )}

               {heightEdge && (
                  <HeightEdge
                     edge={heightEdge}
                     label={tileLabel(tile)}
                     selected={selected}
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
                        bottom: `-${GRID_GAP_PX / 2 + 12}px`,
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

/** How far the tile menu button sits in from a card's top and right edges: level with the title. */
const MENU_INSET_PX = 14;

/** How far the selection stands outside what it selects: its halo. */
export const SELECTION_RING_PX = 7;

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
      ringOffset = BARE_RING_OFFSET_PX,
   }: {
      selected: boolean;
      flash?: boolean;
      bare?: boolean;
      /**
       * How far a bare ring stands off its prose: one distance, or one across
       * and one above and below. Only content outside the grid may go wider
       * than the grid's own.
       */
      ringOffset?: number | { x: number; y: number };
   },
): SystemStyleObject<Theme> => {
   const ui = theme.ui;
   const ring = selected || flash ? theme.accent : "transparent";
   // A soft band outside the ring, so a selection reads as lifted to the
   // front rather than outlined. Wider and stronger for the undo flash.
   const halo = flash
      ? `0 0 0 6px color-mix(in srgb, ${theme.accent} 25%, transparent)`
      : selected
        ? `0 0 0 4px color-mix(in srgb, ${theme.accent} 14%, transparent)`
        : undefined;
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
            inset:
               typeof ringOffset === "number"
                  ? `${-ringOffset}px`
                  : `${-ringOffset.y}px ${-ringOffset.x}px`,
            borderRadius: 1,
            border: `2px solid ${ring}`,
            ...(halo && { boxShadow: halo }),
            pointerEvents: "none",
            transition,
            ...reducedMotionSx,
         },
         // No hover ring: the prose's own wash says it can be edited, and a
         // ring around the wash read as a box drawn round a box.
      };
   // A card's ring IS its edge: the card's own border turns the accent and
   // thickens, with the halo outside it. An outline standing off the card
   // drew a second frame around the first, with a gap between them.
   // Hover deepens the card's own edge and lifts it; the ring is selection's
   // alone. A hover ring the shape of the selection ring made a pointed-at tile
   // and a selected one look alike.
   return {
      borderRadius: ui.radius.tile,
      "& > .MuiPaper-root": {
         transition,
         ...reducedMotionSx,
         ...((selected || flash) && {
            borderColor: theme.accent,
            boxShadow: `0 0 0 1px ${theme.accent}${halo ? `, ${halo}` : ""}`,
         }),
      },
      transition,
      ...reducedMotionSx,
      "&:hover": {
         boxShadow: theme.shadow.lift,
         ...(!(selected || flash) && {
            "& > .MuiPaper-root": {
               borderColor: hoverEdgeColor(theme.cardBorder, theme.tileTitle),
            },
         }),
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
            {tileExpression(tile).replace("->", "→")}
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
            // Behind the tiles: the bands show in the gaps and the free space a
            // tile can grow into, not over its content.
            zIndex: 0,
            display: "grid",
            gridTemplateColumns: `repeat(${columns}, minmax(0, 1fr))`,
            gap: `${GRID_GAP_PX}px`,
         }}
      >
         {Array.from({ length: columns }, (_, column) => (
            <Box
               key={column}
               sx={{
                  // A faint band per column: the tracks a width snaps to.
                  bgcolor: theme.accent,
                  opacity: 0.06,
                  borderRadius: "3px",
               }}
            />
         ))}
      </Box>
   );
}

/**
 * The rows a height snaps to, drawn while a tile's bottom edge is dragged: a
 * faint band per row across the whole canvas, the way {@link GridGuides}
 * draws the columns a width snaps to. Rows are counted from the dragged
 * tile's top, since tiles flow and the grid has no row lines of its own.
 * Behind the tiles and held inside the grid.
 */
export function RowGuides({ top }: { top: number }) {
   const { theme } = usePublisherTheme();
   const step = TILE_ROW_PX + GRID_GAP_PX;
   // From the first whole row above the canvas's top edge to well past its
   // bottom; the container clips what overshoots.
   const first = -Math.ceil(top / step);
   return (
      <Box
         aria-hidden
         sx={{
            position: "absolute",
            inset: 0,
            overflow: "hidden",
            pointerEvents: "none",
            zIndex: 0,
         }}
      >
         {Array.from({ length: -first + 4 * MAX_TILE_ROWS }, (_, n) => (
            <Box
               key={n}
               sx={{
                  position: "absolute",
                  top: top + (first + n) * step,
                  left: 0,
                  right: 0,
                  height: TILE_ROW_PX,
                  bgcolor: theme.accent,
                  opacity: 0.06,
                  borderRadius: "3px",
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

/**
 * A tile's bottom edge: the height handle, laid out like the width handle
 * turned on its side, so the two read as one control.
 */
function HeightEdge({
   edge,
   label,
   selected,
}: {
   edge: NonNullable<Parameters<typeof TileFrame>[0]["heightEdge"]>;
   label: string;
   selected: boolean;
}) {
   const { theme } = usePublisherTheme();
   const ui = theme.ui;
   const { rows, dragging } = edge;
   const fit = rows === undefined;
   return (
      <Box
         className="builder-affordance"
         {...{ [NO_DRAG]: "" }}
         role="separator"
         tabIndex={0}
         aria-orientation="horizontal"
         aria-label={`Height of ${label}`}
         aria-valuemin={edge.min}
         aria-valuemax={MAX_TILE_ROWS}
         {...(fit ? {} : { "aria-valuenow": rows })}
         aria-valuetext={heightLabel(rows)}
         onKeyDown={(event) => {
            const now = rows ?? edge.min;
            const to =
               event.key === "ArrowDown"
                  ? now + 1
                  : event.key === "ArrowUp"
                    ? now - 1
                    : event.key === "Home"
                      ? edge.min
                      : event.key === "End"
                        ? MAX_TILE_ROWS
                        : event.key === "Delete" || event.key === "Backspace"
                          ? null
                          : undefined;
            if (to === undefined) return;
            // The builder's own arrow-key nudge must not act on it too.
            event.preventDefault();
            event.stopPropagation();
            if (to === null) {
               if (!fit) edge.onStep(undefined);
               return;
            }
            const held = Math.min(Math.max(to, edge.min), MAX_TILE_ROWS);
            if (held !== rows) edge.onStep(held);
         }}
         onPointerDown={edge.onStart}
         onPointerMove={edge.onMove}
         onPointerUp={edge.onEnd}
         onPointerCancel={edge.onEnd}
         onClick={(event) => event.stopPropagation()}
         onDoubleClick={(event) => {
            event.stopPropagation();
            if (!fit) edge.onStep(undefined);
         }}
         sx={(mui) => ({
            position: "absolute",
            left: 0,
            right: 0,
            // Straddles the edge, as the width handle does.
            bottom: "-8px",
            height: "16px",
            cursor: "row-resize",
            touchAction: "none",
            zIndex: 2,
            opacity: dragging || selected ? 1 : 0,
            transition: `opacity ${MOTION_FAST}`,
            ...reducedMotionSx,
            "&:hover": { opacity: 1 },
            "&:focus-visible": {
               opacity: 1,
               outline: `2px solid ${theme.accent}`,
               outlineOffset: 1,
               borderRadius: "3px",
            },
            "&::before": {
               content: '""',
               position: "absolute",
               top: "50%",
               left: "50%",
               transform: "translate(-50%, -50%)",
               width: "40px",
               height: "12px",
               boxSizing: "border-box",
               borderRadius: ui.radius.control,
               border: `1px solid ${
                  dragging ? theme.accent : edgeColor(theme.cardBorder)
               }`,
               bgcolor: mui.palette.background.paper,
               boxShadow:
                  "0 1px 2px rgba(16, 24, 40, 0.10), 0 2px 6px rgba(16, 24, 40, 0.08)",
               transition: `border-color ${MOTION_FAST}`,
            },
            "&::after": {
               content: '""',
               position: "absolute",
               top: "50%",
               left: "50%",
               transform: "translate(-50%, -50%)",
               width: "16px",
               height: "5px",
               borderTop: `1.5px solid ${
                  dragging ? theme.accent : mui.palette.text.secondary
               }`,
               borderBottom: `1.5px solid ${
                  dragging ? theme.accent : mui.palette.text.secondary
               }`,
               boxSizing: "border-box",
               opacity: dragging ? 1 : 0.7,
            },
            "&:hover::before": { borderColor: theme.accent },
            "&:hover::after": { borderColor: theme.accent, opacity: 1 },
         })}
      >
         {dragging && (
            <Box
               aria-hidden
               sx={(mui) => ({
                  position: "absolute",
                  left: "50%",
                  bottom: "50%",
                  transform: "translate(32px, 50%)",
                  px: 1,
                  py: 0.5,
                  borderRadius: ui.radius.control,
                  bgcolor: mui.palette.text.primary,
                  color: mui.palette.background.paper,
                  ...ui.type.label,
                  whiteSpace: "nowrap",
                  pointerEvents: "none",
               })}
            >
               {heightLabel(rows)}
            </Box>
         )}
      </Box>
   );
}

/** A height in words, for the drag's label and a screen reader. */
const heightLabel = (rows: number | undefined) =>
   rows === undefined ? "Auto height" : `${rows} rows tall`;
