// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { DragDropProvider } from "@dnd-kit/react";
import AddIcon from "@mui/icons-material/Add";
import { Box, Button } from "@mui/material";
import type { ReactNode, RefObject } from "react";
import {
   DashboardGrid,
   GRID_GAP_PX,
   isPlaced,
   PLACED_DEFAULT_ROWSPAN,
} from "../Dashboard/DashboardGrid";
import type { TileChrome, TileHeadingSlots } from "../Dashboard/TileCard";
import { withSpan, type LayoutRules } from "../Report/layoutRules";
import {
   isQueryTile,
   isReadOnlyTile,
   isTextTile,
   tileKey,
   type DashboardTile,
   type QueryTile,
} from "./document";
import { InlineText } from "./InlineText";
import { gapId, tileEntry, withGaps } from "./layout";
import { minRowsOf, rowsOf, settle, widestFrom } from "./placement";
import { builderSensors, EscapeCancelsDrag } from "./sortable";
import { TextTileBody } from "./TextTileBody";
import {
   GapTarget,
   GridGuides,
   RowGuides,
   TileFrame,
   TilePlaceholder,
} from "./TileFrame";
import { tileDisplayTitle } from "./tileDisplayTitle";
import type { ReportEditorHandle } from "./useReportEditor";
import type { useTileReorder } from "./useTileReorder";
import { shapeLabel, withShape, type useEdgeResize } from "./useEdgeResize";

/**
 * The builder's tiles, on the SAME grid the dashboard renders on, each drawn
 * by the caller's `renderTile` inside a `TileFrame` that carries the
 * affordances: selection, the menu, and its edges and corners.
 */
export function BuilderGrid({
   editor,
   reorder,
   edging,
   place = (tiles) => tiles,
   arrange = true,
   gridBox,
   defaultSpan = 1,
   rules,
   canAdd,
   selected,
   flash,
   menuIndex,
   onSelect,
   onOpenMenu,
   onRemove,
   removeBlocked,
   openAdd,
   editTile,
   renderTile,
   titleOf = tileDisplayTitle,
}: {
   editor: ReportEditorHandle;
   reorder: ReturnType<typeof useTileReorder>;
   /** Every edge and corner of a tile, dragged to resize it. */
   edging: ReturnType<typeof useEdgeResize>;
   /**
    * The tiles after the one at `index` changed size: on a placed report,
    * whatever it now covers pushed down. Identity elsewhere.
    */
   place?: (tiles: DashboardTile[], index: number) => DashboardTile[];
   /**
    * Whether tiles can be moved and widened here. Off while a grid is drawn
    * as one column (a window narrower than the grid), where neither shows.
    */
   arrange?: boolean;
   gridBox: RefObject<HTMLDivElement | null>;
   /** The span a tile with no `# colspan` is drawn at: as wide as the reader draws it. */
   defaultSpan?: number;
   /** How the document arranges tiles: columns, widths, inserting between. */
   rules: LayoutRules;
   /** Whether there is a catalog to add a tile from. */
   canAdd: boolean;
   selected: number | undefined;
   flash: string | undefined;
   /** The tile whose menu is open. */
   menuIndex: number | undefined;
   onSelect: (index: number) => void;
   onOpenMenu: (anchor: HTMLElement, index: number) => void;
   /** Take a tile off the report. */
   onRemove: (index: number) => void;
   /** Why no tile can be removed now; set, each × says so instead. */
   removeBlocked?: string;
   openAdd: (at?: number) => void;
   editTile: (key: string, change: (tile: DashboardTile) => void) => void;
   /** A tile's title when it has none of its own; see `ReportBuilderProps.titleOf`. */
   titleOf?: (tile: QueryTile) => string;
   renderTile?: (
      tile: QueryTile,
      heading?: TileHeadingSlots,
      chrome?: TileChrome,
   ) => ReactNode;
}) {
   const { dragging, preview, onDragStart, onDragMove, onDragOver, onDragEnd } =
      reorder;
   const { columns } = rules;
   const edge = edging.drag;
   // What the grid lays out: the document, except mid-gesture, where it is
   // the preview — the tile being resized at its previewed width, or the tiles
   // in their previewed order. So the row reflows under the pointer exactly as
   // it will once the edit lands.
   const drawn = (tile: DashboardTile): DashboardTile =>
      tile.colspan === undefined ? { ...tile, colspan: defaultSpan } : tile;
   const shown = (
      edge !== undefined
         ? (() => {
              // The tiles as the resize would leave them: the one tile with
              // the edges it moved, and on a placed report everything it now
              // covers pushed down.
              const base = edge.placedHere ? edge.base : editor.document.tiles;
              const tile = base[edge.index];
              if (!tile) return base;
              const resized = base.map((each, index) =>
                 index === edge.index
                    ? withShape(each, edge.shape, edge.edges, edge.placed)
                    : each,
              );
              return edge.placed
                 ? settle(resized, columns, edge.index)
                 : resized;
           })()
         : (preview ?? editor.document.tiles)
   ).map(drawn);
   // Placed, every cell is somewhere to drop, so there are no row ends to offer.
   const placed = isPlaced(shown);
   const gesture = dragging || edge !== undefined;
   // Faintly, while a tile of a placed report is selected and nothing is
   // dragged: the empty cells read as places a tile can go before any drag.
   const resting = !gesture && placed && arrange && selected !== undefined;
   // And, while a drag is live, the empty end of every row as a drop target.
   // While an edge is dragged too: narrowing a tile frees columns its row-mate
   // does not take, and an undrawn hole there read as a broken layout rather
   // than room to fill. Not otherwise: a gap is only shown mid-gesture.
   const empty = editor.document.tiles.length === 0;
   const entries =
      gesture && !placed ? withGaps(shown, columns) : shown.map(tileEntry);
   const heading = (tile: QueryTile) =>
      headingOf(tile, titleOf(tile), editTile);
   return (
      <>
         <DragDropProvider
            sensors={builderSensors}
            onDragStart={onDragStart}
            onDragMove={onDragMove}
            onDragOver={onDragOver}
            onDragEnd={onDragEnd}
         >
            <EscapeCancelsDrag active={dragging} />
            <Box ref={gridBox} sx={{ position: "relative" }}>
               {rules.widths && (gesture || resting) && (
                  <GridGuides columns={columns} faint={!gesture} />
               )}
               {edge !== undefined && !edge.placed
                  ? (edge.edges.s || edge.edges.n) && (
                       <RowGuides top={edge.guideTop} />
                    )
                  : // Placed, a move snaps to rows as well as columns.
                    placed &&
                    (gesture || resting) && (
                       <RowGuides top={0} faint={!gesture} />
                    )}

               <DashboardGrid
                  tiles={entries}
                  columns={columns}
                  rowGapPx={GRID_GAP_PX}
                  defaultRowspan={(entry) =>
                     entry.kind === "tile"
                        ? rowsOf(entry.tile)
                        : PLACED_DEFAULT_ROWSPAN.query
                  }
                  keyOf={(entry) =>
                     entry.kind === "gap"
                        ? gapId(entry.after)
                        : tileKey(entry.tile)
                  }
                  renderTile={(entry) => {
                     if (entry.kind === "gap")
                        return <GapTarget after={entry.after} />;
                     const { tile: each, index } = entry;
                     return (
                        <TileFrame
                           tile={each}
                           index={index}
                           selected={index === selected}
                           flash={tileKey(each) === flash}
                           menuOpen={menuIndex === index}
                           // Prose has no card, in either layout.
                           bare={isTextTile(each)}
                           // Not on the last tile: the "Add tile" under the
                           // list adds there already, and the two drew on top
                           // of each other.
                           {...(rules.insertBetween &&
                           canAdd &&
                           index < editor.document.tiles.length - 1
                              ? {
                                   onInsertAfter: () => openAdd(index + 1),
                                }
                              : {})}
                           fixed={!arrange}
                           {...(rules.widths
                              ? {
                                   edges: {
                                      // Left and top move a place, so they
                                      // need the grid drawn as a grid.
                                      all: arrange,
                                      right: arrange,
                                      start: (event, edges) =>
                                         edging.start(event, index, edges),
                                      move: edging.move,
                                      end: edging.end,
                                      ...(edge?.index === index
                                         ? { label: shapeLabel(edge, columns) }
                                         : {}),
                                   },
                                }
                              : {})}
                           {...(rules.widths && arrange
                              ? {
                                   width: {
                                      span: each.colspan ?? 1,
                                      columns,
                                      max: widestFrom(each, columns),
                                      onStep: (to: number) => {
                                         if (to === (each.colspan ?? 1)) return;
                                         editor.update((draft) => {
                                            if (
                                               !Number.isInteger(index) ||
                                               index < 0
                                            )
                                               return;
                                            const tile = draft.tiles[index];
                                            if (tile)
                                               draft.tiles[index] = withSpan(
                                                  tile,
                                                  to,
                                                  columns,
                                                  defaultSpan,
                                               );
                                            draft.tiles = place(
                                               draft.tiles,
                                               index,
                                            );
                                         });
                                      },
                                   },
                                }
                              : {})}
                           {...(rules.widths
                              ? {
                                   heightEdge: {
                                      rows: each.rowspan,
                                      min: minRowsOf(each),
                                      minFor: (frame) => minRowsOf(each, frame),
                                      onStep: (to) =>
                                         editor.update((draft) => {
                                            if (
                                               !Number.isInteger(index) ||
                                               index < 0
                                            )
                                               return;
                                            const tile = draft.tiles[index];
                                            if (!tile) return;
                                            if (to === undefined)
                                               delete tile.rowspan;
                                            else tile.rowspan = to;
                                            draft.tiles = place(
                                               draft.tiles,
                                               index,
                                            );
                                         }),
                                   },
                                }
                              : {})}
                           onSelect={() => onSelect(index)}
                           onOpenMenu={(anchor) => onOpenMenu(anchor, index)}
                           onRemove={() => onRemove(index)}
                           {...(removeBlocked ? { removeBlocked } : {})}
                        >
                           {isTextTile(each) ? (
                              <TextTileBody
                                 tile={each}
                                 onChange={(markdown) =>
                                    editTile(tileKey(each), (tile) => {
                                       if (isTextTile(tile))
                                          tile.markdown = markdown;
                                    })
                                 }
                              />
                           ) : renderTile ? (
                              renderTile(each, heading(each), "card")
                           ) : (
                              <TilePlaceholder
                                 tile={each}
                                 heading={heading(each)}
                              />
                           )}
                        </TileFrame>
                     );
                  }}
               />
            </Box>
         </DragDropProvider>
         {rules.insertBetween && canAdd && !empty && (
            <Button
               size="small"
               startIcon={<AddIcon />}
               aria-label="Add tile at the end"
               onClick={() => openAdd()}
               sx={{ alignSelf: "center" }}
            >
               Add tile
            </Button>
         )}
      </>
   );
}

/** A query tile's title and subtitle as fields on the tile, unless the model owns them. */
const headingOf = (
   tile: QueryTile,
   fallback: string,
   editTile: (key: string, change: (tile: DashboardTile) => void) => void,
): TileHeadingSlots | undefined => {
   if (isReadOnlyTile(tile)) return undefined;
   const key = tileKey(tile);
   const set = (field: "label" | "subtitle") => (next: string) =>
      editTile(key, (each) => {
         if (!isQueryTile(each)) return;
         if (next === "") delete each[field];
         else each[field] = next;
      });
   return {
      title: (
         <InlineText
            value={tile.label ?? ""}
            placeholder={fallback}
            ariaLabel="Tile title"
            onCommit={set("label")}
         />
      ),
      subtitle: (
         <InlineText
            value={tile.subtitle ?? ""}
            placeholder="Add a subtitle"
            ariaLabel="Tile subtitle"
            faintWhenEmpty
            onCommit={set("subtitle")}
         />
      ),
   };
};
