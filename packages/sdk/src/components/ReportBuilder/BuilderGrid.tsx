// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { DragDropProvider } from "@dnd-kit/react";
import AddIcon from "@mui/icons-material/Add";
import { Box, Button } from "@mui/material";
import type { ReactNode, RefObject } from "react";
import { DashboardGrid, GRID_GAP_PX } from "../Dashboard/DashboardGrid";
import type { TileChrome, TileHeadingSlots } from "../Dashboard/TileCard";
import type { LayoutRules } from "../Report/layoutRules";
import {
   isQueryTile,
   isTextTile,
   tileKey,
   type DashboardTile,
   type QueryTile,
} from "./document";
import { InlineText } from "./InlineText";
import { gapId, tileEntry, withGaps } from "./layout";
import { builderSensors } from "./sortable";
import { TextTileBody } from "./TextTileBody";
import { GapTarget, GridGuides, TileFrame, TilePlaceholder } from "./TileFrame";
import { tileDisplayTitle } from "./tileDisplayTitle";
import type { ReportEditorHandle } from "./useReportEditor";
import type { useTileReorder } from "./useTileReorder";
import type { useTileResize } from "./useTileResize";

/**
 * The builder's tiles, on the SAME grid the dashboard renders on, each drawn
 * by the caller's `renderTile` inside a `TileFrame` that carries the
 * affordances: selection, the grip and menu, the width handle.
 */
export function BuilderGrid({
   editor,
   reorder,
   resizing,
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
}: {
   editor: ReportEditorHandle;
   reorder: ReturnType<typeof useTileReorder>;
   resizing: ReturnType<typeof useTileResize>;
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
   renderTile?: (
      tile: QueryTile,
      heading?: TileHeadingSlots,
      chrome?: TileChrome,
   ) => ReactNode;
}) {
   const { dragging, preview, onDragStart, onDragOver, onDragEnd } = reorder;
   const { columns } = rules;
   const { resize, startResize, onResize, endResize } = resizing;
   // What the grid lays out: the document, except mid-gesture, where it is
   // the preview — the tile being resized at its previewed width, or the tiles
   // in their previewed order. So the row reflows under the pointer exactly as
   // it will once the edit lands.
   const drawn = (tile: DashboardTile): DashboardTile =>
      tile.colspan === undefined ? { ...tile, colspan: defaultSpan } : tile;
   const shown = (
      resize !== undefined
         ? editor.document.tiles.map((each, index) =>
              index === resize.index ? { ...each, colspan: resize.span } : each,
           )
         : (preview ?? editor.document.tiles)
   ).map(drawn);
   // And, while a drag is live, the empty end of every row as a drop target.
   // While an edge is dragged too: narrowing a tile frees columns its row-mate
   // does not take, and an undrawn hole there read as a broken layout rather
   // than room to fill. Not otherwise: a gap is only shown mid-gesture.
   const empty = editor.document.tiles.length === 0;
   const entries =
      dragging || resize !== undefined
         ? withGaps(shown, columns)
         : shown.map(tileEntry);
   const heading = (tile: QueryTile) =>
      headingOf(tile, tileDisplayTitle(tile), editTile);
   return (
      <>
         <DragDropProvider
            sensors={builderSensors}
            onDragStart={onDragStart}
            onDragOver={onDragOver}
            onDragEnd={onDragEnd}
         >
            <Box ref={gridBox} sx={{ position: "relative" }}>
               {rules.widths && (dragging || resize !== undefined) && (
                  <GridGuides columns={columns} />
               )}

               <DashboardGrid
                  tiles={entries}
                  columns={columns}
                  rowGapPx={GRID_GAP_PX}
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
                           {...(rules.widths && resizable(each)
                              ? {
                                   resizing: resize?.index === index,
                                   width: {
                                      span: each.colspan ?? 1,
                                      columns,
                                      onStep: (to: number) => {
                                         if (to === (each.colspan ?? 1)) return;
                                         editor.update((draft) => {
                                            const tile = draft.tiles[index];
                                            if (tile) tile.colspan = to;
                                         });
                                      },
                                   },
                                   onResizeStart: (event) =>
                                      startResize(event, index),
                                   onResizeMove: onResize,
                                   onResizeEnd: endResize,
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
   if (tile.declaration.kind === "inherited") return undefined;
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

/**
 * Whether dragging this tile's edge can be saved: its width is a tag this file
 * owns. An inherited tile's tags live on the model's view, which the builder
 * does not write.
 */
const resizable = (tile: DashboardTile): boolean =>
   isTextTile(tile) || tile.declaration.kind !== "inherited";
