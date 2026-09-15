// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import type { DragEndEvent, DragOverEvent } from "@dnd-kit/react";
import { move } from "@dnd-kit/helpers";
import { DragDropProvider } from "@dnd-kit/react";
import AddIcon from "@mui/icons-material/Add";
import DragIndicatorIcon from "@mui/icons-material/DragIndicator";
import FilterListIcon from "@mui/icons-material/FilterList";
import MoreVertIcon from "@mui/icons-material/MoreVert";
import {
   Alert,
   Box,
   Button,
   Chip,
   IconButton,
   Paper,
   Stack,
   Tooltip,
   Typography,
} from "@mui/material";
import { useEffect, useMemo, useRef, useState, type ReactNode } from "react";
import { usePublisherTheme } from "../../theme/ThemeContext";
import { DashboardProse } from "../Dashboard/Dashboard";
import {
   DashboardGrid,
   DEFAULT_COLUMNS,
   GRID_GAP_PX,
   type GridTile,
} from "../Dashboard/DashboardGrid";
import {
   acceptsField,
   applyMapping,
   controlsOf,
   declareControl,
   removeControl,
   type BuilderControl,
   type BuilderGiven,
   type MappingRow,
} from "./controls";
import { BuilderToolbar } from "./BuilderToolbar";
import { filterableFields, type PackageCatalog } from "./catalog";
import type { DashboardDocument, DashboardTile, LocalGiven } from "./document";
import { FilterDialog } from "./FilterDialog";
import {
   builderSensors,
   GAP_TYPE,
   type GapData,
   GapDroppable,
   TileSortable,
} from "./sortable";
import { TileMenu } from "./TileMenu";
import { useBuilderShortcuts } from "./useBuilderShortcuts";
import { useDashboardEditor } from "./useDashboardEditor";

export type { BuilderGiven } from "./controls";

/**
 * The dashboard builder's editing surface.
 *
 * Lays the tiles out on the SAME grid the dashboard renders on — literally the
 * same component, `DashboardGrid`, rather than a grid restated here to match.
 * What you arrange is what a reader will see, and the two cannot drift.
 *
 * The TILE ITSELF is the caller's business, through `renderTile`. A tile's
 * result comes from running a query, and where that query runs differs between
 * a saved package dashboard and an unsaved draft; keeping it out here means the
 * editing surface can be mounted, tested and reviewed without a server.
 *
 * `renderTile` replaces the tile rather than filling it, because `DashboardTile`
 * draws its own card AND its own heading. Nesting one inside a card of ours
 * would show a card in a card under two titles — which is precisely not what a
 * reader sees. So every affordance is drawn AROUND whatever the caller renders:
 * an outline for selection, a grip and a menu in the corners, a handle at the
 * edge.
 *
 * **Everything here is on the thing it changes.** Drag a tile's right edge to
 * set its width, and the tile itself to reorder — into the empty end of a row
 * to change which row it is in. The tile's own menu holds its title and
 * subtitle. FILTERS are configured in exactly one place, the strip under the
 * header: each chip opens Looker's "tiles to update" window for that control,
 * which is where a tile is bound or unbound, and "Add filter" declares a new
 * one in this file — the convention {@link LocalGiven} describes — or binds one
 * the model offers. Nothing about filters is on the tiles themselves: a second
 * place to edit the same binding is a second place for it to be wrong.
 *
 * Adding or removing tiles is refused by the writer, so it is not offered here
 * either — a control that always fails is worse than no control. Reordering is
 * NOT in that class and is offered: see `onDragOver`.
 */
export interface DashboardBuilderProps {
   /** The file being edited. */
   source: string;
   /** The document that file produced. */
   document: DashboardDocument;
   /** Persist the patched file. Left out, the builder edits without saving. */
   onSave?: (source: string) => Promise<void> | void;
   /**
    * The document as it stands, on every edit — including the first render.
    *
    * For the host's LIVE VIEW. A tile's result and the control row are the
    * host's to render (`renderTile`, `controls`), and both have to follow the
    * document rather than the saved file, or an edit looks like it did
    * nothing: unbinding a tile left it filtering, removing a control left it
    * in the row. `preview.ts` turns the document handed here into what a reader
    * would see.
    */
   onChange?: (document: DashboardDocument) => void;
   /**
    * Renders a tile, card and heading included — this is where a real
    * `DashboardTile` goes. Without it, tiles show what they will run.
    */
   renderTile?: (tile: DashboardTile) => ReactNode;
   /**
    * The control row, in the slot the reader puts it — this is where a real
    * `GivensPanel` goes.
    *
    * A seam for the same reason as `renderTile`, and not an oversight: a
    * control's SPEC (its type, its control tag, its suggestions) is resolved by
    * the server across the model and everything it imports, and a
    * `DashboardDocument` holds only what this one file declares. The storefront
    * dashboard is the ordinary case — it imports every given from
    * `../givens.malloy` — so a bar built from the document alone would come up
    * empty and claim the dashboard has no filters. Better to let a caller that
    * has the manifest hand over the real one.
    *
    * It matters to LAYOUT, not just to fidelity: the bar takes vertical space
    * above the grid, so an author arranging tiles without it is arranging
    * against a page the reader never sees.
    */
   controls?: ReactNode;
   /**
    * Givens a tile can be bound to, for the palette above the grid.
    *
    * A prop for the same reason as `controls`: which givens exist is resolved
    * by the server across the model and its imports, and a `DashboardDocument`
    * knows only what its own file declares.
    */
   givens?: BuilderGiven[];
   /**
    * What the package offers, for the filter window to SEARCH a field rather
    * than take one on trust: the dimensions of the source the tiles read are
    * offered, an unknown name is marked where it stands, and a binding to a
    * field the source does not have cannot be applied. Absent, any name is
    * accepted — a binding to a missing field then fails at package load, which
    * is the worst place for it.
    */
   catalog?: PackageCatalog;
}

/**
 * Reordering keeps the ROW STRUCTURE and moves only the tiles through it.
 *
 * `# break` starts a fresh row. It is written as a tag on a tile, so the
 * obvious reading is that it belongs to the tile and should travel with it —
 * and that is what this did at first. The result was wrong in practice: drag
 * one of four half-width tiles and its `break` lands mid-row, forcing a new
 * row and leaving the half beside it empty. Four `colspan=6` tiles stopped
 * being a 2x2 the moment you moved one.
 *
 * The tag is positional in MEANING even though it is stored per tile: it
 * says "a row starts here", which is a fact about the grid, not about the
 * view that happens to sit there. So a move re-applies the break pattern by
 * POSITION, and the surrounding tiles close up behind the one that left.
 * Widths still travel with their tiles, because a width really is the tile's.
 */
export const keepRowStructure = (
   reordered: DashboardTile[],
   pattern: readonly boolean[],
) =>
   reordered.map((tile, index) => {
      const starts = pattern[index] ?? false;
      if (starts === (tile.break ?? false)) return tile;
      const next = { ...tile };
      if (starts) next.break = true;
      else delete next.break;
      return next;
   });

/**
 * Moving a tile INTO A GAP — the empty end of a row — is the one move that
 * must change the row structure, because the gap IS the row structure.
 *
 * A gap exists for one reason: the tile after the row's last tile carries a
 * `# break`, so it starts a fresh row instead of filling the space. Dropping a
 * tile into that space says, as plainly as a gesture can, "this belongs up
 * here" — and {@link keepRowStructure} would refuse it: re-applying the break
 * by position hands the break to the moved tile, which then starts its own row
 * and leaves the gap exactly where it was. Measured on the storefront overview
 * with the trend narrowed to half width: the map could not be dragged up
 * beside it at all.
 *
 * So a gap drop lets the break TRAVEL instead: the moved tile arrives with
 * none, so it flows into the row; the tile that started the next row keeps its
 * break and still does. And the row the tile LEFT closes up — if it was that
 * row's first tile, the tile after it starts the row now, which is what the
 * positional rule would have done there too.
 *
 * `to` is where the tile lands in the array it is spliced back into, after
 * `from` has been removed — the same convention the tile-target move uses.
 */
export const moveIntoGap = (
   tiles: readonly DashboardTile[],
   from: number,
   to: number,
) => {
   const next = [...tiles];
   const [moved] = next.splice(from, 1);
   if (!moved) return [...tiles];
   // The tile that followed the one leaving, now at `from`, inherits a row
   // start it did not have — the row must still begin somewhere.
   const follower = next[from];
   if (moved.break && follower && !follower.break)
      next[from] = { ...follower, break: true };
   const placed = { ...moved };
   delete placed.break;
   next.splice(to, 0, placed);
   return next;
};

/** A tile's identity across a reorder: what the grid keys on and a drag names. */
const tileKey = (tile: DashboardTile) => `${tile.source}.${tile.name}`;

/** One thing the grid lays out: a tile, or the empty end of a row. */
type GridEntry = GridTile &
   (
      | {
           kind: "tile";
           tile: DashboardTile;
           /** Its place among the TILES — what a sortable item is told. */
           index: number;
        }
      | { kind: "gap"; after: string }
   );

const tileEntry = (tile: DashboardTile, index: number): GridEntry => ({
   kind: "tile",
   tile,
   index,
   colspan: tile.colspan,
   break: tile.break,
});

/** A gap's id: its drop target's, and the grid's key for it. */
const gapId = (after: string) => `gap.${after}`;

/**
 * The grid's entries with a drop target in every gap.
 *
 * A flow grid has no element where a row runs out; the space is simply not
 * painted, so there is nothing for a drag to be "over". This walks the tiles
 * as the grid will place them — `# break` starts a row, and so does a tile too
 * wide for what is left of the current one — and wherever a row ends short of
 * the last column, lays a gap entry spanning exactly the columns left over. It
 * fills space that was already empty, so adding it moves nothing; and it
 * carries the key of the tile it follows, which is what a drop on it needs.
 */
export const withGaps = (
   tiles: readonly DashboardTile[],
   columns: number,
): GridEntry[] => {
   const entries: GridEntry[] = [];
   let used = 0;
   tiles.forEach((tile, index) => {
      const span = Math.min(tile.colspan ?? 1, columns);
      const startsRow =
         index === 0 || tile.break === true || used + span > columns;
      if (startsRow && index > 0 && used < columns)
         entries.push({
            kind: "gap",
            after: tileKey(tiles[index - 1]),
            colspan: columns - used,
         });
      if (startsRow) used = 0;
      entries.push(tileEntry(tile, index));
      used += span;
   });
   const last = tiles.at(-1);
   if (last && used < columns)
      entries.push({
         kind: "gap",
         after: tileKey(last),
         colspan: columns - used,
      });
   return entries;
};

/**
 * While a RESIZE is live, the page has to stop behaving like a document:
 * dragging an edge across a dashboard otherwise selects the text it crosses and
 * leaves the cursor as whatever it was over. (A MOVE gets the same from the
 * library's own plugins.)
 */
const dragChrome = (on: boolean) => {
   const body = window.document.body;
   body.style.userSelect = on ? "none" : "";
   body.style.cursor = on ? "grabbing" : "";
};

export function DashboardBuilder({
   source,
   document,
   onSave,
   onChange,
   renderTile,
   controls,
   givens,
   catalog,
}: DashboardBuilderProps) {
   const editor = useDashboardEditor({
      source,
      document,
      ...(onSave ? { onSave } : {}),
   });
   const [selected, setSelected] = useState<number | undefined>(undefined);
   const [saving, setSaving] = useState(false);
   // A resize in flight. `span` is what the tile WOULD be; it is previewed by
   // handing the grid a changed copy of the tiles, and only written to the
   // document on release — see `endResize`.
   const [resize, setResize] = useState<
      { index: number; span: number; left: number; track: number } | undefined
   >(undefined);
   // Wraps the grid, so its content box IS the grid's: what a dragged edge has
   // to be measured against to work out a column count.
   const gridBox = useRef<HTMLDivElement>(null);
   // A move in flight: the tiles as they will stand if the drag ends now.
   // CUMULATIVE — each report moves the tile from where the last report left
   // it, which is the sortable convention and what the library's optimistic
   // sorting assumes. The first version rebuilt the order from the document on
   // every report, and as the row reflowed under the pointer the tile beneath
   // it changed, so the target flipped back and forth. State for the render,
   // and a ref for `onDragEnd`, which can fire before React has committed it.
   const [preview, setPreview] = useState<DashboardTile[] | undefined>(
      undefined,
   );
   const previewRef = useRef<DashboardTile[] | undefined>(undefined);
   // Whether a drag is live at all: what turns the row-end gaps into drop
   // targets and draws the grid guides.
   const [dragging, setDragging] = useState(false);
   // The filter window: open on a control, or open to add one.
   const [filterDialog, setFilterDialog] = useState<
      { control?: BuilderControl } | undefined
   >(undefined);
   // A tile's menu, anchored to the button that opened it.
   const [menu, setMenu] = useState<
      { anchor: HTMLElement; index: number } | undefined
   >(undefined);
   const theme = usePublisherTheme().theme;

   const columns = editor.document.columns ?? DEFAULT_COLUMNS;

   useEffect(() => {
      onChange?.(editor.document);
   }, [editor.document, onChange]);

   // The givens the MODEL offers: the caller's list, less any the opened file
   // declared itself. A caller gets that list from the server's manifest, which
   // resolves givens across the file and its imports without saying which is
   // which — so it names this file's own declarations too, and keeps naming a
   // control after this document removes it, until a save is written and the
   // package reloads. Without this, "Remove from dashboard" took the chip off
   // and it came straight back, faint, labelled "from the model".
   const modelGivens = useMemo(() => {
      const ownDeclarations = new Set(
         (document.localGivens ?? []).map((given) => given.name),
      );
      return (givens ?? []).filter((given) => !ownDeclarations.has(given.name));
   }, [document, givens]);
   // Every control the builder can offer: this file's own, then the model's.
   const controlList = useMemo(
      () => controlsOf(editor.document, modelGivens),
      [editor.document, modelGivens],
   );
   // The fields a binding may name: the dimensions of the source the tiles
   // read, when the host knows them. Undefined otherwise, and nothing checks.
   const fieldSource = editor.document.sources[0]?.base;
   const knownFields = useMemo(
      () => filterableFields(catalog, fieldSource),
      [catalog, fieldSource],
   );
   // Bindings the source cannot take, per control — a field it does not have,
   // or one of a type the given cannot compare: marked on the chip, so a broken
   // binding is seen before the package refuses it.
   const unknownFieldsOf = (
      name: string,
      type: string | undefined,
   ): string[] => {
      if (!knownFields) return [];
      const known = new Map(
         knownFields.map((field) => [field.name, field.type]),
      );
      const out: string[] = [];
      for (const tile of editor.document.tiles)
         for (const filter of tile.filters ?? []) {
            if (filter.given !== name) continue;
            if (
               !known.has(filter.field) ||
               !acceptsField(type, known.get(filter.field))
            )
               out.push(`${filter.field} on ${tile.label ?? tile.name}`);
         }
      return out;
   };
   // Model givens nothing binds yet: what "From the model" offers.
   const available = useMemo(
      () =>
         controlList.filter((c) => c.origin === "model" && c.boundTiles === 0),
      [controlList],
   );

   const save = () => {
      if (!onSave || !editor.dirty || saving) return;
      setSaving(true);
      void editor.save().finally(() => setSaving(false));
   };

   useBuilderShortcuts({
      undo: editor.undo,
      redo: editor.redo,
      ...(onSave ? { save } : {}),
      // Escape drops the selection — unless the menu or the filter window is
      // open, in which case the key is theirs and they close on it themselves.
      escape: () => {
         if (!menu && !filterDialog) setSelected(undefined);
      },
      nudge: (delta) => {
         if (selected === undefined) return;
         const tile = editor.document.tiles[selected];
         if (!tile || tile.declaration.kind === "inherited") return;
         const span = Math.min(
            Math.max((tile.colspan ?? 1) + delta, 1),
            columns,
         );
         if (span === (tile.colspan ?? 1)) return;
         editor.update((draft) => {
            draft.tiles[selected].colspan = span;
         });
      },
   });

   /**
    * Width is the ONLY thing a resize can change.
    *
    * The format lays tiles out as a flow — `# colspan` for width, `# break` to
    * start a row — with no row index, no column position and no height. So a
    * right edge maps onto `colspan` and persists; a bottom edge has nothing to
    * be written as, and a left edge would mean placing the tile, which the grid
    * cannot express either. Offering those handles would be offering a drag the
    * writer then refuses, which is worse than not offering it.
    */
   const startResize = (
      event: React.PointerEvent<HTMLDivElement>,
      index: number,
   ) => {
      const grid = gridBox.current?.getBoundingClientRect();
      const tile = event.currentTarget.parentElement?.getBoundingClientRect();
      if (!grid || !tile) return;
      // Stops the tile's own click selecting a second time on release.
      event.stopPropagation();
      event.preventDefault();
      event.currentTarget.setPointerCapture(event.pointerId);
      dragChrome(true);
      setSelected(index);
      setResize({
         index,
         span: editor.document.tiles[index]?.colspan ?? 1,
         left: tile.left,
         // One column track: the row's width less every gutter in it.
         track: (grid.width - GRID_GAP_PX * (columns - 1)) / columns,
      });
   };

   const onResize = (event: React.PointerEvent<HTMLDivElement>) => {
      if (!resize) return;
      const width = event.clientX - resize.left;
      // A tile of N tracks is N tracks plus the N-1 gutters between them, so
      // adding one gutter back makes the division land on whole columns.
      const span = Math.round(
         (width + GRID_GAP_PX) / (resize.track + GRID_GAP_PX),
      );
      const clamped = Math.min(Math.max(span, 1), columns);
      if (clamped !== resize.span) setResize({ ...resize, span: clamped });
   };

   const endResize = (event: React.PointerEvent<HTMLDivElement>) => {
      if (event.currentTarget.hasPointerCapture(event.pointerId))
         event.currentTarget.releasePointerCapture(event.pointerId);
      dragChrome(false);
      if (!resize) return;
      const { index, span } = resize;
      setResize(undefined);
      // ONE history entry for the whole drag. Writing on every pointer move
      // would put a dozen documents in the stack for one gesture, and undo
      // would walk back through the drag a column at a time.
      editor.update((draft) => {
         draft.tiles[index].colspan = span;
      });
   };

   /**
    * Reordering, unlike resizing, is offered on EVERY tile — an inherited one
    * included.
    *
    * The two edits touch different parts of the file. A width is a `# colspan`
    * tag on the view, so a tile whose view lives in the model cannot be
    * resized here; but order is the `tiles=[…]` array on this file's own
    * `## artifact` tag, which this file always owns. So a tile the properties
    * panel refuses can still be moved.
    *
    * The gesture itself is `@dnd-kit/react`'s — see `sortable.tsx`. What is
    * decided HERE is what a drop means for the file. The library reports which
    * target the tile is over; these handlers turn that into an order and a set
    * of row starts, through `keepRowStructure` for a drop onto a tile and
    * `moveIntoGap` for a drop into the empty end of a row. The preview is
    * rebuilt from the document on each report and written once, on release:
    * ONE history entry for the whole drag, however far it wandered.
    */
   const onDragStart = () => {
      setDragging(true);
      previewRef.current = editor.document.tiles;
   };

   const onDragOver = (event: DragOverEvent) => {
      const { source, target } = event.operation;
      if (!source || !target) return;
      const current = previewRef.current ?? editor.document.tiles;
      let next: DashboardTile[];
      if (target.type === GAP_TYPE) {
         const { after } = target.data as GapData;
         const from = current.findIndex((tile) => tileKey(tile) === source.id);
         // The gap right after the dragged tile is the one it is already
         // previewed in; there is nothing to change.
         if (from < 0 || after === source.id) return;
         const rest = current.filter((_, index) => index !== from);
         const to = rest.findIndex((tile) => tileKey(tile) === after) + 1;
         next = moveIntoGap(current, from, to);
      } else {
         // Onto a tile: the library's own `move` — the same arithmetic every
         // sortable list built on it uses, over the order as it stands. Then
         // the row starts re-applied by position, so the rows keep their shape
         // and the tiles flow through them.
         const keys = current.map(tileKey);
         const reordered = move(keys, event);
         if (reordered.every((key, index) => key === keys[index])) return;
         const byKey = new Map(current.map((tile) => [tileKey(tile), tile]));
         next = keepRowStructure(
            reordered.map((key) => byKey.get(key) as DashboardTile),
            editor.document.tiles.map((tile) => tile.break ?? false),
         );
      }
      previewRef.current = next;
      setPreview(next);
   };

   const onDragEnd = (event: DragEndEvent) => {
      setDragging(false);
      const next = previewRef.current;
      previewRef.current = undefined;
      setPreview(undefined);
      if (event.canceled || !next) return;
      // ONE history entry for the whole drag; a drag that ends where it began
      // changes nothing and makes none.
      editor.update((draft) => {
         draft.tiles = next;
      });
      const landing = next.findIndex(
         (tile) => tileKey(tile) === event.operation.source?.id,
      );
      if (landing >= 0) setSelected(landing);
   };

   /** The filter window's result: bind, and declare when it is new or retagged. */
   const applyFilter = (
      given: string,
      rows: MappingRow[],
      declare?: LocalGiven,
   ) => {
      setFilterDialog(undefined);
      // One history entry for the whole filter, however many tiles it touches.
      editor.update((draft) => {
         if (declare) declareControl(draft, declare);
         applyMapping(draft, given, rows);
      });
   };

   /** Take a control off the dashboard: its declaration, if ours, and every binding. */
   const dropControl = (given: string) => {
      setFilterDialog(undefined);
      editor.update((draft) => removeControl(draft, given));
   };

   // What the grid lays out: the document, except mid-gesture, where it is
   // the preview — the tile being resized at its previewed width, or the tiles
   // in their previewed order. So the row reflows under the pointer exactly as
   // it will once the edit lands.
   const shown = (() => {
      const tiles = editor.document.tiles;
      if (resize !== undefined)
         return tiles.map((each, index) =>
            index === resize.index ? { ...each, colspan: resize.span } : each,
         );
      return preview ?? tiles;
   })();
   // And, while a drag is live, the empty end of every row as a drop target.
   // Not otherwise: a gap is only a place to land while something is in hand.
   const entries = dragging ? withGaps(shown, columns) : shown.map(tileEntry);
   return (
      // The padding is for the SELECTION RING. An outline is painted outside
      // the element's border box, so a selected tile's ring lands beyond the
      // grid — and a host that scrolls this surface clips it: `overflow-y:
      // auto` computes `overflow-x` to `auto` as well, so the ring's left and
      // right edges were cut off against the scroll box while its top and
      // bottom showed. Reserved HERE, on the whole surface, rather than on the
      // grid alone, so the header, the control row and the tiles all inset
      // together and stay aligned with each other. 4px = the ring's 2px offset
      // plus its 2px width.
      <Stack sx={{ gap: 2, p: "4px" }}>
         {/* The READER's own header, over the document being edited: the title
             and the `##"` description as markdown, from the same component the
             dashboard renders. Only the title was drawn here before, so a
             dashboard carrying a narrative header lost it the moment it was
             opened — on the one surface whose whole job is showing the author
             what a reader will get. */}
         <BuilderToolbar
            canUndo={editor.canUndo}
            canRedo={editor.canRedo}
            onUndo={editor.undo}
            onRedo={editor.redo}
            dirty={editor.dirty}
            saving={saving}
            {...(onSave ? { onSave: save } : {})}
         />

         <DashboardProse
            title={editor.document.title || "Untitled dashboard"}
            {...(editor.document.description
               ? { description: editor.document.description }
               : {})}
         />

         {/* The filter band — Looker's, for this format. The header is the
             dashboard's controls as this FILE has them: a chip per control,
             which opens its window — the one place a control is edited, bound
             or removed, so the consequences are in view when it happens. A ×
             on the chip was a second place, with none of them. The live control row the caller
             passes in sits directly under, showing the same controls as a
             reader gets them — from the saved file. */}
         <Stack sx={{ gap: 1 }}>
            <Stack
               direction="row"
               aria-label="Filters"
               sx={{
                  gap: 1,
                  alignItems: "center",
                  flexWrap: "wrap",
                  minHeight: 32,
               }}
            >
               <FilterListIcon
                  sx={{ fontSize: 18, color: theme.tileTitle, opacity: 0.7 }}
               />
               <Typography
                  variant="subtitle2"
                  sx={{ color: theme.tileTitle, mr: 0.5 }}
               >
                  Filters
               </Typography>
               {controlList.length === 0 && (
                  <Typography
                     variant="body2"
                     sx={{ color: theme.tileTitle, opacity: 0.8 }}
                  >
                     None yet.
                  </Typography>
               )}
               {controlList.map((control) => {
                  const unknown = unknownFieldsOf(control.name, control.type);
                  return (
                     <Tooltip
                        key={control.name}
                        title={
                           unknown.length > 0
                              ? `$${control.name} · ${fieldSource} cannot filter on: ${unknown.join(", ")}`
                              : `$${control.name} · ${
                                   control.origin === "dashboard"
                                      ? "declared here"
                                      : "from the model"
                                } · ${control.boundTiles} of ${editor.document.tiles.length} tiles`
                        }
                     >
                        <Chip
                           size="small"
                           label={control.label ?? control.name}
                           aria-label={`Edit filter ${control.name}`}
                           // Warning where a binding names a field the source
                           // does not have: the package would refuse the file.
                           color={unknown.length > 0 ? "warning" : "default"}
                           variant={
                              control.origin === "dashboard"
                                 ? "filled"
                                 : "outlined"
                           }
                           onClick={() => setFilterDialog({ control })}
                           sx={{
                              // Faint when nothing binds it: declared, but not yet a
                              // control a reader would see.
                              opacity: control.boundTiles === 0 ? 0.6 : 1,
                              cursor: "pointer",
                              transition: "opacity 120ms",
                           }}
                        />
                     </Tooltip>
                  );
               })}
               <Button
                  size="small"
                  variant="outlined"
                  startIcon={<AddIcon fontSize="small" />}
                  onClick={() => setFilterDialog({})}
                  sx={{ ml: "auto" }}
               >
                  Add filter
               </Button>
            </Stack>

            {controls}
         </Stack>

         {editor.error && (
            // The edit is still here; the message says what stopped it reaching
            // the file, which is a different thing from losing the work.
            <Alert severity="warning">{editor.error}</Alert>
         )}

         <DragDropProvider
            sensors={builderSensors}
            onDragStart={onDragStart}
            onDragOver={onDragOver}
            onDragEnd={onDragEnd}
         >
            <Box ref={gridBox} sx={{ position: "relative" }}>
               {/* Looker shows blue grid lines while a tile is being dragged,
                and the reason is sound: a flow grid is invisible until you are
                trying to land on it. Drawn only during a gesture, and never in
                the pointer's way. */}
               {(resize !== undefined || dragging) && (
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
               )}

               <DashboardGrid
                  tiles={entries}
                  columns={columns}
                  keyOf={(entry) =>
                     entry.kind === "gap"
                        ? gapId(entry.after)
                        : tileKey(entry.tile)
                  }
                  renderTile={(entry) => {
                     if (entry.kind === "gap")
                        return (
                           // The empty end of a row, made a target while a
                           // drag is live. Dashed like the guides, and lit
                           // when the tile in hand is over it.
                           <GapDroppable
                              id={gapId(entry.after)}
                              after={entry.after}
                           >
                              {({ ref, isDropTarget }) => (
                                 <Box
                                    ref={ref}
                                    aria-hidden
                                    sx={{
                                       minHeight: 48,
                                       borderRadius: 1,
                                       border: `1px dashed ${theme.drillLink}`,
                                       bgcolor: isDropTarget
                                          ? theme.tile
                                          : "transparent",
                                       opacity: isDropTarget ? 0.95 : 0.4,
                                       transition:
                                          "opacity 120ms, background-color 120ms",
                                    }}
                                 />
                              )}
                           </GapDroppable>
                        );
                     const { tile: each, index } = entry;
                     return (
                        <TileSortable id={tileKey(each)} index={index}>
                           {({ ref, handleRef, isDragSource }) => (
                              <Box
                                 ref={ref}
                                 onClick={() => setSelected(index)}
                                 // Selected on the press, so a tile reads as
                                 // held the moment it is. The drag itself is
                                 // the sensor's — see `sortable.tsx`.
                                 onPointerDown={(event) => {
                                    if (event.button === 0) setSelected(index);
                                 }}
                                 aria-label={`Tile ${each.name}`}
                                 aria-current={index === selected}
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
                                    outline:
                                       index === selected
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
                                    // pointer finds each.
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
                                    // in is the faded, dashed one. Looker draws
                                    // the same pair. Doubled so they outrank
                                    // the hover rule below on the tile in hand.
                                    "&&[data-dnd-dragging]": {
                                       opacity: 1,
                                       outline: "none",
                                       boxShadow:
                                          "0 12px 32px rgba(0, 0, 0, 0.22)",
                                    },
                                    "&&[data-dnd-placeholder]": {
                                       opacity: 0.45,
                                       outline: `2px dashed ${theme.drillLink}`,
                                       boxShadow: "none",
                                    },
                                    "&:hover .builder-affordance, &:focus-within .builder-affordance":
                                       { opacity: 1 },
                                    // A hovered tile lifts, the way Looker's
                                    // does in edit mode: the one card that
                                    // will respond to the pointer, told apart
                                    // from the ones that will not.
                                    "&:hover": {
                                       outlineColor:
                                          index === selected
                                             ? theme.drillLink
                                             : theme.border,
                                       boxShadow:
                                          "0 2px 10px rgba(0, 0, 0, 0.10)",
                                    },
                                 }}
                              >
                                 {renderTile ? (
                                    renderTile(each)
                                 ) : (
                                    // No caller-supplied tile: say what this
                                    // one will run, so the surface is still
                                    // legible without a server.
                                    <Paper
                                       elevation={0}
                                       sx={{
                                          p: 2,
                                          minHeight: 140,
                                          background: theme.tile,
                                          borderRadius: 1,
                                          border: theme.border,
                                       }}
                                    >
                                       <Typography
                                          variant="subtitle2"
                                          sx={{
                                             fontWeight: 500,
                                             color: theme.tileTitle,
                                          }}
                                       >
                                          {each.label ?? each.name}
                                       </Typography>
                                       {each.subtitle && (
                                          <Typography
                                             variant="caption"
                                             sx={{
                                                display: "block",
                                                color: theme.tileTitle,
                                                opacity: 0.8,
                                             }}
                                          >
                                             {each.subtitle}
                                          </Typography>
                                       )}
                                       <Typography
                                          variant="caption"
                                          sx={{
                                             display: "block",
                                             mt: 1,
                                             color: theme.tileTitle,
                                             opacity: 0.7,
                                          }}
                                       >
                                          {each.source} → {each.name}
                                       </Typography>
                                    </Paper>
                                 )}

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
                                    aria-label={`Move ${each.label ?? each.name}`}
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
                                       opacity:
                                          isDragSource || index === selected
                                             ? 0.9
                                             : 0,
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
                                 {resize?.index !== index && (
                                    <IconButton
                                       className="builder-affordance"
                                       size="small"
                                       aria-label={`Settings for ${each.label ?? each.name}`}
                                       onClick={(event) => {
                                          event.stopPropagation();
                                          setSelected(index);
                                          setMenu({
                                             anchor: event.currentTarget,
                                             index,
                                          });
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
                                          opacity:
                                             index === selected ||
                                             menu?.index === index
                                                ? 0.9
                                                : 0,
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

                                 {/* Looker reports a tile's width, height and
                                  share of the dashboard while you drag it.
                                  Height is not ours to show, but the span and
                                  its share are exactly what a flow grid leaves
                                  you guessing at. */}
                                 {resize?.index === index && (
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
                                       {resize.span} of {columns} ·{" "}
                                       {Math.round(
                                          (resize.span / columns) * 100,
                                       )}
                                       %
                                    </Box>
                                 )}

                                 {/* The right edge, draggable — but only on a
                                  tile whose tags this file owns. An inherited
                                  tile's tags live on the model's view, which
                                  the builder does not write, so a drag here
                                  could not be saved. Its own pointer handling
                                  stops the press reaching the sortable, and
                                  the sensor refuses a separator regardless. */}
                                 {each.declaration.kind !== "inherited" && (
                                    <Box
                                       className="builder-affordance"
                                       role="separator"
                                       aria-orientation="vertical"
                                       aria-label={`Resize ${each.label ?? each.name}`}
                                       onPointerDown={(event) =>
                                          startResize(event, index)
                                       }
                                       onPointerMove={onResize}
                                       onPointerUp={endResize}
                                       onPointerCancel={endResize}
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
                                          opacity:
                                             resize?.index === index ||
                                             index === selected
                                                ? 1
                                                : 0,
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
                  }}
               />
            </Box>
         </DragDropProvider>
         <FilterDialog
            open={filterDialog !== undefined}
            document={editor.document}
            {...(filterDialog?.control
               ? { control: filterDialog.control }
               : {})}
            available={available}
            {...(knownFields ? { fields: knownFields } : {})}
            {...(fieldSource ? { fieldsOf: fieldSource } : {})}
            onClose={() => setFilterDialog(undefined)}
            onApply={applyFilter}
            onRemove={dropControl}
         />
         <TileMenu
            anchor={menu?.anchor ?? null}
            tile={
               menu === undefined
                  ? undefined
                  : editor.document.tiles[menu.index]
            }
            onClose={() => setMenu(undefined)}
            onCommit={(next) => {
               const at = menu?.index;
               if (at === undefined) return;
               editor.update((draft) => {
                  draft.tiles[at] = next;
               });
            }}
         />
      </Stack>
   );
}
