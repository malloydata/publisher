// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { DragDropProvider } from "@dnd-kit/react";
import AddIcon from "@mui/icons-material/Add";
import { Alert, Box, Button, Stack, Typography } from "@mui/material";
import {
   useCallback,
   useEffect,
   useMemo,
   useRef,
   useState,
   type ReactNode,
} from "react";
import {
   DashboardGrid,
   DEFAULT_COLUMNS,
   nudgedSpan,
} from "../Dashboard/DashboardGrid";
import { tileTitle } from "../Dashboard/DashboardTile";
import type { TileHeadingSlots } from "../Dashboard/TileCard";
import type { SavesTo } from "./documentSession";
import type { BuilderEvent } from "./telemetry";
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
import { UnsavedChangesDialog } from "../UnsavedChangesDialog";
import { BuilderToolbar } from "./BuilderToolbar";
import { changedTileKey } from "./changedTile";
import { InlineMarkdown } from "./InlineMarkdown";
import { InlineText } from "./InlineText";
import { OpenDraftContext, type OpenDraftSink } from "./openDraft";
import { filterableFields, type PackageCatalog } from "./catalog";
import {
   isQueryTile,
   isTextTile,
   tileKey,
   type DashboardDocument,
   type DashboardTile,
   type LocalGiven,
   type QueryTile,
} from "./document";
import { AddTileDialog, type NewTile } from "./AddTileDialog";
import { SaveNotice } from "./SaveNotice";
import { DrillDialog } from "./DrillDialog";
import { FilterDialog } from "./FilterDialog";
import { SettingsPopover, settingsOf } from "./SettingsPopover";
import { FilterStrip } from "./FilterStrip";
import { gapId, tileEntry, withGaps } from "./layout";
import { builderSensors } from "./sortable";
import { GapTarget, GridGuides, TileFrame, TilePlaceholder } from "./TileFrame";
import { TextTileBody } from "./TextTileBody";
import { useTileReorder } from "./useTileReorder";
import { TileMenu } from "./TileMenu";
import { useBuilderSession } from "./useBuilderSession";
import { useDashboardEditor } from "./useDashboardEditor";
import type { SaveHandler } from "./useDocumentEditor";

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
 * an outline for selection, a grip and a menu in the corners.
 *
 * **Everything here is on the thing it changes.** Set a tile's width from its menu, and drag
 * the tile itself to reorder — into the empty end of a row
 * to change which row it is in. Click a title, subtitle, description or text
 * tile to edit it where it stands. FILTERS are configured in exactly one place, the strip under the
 * header: each chip opens the tiles-to-update window for that control,
 * which is where a tile is bound or unbound, and "Add filter" declares a new
 * one in this file — the convention {@link LocalGiven} describes — or binds one
 * the model offers. Nothing about filters is on the tiles themselves: a second
 * place to edit the same binding is a second place for it to be wrong.
 *
 * Tiles are added from the package's catalog and removed from their menu.
 */
export interface DashboardBuilderProps {
   /** The file being edited. */
   source: string;
   /** The document that file produced. */
   document: DashboardDocument;
   /**
    * Persist the patched file. Left out, the builder edits without saving.
    * Undo save calls it too, with `purpose: "undo"` and the text from before
    * the save, so it must write through the same channel and checks.
    */
   onSave?: SaveHandler<DashboardDocument>;
   /** Whether the save notice (View change, Undo save) is showing, for a host that must not replace the document under it. */
   onSaveNoticeChange?: (showing: boolean) => void;
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
    *
    * `heading` is the tile's title and subtitle as fields edited in place;
    * absent for a tile whose title the model owns. A host that draws the
    * heading itself passes it on (`DashboardTile`'s `heading`), or the tile
    * cannot be retitled on the page.
    */
   renderTile?: (tile: QueryTile, heading?: TileHeadingSlots) => ReactNode;
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
   /**
    * The other dashboards in the package, by slug — where a clicked cell can
    * go. Absent, a drill can only filter this dashboard.
    */
   dashboards?: string[];
   /** Saves and refusals, for the host to log; see `DashboardEvent`. A notebook-kind document reports `NotebookEvent`s instead. */
   onEvent?: (event: BuilderEvent) => void;
   /**
    * The document is a conversion of a cell-format notebook: `from` is the text
    * on disk, `to` the layout text it converts to. Save writes `to` with the
    * edits spliced in, and Undo save puts `from` back.
    */
   conversion?: { from: string; to: string };
   /**
    * Whether the document differs from what was last saved, on every change
    * and on mount.
    *
    * For a host that has to decide whether it may replace what is open — a
    * newer version arriving from its store, a navigation away. `onChange`
    * cannot answer that: the saved baseline lives in this hook, so a parent
    * comparing the document it is handed against the one it passed in reads
    * dirty immediately after a successful save.
    */
   onDirtyChange?: (dirty: boolean) => void;
   /**
    * Where `onSave` puts the file, for the event it reports. The builder
    * cannot tell — it is handed a function — and a save into the package, into
    * this browser, and into a host store that holds the record are not the
    * same event.
    */
   savesTo?: SavesTo;
   /** The file a save overwrites when `source` is a draft of it, so Undo save restores that file rather than the draft. */
   replaces?: string;
   /** The document's file within the package, so a kind switch tags what its folder would otherwise misread. */
   modelPath?: string;
   /**
    * The host's own extra actions for the edit bar, rendered beside undo, redo
    * and save. Leaving is `onExit`, not this: the builder draws Done itself.
    */
   toolbar?: ReactNode;
   /** Leave the builder: renders "Close", which asks first when there are unsaved edits. */
   onExit?: () => void;
}

export function DashboardBuilder({
   source,
   document,
   onSave,
   onChange,
   onDirtyChange,
   onSaveNoticeChange,
   renderTile,
   controls,
   givens,
   catalog,
   toolbar,
   onExit,
   dashboards,
   onEvent,
   conversion,
   savesTo = "package",
   replaces,
   modelPath,
}: DashboardBuilderProps) {
   const editor = useDashboardEditor({
      source,
      document,
      ...(onSave ? { onSave } : {}),
      ...(conversion ? { conversion } : {}),
      ...(replaces !== undefined ? { replaces } : {}),
      ...(modelPath !== undefined ? { modelPath } : {}),
   });
   const [selected, setSelected] = useState<number | undefined>(undefined);
   // Undo and redo point at the tile they changed: lit briefly, and scrolled to.
   const [flash, setFlash] = useState<string | undefined>(undefined);
   const stepping = useRef(false);
   const lastDocument = useRef(editor.document);
   // A notebook is one column whatever the file says, and the builder never writes its width.
   const notebook = editor.document.kind === "notebook";
   const columns = notebook ? 1 : (editor.document.columns ?? DEFAULT_COLUMNS);
   const gridBox = useRef<HTMLDivElement>(null);
   const { dragging, preview, onDragStart, onDragOver, onDragEnd } =
      useTileReorder({
         tiles: editor.document.tiles,
         commit: (next) =>
            editor.update((draft) => {
               draft.tiles = next;
            }),
         onLanded: setSelected,
      });
   // The filter window: open on a control, or open to add one.
   const [filterDialog, setFilterDialog] = useState<
      { control?: BuilderControl } | undefined
   >(undefined);
   // A tile's menu, anchored to the button that opened it.
   const [menu, setMenu] = useState<
      { anchor: HTMLElement; index: number } | undefined
   >(undefined);
   // The add-tile picker.
   const [addingTile, setAddingTile] = useState(false);
   // Where the next added tile lands; undefined appends.
   const [insertAt, setInsertAt] = useState<number | undefined>(undefined);
   const openAdd = (at?: number) => {
      setInsertAt(at);
      setAddingTile(true);
   };
   // The clickable-cells window, for one tile's source.
   const [drillSource, setDrillSource] = useState<string | undefined>(
      undefined,
   );
   // The page's settings, anchored to the toolbar button that opened them.
   const [settingsAnchor, setSettingsAnchor] = useState<HTMLElement | null>(
      null,
   );
   // One object per document, or the popover's draft would reset on every
   // render of the builder while it is open.
   const settings = useMemo(
      () => settingsOf(editor.document),
      [editor.document],
   );

   // Imported sources a tile or extension reads, which the settings cannot take off.
   const sourcesInUse = useMemo(
      () =>
         new Set([
            ...editor.document.sources.map((source) => source.base),
            ...editor.document.tiles.filter(isQueryTile).map((t) => t.source),
         ]),
      [editor.document],
   );

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
   // The fields a binding may name, PER SOURCE: the dimensions of the model
   // source each of this file's extensions is built on, when the host knows
   // them. A composite spans sources, so one list would call a field the second
   // source has "unknown" and block the window on it. Undefined for a source
   // the catalog does not have, and nothing checks that source's tiles.
   const fieldsBySource = useMemo(
      () =>
         new Map(
            editor.document.sources.map((source) => [
               source.name,
               filterableFields(catalog, source.base),
            ]),
         ),
      [catalog, editor.document.sources],
   );
   const fieldsFor = useCallback(
      (tile: QueryTile) =>
         fieldsBySource.get(tile.source) ??
         // A tile on an imported source with no extension of its own: its
         // source IS a model source, and may be in the catalog directly.
         filterableFields(catalog, tile.source),
      [fieldsBySource, catalog],
   );
   // The catalog's view behind the tile whose menu is open, for the charts the
   // picker may offer: a reference tile's base view, else a view of the tile's own name.
   const menuAt =
      menu === undefined ? undefined : editor.document.tiles[menu.index];
   const menuTile = menuAt && isQueryTile(menuAt) ? menuAt : undefined;
   const menuView = (() => {
      if (!menuTile || !catalog) return undefined;
      const base =
         editor.document.sources.find((s) => s.name === menuTile.source)
            ?.base ?? menuTile.source;
      const viewName =
         menuTile.declaration.kind === "reference"
            ? menuTile.declaration.from
            : menuTile.name;
      return catalog.sources
         .find((s) => s.name === base)
         ?.views.find((v) => v.name === viewName);
   })();
   // Bindings a tile's source cannot take, per control — a field it does not
   // have, or one of a type the given cannot compare: marked on the chip, so a
   // broken binding is seen before the package refuses it.
   const unknownFieldsOf = (
      name: string,
      type: string | undefined,
   ): string[] => {
      const out: string[] = [];
      for (const tile of editor.document.tiles) {
         if (!isQueryTile(tile)) continue;
         const known = fieldsFor(tile);
         if (!known) continue;
         const types = new Map(known.map((field) => [field.name, field.type]));
         for (const filter of tile.filters ?? []) {
            if (filter.given !== name) continue;
            if (
               !types.has(filter.field) ||
               !acceptsField(type, types.get(filter.field))
            )
               out.push(
                  `${filter.field} on ${tile.label ?? tile.name} (${
                     editor.document.sources.find((s) => s.name === tile.source)
                        ?.base ?? tile.source
                  })`,
               );
         }
      }
      return out;
   };
   // Model givens nothing binds yet: what "From the model" offers.
   const available = useMemo(
      () =>
         controlList.filter((c) => c.origin === "model" && c.boundTiles === 0),
      [controlList],
   );

   const shortcuts = useMemo(
      () => ({
         // Escape drops the selection — unless the menu or the filter window is open, in which case the key is theirs and they close on it themselves.
         escape: () => {
            if (!menu && !filterDialog) setSelected(undefined);
         },
         nudge: (delta: 1 | -1) => {
            // The drag's keyboard sensor also reads the arrows, and a drop would write the drag-start width back.
            if (dragging || selected === undefined || notebook) return;
            const tile = editor.document.tiles[selected];
            if (
               !tile ||
               (isQueryTile(tile) && tile.declaration.kind === "inherited")
            )
               return;
            const span = nudgedSpan(tile.colspan ?? 1, delta, columns);
            if (span === (tile.colspan ?? 1)) return;
            editor.update((draft) => {
               draft.tiles[selected].colspan = span;
            });
         },
      }),
      [editor, menu, filterDialog, selected, columns, notebook, dragging],
   );
   useEffect(() => {
      const before = lastDocument.current;
      lastDocument.current = editor.document;
      if (!stepping.current) return;
      stepping.current = false;
      const key = changedTileKey(before, editor.document);
      if (key === undefined) return;
      setFlash(key);
      const target = Array.from(
         gridBox.current?.querySelectorAll<HTMLElement>("[data-tile-key]") ??
            [],
      ).find((element) => element.dataset.tileKey === key);
      // jsdom has no layout, so no scrollIntoView.
      target?.scrollIntoView?.({ block: "nearest", behavior: "smooth" });
   }, [editor.document, gridBox]);
   useEffect(() => {
      if (flash === undefined) return;
      const timer = setTimeout(() => setFlash(undefined), 1500);
      return () => clearTimeout(timer);
   }, [flash]);
   const stepEditor = useMemo(
      () => ({
         ...editor,
         undo: () => {
            stepping.current = true;
            editor.undo();
         },
         redo: () => {
            stepping.current = true;
            editor.redo();
         },
      }),
      [editor],
   );
   const [draftDirty, setDraftDirty] = useState(false);
   const draftCommit = useRef<(() => boolean) | undefined>(undefined);
   const afterCommit = useRef<(() => void) | undefined>(undefined);
   const openDraft = useMemo<OpenDraftSink>(
      () => ({ setDirty: setDraftDirty, commitRef: draftCommit }),
      [],
   );
   // A save reads the committed document, so an open draft is committed first and the save runs once that has rendered.
   const prepare = useCallback(
      (run: () => Promise<void> | void): Promise<void> | void => {
         if (!draftCommit.current) return run();
         if (!draftCommit.current()) return;
         return new Promise<void>((resolve) => {
            afterCommit.current = () => resolve(run());
         });
      },
      [],
   );
   useEffect(() => {
      if (draftDirty || !afterCommit.current) return;
      const run = afterCommit.current;
      afterCommit.current = undefined;
      run();
   }, [draftDirty, editor.dirty]);
   const session = useBuilderSession<DashboardDocument>({
      editor: stepEditor,
      extraDirty: draftDirty,
      prepare,
      unit: { name: "tile", count: (document) => document.tiles.length },
      onSave,
      onExit,
      onDirtyChange,
      onSaveNoticeChange,
      onChange,
      shortcuts,
      report: {
         size: editor.document.tiles.length,
         // A notebook-kind document keeps the `notebook.*` names hosts already count.
         saved: ({ size, structural, durationMs, fromOpen }) =>
            onEvent?.(
               notebook
                  ? {
                       type: "notebook.saved",
                       cells: size,
                       structural,
                       converted: fromOpen,
                       where: savesTo,
                       durationMs,
                    }
                  : {
                       type: "dashboard.saved",
                       tiles: size,
                       structural,
                       where: savesTo,
                       durationMs,
                    },
            ),
         refused: (reason) =>
            onEvent?.({
               type: notebook
                  ? "notebook.save_refused"
                  : "dashboard.save_refused",
               reason,
            }),
         undone: ({ size, structural, durationMs }) =>
            onEvent?.(
               notebook
                  ? {
                       type: "notebook.save_undone",
                       cells: size,
                       structural,
                       where: savesTo,
                       durationMs,
                    }
                  : {
                       type: "dashboard.save_undone",
                       tiles: size,
                       structural,
                       where: savesTo,
                       durationMs,
                    },
            ),
         undoRefused: (reason) =>
            onEvent?.({
               type: notebook
                  ? "notebook.save_undo_refused"
                  : "dashboard.save_undo_refused",
               reason,
            }),
      },
   });

   /** A tile from the picker: on the extension of its source, or a new one. */
   const addTile = (tile: NewTile) => {
      setAddingTile(false);
      editor.update((draft) => {
         let extension = draft.sources.find((s) => s.base === tile.base);
         if (!extension) {
            // A name of the file's own: the base's, suffixed, since an
            // extension cannot share its base's name.
            const taken = new Set(draft.sources.map((s) => s.name));
            let name = `${tile.base}_tiles`;
            for (let n = 2; taken.has(name); n++)
               name = `${tile.base}_tiles_${n}`;
            extension = { name, base: tile.base };
            draft.sources.push(extension);
         }
         // The view's name in the extension: the base view's, suffixed,
         // because an extension inherits its base's views and cannot redeclare
         // one under the same name; then kept distinct from its siblings.
         const used = new Set(
            draft.tiles
               .filter(isQueryTile)
               .filter((t) => t.source === extension!.name)
               .map((t) => t.name),
         );
         let name = `${tile.view}_tile`;
         for (let n = 2; used.has(name); n++) name = `${tile.view}_tile_${n}`;
         draft.tiles.splice(insertAt ?? draft.tiles.length, 0, {
            name,
            source: extension.name,
            declaration: { kind: "reference", from: tile.view },
            ...(notebook ? {} : { colspan: tile.colspan }),
            ...(tile.label ? { label: tile.label } : {}),
            ...(tile.chart ? { chart: tile.chart } : {}),
            ...(tile.chartCarried ? { chartCarried: tile.chartCarried } : {}),
         });
      });
      setSelected(insertAt ?? editor.document.tiles.length);
   };

   /** An empty text tile at the end, named for the first free `text_N`. */
   const addText = () => {
      setAddingTile(false);
      editor.update((draft) => {
         const taken = new Set(
            draft.tiles.filter(isTextTile).map((t) => t.name),
         );
         let n = 1;
         while (taken.has(`text_${n}`)) n++;
         draft.tiles.splice(insertAt ?? draft.tiles.length, 0, {
            kind: "text",
            name: `text_${n}`,
            markdown: "",
            ...(notebook ? {} : { colspan: columns }),
         });
      });
      setSelected(insertAt ?? editor.document.tiles.length);
   };

   /** Change one tile where it stands, found by key so a preview order cannot misdirect it. */
   const editTile = (key: string, change: (tile: DashboardTile) => void) =>
      editor.update((draft) => {
         const tile = draft.tiles.find((each) => tileKey(each) === key);
         if (tile) change(tile);
      });

   /** A query tile's title and subtitle as fields on the tile, unless the model owns them. */
   const headingOf = (
      tile: QueryTile,
      fallback: string,
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

   const removeTile = (index: number) => {
      setMenu(undefined);
      setSelected(undefined);
      editor.update((draft) => {
         draft.tiles.splice(index, 1);
      });
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
   // the preview — the tiles in their previewed order. So the row reflows under
   // the pointer exactly as it will once the edit lands.
   const shown = preview ?? editor.document.tiles;
   // And, while a drag is live, the empty end of every row as a drop target.
   // Not otherwise: a gap is only a place to land while something is in hand.
   const empty = editor.document.tiles.length === 0;
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
      // The bar carries its own gap to what it sits over, so the surface adds
      // none between the two.
      <OpenDraftContext.Provider value={openDraft}>
         <Stack sx={{ gap: 0 }}>
            {/* Outside the ring's inset, so the bar lines up to the pixel with the
             reader's — the whole point of it being the same bar. */}
            <BuilderToolbar
               {...session.toolbarProps}
               {...(toolbar ? { actions: toolbar } : {})}
               {...(catalog ? { onAddTile: () => openAdd() } : {})}
               onSettings={setSettingsAnchor}
               savesTo={savesTo}
            />

            {/* The ring's inset, minus the top: nothing at the top of this stack
             can be selected (the prose and the control row are not tiles), and
             4px there would put the title 4px further from the bar than the
             reader's is. */}
            <Stack sx={{ gap: 2, px: "4px", pb: "4px" }}>
               {editor.pendingOpen && (
                  <Alert severity="info">
                     This notebook is in the cell format. Saving rewrites it as
                     a layout notebook, and a named query run once becomes that
                     tile&apos;s view; Undo save puts it back.
                  </Alert>
               )}
               <SaveNotice {...session.notice} />
               <Box>
                  <Typography variant="h5" sx={{ fontWeight: 600 }}>
                     <InlineText
                        value={editor.document.title}
                        placeholder={
                           notebook ? "Untitled notebook" : "Untitled dashboard"
                        }
                        ariaLabel={
                           notebook ? "Notebook title" : "Dashboard title"
                        }
                        onCommit={(next) =>
                           editor.update((draft) => {
                              draft.title = next;
                           })
                        }
                     />
                  </Typography>
                  <InlineMarkdown
                     variant="caption"
                     markdown={editor.document.description ?? ""}
                     placeholder="Add a description"
                     onCommit={(next) =>
                        editor.update((draft) => {
                           if (next.trim() === "") delete draft.description;
                           else draft.description = next;
                        })
                     }
                  />
               </Box>

               {empty ? (
                  <Stack
                     sx={{
                        alignItems: "flex-start",
                        gap: 1,
                        py: 3,
                        px: 2,
                        border: 1,
                        borderStyle: "dashed",
                        borderColor: "divider",
                        borderRadius: 1,
                     }}
                  >
                     <Typography variant="body2">
                        This {notebook ? "notebook" : "dashboard"} is not served
                        until it has a tile.
                     </Typography>
                     {catalog && (
                        <Button
                           size="small"
                           variant="contained"
                           onClick={() => openAdd()}
                        >
                           Add tile
                        </Button>
                     )}
                  </Stack>
               ) : (
                  <FilterStrip
                     controls={controlList}
                     tileCount={editor.document.tiles.length}
                     unknownFieldsOf={unknownFieldsOf}
                     onEdit={(control) => setFilterDialog({ control })}
                     onAdd={() => setFilterDialog({})}
                     onRemove={dropControl}
                  >
                     {controls}
                  </FilterStrip>
               )}

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
                     {!notebook && dragging && <GridGuides columns={columns} />}

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
                              return <GapTarget after={entry.after} />;
                           const { tile: each, index } = entry;
                           return (
                              <TileFrame
                                 tile={each}
                                 index={index}
                                 selected={index === selected}
                                 flash={tileKey(each) === flash}
                                 menuOpen={menu?.index === index}
                                 {...(notebook && catalog
                                    ? {
                                         onInsertAfter: () =>
                                            openAdd(index + 1),
                                      }
                                    : {})}
                                 onSelect={() => setSelected(index)}
                                 onOpenMenu={(anchor) => {
                                    setSelected(index);
                                    setMenu({ anchor, index });
                                 }}
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
                                 ) : renderTile && !editor.pendingOpen ? (
                                    // Until the conversion is saved the package has none of its views to run.
                                    renderTile(
                                       each,
                                       headingOf(
                                          each,
                                          tileTitle(
                                             `${each.source} -> ${each.name}`,
                                          ),
                                       ),
                                    )
                                 ) : (
                                    <TilePlaceholder
                                       tile={each}
                                       heading={headingOf(each, each.name)}
                                       {...(editor.pendingOpen
                                          ? {
                                               note: "Preview appears after you Save",
                                            }
                                          : {})}
                                    />
                                 )}
                              </TileFrame>
                           );
                        }}
                     />
                  </Box>
               </DragDropProvider>
               {notebook && catalog && !empty && (
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
               <FilterDialog
                  open={filterDialog !== undefined}
                  document={editor.document}
                  {...(filterDialog?.control
                     ? { control: filterDialog.control }
                     : {})}
                  available={available}
                  {...(catalog ? { fieldsFor } : {})}
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
                  onRemove={() => {
                     if (menu !== undefined) removeTile(menu.index);
                  }}
                  {...(editor.document.tiles.length === 1 &&
                  editor.saved.tiles.length > 0
                     ? {
                          removeBlocked:
                             "A saved dashboard needs at least one tile.",
                       }
                     : {})}
                  columns={columns}
                  view={menuView}
                  onDrills={() => {
                     setDrillSource(menuTile?.source);
                  }}
               />
               <DrillDialog
                  open={drillSource !== undefined}
                  document={editor.document}
                  source={editor.document.sources.find(
                     (s) => s.name === drillSource,
                  )}
                  givenNames={controlList.map((c) => c.name)}
                  dashboards={dashboards ?? []}
                  onClose={() => setDrillSource(undefined)}
                  onApply={(drills) =>
                     editor.update((draft) => {
                        const kept = (draft.drills ?? []).filter(
                           (d) => d.source !== drillSource,
                        );
                        const next = [...kept, ...drills];
                        if (next.length === 0) delete draft.drills;
                        else draft.drills = next;
                     })
                  }
               />
               <SettingsPopover
                  anchor={settingsAnchor}
                  settings={settings}
                  catalog={catalog}
                  inUse={sourcesInUse}
                  onClose={() => setSettingsAnchor(null)}
                  onCommit={(next) =>
                     editor.update((draft) => {
                        if (next.kind === undefined) delete draft.kind;
                        else draft.kind = next.kind;
                        if (next.columns === undefined) delete draft.columns;
                        else draft.columns = next.columns;
                        if (next.autorun === undefined) delete draft.autorun;
                        else draft.autorun = next.autorun;
                        draft.imports = next.imports;
                     })
                  }
               />
               <AddTileDialog
                  open={addingTile}
                  document={editor.document}
                  catalog={catalog}
                  columns={columns}
                  onClose={() => setAddingTile(false)}
                  onAdd={addTile}
                  onAddText={addText}
               />
               <UnsavedChangesDialog {...session.exitGuard.dialog} />
            </Stack>
         </Stack>
      </OpenDraftContext.Provider>
   );
}
