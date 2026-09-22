// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { DragDropProvider } from "@dnd-kit/react";
import { Alert, Box, Stack } from "@mui/material";
import {
   useCallback,
   useEffect,
   useMemo,
   useState,
   type ReactNode,
} from "react";
import { DashboardProse } from "../Dashboard/Dashboard";
import { DashboardGrid, DEFAULT_COLUMNS } from "../Dashboard/DashboardGrid";
import { now, type DashboardEventHandler } from "../Dashboard/telemetry";
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
import {
   tileKey,
   type DashboardDocument,
   type DashboardTile,
   type LocalGiven,
} from "./document";
import { AddTileDialog, type NewTile } from "./AddTileDialog";
import { DiffDialog } from "./DiffDialog";
import { DrillDialog } from "./DrillDialog";
import { FilterDialog } from "./FilterDialog";
import { SettingsPopover, settingsOf } from "./SettingsPopover";
import { FilterStrip } from "./FilterStrip";
import { gapId, tileEntry, withGaps } from "./layout";
import { builderSensors } from "./sortable";
import { GapTarget, GridGuides, TileFrame, TilePlaceholder } from "./TileFrame";
import { useTileReorder } from "./useTileReorder";
import { useTileResize } from "./useTileResize";
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
 * header: each chip opens the tiles-to-update window for that control,
 * which is where a tile is bound or unbound, and "Add filter" declares a new
 * one in this file — the convention {@link LocalGiven} describes — or binds one
 * the model offers. Nothing about filters is on the tiles themselves: a second
 * place to edit the same binding is a second place for it to be wrong.
 *
 * Tiles are added from the package's catalog and removed from their menu; a
 * save that adds or removes one shows the file's diff first, because those
 * moves relocate declarations and the comments beside them.
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
   /**
    * The other dashboards in the package, by slug — where a clicked cell can
    * go. Absent, a drill can only filter this dashboard.
    */
   dashboards?: string[];
   /** Saves and refusals, for the host to log; see `DashboardEvent`. */
   onEvent?: DashboardEventHandler;
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
   savesTo?: "package" | "browser" | "host";
   /**
    * The host's own actions for the edit bar — Done — rendered beside
    * undo, redo and save. The builder owns the edits; where the file goes
    * afterwards is the host's, so its buttons sit in the host's slot.
    */
   toolbar?: ReactNode;
}

export function DashboardBuilder({
   source,
   document,
   onSave,
   onChange,
   onDirtyChange,
   renderTile,
   controls,
   givens,
   catalog,
   toolbar,
   dashboards,
   onEvent,
   savesTo = "package",
}: DashboardBuilderProps) {
   const editor = useDashboardEditor({
      source,
      document,
      ...(onSave ? { onSave } : {}),
   });
   const [selected, setSelected] = useState<number | undefined>(undefined);
   const [saving, setSaving] = useState(false);
   const columns = editor.document.columns ?? DEFAULT_COLUMNS;
   const { resize, gridBox, startResize, onResize, endResize } = useTileResize({
      tiles: editor.document.tiles,
      columns,
      onStart: setSelected,
      commit: (index, span) =>
         editor.update((draft) => {
            draft.tiles[index].colspan = span;
         }),
   });
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
   // The add-tile picker, and the diff a structural save shows first.
   const [addingTile, setAddingTile] = useState(false);
   // The clickable-cells window, for one tile's source.
   const [drillSource, setDrillSource] = useState<string | undefined>(
      undefined,
   );
   // The page's settings, anchored to the toolbar button that opened them.
   const [settingsAnchor, setSettingsAnchor] = useState<HTMLElement | null>(
      null,
   );
   const [pendingSave, setPendingSave] = useState<
      { before: string; after: string } | undefined
   >(undefined);

   useEffect(() => {
      onChange?.(editor.document);
   }, [editor.document, onChange]);
   // Also on mount, so a host that remounted the builder on new text is told
   // the slate is clean rather than carrying the previous mount's answer.
   useEffect(() => {
      onDirtyChange?.(editor.dirty);
   }, [editor.dirty, onDirtyChange]);
   // One object per document, or the popover's draft would reset on every
   // render of the builder while it is open.
   const settings = useMemo(
      () => settingsOf(editor.document),
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
      (tile: DashboardTile) =>
         fieldsBySource.get(tile.source) ??
         // A tile on an imported source with no extension of its own: its
         // source IS a model source, and may be in the catalog directly.
         filterableFields(catalog, tile.source),
      [fieldsBySource, catalog],
   );
   // Bindings a tile's source cannot take, per control — a field it does not
   // have, or one of a type the given cannot compare: marked on the chip, so a
   // broken binding is seen before the package refuses it.
   const unknownFieldsOf = (
      name: string,
      type: string | undefined,
   ): string[] => {
      const out: string[] = [];
      for (const tile of editor.document.tiles) {
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

   const commitSave = useCallback(() => {
      setPendingSave(undefined);
      setSaving(true);
      const started = now();
      const { structural } = editor;
      const tiles = editor.document.tiles.length;
      void editor
         .save()
         .then((outcome) => {
            if (outcome.ok === true)
               onEvent?.({
                  type: "dashboard.saved",
                  tiles,
                  structural,
                  where: savesTo,
                  durationMs: now() - started,
               });
            else
               onEvent?.({
                  type: "dashboard.save_refused",
                  reason: outcome.reason,
               });
         })
         .finally(() => setSaving(false));
   }, [editor, onEvent, savesTo]);
   const save = useCallback(() => {
      if (!onSave || !editor.dirty || saving) return;
      if (!editor.structural) {
         commitSave();
         return;
      }
      // A tile was added or removed: show what that does to the file first.
      void editor.preview().then((result) => {
         if (result.ok)
            setPendingSave({ before: editor.source, after: result.source });
         // A refusal surfaces through the same path a save's would.
         else commitSave();
      });
   }, [onSave, editor, saving, commitSave]);

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
               .filter((t) => t.source === extension!.name)
               .map((t) => t.name),
         );
         let name = `${tile.view}_tile`;
         for (let n = 2; used.has(name); n++) name = `${tile.view}_tile_${n}`;
         draft.tiles.push({
            name,
            source: extension.name,
            declaration: { kind: "reference", from: tile.view },
            colspan: tile.colspan,
            ...(tile.label ? { label: tile.label } : {}),
         });
      });
      setSelected(editor.document.tiles.length);
   };

   const removeTile = (index: number) => {
      setMenu(undefined);
      setSelected(undefined);
      editor.update((draft) => {
         draft.tiles.splice(index, 1);
      });
   };

   useBuilderShortcuts(
      // One handlers object per change of what they read, so the key listener
      // is not torn down and re-bound on every render.
      useMemo(
         () => ({
            undo: editor.undo,
            redo: editor.redo,
            ...(onSave ? { save } : {}),
            // Escape drops the selection — unless the menu or the filter
            // window is open, in which case the key is theirs and they close
            // on it themselves.
            escape: () => {
               if (!menu && !filterDialog) setSelected(undefined);
            },
            nudge: (delta: 1 | -1) => {
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
         }),
         [editor, onSave, save, menu, filterDialog, selected, columns],
      ),
   );

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
      // The bar carries its own gap to what it sits over, so the surface adds
      // none between the two.
      <Stack sx={{ gap: 0 }}>
         {/* Outside the ring's inset, so the bar lines up to the pixel with the
             reader's — the whole point of it being the same bar. */}
         <BuilderToolbar
            canUndo={editor.canUndo}
            canRedo={editor.canRedo}
            onUndo={editor.undo}
            onRedo={editor.redo}
            dirty={editor.dirty}
            saving={saving}
            {...(onSave ? { onSave: save } : {})}
            {...(toolbar ? { actions: toolbar } : {})}
            {...(catalog ? { onAddTile: () => setAddingTile(true) } : {})}
            onSettings={setSettingsAnchor}
         />

         {/* The ring's inset, minus the top: nothing at the top of this stack
             can be selected (the prose and the control row are not tiles), and
             4px there would put the title 4px further from the bar than the
             reader's is. */}
         <Stack sx={{ gap: 2, px: "4px", pb: "4px" }}>
            <DashboardProse
               title={editor.document.title || "Untitled dashboard"}
               {...(editor.document.description
                  ? { description: editor.document.description }
                  : {})}
            />

            <FilterStrip
               controls={controlList}
               tileCount={editor.document.tiles.length}
               unknownFieldsOf={unknownFieldsOf}
               onEdit={(control) => setFilterDialog({ control })}
               onAdd={() => setFilterDialog({})}
            >
               {controls}
            </FilterStrip>

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
                  {(resize !== undefined || dragging) && (
                     <GridGuides columns={columns} />
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
                           return <GapTarget after={entry.after} />;
                        const { tile: each, index } = entry;
                        return (
                           <TileFrame
                              tile={each}
                              index={index}
                              selected={index === selected}
                              menuOpen={menu?.index === index}
                              resizeSpan={
                                 resize?.index === index
                                    ? resize.span
                                    : undefined
                              }
                              columns={columns}
                              onSelect={() => setSelected(index)}
                              onOpenMenu={(anchor) => {
                                 setSelected(index);
                                 setMenu({ anchor, index });
                              }}
                              onResizeStart={(event) =>
                                 startResize(event, index)
                              }
                              onResizeMove={onResize}
                              onResizeEnd={endResize}
                           >
                              {renderTile ? (
                                 renderTile(each)
                              ) : (
                                 <TilePlaceholder tile={each} />
                              )}
                           </TileFrame>
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
               columns={columns}
               onDrills={() => {
                  if (menu !== undefined)
                     setDrillSource(editor.document.tiles[menu.index]?.source);
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
               onClose={() => setSettingsAnchor(null)}
               onCommit={(next) =>
                  editor.update((draft) => {
                     draft.title = next.title;
                     if (next.description === undefined)
                        delete draft.description;
                     else draft.description = next.description;
                     if (next.columns === undefined) delete draft.columns;
                     else draft.columns = next.columns;
                     if (next.autorun === undefined) delete draft.autorun;
                     else draft.autorun = next.autorun;
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
            />
            <DiffDialog
               open={pendingSave !== undefined}
               before={pendingSave?.before ?? ""}
               after={pendingSave?.after ?? ""}
               onConfirm={commitSave}
               onClose={() => setPendingSave(undefined)}
            />
         </Stack>
      </Stack>
   );
}
