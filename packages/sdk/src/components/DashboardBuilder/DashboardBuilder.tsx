// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import AddIcon from "@mui/icons-material/Add";
import { Alert, Button, Stack, Typography } from "@mui/material";
import { useMemo, useRef, useState, type ReactNode } from "react";
import { DEFAULT_COLUMNS, nudgedSpan } from "../Dashboard/DashboardGrid";
import type { TileChrome, TileHeadingSlots } from "../Dashboard/TileCard";
import type { SavesTo } from "./documentSession";
import type { BuilderEvent } from "./telemetry";
import type { BuilderGiven } from "./controls";
import { BuilderDialogs } from "./BuilderDialogs";
import { BuilderGrid } from "./BuilderGrid";
import { BuilderHeader } from "./BuilderHeader";
import { builderReport } from "./builderReport";
import { BuilderToolbar } from "./BuilderToolbar";
import { OpenDraftContext } from "./openDraft";
import type { PackageCatalog } from "./catalog";
import {
   isQueryTile,
   type DashboardDocument,
   type QueryTile,
} from "./document";
import { FilterStrip } from "./FilterStrip";
import { SELECTION_RING_PX } from "./TileFrame";
import { useBuilderSelection } from "./useBuilderSelection";
import { useBuilderSession } from "./useBuilderSession";
import { useControlBindings } from "./useControlBindings";
import { useConversionConfirm } from "./useConversionConfirm";
import { useDashboardEditor } from "./useDashboardEditor";
import type { SaveHandler } from "./useDocumentEditor";
import { useOpenDraftCommit } from "./useOpenDraftCommit";
import { useTileEditing } from "./useTileEditing";
import { useTileReorder } from "./useTileReorder";
import { useTileResize } from "./useTileResize";

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
   renderTile?: (
      tile: QueryTile,
      heading?: TileHeadingSlots,
      /** The document's tile chrome — bare on a notebook — so the live tile reads as the reader's. */
      chrome?: TileChrome,
   ) => ReactNode;
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
   /** The backend's own words for where Save writes, in place of the generic line for `savesTo`. */
   saveLabel?: string;
   /** The file a save overwrites when `source` is a draft of it, so Undo save restores that file rather than the draft. */
   replaces?: string;
   /** The document's file within the package, so a kind switch tags what its folder would otherwise misread. */
   modelPath?: string;
   /** The document is held as text, which the server reads by its tags rather than its folder: the tag always carries `kind=`. */
   explicitKind?: boolean;
   /**
    * Why "Add filter" is off, or absent when it is on. A document kept as bare
    * text has no `given:` of its own to write, so only the model's givens can
    * be bound, from the chips.
    */
   addFilterDisabledReason?: string;
   /**
    * The host's own extra actions for the edit bar, rendered beside undo, redo
    * and save.
    */
   toolbar?: ReactNode;
   /** Leave the builder, opted into: draws Close, which asks first when edits are unsaved. */
   onExit?: () => void;
}

export function DashboardBuilder({
   source,
   document,
   onSave,
   onChange,
   onDirtyChange,
   onExit,
   renderTile,
   controls,
   givens,
   catalog,
   toolbar,
   dashboards,
   onEvent,
   conversion,
   savesTo = "package",
   saveLabel,
   replaces,
   modelPath,
   explicitKind,
   addFilterDisabledReason,
}: DashboardBuilderProps) {
   const editor = useDashboardEditor({
      source,
      document,
      ...(onSave ? { onSave } : {}),
      ...(conversion ? { conversion } : {}),
      ...(replaces !== undefined ? { replaces } : {}),
      ...(modelPath !== undefined ? { modelPath } : {}),
      ...(explicitKind ? { explicitKind } : {}),
   });
   const gridBox = useRef<HTMLDivElement>(null);
   const {
      selected,
      descriptionSelected,
      selectTile,
      deselectTile,
      selectDescription,
      clearSelection,
      flash,
      stepEditor,
   } = useBuilderSelection({ editor, gridBox });
   // A notebook is one column whatever the file says, and the builder never writes its width.
   const notebook = editor.document.kind === "notebook";
   const columns = notebook ? 1 : (editor.document.columns ?? DEFAULT_COLUMNS);
   // The reader's tile chrome for this document: cards on a dashboard, bare
   // flow on a notebook. Every tile, text block and the description take it,
   // so editing a document draws it the way reading it does.
   const chrome: TileChrome = notebook ? "none" : "card";
   // The name a reader's view titles an untitled document with.
   const documentSlug = modelPath
      ?.split("/")
      .at(-1)
      ?.replace(/\.malloy$/, "");
   const reorder = useTileReorder({
      tiles: editor.document.tiles,
      commit: (next) =>
         editor.update((draft) => {
            draft.tiles = next;
         }),
      onLanded: selectTile,
   });
   const { dragging } = reorder;
   // Dragging a tile's right edge sets its width, written once on release.
   const resizing = useTileResize({
      tiles: editor.document.tiles,
      columns,
      gridBox,
      onStart: selectTile,
      commit: (index, span) =>
         editor.update((draft) => {
            const tile = draft.tiles[index];
            if (tile) tile.colspan = span;
         }),
   });
   const tiles = useTileEditing({
      editor,
      notebook,
      columns,
      modelPath,
      selectTile,
      deselectTile,
   });
   const { menu, addingTile, openAdd } = tiles;
   const bindings = useControlBindings({
      editor,
      opened: document,
      givens,
      catalog,
   });
   const { filterDialog, setFilterDialog } = bindings;
   // The clickable-cells window, for one tile's source.
   const [drillSource, setDrillSource] = useState<string | undefined>(
      undefined,
   );
   const { draftDirty, openDraft, prepare } = useOpenDraftCommit({
      editorDirty: editor.dirty,
   });
   const { confirmConversion, prepareSave } = useConversionConfirm({
      pendingOpen: editor.pendingOpen,
      prepare,
   });

   const shortcuts = useMemo(
      () => ({
         // Escape drops the selection — unless the menu or the filter window is open, in which case the key is theirs and they close on it themselves.
         escape: () => {
            // Not while a menu or a window is open: the key is theirs, and
            // they close on it themselves.
            if (
               !menu &&
               !filterDialog &&
               !addingTile &&
               drillSource === undefined &&
               confirmConversion === undefined
            )
               clearSelection();
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
      [
         editor,
         menu,
         filterDialog,
         addingTile,
         drillSource,
         confirmConversion,
         clearSelection,
         selected,
         columns,
         notebook,
         dragging,
      ],
   );
   const session = useBuilderSession<DashboardDocument>({
      editor: stepEditor,
      extraDirty: draftDirty,
      prepare: prepareSave,
      unit: { name: "tile", count: (document) => document.tiles.length },
      onSave,
      onDirtyChange,
      onExit,
      onChange,
      shortcuts,
      report: builderReport({
         size: editor.document.tiles.length,
         notebook,
         savesTo,
         onEvent,
      }),
   });

   const empty = editor.document.tiles.length === 0;
   // The builder's actions: on the title's line, at the right, above the
   // description.
   const actions = (
      <BuilderToolbar
         {...session.toolbarProps}
         {...(toolbar ? { actions: toolbar } : {})}
         {...(catalog ? { onAddTile: () => openAdd() } : {})}
         savesTo={savesTo}
         {...(saveLabel ? { saveLabel } : {})}
      />
   );
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
      <OpenDraftContext.Provider value={openDraft}>
         {/* Pulled out by the ring's inset, so the content inside it lands on
          the same edges as the reader's view: editing and reading are the same
          page, and the margins must not move between them. */}
         <Stack sx={{ gap: 0, mx: `-${SELECTION_RING_PX}px` }}>
            {/* The ring's inset, minus the top: nothing at the top of this stack
             can be selected (the prose and the control row are not tiles). */}
            <Stack
               sx={{
                  gap: 2,
                  px: `${SELECTION_RING_PX}px`,
                  pb: `${SELECTION_RING_PX}px`,
               }}
            >
               {editor.pendingOpen && (
                  <Alert severity="info">
                     This notebook is in the cell format. Saving rewrites it as
                     a layout notebook, and a named query run once becomes that
                     tile&apos;s view.
                  </Alert>
               )}
               <BuilderHeader
                  editor={editor}
                  notebook={notebook}
                  documentSlug={documentSlug}
                  chrome={chrome}
                  actions={actions}
                  descriptionSelected={descriptionSelected}
                  selectDescription={selectDescription}
               />

               {empty ? (
                  <>
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
                           This {notebook ? "notebook" : "dashboard"} is not
                           served until it has a tile.
                        </Typography>
                        {catalog && (
                           <Button
                              size="small"
                              startIcon={<AddIcon />}
                              onClick={() => openAdd()}
                           >
                              Add tile
                           </Button>
                        )}
                     </Stack>
                  </>
               ) : (
                  <FilterStrip
                     controls={bindings.controlList}
                     tileCount={editor.document.tiles.length}
                     unknownFieldsOf={bindings.unknownFieldsOf}
                     onEdit={(control) => setFilterDialog({ control })}
                     onAdd={() => setFilterDialog({})}
                     {...(addFilterDisabledReason
                        ? { addDisabledReason: addFilterDisabledReason }
                        : {})}
                     onRemove={bindings.dropControl}
                  >
                     {controls}
                  </FilterStrip>
               )}

               {editor.error && (
                  // The edit is still here; the message says what stopped it reaching
                  // the file, which is a different thing from losing the work.
                  <Alert severity="warning">{editor.error}</Alert>
               )}

               <BuilderGrid
                  editor={editor}
                  reorder={reorder}
                  resizing={resizing}
                  gridBox={gridBox}
                  columns={columns}
                  notebook={notebook}
                  chrome={chrome}
                  canAdd={!!catalog}
                  selected={selected}
                  flash={flash}
                  menuIndex={menu?.index}
                  onSelect={selectTile}
                  onOpenMenu={(anchor, index) => {
                     selectTile(index);
                     tiles.setMenu({ anchor, index });
                  }}
                  openAdd={openAdd}
                  editTile={tiles.editTile}
                  {...(renderTile ? { renderTile } : {})}
               />
               <BuilderDialogs
                  editor={editor}
                  catalog={catalog}
                  columns={columns}
                  dashboards={dashboards}
                  bindings={bindings}
                  tiles={tiles}
                  drillSource={drillSource}
                  setDrillSource={setDrillSource}
                  confirmConversion={confirmConversion}
                  exitDialog={session.exitGuard.dialog}
               />
            </Stack>
         </Stack>
      </OpenDraftContext.Provider>
   );
}
