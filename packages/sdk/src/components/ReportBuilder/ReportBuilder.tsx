// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { reportClasses } from "../../theme/classes";
import AddIcon from "@mui/icons-material/Add";
import { Alert, Button, Stack, Typography } from "@mui/material";
import { useMemo, useRef, useState, type ReactNode } from "react";
import { nudgedSpan, useDefaultTileSpan } from "../Dashboard/DashboardGrid";
import type { TileChrome, TileHeadingSlots } from "../Dashboard/TileCard";
import type { SavesTo } from "./documentSession";
import type { ReportEventHandler } from "../Report/telemetry";
import type { BuilderGiven } from "./controls";
import { BuilderDialogs } from "./BuilderDialogs";
import type { ViewPreview } from "./AddTileDialog";
import { BuilderGrid } from "./BuilderGrid";
import { BuilderHeader } from "./BuilderHeader";
import { builderReport } from "./builderReport";
import { BuilderToolbar, SAVE_TARGET } from "./BuilderToolbar";
import {
   useReportControls,
   type BuilderControlsHandler,
} from "./builderControls";
import { OpenDraftContext } from "./openDraft";
import type { PackageCatalog } from "./catalog";
import {
   isQueryTile,
   isReadOnlyTile,
   type DashboardDocument,
   type QueryTile,
} from "./document";
import { FilterStrip, type FixedFilter } from "./FilterStrip";
import { clockTime, type SaveStatusProps } from "./SaveStatus";
import { SELECTION_RING_PX } from "./TileFrame";
import { useBuilderSelection } from "./useBuilderSelection";
import { useBuilderSession } from "./useBuilderSession";
import { useControlBindings } from "./useControlBindings";
import { useReportDraft } from "./useReportDraft";
import { useReportEditor } from "./useReportEditor";
import type { SaveHandler } from "./useDocumentEditor";
import { useOpenDraftCommit } from "./useOpenDraftCommit";
import { useTileEditing } from "./useTileEditing";
import { useTileReorder } from "./useTileReorder";
import { useTileResize } from "./useTileResize";
import { layoutRules, withSpan } from "../Report/layoutRules";
import { ReportPage } from "../Report/ReportPage";

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
export interface ReportBuilderProps {
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
      /** Always `card`, in either layout; kept for hosts written against it. */
      chrome?: TileChrome,
   ) => ReactNode;
   /** A view picked in Add a tile, run as the tile it would add; absent, the picker says what will run. */
   renderViewPreview?: (view: ViewPreview) => ReactNode;
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
   /**
    * Opens, saves and refusals, for the host to log; see
    * `ReportEvent`. Each is stamped with `modelPath` (empty when the builder
    * was given none) and the document's layout as it stands.
    */
   onEvent?: ReportEventHandler;
   /**
    * Whether the document differs from what was last saved, on every change
    * and on mount.
    *
    * For a host that has to decide whether it may replace what is open — a
    * newer version arriving from its store, a navigation away. `onChange`
    * cannot answer that: the saved baseline lives in this hook, so a parent
    * comparing the document it is handed against the one it passed in reads
    * dirty immediately after a successful save.
    *
    * The second argument is whether leaving now would LOSE edits: the same,
    * unless a draft is kept (`draftKey`), when it is only text still being
    * typed into an open field. A leave guard reads that one; anything
    * deciding whether to replace the document reads the first.
    */
   onDirtyChange?: (dirty: boolean, wouldLose: boolean) => void;
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
   /** The document's file within the package: the name an untitled document is shown under, and what a new tile is written beside. */
   modelPath?: string;
   /** Set for a document held as text: the sources it sees without an `import`. A tile on one of them adds no import. */
   visibleSources?: readonly string[];
   /**
    * Why "Add filter" is off, or absent when it is on. A document kept as bare
    * text has no `given:` of its own to write, so only the model's givens can
    * be bound, from the chips.
    */
   addFilterDisabledReason?: string;
   /**
    * Filters in the row a tile reads other than through a binding the
    * builder writes (the model's own `where:`, or this report's own
    * `source:`), each with why it cannot be changed here. They can be set,
    * as in the reader, but not removed or configured.
    */
   fixedFilters?: ReadonlyMap<string, FixedFilter>;
   /**
    * Open a model by its package path: offered as "Open the model" on a filter
    * the model sets. Omitted, the filter only says where it is set.
    */
   onOpenModel?: (modelPath: string) => void;
   /**
    * Names a filter the builder adds must not take, beyond the givens on the
    * page: a host that resolves held text itself passes every given the model
    * declares, so a new filter never takes a model given's name.
    */
   takenGivenNames?: readonly string[];
   /**
    * The host's own extra actions for the edit bar, rendered beside undo, redo
    * and save.
    */
   toolbar?: ReactNode;
   /** Leave the builder, opted into: draws Close, which asks first when edits are unsaved. */
   onExit?: () => void;
   /**
    * Take over the page-level controls (save status and Save, undo and redo,
    * and adding a tile, text or filter): the builder hands
    * them over as `BuilderControls` whenever they change, and draws no
    * toolbar of its own. Draw them with `BuilderBar`, or place each in
    * the host's own chrome. `undefined` is handed over when the builder goes.
    */
   onControlsChange?: BuilderControlsHandler;
   /**
    * Keep unsaved edits in this browser under this key, and put them back
    * when the builder next opens on the same key: so leaving never loses work,
    * and a host can offer one way out, `done` (write, then leave), with no
    * "leave with unsaved changes?" on the way. `onDirtyChange`'s second
    * argument then reports only what a draft cannot hold, text still being
    * typed into an open field.
    * One key per report and place it is saved to. Absent, nothing is kept.
    */
   draftKey?: string;
}

export function ReportBuilder({
   source,
   document,
   onSave,
   onChange,
   onDirtyChange,
   onExit,
   renderTile,
   renderViewPreview,
   controls,
   givens,
   catalog,
   toolbar,
   dashboards,
   onEvent,
   savesTo = "package",
   saveLabel,
   replaces,
   modelPath,
   visibleSources,
   addFilterDisabledReason,
   fixedFilters,
   onOpenModel,
   takenGivenNames,
   onControlsChange,
   draftKey,
}: ReportBuilderProps) {
   const editor = useReportEditor({
      source,
      document,
      ...(onSave ? { onSave } : {}),
      ...(replaces !== undefined ? { replaces } : {}),
      ...(visibleSources ? { visibleSources } : {}),
   });
   const draft = useReportDraft(draftKey, editor, source);
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
   // What the layout decides: how tiles are arranged, never how they look.
   const rules = layoutRules(editor.document.layout, editor.document.columns);
   const { columns } = rules;
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
   // A tile with no `# colspan` is drawn as the reader draws it, at least
   // MIN_TILE_PX wide, rather than one column: the width handle and the row
   // arithmetic start from that span. Only drawn; nothing is written until
   // the author sets a width.
   const defaultSpan = useDefaultTileSpan(gridBox, columns);
   const laidOut = useMemo(
      () =>
         editor.document.tiles.map((tile) =>
            tile.colspan === undefined
               ? { ...tile, colspan: defaultSpan }
               : tile,
         ),
      [editor.document.tiles, defaultSpan],
   );
   const resizing = useTileResize({
      tiles: laidOut,
      columns,
      gridBox,
      onStart: selectTile,
      commit: (index, span) =>
         editor.update((draft) => {
            const tile = draft.tiles[index];
            if (tile)
               draft.tiles[index] = withSpan(tile, span, columns, defaultSpan);
         }),
   });
   const tiles = useTileEditing({
      editor,
      rules,
      modelPath,
      textHeld: visibleSources !== undefined,
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
               drillSource === undefined
            )
               clearSelection();
         },
         nudge: (delta: 1 | -1) => {
            // The drag's keyboard sensor also reads the arrows, and a drop would write the drag-start width back.
            if (dragging || selected === undefined || !rules.widths) return;
            const tile = editor.document.tiles[selected];
            if (!tile || (isQueryTile(tile) && isReadOnlyTile(tile))) return;
            const span = nudgedSpan(
               tile.colspan ?? defaultSpan,
               delta,
               columns,
            );
            if (span === (tile.colspan ?? defaultSpan)) return;
            editor.update((draft) => {
               draft.tiles[selected] = withSpan(
                  draft.tiles[selected],
                  span,
                  columns,
                  defaultSpan,
               );
            });
         },
      }),
      [
         defaultSpan,
         editor,
         menu,
         filterDialog,
         addingTile,
         drillSource,
         clearSelection,
         selected,
         columns,
         rules,
         dragging,
      ],
   );
   const session = useBuilderSession<DashboardDocument>({
      editor: stepEditor,
      extraDirty: draftDirty,
      // Only while the browser is actually holding them: a refused write
      // means leaving would lose the edits, and the host's guard must ask.
      keepsEdits: draftKey !== undefined && draft.keeping,
      prepare,
      unit: { name: "tile", count: (document) => document.tiles.length },
      onSave,
      onDirtyChange,
      onExit,
      onChange,
      shortcuts,
      report: builderReport({
         size: editor.document.tiles.length,
         path: modelPath ?? "",
         layout: editor.document.layout,
         savesTo,
         onEvent,
      }),
   });

   const empty = editor.document.tiles.length === 0;
   const { toolbarProps } = session;
   const restored = draft.restored;
   // Where the edits stand, for the status on either surface: the host's bar
   // or the page's own toolbar.
   const saveStatus: SaveStatusProps = {
      status: session.saving
         ? "saving"
         : toolbarProps.dirty
           ? "unsaved"
           : "saved",
      saveTarget: saveLabel ?? SAVE_TARGET[savesTo],
      ...(draft.keptAt !== undefined ? { draftKeptAt: draft.keptAt } : {}),
      // An offered draft is discarded without being applied, so Discard is
      // an answer before there is anything unsaved.
      ...((toolbarProps.dirty || restored?.pending) && draftKey !== undefined
         ? { discard: draft.discard }
         : {}),
      ...(restored ? { restored } : {}),
      ...(restored && !restored.pending
         ? { dismissRestored: draft.dismissRestored }
         : {}),
   };
   useReportControls(onControlsChange, {
      ...saveStatus,
      ...(toolbarProps.onSave ? { save: toolbarProps.onSave } : {}),
      canUndo: toolbarProps.canUndo,
      undo: toolbarProps.onUndo,
      canRedo: toolbarProps.canRedo,
      redo: toolbarProps.onRedo,
      add: {
         ...(catalog ? { tile: () => tiles.openAdd() } : {}),
         text: tiles.appendText,
         // A filter is declared for the tiles to read, so there has to be one.
         ...(addFilterDisabledReason !== undefined
            ? { filterUnavailable: addFilterDisabledReason }
            : empty
              ? { filterUnavailable: "Add a tile before a filter." }
              : { filter: () => setFilterDialog({}) }),
      },
      ...(toolbarProps.onExit ? { exit: toolbarProps.onExit } : {}),
      ...(session.done ? { done: session.done } : {}),
      ...(restored?.pending ? { restore: draft.restoreOffered } : {}),
   });
   // The builder's actions: on the title's line, at the right, above the
   // description.
   const actions = onControlsChange ? undefined : (
      <BuilderToolbar
         {...session.toolbarProps}
         {...(toolbar ? { actions: toolbar } : {})}
         {...(catalog ? { onAddTile: () => openAdd() } : {})}
         savesTo={savesTo}
         {...(saveLabel ? { saveLabel } : {})}
         {...(draftKey !== undefined && toolbarProps.onSave
            ? { saveStatus }
            : {})}
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
         <Stack
            // Where Save writes, for a host or a test to read: the words are
            // the status's tooltip, not a line on the page.
            className={reportClasses.builder}
            data-saves-to={savesTo}
            sx={{ gap: 0, mx: `-${SELECTION_RING_PX}px` }}
         >
            {/* The reader's page, with the ring's inset (minus the top:
             nothing at the top of the page can be selected). */}
            <ReportPage
               layout={rules.layout}
               sx={{
                  px: `${SELECTION_RING_PX}px`,
                  pb: `${SELECTION_RING_PX}px`,
               }}
               header={
                  <BuilderHeader
                     editor={editor}
                     documentSlug={documentSlug}
                     actions={actions}
                     descriptionSelected={descriptionSelected}
                     selectDescription={selectDescription}
                  />
               }
               controls={
                  empty ? undefined : (
                     <FilterStrip
                        controls={bindings.controlList}
                        unknownFieldsOf={bindings.unknownFieldsOf}
                        onEdit={(control) => setFilterDialog({ control })}
                        onAdd={() => setFilterDialog({})}
                        {...(addFilterDisabledReason
                           ? { addDisabledReason: addFilterDisabledReason }
                           : {})}
                        onRemove={bindings.dropControl}
                        {...(fixedFilters ? { fixedFilters } : {})}
                        {...(onOpenModel ? { onOpenModel } : {})}
                        {...(editor.document.startingGivens
                           ? { startingValues: editor.document.startingGivens }
                           : {})}
                        onStartingValue={(name, text) =>
                           editor.update((draft) => {
                              const next = { ...(draft.startingGivens ?? {}) };
                              if (text === undefined) delete next[name];
                              else next[name] = text;
                              if (Object.keys(next).length > 0)
                                 draft.startingGivens = next;
                              else delete draft.startingGivens;
                           })
                        }
                     >
                        {controls}
                     </FilterStrip>
                  )
               }
            >
               {empty && (
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
                        This report is not served until it has a tile.
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
               )}

               {/* Edits put back on opening are said in the save status, so
                the page does not move. A stale draft is a choice: put back,
                it replaces the newer version; dropped, the edits are gone.
                So it is asked here, with no close, until it is answered (or
                an edit answers it). */}
               {restored?.pending && (
                  <Alert
                     severity="warning"
                     action={
                        <>
                           <Button
                              color="inherit"
                              size="small"
                              onClick={draft.restoreOffered}
                           >
                              Restore them
                           </Button>
                           <Button
                              color="inherit"
                              size="small"
                              onClick={draft.discard}
                           >
                              Discard
                           </Button>
                        </>
                     }
                  >
                     {`You have unsaved edits from ${clockTime(restored.savedAt)}, made before this report changed. Restoring them replaces the current version.`}
                  </Alert>
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
                  defaultSpan={defaultSpan}
                  rules={rules}
                  canAdd={!!catalog}
                  selected={selected}
                  flash={flash}
                  menuIndex={menu?.index}
                  onSelect={selectTile}
                  onOpenMenu={(anchor, index) => {
                     selectTile(index);
                     tiles.setMenu({ anchor, index });
                  }}
                  onRemove={tiles.removeTile}
                  {...(editor.document.tiles.length === 1 &&
                  editor.saved.tiles.length > 0
                     ? {
                          removeBlocked:
                             "A saved report needs at least one tile.",
                       }
                     : {})}
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
                  exitDialog={session.exitGuard.dialog}
                  {...(renderViewPreview ? { renderViewPreview } : {})}
                  {...(takenGivenNames ? { takenGivenNames } : {})}
               />
            </ReportPage>
         </Stack>
      </OpenDraftContext.Provider>
   );
}
