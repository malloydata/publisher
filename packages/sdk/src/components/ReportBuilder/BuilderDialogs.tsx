// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { isPlaced } from "../Dashboard/DashboardGrid";
import { holdChartRows, settle } from "./placement";
import {
   useEffect,
   useRef,
   useState,
   type ComponentProps,
   type ReactNode,
} from "react";
import { UnsavedChangesDialog } from "../UnsavedChangesDialog";
import { AddTileDialog, type ViewPreview } from "./AddTileDialog";
import type { PackageCatalog } from "./catalog";
import { duplicateBlocked } from "./addTileToDocument";
import { isInlineStage, isQueryTile, tileKey } from "./document";
import { ClickActionsDialog } from "./ClickActionsDialog";
import { viewOfResult, type LinkTarget } from "./clickActions";
import { FilterDialog } from "./FilterDialog";
import { TileMenu } from "./TileMenu";
import { tileDisplayTitle } from "./tileDisplayTitle";
import type { useControlBindings } from "./useControlBindings";
import type { ReportEditorHandle } from "./useReportEditor";
import type { useTileEditing } from "./useTileEditing";

/**
 * Every window the builder opens, each driven by state its hooks hold: the
 * filter window, a tile's menu, "What clicking a value does", the
 * unsaved-changes guard and the add-tile picker.
 */
export function BuilderDialogs({
   editor,
   catalog,
   columns,
   dashboards,
   bindings,
   tiles,
   drillSource,
   setDrillSource,
   exitDialog,
   renderViewPreview,
   takenGivenNames,
   linkTargets,
   tileResult,
}: {
   editor: ReportEditorHandle;
   catalog: PackageCatalog | undefined;
   columns: number;
   dashboards: string[] | undefined;
   bindings: ReturnType<typeof useControlBindings>;
   tiles: ReturnType<typeof useTileEditing>;
   /** The key of the tile whose click actions are being set, if any. */
   drillSource: string | undefined;
   setDrillSource: (tile: string | undefined) => void;
   exitDialog: ComponentProps<typeof UnsavedChangesDialog>;
   renderViewPreview?: (view: ViewPreview) => ReactNode;
   takenGivenNames?: readonly string[];
   /** The package's other reports, with the filters each shows. Absent, `dashboards` is offered with its filters unchecked. */
   linkTargets?: readonly LinkTarget[];
   /** A tile's last result as the canvas ran it, by tile key. */
   tileResult?: (tile: string) => string | undefined;
}) {
   const { filterDialog, setFilterDialog } = bindings;
   const { menu, setMenu } = tiles;
   // A chart or colour pick on a tile that names a served view makes it a
   // query of the report's own, under a new name, so React mounts the tile
   // afresh and the button its menu opened from is gone. The same tile's new
   // button stands in: the menu stays by it, and focus goes back to it on
   // close. Without that, focus stays on the closing menu, and the ⌘S that
   // follows lands in an overlay and is ignored.
   const settingsButton = (index: number): HTMLElement | null => {
      const tile = editor.document.tiles[index];
      if (!tile || typeof document === "undefined") return null;
      return document.querySelector<HTMLElement>(
         `[data-tile-key="${CSS.escape(tileKey(tile))}"] [aria-label^="Settings for "]`,
      );
   };
   const menuAnchor =
      menu === undefined
         ? null
         : menu.anchor.isConnected
           ? menu.anchor
           : (settingsButton(menu.index) ?? menu.anchor);
   const refocus = useRef<number | undefined>(undefined);
   useEffect(() => {
      if (menu !== undefined || refocus.current === undefined) return;
      settingsButton(refocus.current)?.focus();
      refocus.current = undefined;
   });
   // The tile "What clicking a value does" was opened from, for its window to name.
   const [drillTile, setDrillTile] = useState<string | undefined>();
   // The catalog's view behind the tile whose menu is open, for the charts the
   // picker may offer: a reference tile's base view, else a view of the tile's own name.
   const menuAt =
      menu === undefined ? undefined : editor.document.tiles[menu.index];
   const menuTile = menuAt && isQueryTile(menuAt) ? menuAt : undefined;
   const menuView = (() => {
      // An inline stage is no view in any model: its shape is read off the
      // result the canvas drew for it, as a model view's is off its schema.
      if (
         menuTile?.declaration.kind === "named" &&
         isInlineStage(menuTile.declaration.from)
      )
         return viewOfResult(tileResult?.(tileKey(menuTile)));
      if (!menuTile || !catalog) return undefined;
      // A view this file declares is read off the file's own compiled model:
      // an inline body is in no other model the catalog has.
      const declared = catalog.documentViews
         ?.get(menuTile.source ?? "")
         ?.get(menuTile.name);
      if (
         declared &&
         editor.document.sources.some((s) => s.name === menuTile.source)
      )
         return declared;
      // An inline or opaque body is not a view of the base source, so a base
      // view that happens to share its name says nothing about its shape.
      if (
         menuTile.declaration.kind === "inline" ||
         menuTile.declaration.kind === "opaque"
      )
         return undefined;
      const base =
         editor.document.sources.find((s) => s.name === menuTile.source)
            ?.base ?? menuTile.source;
      const viewName =
         menuTile.declaration.kind === "reference" ||
         menuTile.declaration.kind === "named"
            ? menuTile.declaration.from
            : menuTile.name;
      return catalog.sources
         .find((s) => s.name === base)
         ?.views.find((v) => v.name === viewName);
   })();
   return (
      <>
         <FilterDialog
            open={filterDialog !== undefined}
            document={editor.document}
            {...(filterDialog?.control
               ? { control: filterDialog.control }
               : {})}
            available={bindings.available}
            {...(takenGivenNames ? { takenNames: takenGivenNames } : {})}
            {...(catalog ? { fieldsFor: bindings.fieldsFor } : {})}
            onClose={() => setFilterDialog(undefined)}
            onApply={bindings.applyFilter}
            onRemove={bindings.dropControl}
         />
         <TileMenu
            anchor={menuAnchor}
            tile={
               menu === undefined
                  ? undefined
                  : editor.document.tiles[menu.index]
            }
            onClose={() => {
               if (menu && !menu.anchor.isConnected)
                  refocus.current = menu.index;
               setMenu(undefined);
            }}
            onCommit={(next) => {
               const at = menu?.index;
               if (at === undefined) return;
               // A chart picked on a tile too short to plot it raises the
               // tile to a chart's rows, pushing down what it now covers.
               const held = holdChartRows(next);
               editor.update((draft) => {
                  draft.tiles[at] = held;
                  if (held !== next && isPlaced(draft.tiles))
                     draft.tiles = settle(draft.tiles, columns, at);
               });
            }}
            view={menuView}
            {...(menuView === undefined && menuTile
               ? { viewStatus: catalog ? "unlisted" : "loading" }
               : {})}
            onDuplicate={() => {
               if (menu) tiles.duplicate(menu.index);
            }}
            {...(menuTile && duplicateBlocked(menuTile) !== undefined
               ? { duplicateBlocked: duplicateBlocked(menuTile) }
               : {})}
            {...(menuTile
               ? {
                    onDrills: () => {
                       setDrillTile(tileDisplayTitle(menuTile));
                       setDrillSource(tileKey(menuTile));
                    },
                 }
               : {})}
         />
         <ClickActionsDialog
            open={drillSource !== undefined}
            tileTitle={drillTile ?? ""}
            result={drillSource ? tileResult?.(drillSource) : undefined}
            drill={(() => {
               const tile = editor.document.tiles.find(
                  (t) => tileKey(t) === drillSource,
               );
               return tile && isQueryTile(tile) ? tile.drill : undefined;
            })()}
            controls={bindings.controlList.map((c) => c.name)}
            reports={
               linkTargets ??
               (dashboards ?? []).map((path) => ({ path, title: path }))
            }
            onClose={() => setDrillSource(undefined)}
            onApply={(drill) =>
               editor.update((draft) => {
                  const tile = draft.tiles.find(
                     (t) => tileKey(t) === drillSource,
                  );
                  if (!tile || !isQueryTile(tile)) return;
                  if (drill) tile.drill = drill;
                  else delete tile.drill;
               })
            }
         />
         <UnsavedChangesDialog {...exitDialog} />
         <AddTileDialog
            open={tiles.addingTile}
            document={editor.document}
            catalog={catalog}
            columns={columns}
            onClose={tiles.closeAdd}
            onAdd={tiles.addTile}
            onAddText={tiles.addText}
            {...(renderViewPreview ? { renderPreview: renderViewPreview } : {})}
         />
      </>
   );
}
