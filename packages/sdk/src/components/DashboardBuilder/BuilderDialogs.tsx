// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { Button } from "@mui/material";
import type { ComponentProps } from "react";
import { AppDialog } from "../AppDialog";
import { UnsavedChangesDialog } from "../UnsavedChangesDialog";
import { AddTileDialog } from "./AddTileDialog";
import type { PackageCatalog } from "./catalog";
import { isQueryTile } from "./document";
import { DrillDialog } from "./DrillDialog";
import { FilterDialog } from "./FilterDialog";
import { TileMenu } from "./TileMenu";
import type { ConversionConfirm } from "./useConversionConfirm";
import type { useControlBindings } from "./useControlBindings";
import type { DashboardEditor } from "./useDashboardEditor";
import type { useTileEditing } from "./useTileEditing";

/**
 * Every window the builder opens, each driven by state its hooks hold: the
 * filter window, a tile's menu, the clickable-cells window, the conversion
 * question, the unsaved-changes guard and the add-tile picker.
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
   confirmConversion,
   exitDialog,
}: {
   editor: DashboardEditor;
   catalog: PackageCatalog | undefined;
   columns: number;
   dashboards: string[] | undefined;
   bindings: ReturnType<typeof useControlBindings>;
   tiles: ReturnType<typeof useTileEditing>;
   /** The source whose clickable cells are being edited, if any. */
   drillSource: string | undefined;
   setDrillSource: (source: string | undefined) => void;
   confirmConversion: ConversionConfirm | undefined;
   exitDialog: ComponentProps<typeof UnsavedChangesDialog>;
}) {
   const { filterDialog, setFilterDialog } = bindings;
   const { menu, setMenu } = tiles;
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
   return (
      <>
         <FilterDialog
            open={filterDialog !== undefined}
            document={editor.document}
            {...(filterDialog?.control
               ? { control: filterDialog.control }
               : {})}
            available={bindings.available}
            {...(catalog ? { fieldsFor: bindings.fieldsFor } : {})}
            onClose={() => setFilterDialog(undefined)}
            onApply={bindings.applyFilter}
            onRemove={bindings.dropControl}
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
               if (menu !== undefined) tiles.removeTile(menu.index);
            }}
            {...(editor.document.tiles.length === 1 &&
            editor.saved.tiles.length > 0
               ? {
                    removeBlocked: "A saved dashboard needs at least one tile.",
                 }
               : {})}
            view={menuView}
            onDrills={() => {
               setDrillSource(menuTile?.source);
            }}
         />
         <DrillDialog
            open={drillSource !== undefined}
            document={editor.document}
            source={editor.document.sources.find((s) => s.name === drillSource)}
            givenNames={bindings.controlList.map((c) => c.name)}
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
         <AppDialog
            open={confirmConversion !== undefined}
            onClose={() => confirmConversion?.stop()}
            title="Convert this notebook?"
            description="Saving rewrites this notebook in the tile layout. The builder cannot take that back; the file's history in your repository can."
            actions={
               <>
                  <Button onClick={() => confirmConversion?.stop()}>
                     Cancel
                  </Button>
                  <Button
                     variant="contained"
                     onClick={() => confirmConversion?.go()}
                  >
                     Convert and save
                  </Button>
               </>
            }
         >
            {null}
         </AppDialog>
         <UnsavedChangesDialog {...exitDialog} />
         <AddTileDialog
            open={tiles.addingTile}
            document={editor.document}
            catalog={catalog}
            columns={columns}
            onClose={tiles.closeAdd}
            onAdd={tiles.addTile}
            onAddText={tiles.addText}
         />
      </>
   );
}
