// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { useCallback, useMemo, useState } from "react";
import { filterableFields, type PackageCatalog } from "./catalog";
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
import {
   isQueryTile,
   type DashboardDocument,
   type LocalGiven,
   type QueryTile,
} from "./document";
import type { DashboardEditor } from "./useDashboardEditor";

/**
 * The document's controls, the fields each tile may bind one to, and the
 * filter window that edits them — the one place a filter is configured.
 */
export function useControlBindings({
   editor,
   opened,
   givens,
   catalog,
}: {
   editor: DashboardEditor;
   /** The document as the host opened it, before any edit here. */
   opened: DashboardDocument;
   /** The givens the host resolved across the model and its imports. */
   givens: BuilderGiven[] | undefined;
   catalog: PackageCatalog | undefined;
}) {
   // The filter window: open on a control, or open to add one.
   const [filterDialog, setFilterDialog] = useState<
      { control?: BuilderControl } | undefined
   >(undefined);
   // The givens the MODEL offers: the caller's list, less any the opened file
   // declared itself. A caller gets that list from the server's manifest, which
   // resolves givens across the file and its imports without saying which is
   // which — so it names this file's own declarations too, and keeps naming a
   // control after this document removes it, until a save is written and the
   // package reloads. Without this, "Remove filter" took the chip off
   // and it came straight back, faint, labelled "from the model".
   const modelGivens = useMemo(() => {
      const ownDeclarations = new Set(
         (opened.localGivens ?? []).map((given) => given.name),
      );
      return (givens ?? []).filter((given) => !ownDeclarations.has(given.name));
   }, [opened, givens]);
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

   return {
      filterDialog,
      setFilterDialog,
      controlList,
      fieldsFor,
      unknownFieldsOf,
      available,
      applyFilter,
      dropControl,
   };
}
