// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { Stack } from "@mui/material";
import { useMemo, type ReactNode } from "react";
import { StickyControls } from "../Report/ReportPage";
import {
   FilterEditingContext,
   type FilterEditing,
} from "../given/filterEditing";
import { AddFilterButton, FilterSettingsButton } from "../given/FilterButton";
import type { BuilderControl } from "./controls";

/**
 * The builder's filter row: the ONE place a report's filters are used and
 * configured. It is the reader's own row of filter buttons (the host's
 * `children`), with the builder's editing added through
 * {@link FilterEditingContext}: a remove badge on each button's corner, its
 * settings from its popover, and "+ Filter" at the end. A filter no tile reads
 * yet has no control in the reader's row, so it is drawn after the row's own,
 * dashed and marked unused, and opens its settings.
 *
 * There used to be a second row above this one: a chip per filter, naming the
 * same filters again, which is where they were edited. Two rows of look-alike
 * things, each doing half the job, read as a duplicate.
 *
 * Without a live row (no server to run the controls), every filter is drawn
 * as a settings button, so the file's filters can still be edited.
 */
export function FilterStrip({
   controls: controlList,
   unknownFieldsOf,
   onEdit,
   onAdd,
   addDisabledReason,
   onRemove,
   fixedFilters,
   children,
}: {
   controls: BuilderControl[];
   /** Bindings a control has that its tiles' sources cannot take. */
   unknownFieldsOf: (name: string, type: string | undefined) => string[];
   onEdit: (control: BuilderControl) => void;
   onAdd: () => void;
   /** Why adding is off; set, the add button is disabled and says why. */
   addDisabledReason?: string;
   /** Take a control off the dashboard, as its window's Remove does. */
   onRemove: (name: string) => void;
   /** Filters a tile reads other than through a binding, by name, with why they are fixed here. */
   fixedFilters?: ReadonlyMap<string, string>;
   /** The host's live control row. */
   children?: ReactNode;
}) {
   const live = children !== undefined && children !== null;
   const editing = useMemo<FilterEditing>(() => {
      const byName = new Map(controlList.map((c) => [c.name, c]));
      return {
         stateOf: (name) => {
            // Read other than through a binding the builder writes: fixed
            // here, whatever the builder knows of it.
            const fixed = fixedFilters?.get(name);
            if (fixed !== undefined) return { origin: "model", fixed };
            const control = byName.get(name);
            if (!control) return undefined;
            const unknown = unknownFieldsOf(control.name, control.type);
            return {
               origin: control.origin,
               ...(unknown.length > 0
                  ? {
                       problem: `Cannot filter on ${unknown.join(", ")}: a tile it applies to has no such field.`,
                    }
                  : {}),
            };
         },
         // The reader's row draws a control only for a filter some tile
         // reads, so the rest are drawn here.
         unrendered: live
            ? controlList
                 // A fixed filter has its control in the row already.
                 .filter(
                    (c) => c.boundTiles === 0 && !fixedFilters?.has(c.name),
                 )
                 .map((c) => ({ name: c.name, label: c.label ?? c.name }))
            : [],
         onEdit: (name) => {
            const control = byName.get(name);
            if (control) onEdit(control);
         },
         onRemove,
         onAdd,
         ...(addDisabledReason !== undefined ? { addDisabledReason } : {}),
      };
   }, [
      controlList,
      unknownFieldsOf,
      live,
      onEdit,
      onRemove,
      onAdd,
      addDisabledReason,
      fixedFilters,
   ]);
   return (
      // Pinned as the reader's control row is.
      <StickyControls>
         <FilterEditingContext.Provider value={editing}>
            {live ? (
               children
            ) : (
               <Stack
                  direction="row"
                  role="group"
                  aria-label="Filters"
                  useFlexGap
                  sx={{ flexWrap: "wrap", alignItems: "center", gap: 1 }}
               >
                  {controlList.map((control) => (
                     <FilterSettingsButton
                        key={control.name}
                        name={control.name}
                        label={control.label ?? control.name}
                        unused={control.boundTiles === 0}
                     />
                  ))}
                  <AddFilterButton />
               </Stack>
            )}
         </FilterEditingContext.Provider>
      </StickyControls>
   );
}
