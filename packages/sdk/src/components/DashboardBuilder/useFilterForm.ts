// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { useEffect, useMemo, useRef, useState } from "react";
import type { CatalogField } from "./catalog";
import {
   acceptsField,
   canBind,
   defaultOperator,
   givenNameFor,
   kindForFieldType,
   mappingOf,
   newLocalGiven,
   typeLabel,
   type BuilderControl,
   type ControlKind,
   type MappingRow,
} from "./controls";
import type { DashboardDocument, DashboardTile, LocalGiven } from "./document";

type Source = { kind: "existing"; name: string } | { kind: "new" };

/** `STATUS` → `Status`: the label a new control gets when none is typed. */
export const titleCase = (name: string) =>
   name.charAt(0) + name.slice(1).toLowerCase().replace(/_/g, " ");

/**
 * The filter window's state and every derivation it renders from: what the
 * control is (new here, or one the model offers), which tiles it binds and on
 * which field, what stands in the way of applying, and the payload an Apply
 * hands back. No MUI in here, so it can be exercised without a dialog.
 */
export function useFilterForm({
   open,
   document,
   control,
   available,
   fieldsFor,
   onApply,
}: {
   open: boolean;
   document: DashboardDocument;
   control: BuilderControl | undefined;
   available: readonly BuilderControl[];
   fieldsFor?: (tile: DashboardTile) => readonly CatalogField[] | undefined;
   onApply: (given: string, rows: MappingRow[], declare?: LocalGiven) => void;
}) {
   // What the window SHOWS: the control while open, and the same control while
   // it fades out. The builder drops `control` the instant the window closes,
   // and without this the title flipped to "Add a filter" for the length of
   // the exit transition.
   const shownRef = useRef(control);
   if (open) shownRef.current = control;
   const shown = open ? control : shownRef.current;
   const editing = shown !== undefined;

   const [source, setSource] = useState<Source>({ kind: "new" });
   const [label, setLabel] = useState("");
   const [kind, setKind] = useState<ControlKind>("select");
   const [dateDefault, setDateDefault] = useState("");
   // The field every ticked tile filters on, and the rows themselves. The
   // field is the common case and, until "Adjust per tile" is pressed, the ONE
   // that is applied: a row's own field only counts once the author has asked
   // to set them one by one. That is what keeps a fallback name a row happened
   // to carry from being written when the box above it was empty.
   const [field, setField] = useState("");
   const [rows, setRows] = useState<MappingRow[]>([]);
   const [perTile, setPerTile] = useState(false);

   /** The model source a tile's extension is built on, which its fields live on. */
   const baseOf = (tile: DashboardTile) =>
      document.sources.find((source) => source.name === tile.source)?.base ??
      tile.source;

   // Reset on open, on what is true now.
   useEffect(() => {
      if (!open) return;
      if (control) {
         setSource({ kind: "existing", name: control.name });
         setLabel(control.label ?? "");
         const mapped = mappingOf(document, control);
         setRows(mapped);
         // One field when every bound tile agrees; otherwise the rows already
         // differ and the per-tile view is the honest one to open on. A control
         // bound nowhere starts from what its own suggest names, or empty — and
         // empty has to be filled before it can apply.
         const bound = mapped.filter((row) => row.include);
         const fieldsBound = new Set(bound.map((row) => row.field));
         setField(
            fieldsBound.size === 1 ? bound[0].field : (control.field ?? ""),
         );
         setPerTile(fieldsBound.size > 1);
      } else {
         setSource({ kind: "new" });
         setLabel("");
         setKind("select");
         setField("");
         setDateDefault(new Date().toISOString().slice(0, 10));
         setRows(
            document.tiles.map((tile) => ({
               include: canBind(tile),
               field: "",
            })),
         );
         setPerTile(false);
      }
   }, [open, control, document]);

   // A new control's name follows its field, distinct from anything declared.
   const taken = useMemo(
      () => [
         ...(document.localGivens ?? []).map((g) => g.name),
         ...available.map((g) => g.name),
      ],
      [document, available],
   );
   const newName = useMemo(
      () => (field.trim() ? givenNameFor(field.trim(), taken) : ""),
      [field, taken],
   );

   // The given being mapped, with the type that decides how each row compares.
   const target: { name: string; type?: string } | undefined =
      source.kind === "existing"
         ? (control ?? available.find((g) => g.name === source.name))
         : newName
           ? {
                name: newName,
                type: newLocalGiven({ name: newName, label, kind, field }).type,
             }
           : undefined;
   // A `date` or `number` given is a value, not a filter expression: it needs
   // a comparison, and `~` is not one it accepts.
   const valueTyped =
      target?.type !== undefined && !target.type.startsWith("filter<");
   const commonOp =
      rows.find((row) => row.include)?.op ?? defaultOperator(target?.type);

   const setRow = (index: number, patch: Partial<MappingRow>) =>
      setRows((previous) =>
         previous.map((row, i) => (i === index ? { ...row, ...patch } : row)),
      );
   const setCommonOp = (next: string) =>
      setRows((previous) => previous.map((row) => ({ ...row, op: next })));
   /** Rows follow a given's type: the comparison it needs, or none. */
   const retype = (type: string | undefined) => {
      const op = defaultOperator(type);
      setRows((previous) =>
         previous.map(({ op: _was, ...row }) => (op ? { ...row, op } : row)),
      );
   };
   const pickExisting = (given: BuilderControl) => {
      setSource({ kind: "existing", name: given.name });
      setField(given.field ?? "");
      retype(given.type);
   };
   const pickKind = (next: ControlKind) => {
      setKind(next);
      retype(newLocalGiven({ name: "X", label, kind: next, field }).type);
   };

   const bindable = document.tiles.map(canBind);
   const bindableCount = bindable.filter(Boolean).length;
   const included = rows.filter((row, i) => row.include && bindable[i]).length;
   // The tiles a common field has to fit: the ticked, bindable ones — or every
   // bindable one while nothing is ticked yet, so the picker has something to
   // offer before the first tick.
   const ticked = document.tiles.filter(
      (_, i) => (rows[i]?.include ?? false) && bindable[i],
   );
   const pool =
      ticked.length > 0 ? ticked : document.tiles.filter((_, i) => bindable[i]);
   // What the common picker offers: every field any tile in the pool can take,
   // once by name. Undefined when no tile has a list, which is "accept anything".
   const commonFields = useMemo(() => {
      const byName = new Map<string, CatalogField>();
      let any = false;
      for (const tile of pool) {
         const list = fieldsFor?.(tile);
         if (!list) continue;
         any = true;
         for (const field of list)
            if (!byName.has(field.name)) byName.set(field.name, field);
      }
      return any ? [...byName.values()] : undefined;
      // eslint-disable-next-line react-hooks/exhaustive-deps -- pool is derived from rows and tiles
   }, [rows, document.tiles, fieldsFor]);
   // Where a NEW control's picker reads its options: the source of the first
   // ticked tile that has the field — a `suggest` has to resolve in this file,
   // and a tile's own base always does. A dashboard over imported sources
   // alone has no extension, so the tile's source is the base itself.
   const suggestSource = (() => {
      const name = field.trim();
      for (const tile of ticked) {
         const list = fieldsFor?.(tile);
         if (!list || list.some((f) => f.name === name)) return baseOf(tile);
      }
      return ticked[0] ? baseOf(ticked[0]) : document.sources[0]?.base;
   })();
   const setAll = (include: boolean) =>
      setRows((previous) =>
         previous.map((row, i) => ({
            ...row,
            include: include && bindable[i],
         })),
      );

   // The rows as they will be APPLIED: the common field on every row until
   // the author has asked to set them one by one.
   const effective = useMemo(
      () => (perTile ? rows : rows.map((row) => ({ ...row, field }))),
      [perTile, rows, field],
   );

   // Validation, per TILE, where its source's fields are known. Unknown is a
   // name that tile's source does not have; empty is no name at all. Either on
   // a ticked tile holds Apply, and is marked where it stands.
   const typeOf = (name: string) => {
      for (const tile of pool) {
         const found = fieldsFor?.(tile)?.find((f) => f.name === name.trim());
         if (found?.type) return found.type;
      }
      return undefined;
   };
   // What this given can compare: the picker offers only these, and a name
   // typed past the list is held to the same rule.
   const accepts = (candidate: CatalogField) =>
      acceptsField(target?.type, candidate.type);
   const problemWith = (
      name: string,
      tile: DashboardTile | undefined,
   ): string | undefined => {
      if (name.trim() === "") return "Pick the field this filter compares.";
      const list = tile ? fieldsFor?.(tile) : undefined;
      if (!list) return undefined;
      const found = list.find((f) => f.name === name.trim());
      if (!found)
         return `Not a field of ${tile ? baseOf(tile) : "the tiles' source"}.`;
      if (found.type && !acceptsField(target?.type, found.type))
         return `${name.trim()} is ${typeLabel(found.type)}; this filter compares ${typeLabel(
            target?.type?.replace(/^filter<(.+)>$/, "$1"),
         )}.`;
      return undefined;
   };
   // A NEW control follows the field it is given: pick a number and it becomes
   // a number range, a date and it becomes a date picker. Explicit kind changes
   // still win afterwards; this only moves a kind the field cannot take.
   const pickField = (next: string) => {
      setField(next);
      if (editing || source.kind !== "new") return;
      const fieldType = typeOf(next);
      const kindType = newLocalGiven({
         name: "X",
         label,
         kind,
         field: next,
      }).type;
      if (fieldType && !acceptsField(kindType, fieldType))
         pickKind(kindForFieldType(fieldType));
   };
   const rowProblems = effective.map((row, i) =>
      row.include && bindable[i]
         ? problemWith(row.field, document.tiles[i])
         : undefined,
   );
   const fieldsResolve = rowProblems.every((problem) => problem === undefined);
   // In the common case one box speaks for every row, so its message is the
   // rows' message; the box is only marked once a tile is ticked to bind.
   // Every ticked tile has to take it; the first that cannot says why.
   const commonProblem =
      !perTile && included > 0
         ? ticked.map((tile) => problemWith(field, tile)).find(Boolean)
         : undefined;

   const canApply =
      target !== undefined &&
      fieldsResolve &&
      (source.kind === "existing" || (field.trim() !== "" && newName !== ""));

   const apply = () => {
      if (!target || !canApply) return;
      if (source.kind === "new") {
         onApply(
            target.name,
            effective,
            newLocalGiven({
               name: target.name,
               // Untyped, the label is what the placeholder promised — `Status`
               // for `$STATUS` — not the shouted name.
               label: label.trim() || titleCase(target.name),
               kind,
               field: field.trim(),
               ...(suggestSource ? { source: suggestSource } : {}),
               ...(dateDefault ? { dateDefault } : {}),
            }),
         );
      } else if (control?.origin === "dashboard" && control.local) {
         // Retag in place when the label changed; the declaration is otherwise
         // the one already in the file.
         const trimmed = label.trim();
         const changed = trimmed !== (control.local.label ?? "");
         onApply(
            target.name,
            effective,
            changed
               ? { ...control.local, ...(trimmed ? { label: trimmed } : {}) }
               : undefined,
         );
      } else {
         onApply(target.name, effective);
      }
   };

   const fromModel = editing && shown.origin === "model";

   return {
      editing,
      shown,
      fromModel,
      source,
      setSource,
      label,
      setLabel,
      kind,
      dateDefault,
      setDateDefault,
      field,
      rows,
      setRows,
      perTile,
      setPerTile,
      newName,
      target,
      valueTyped,
      commonOp,
      setRow,
      setCommonOp,
      pickExisting,
      pickKind,
      bindable,
      bindableCount,
      included,
      commonFields,
      setAll,
      accepts,
      pickField,
      rowProblems,
      commonProblem,
      canApply,
      apply,
   };
}
