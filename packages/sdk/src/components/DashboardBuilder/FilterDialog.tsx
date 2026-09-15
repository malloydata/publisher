// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import {
   Autocomplete,
   Box,
   Button,
   Checkbox,
   Dialog,
   DialogActions,
   DialogContent,
   DialogTitle,
   MenuItem,
   Stack,
   TextField,
   ToggleButton,
   ToggleButtonGroup,
   Tooltip,
   Typography,
   type SxProps,
   type Theme,
} from "@mui/material";
import { useEffect, useMemo, useRef, useState } from "react";
import { usePublisherTheme } from "../../theme/ThemeContext";
import type { CatalogField } from "./catalog";
import {
   acceptsField,
   canBind,
   CONTROL_KINDS,
   defaultOperator,
   givenNameFor,
   kindForFieldType,
   mappingOf,
   newLocalGiven,
   OPERATORS,
   typeLabel,
   type BuilderControl,
   type ControlKind,
   type MappingRow,
} from "./controls";
import type { DashboardDocument, DashboardTile, LocalGiven } from "./document";

/**
 * A filter control: what it is, and which tiles it drives.
 *
 * Two questions, asked in that order and nothing else. WHAT: its label, and
 * the field it filters on — one field, because a control almost always filters
 * the same field on every tile, and the case where it does not is one click
 * away ("Adjust per tile"), not five text boxes on every open. WHICH: a list
 * of the dashboard's tiles with a checkbox each, and one checkbox over them
 * all. A tile that cannot take a filter says why in place.
 *
 * The field is SEARCHED, not typed blind, when the host has handed over the
 * source's fields: the picker offers the dimensions the tiles' source has,
 * an unknown name is marked as such where it stands, and Apply waits until
 * every ticked tile's field resolves. A binding to a field that does not exist
 * fails at package load, which is the worst place for it to fail; here it
 * cannot be written.
 *
 * The same window adds, edits and removes. It opens on the mapping the control
 * has NOW, so unticking a tile unbinds it, and a control this dashboard
 * declares can be taken off the page from here.
 *
 * What it writes is the convention {@link LocalGiven} describes: a new control
 * is a `given:` declared in this file, bound on each ticked tile as a
 * `+ { where: field ~ $GIVEN }` refinement. A control the model declares can be
 * bound here but not changed, since the builder never edits model files.
 */
export interface FilterDialogProps {
   open: boolean;
   document: DashboardDocument;
   /** The control being edited, or undefined to add one. */
   control?: BuilderControl;
   /** Model givens not yet bound anywhere, offered as "from the model". */
   available: readonly BuilderControl[];
   /**
    * The fields a binding on THIS tile may name: the dimensions of the source
    * it reads. Per tile, because a composite dashboard exists to span sources,
    * and a field one source has another may not. Undefined for a tile whose
    * source the host has no catalog for, in which case any name is accepted
    * for it and nothing is searched.
    */
   fieldsFor?: (tile: DashboardTile) => readonly CatalogField[] | undefined;
   onClose: () => void;
   /**
    * Bind a control. `declare` is set when the control is new to this file, or
    * when its declaration changed, and is the declaration to write.
    */
   onApply: (given: string, rows: MappingRow[], declare?: LocalGiven) => void;
   /** Take a control off the dashboard; offered for one this file declares. */
   onRemove: (given: string) => void;
}

type Source = { kind: "existing"; name: string } | { kind: "new" };

/** `BRAND` -> `Brand`: the label a new control gets if the author types none. */
const titleCase = (name: string) =>
   name.charAt(0) + name.slice(1).toLowerCase().replace(/_/g, " ");

/**
 * A field name, searched from the source's dimensions and still free to type:
 * the catalog may lag a model edit, and a host without one has no list at all.
 */
function FieldPicker({
   value,
   onChange,
   fields,
   accepts,
   label,
   ariaLabel,
   error,
   helperText,
   disabled,
   autoFocus,
   placeholder,
   sx,
}: {
   value: string;
   onChange: (next: string) => void;
   fields: readonly CatalogField[] | undefined;
   /** Which of the fields to offer; the rest are still accepted if typed. */
   accepts?: (field: CatalogField) => boolean;
   label: string;
   ariaLabel: string;
   error?: boolean;
   helperText?: string;
   disabled?: boolean;
   autoFocus?: boolean;
   placeholder?: string;
   sx?: SxProps<Theme>;
}) {
   const options = useMemo(
      () =>
         (fields ?? [])
            .filter((field) => accepts?.(field) ?? true)
            .map((field) => field.name),
      [fields, accepts],
   );
   const types = useMemo(
      () => new Map((fields ?? []).map((field) => [field.name, field.type])),
      [fields],
   );
   return (
      <Autocomplete
         freeSolo
         size="small"
         options={options}
         inputValue={value}
         onInputChange={(_, next) => onChange(next)}
         value={value}
         onChange={(_, next) => onChange(typeof next === "string" ? next : "")}
         disabled={disabled}
         disableClearable
         renderOption={(props, option) => (
            <li {...props} key={option}>
               <Typography variant="body2" sx={{ flex: 1 }}>
                  {option}
               </Typography>
               {types.get(option) && (
                  <Typography variant="caption" sx={{ opacity: 0.6 }}>
                     {types.get(option)?.replace(/_type$/, "")}
                  </Typography>
               )}
            </li>
         )}
         renderInput={(params) => (
            <TextField
               {...params}
               label={label}
               placeholder={placeholder}
               autoFocus={autoFocus}
               error={error}
               helperText={helperText}
               inputProps={{ ...params.inputProps, "aria-label": ariaLabel }}
            />
         )}
         sx={sx}
      />
   );
}

export function FilterDialog({
   open,
   document,
   control,
   available,
   fieldsFor,
   onClose,
   onApply,
   onRemove,
}: FilterDialogProps) {
   const { theme } = usePublisherTheme();
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
   const operatorField = (
      value: string | undefined,
      onChange: (op: string) => void,
      ariaLabel: string,
   ) => (
      <TextField
         select
         size="small"
         label="Compares"
         value={value ?? ">="}
         onChange={(event) => onChange(event.target.value)}
         inputProps={{ "aria-label": ariaLabel }}
         sx={{ width: 130 }}
      >
         {OPERATORS.filter((o) => o.op !== "~").map((o) => (
            <MenuItem key={o.op} value={o.op}>
               {o.label}
            </MenuItem>
         ))}
      </TextField>
   );

   return (
      <Dialog open={open} onClose={onClose} maxWidth="sm" fullWidth>
         <DialogTitle sx={{ pb: 0.5 }}>
            {editing ? (shown.label ?? shown.name) : "Add a filter"}
            {editing && (
               <Typography
                  component="div"
                  variant="caption"
                  sx={{ color: theme.tileTitle, mt: 0.25 }}
               >
                  <Box
                     component="span"
                     sx={{ fontFamily: "ui-monospace, monospace" }}
                  >
                     ${shown.name}
                  </Box>
                  {fromModel
                     ? " · declared in the model, so its label and options are set there"
                     : " · declared in this dashboard"}
               </Typography>
            )}
         </DialogTitle>
         <DialogContent>
            {/* Top margin on the stack, not padding on the content: MUI zeroes
                a DialogContent's top padding after a DialogTitle with a rule
                that outranks `sx`, and the fields' floating labels need the
                room or they clip. */}
            <Stack sx={{ gap: 2.5, pt: 1 }}>
               {/* WHAT the filter is. */}
               {!editing && available.length > 0 && (
                  <ToggleButtonGroup
                     exclusive
                     size="small"
                     value={source.kind}
                     onChange={(_, value: "new" | "existing" | null) => {
                        if (value === "new") setSource({ kind: "new" });
                        else if (value === "existing" && available[0])
                           pickExisting(available[0]);
                     }}
                     aria-label="Where the filter comes from"
                  >
                     <ToggleButton value="new">
                        New in this dashboard
                     </ToggleButton>
                     <ToggleButton value="existing">
                        From the model
                     </ToggleButton>
                  </ToggleButtonGroup>
               )}

               {!editing && source.kind === "existing" ? (
                  <TextField
                     select
                     size="small"
                     label="Control"
                     value={source.name}
                     onChange={(event) => {
                        const picked = available.find(
                           (g) => g.name === event.target.value,
                        );
                        if (picked) pickExisting(picked);
                     }}
                  >
                     {available.map((given) => (
                        <MenuItem key={given.name} value={given.name}>
                           {given.label ?? given.name}
                           <Typography
                              component="span"
                              variant="caption"
                              sx={{ ml: 1, opacity: 0.6 }}
                           >
                              ${given.name}
                           </Typography>
                        </MenuItem>
                     ))}
                  </TextField>
               ) : null}

               <Stack direction="row" sx={{ gap: 1.5, flexWrap: "wrap" }}>
                  {/* Hidden while per-tile fields are open: they speak for
                      themselves then, and a box above them that applied to
                      nothing would read as one more thing to fill. */}
                  {!perTile && (
                     <FieldPicker
                        value={field}
                        onChange={pickField}
                        fields={commonFields}
                        accepts={accepts}
                        label="Field to filter"
                        ariaLabel="Field to filter"
                        placeholder="category"
                        autoFocus={!editing}
                        error={commonProblem !== undefined}
                        helperText={
                           commonProblem ??
                           (!editing && newName
                              ? `Declared as $${newName}`
                              : undefined)
                        }
                        sx={{ flex: 1, minWidth: 200 }}
                     />
                  )}
                  {!fromModel && (
                     <TextField
                        size="small"
                        label="Label"
                        placeholder={newName ? titleCase(newName) : ""}
                        value={label}
                        onChange={(event) => setLabel(event.target.value)}
                        inputProps={{ "aria-label": "Control label" }}
                        sx={{ flex: 1, minWidth: 160 }}
                     />
                  )}
                  {valueTyped &&
                     !perTile &&
                     operatorField(commonOp, setCommonOp, "Comparison")}
               </Stack>
               {!editing && source.kind === "new" && (
                  <Stack direction="row" sx={{ gap: 1.5 }}>
                     <TextField
                        select
                        size="small"
                        label="Control"
                        value={kind}
                        onChange={(event) =>
                           pickKind(event.target.value as ControlKind)
                        }
                        inputProps={{ "aria-label": "Kind of control" }}
                        slotProps={{
                           select: {
                              renderValue: (value) =>
                                 CONTROL_KINDS.find((k) => k.kind === value)
                                    ?.label ?? String(value),
                           },
                        }}
                        sx={{ flex: 1 }}
                     >
                        {CONTROL_KINDS.map((option) => (
                           <MenuItem key={option.kind} value={option.kind}>
                              {option.label}
                              <Typography
                                 component="span"
                                 variant="caption"
                                 sx={{ ml: 1, opacity: 0.6 }}
                              >
                                 {option.hint}
                              </Typography>
                           </MenuItem>
                        ))}
                     </TextField>
                     {kind === "date" && (
                        <TextField
                           size="small"
                           type="date"
                           label="Starts at"
                           value={dateDefault}
                           onChange={(event) =>
                              setDateDefault(event.target.value)
                           }
                           InputLabelProps={{ shrink: true }}
                           sx={{ flex: 1 }}
                        />
                     )}
                  </Stack>
               )}

               {/* WHICH tiles. */}
               <Box>
                  <Stack
                     direction="row"
                     sx={{ alignItems: "center", gap: 0.5 }}
                  >
                     <Checkbox
                        size="small"
                        checked={
                           bindableCount > 0 && included === bindableCount
                        }
                        indeterminate={included > 0 && included < bindableCount}
                        disabled={bindableCount === 0}
                        onChange={(event) => setAll(event.target.checked)}
                        inputProps={{ "aria-label": "All tiles" }}
                     />
                     <Typography variant="subtitle2" sx={{ flex: 1 }}>
                        Applies to {included} of {document.tiles.length} tiles
                     </Typography>
                     <Button
                        size="small"
                        onClick={() => {
                           // Opening per-tile starts every row from the common
                           // field; closing it goes back to one box for all.
                           if (!perTile)
                              setRows((previous) =>
                                 previous.map((row) => ({ ...row, field })),
                              );
                           setPerTile((was) => !was);
                        }}
                     >
                        {perTile ? "Same field for all" : "Adjust per tile"}
                     </Button>
                  </Stack>
                  <Stack sx={{ gap: perTile ? 1 : 0 }}>
                     {document.tiles.map((tile, index) => {
                        const row = rows[index];
                        const title = tile.label ?? tile.name;
                        const problem = perTile
                           ? rowProblems[index]
                           : undefined;
                        return (
                           <Stack
                              key={`${tile.source}.${tile.name}`}
                              direction="row"
                              sx={{
                                 gap: 1,
                                 alignItems: perTile ? "flex-start" : "center",
                                 opacity: bindable[index] ? 1 : 0.6,
                              }}
                           >
                              <Checkbox
                                 size="small"
                                 disabled={!bindable[index]}
                                 checked={
                                    (row?.include ?? false) && bindable[index]
                                 }
                                 inputProps={{
                                    "aria-label": `Filter ${title}`,
                                 }}
                                 onChange={(event) =>
                                    setRow(index, {
                                       include: event.target.checked,
                                    })
                                 }
                              />
                              <Typography
                                 variant="body2"
                                 sx={{
                                    flex: 1,
                                    minWidth: 0,
                                    pt: perTile ? 1 : 0,
                                 }}
                                 noWrap
                              >
                                 {title}
                              </Typography>
                              {!bindable[index] ? (
                                 <Tooltip
                                    title={
                                       tile.declaration.kind === "inherited"
                                          ? "Declared on its source, which this dashboard does not write."
                                          : "Its query is written out here rather than named, so a filter cannot be added to it."
                                    }
                                 >
                                    <Typography
                                       variant="caption"
                                       sx={{ color: theme.tileTitle }}
                                    >
                                       {tile.declaration.kind === "inherited"
                                          ? "From the model"
                                          : "Inline query"}
                                    </Typography>
                                 </Tooltip>
                              ) : (
                                 perTile && (
                                    <>
                                       {valueTyped &&
                                          operatorField(
                                             row?.op,
                                             (op) => setRow(index, { op }),
                                             `Comparison for ${title}`,
                                          )}
                                       <FieldPicker
                                          value={row?.field ?? ""}
                                          onChange={(next) =>
                                             setRow(index, { field: next })
                                          }
                                          fields={fieldsFor?.(tile)}
                                          accepts={accepts}
                                          label="Field"
                                          ariaLabel={`Field for ${title}`}
                                          disabled={!row?.include}
                                          error={problem !== undefined}
                                          helperText={problem}
                                          sx={{ width: 220 }}
                                       />
                                    </>
                                 )
                              )}
                           </Stack>
                        );
                     })}
                  </Stack>
                  {included === 0 && (
                     <Typography
                        variant="caption"
                        sx={{ color: "warning.main", display: "block", mt: 1 }}
                     >
                        No tile is ticked, so this control will not appear on
                        the dashboard.
                     </Typography>
                  )}
               </Box>
            </Stack>
         </DialogContent>
         <DialogActions sx={{ px: 3, py: 1.5 }}>
            {editing && (
               <Button
                  color="error"
                  onClick={() => onRemove(shown.name)}
                  aria-label={`Remove control ${shown.name}`}
                  sx={{ mr: "auto" }}
               >
                  Remove from dashboard
               </Button>
            )}
            <Button onClick={onClose}>Cancel</Button>
            <Tooltip
               title={
                  canApply || target === undefined
                     ? ""
                     : "A ticked tile has no field, or names one its source does not have."
               }
            >
               <span>
                  <Button
                     variant="contained"
                     onClick={apply}
                     disabled={!canApply}
                  >
                     {editing ? "Apply" : "Add filter"}
                  </Button>
               </span>
            </Tooltip>
         </DialogActions>
      </Dialog>
   );
}
