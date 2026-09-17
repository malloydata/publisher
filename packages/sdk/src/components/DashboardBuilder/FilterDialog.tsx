// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import {
   Autocomplete,
   Box,
   Button,
   Checkbox,
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
import { useMemo } from "react";
import { titleCase, useFilterForm } from "./useFilterForm";
import { usePublisherTheme } from "../../theme/ThemeContext";
import type { CatalogField } from "./catalog";
import {
   CONTROL_KINDS,
   OPERATORS,
   type BuilderControl,
   type ControlKind,
   type MappingRow,
} from "./controls";
import type { DashboardDocument, DashboardTile, LocalGiven } from "./document";
import { AppDialog } from "../AppDialog";

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

/** `BRAND` -> `Brand`: the label a new control gets if the author types none. */
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
   const {
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
   } = useFilterForm({
      open,
      document,
      control,
      available,
      fieldsFor,
      onApply,
   });

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
      <AppDialog
         open={open}
         onClose={onClose}
         title={editing ? (shown.label ?? shown.name) : "Add a filter"}
         description={
            editing ? (
               <>
                  <Box
                     component="span"
                     sx={{ fontFamily: "ui-monospace, monospace" }}
                  >
                     ${shown.name}
                  </Box>
                  {fromModel
                     ? " · declared in the model, so its label and options are set there"
                     : " · declared in this dashboard"}
               </>
            ) : (
               "A control on the page, and the tiles it filters."
            )
         }
         actions={
            <>
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
            </>
         }
      >
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
                  <ToggleButton value="new">New in this dashboard</ToggleButton>
                  <ToggleButton value="existing">From the model</ToggleButton>
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
                        onChange={(event) => setDateDefault(event.target.value)}
                        InputLabelProps={{ shrink: true }}
                        sx={{ flex: 1 }}
                     />
                  )}
               </Stack>
            )}

            {/* WHICH tiles. */}
            <Box>
               <Stack direction="row" sx={{ alignItems: "center", gap: 0.5 }}>
                  <Checkbox
                     size="small"
                     checked={bindableCount > 0 && included === bindableCount}
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
                     const problem = perTile ? rowProblems[index] : undefined;
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
                              <Tooltip title="Declared on its source, which this dashboard does not write.">
                                 <Typography
                                    variant="caption"
                                    sx={{ color: theme.tileTitle }}
                                 >
                                    From the model
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
                     No tile is ticked, so this control will not appear on the
                     dashboard.
                  </Typography>
               )}
            </Box>
         </Stack>
      </AppDialog>
   );
}
