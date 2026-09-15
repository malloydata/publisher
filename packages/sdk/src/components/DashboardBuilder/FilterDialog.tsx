// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import {
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
} from "@mui/material";
import { useEffect, useMemo, useState } from "react";
import { usePublisherTheme } from "../../theme/ThemeContext";
import {
   canBind,
   CONTROL_KINDS,
   defaultOperator,
   givenNameFor,
   mappingOf,
   newLocalGiven,
   OPERATORS,
   type BuilderControl,
   type ControlKind,
   type MappingRow,
} from "./controls";
import type { DashboardDocument, LocalGiven } from "./document";

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

export function FilterDialog({
   open,
   document,
   control,
   available,
   onClose,
   onApply,
   onRemove,
}: FilterDialogProps) {
   const { theme } = usePublisherTheme();
   const editing = control !== undefined;

   const [source, setSource] = useState<Source>({ kind: "new" });
   const [label, setLabel] = useState("");
   const [kind, setKind] = useState<ControlKind>("select");
   const [dateDefault, setDateDefault] = useState("");
   // The field every ticked tile filters on, and the rows themselves. The
   // field is the common case; the rows are the truth. Typing in the field
   // writes every row, and "Adjust per tile" opens the rows up to differ.
   const [field, setField] = useState("");
   const [rows, setRows] = useState<MappingRow[]>([]);
   const [perTile, setPerTile] = useState(false);

   // The base of the dashboard's own extension is the source every field here
   // lives on, and the one a picker's options can be read from. A dashboard
   // over imported sources alone has none; the control is then declared as
   // text.
   const suggestSource = document.sources[0]?.base;

   // Reset on open, on what is true now.
   useEffect(() => {
      if (!open) return;
      if (control) {
         setSource({ kind: "existing", name: control.name });
         setLabel(control.label ?? "");
         const mapped = mappingOf(document, control);
         setRows(mapped);
         // One field when every bound tile agrees; otherwise the rows already
         // differ and the per-tile view is the honest one to open on.
         const bound = mapped.filter((row) => row.include);
         const fields = new Set(bound.map((row) => row.field));
         setField(fields.size === 1 ? bound[0].field : (control.field ?? ""));
         setPerTile(fields.size > 1);
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

   const setCommonField = (next: string) => {
      setField(next);
      setRows((previous) => previous.map((row) => ({ ...row, field: next })));
   };
   const setCommonOp = (next: string) =>
      setRows((previous) => previous.map((row) => ({ ...row, op: next })));
   const setRow = (index: number, patch: Partial<MappingRow>) =>
      setRows((previous) =>
         previous.map((row, i) => (i === index ? { ...row, ...patch } : row)),
      );
   /** Rows follow a given's type: the comparison it needs, or none. */
   const retype = (type: string | undefined) => {
      const op = defaultOperator(type);
      setRows((previous) =>
         previous.map(({ op: _was, ...row }) => (op ? { ...row, op } : row)),
      );
   };
   const pickExisting = (given: BuilderControl) => {
      setSource({ kind: "existing", name: given.name });
      setCommonField(given.field ?? given.name.toLowerCase());
      retype(given.type);
   };
   const pickKind = (next: ControlKind) => {
      setKind(next);
      retype(newLocalGiven({ name: "X", label, kind: next, field }).type);
   };

   const bindable = document.tiles.map(canBind);
   const bindableCount = bindable.filter(Boolean).length;
   const included = rows.filter((row, i) => row.include && bindable[i]).length;
   const setAll = (include: boolean) =>
      setRows((previous) =>
         previous.map((row, i) => ({
            ...row,
            include: include && bindable[i],
         })),
      );

   const canApply =
      target !== undefined &&
      (source.kind === "existing" || (field.trim() !== "" && newName !== ""));

   const apply = () => {
      if (!target) return;
      if (source.kind === "new") {
         onApply(
            target.name,
            rows,
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
            rows,
            changed
               ? { ...control.local, ...(trimmed ? { label: trimmed } : {}) }
               : undefined,
         );
      } else {
         onApply(target.name, rows);
      }
   };

   const fromModel = editing && control.origin === "model";
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
            {editing ? (control.label ?? control.name) : "Add a filter"}
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
                     ${control.name}
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
               ) : (
                  <Stack direction="row" sx={{ gap: 1.5, flexWrap: "wrap" }}>
                     <TextField
                        size="small"
                        label="Field to filter"
                        placeholder="category"
                        value={field}
                        autoFocus={!editing}
                        onChange={(event) => setCommonField(event.target.value)}
                        inputProps={{ "aria-label": "Field to filter" }}
                        helperText={
                           !editing && newName
                              ? `Declared as $${newName}`
                              : undefined
                        }
                        sx={{ flex: 1, minWidth: 160 }}
                     />
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
                        operatorField(commonOp, setCommonOp, "Comparison")}
                  </Stack>
               )}
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
                           if (perTile) setCommonField(field);
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
                        return (
                           <Stack
                              key={`${tile.source}.${tile.name}`}
                              direction="row"
                              sx={{
                                 gap: 1,
                                 alignItems: "center",
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
                                 sx={{ flex: 1, minWidth: 0 }}
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
                                       <TextField
                                          size="small"
                                          label="Field"
                                          value={row?.field ?? ""}
                                          disabled={!row?.include}
                                          inputProps={{
                                             "aria-label": `Field for ${title}`,
                                          }}
                                          onChange={(event) =>
                                             setRow(index, {
                                                field: event.target.value,
                                             })
                                          }
                                          sx={{ width: 180 }}
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
                  onClick={() => onRemove(control.name)}
                  aria-label={`Remove control ${control.name}`}
                  sx={{ mr: "auto" }}
               >
                  Remove from dashboard
               </Button>
            )}
            <Button onClick={onClose}>Cancel</Button>
            <Button variant="contained" onClick={apply} disabled={!canApply}>
               {editing ? "Apply" : "Add filter"}
            </Button>
         </DialogActions>
      </Dialog>
   );
}
