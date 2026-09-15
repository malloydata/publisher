// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import {
   Alert,
   Box,
   Button,
   Checkbox,
   Dialog,
   DialogActions,
   DialogContent,
   DialogTitle,
   Divider,
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
 * Looker's Add-filter window, for this format.
 *
 * Two halves, and the second is the one that matters. The FIRST says what the
 * control is: an existing given the model offers, or a new one declared in this
 * dashboard from a field. The SECOND is "tiles to update": which tiles the
 * control drives, and on which field each one filters — because a control is
 * almost never for one tile, and per tile is the only place to say that one
 * filters on `products.brand` where another says `brand`, or that a tile opts
 * out.
 *
 * The same window edits and removes. It opens on the mapping the control has
 * NOW, so unticking a tile unbinds it, and a control this dashboard declares
 * can be taken off the page outright.
 */
export interface FilterDialogProps {
   open: boolean;
   document: DashboardDocument;
   /** The control being edited, or undefined to add one. */
   control?: BuilderControl;
   /** Model givens not yet bound anywhere, offered as "existing". */
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

   // Where the control comes from. Editing fixes it; adding starts on "new",
   // since declaring in the dashboard is the convention, and offers the model's
   // unbound givens beside it.
   const [source, setSource] = useState<Source>({ kind: "new" });
   const [label, setLabel] = useState("");
   const [field, setField] = useState("");
   const [kind, setKind] = useState<ControlKind>("select");
   const [dateDefault, setDateDefault] = useState("");
   const [rows, setRows] = useState<MappingRow[]>([]);
   // Which rows the reader has typed into. Seeding fills the rest and never
   // those: the first version marked its OWN fill as an edit, so typing
   // "status" seeded every row with "s" and left it there.
   const [touched, setTouched] = useState<boolean[]>([]);

   // The base of the dashboard's own extension is the source every field here
   // lives on, and the one a picker's options can be read from. A dashboard
   // over imported sources alone has none; the picker then has no options
   // source and the control is declared as text.
   const suggestSource = document.sources[0]?.base;

   // Reset on open, on what is true now.
   useEffect(() => {
      if (!open) return;
      setTouched(document.tiles.map(() => false));
      if (control) {
         setSource({ kind: "existing", name: control.name });
         setLabel(control.label ?? "");
         setField(control.field ?? "");
         setRows(mappingOf(document, control));
      } else {
         setSource({ kind: "new" });
         setLabel("");
         setField("");
         setKind("select");
         setDateDefault(new Date().toISOString().slice(0, 10));
         setRows(
            document.tiles.map((tile) => ({
               include: canBind(tile),
               field: "",
            })),
         );
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
   const target: { name: string; type?: string; field?: string } | undefined =
      source.kind === "existing"
         ? (control ?? available.find((g) => g.name === source.name))
         : newName
           ? {
                name: newName,
                type: newLocalGiven({ name: newName, label, kind, field }).type,
                field,
             }
           : undefined;

   // Picking an existing given, or typing a field for a new one, seeds every
   // row's field and comparison the way Looker maps a filter to "the same
   // field" on each tile. Only rows the reader has not typed into move.
   const seedRows = (next: { field?: string; type?: string }) =>
      setRows((previous) =>
         previous.map((row, index) => ({
            ...row,
            field: touched[index] ? row.field : (next.field ?? row.field),
            ...(defaultOperator(next.type)
               ? { op: defaultOperator(next.type) }
               : {}),
         })),
      );

   const valueTyped =
      target?.type !== undefined && !target.type.startsWith("filter<");
   const anyBound = rows.some(
      (row, i) => row.include && canBind(document.tiles[i]),
   );
   const canApply =
      target !== undefined &&
      (source.kind === "existing" || (field.trim() !== "" && newName !== ""));

   const apply = () => {
      if (!target) return;
      if (source.kind === "new") {
         const declared = newLocalGiven({
            name: target.name,
            label,
            kind,
            field: field.trim(),
            ...(suggestSource ? { source: suggestSource } : {}),
            ...(dateDefault ? { dateDefault } : {}),
         });
         onApply(target.name, rows, declared);
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

   const setRow = (index: number, patch: Partial<MappingRow>) =>
      setRows((previous) =>
         previous.map((row, i) => (i === index ? { ...row, ...patch } : row)),
      );

   return (
      <Dialog open={open} onClose={onClose} maxWidth="sm" fullWidth>
         <DialogTitle sx={{ pb: 1 }}>
            {editing ? (
               <>
                  Filter{" "}
                  <Box
                     component="span"
                     sx={{
                        fontFamily: "ui-monospace, monospace",
                        fontSize: "0.85em",
                     }}
                  >
                     ${control.name}
                  </Box>
               </>
            ) : (
               "Add a filter"
            )}
         </DialogTitle>
         <DialogContent dividers>
            <Stack sx={{ gap: 2 }}>
               {!editing && (
                  <Stack sx={{ gap: 1.5 }}>
                     <ToggleButtonGroup
                        exclusive
                        size="small"
                        value={source.kind}
                        onChange={(_, value: "new" | "existing" | null) => {
                           if (value === "new") setSource({ kind: "new" });
                           else if (value === "existing" && available[0]) {
                              setSource({
                                 kind: "existing",
                                 name: available[0].name,
                              });
                              seedRows(available[0]);
                           }
                        }}
                        aria-label="Where the filter comes from"
                     >
                        <ToggleButton value="new">
                           New, in this dashboard
                        </ToggleButton>
                        <ToggleButton
                           value="existing"
                           disabled={available.length === 0}
                        >
                           From the model
                        </ToggleButton>
                     </ToggleButtonGroup>

                     {source.kind === "existing" ? (
                        <TextField
                           select
                           size="small"
                           label="Control"
                           value={source.name}
                           onChange={(event) => {
                              const picked = available.find(
                                 (g) => g.name === event.target.value,
                              );
                              setSource({
                                 kind: "existing",
                                 name: event.target.value,
                              });
                              if (picked) seedRows(picked);
                           }}
                           helperText="Declared in the model. You can bind it here, not change it."
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
                        <Stack sx={{ gap: 1.5 }}>
                           <Stack direction="row" sx={{ gap: 1.5 }}>
                              <TextField
                                 size="small"
                                 label="Field"
                                 placeholder="category"
                                 value={field}
                                 autoFocus
                                 onChange={(event) => {
                                    setField(event.target.value);
                                    seedRows({
                                       field: event.target.value.trim(),
                                    });
                                 }}
                                 helperText={
                                    newName
                                       ? `Declared as $${newName}`
                                       : "A dimension the tiles' source has"
                                 }
                                 inputProps={{
                                    "aria-label": "Field to filter",
                                 }}
                                 sx={{ flex: 1 }}
                              />
                              <TextField
                                 size="small"
                                 label="Label"
                                 placeholder={
                                    newName
                                       ? newName.charAt(0) +
                                         newName.slice(1).toLowerCase()
                                       : ""
                                 }
                                 value={label}
                                 onChange={(event) =>
                                    setLabel(event.target.value)
                                 }
                                 inputProps={{ "aria-label": "Control label" }}
                                 sx={{ flex: 1 }}
                              />
                           </Stack>
                           <TextField
                              select
                              size="small"
                              label="Control"
                              value={kind}
                              onChange={(event) => {
                                 const next = event.target.value as ControlKind;
                                 setKind(next);
                                 seedRows({
                                    type: newLocalGiven({
                                       name: "X",
                                       label,
                                       kind: next,
                                       field,
                                    }).type,
                                 });
                              }}
                              inputProps={{ "aria-label": "Kind of control" }}
                           >
                              {CONTROL_KINDS.map((option) => (
                                 <MenuItem
                                    key={option.kind}
                                    value={option.kind}
                                 >
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
                                 helperText="The control's default; readers can move it"
                              />
                           )}
                        </Stack>
                     )}
                  </Stack>
               )}

               {editing && control.origin === "dashboard" && (
                  <TextField
                     size="small"
                     label="Label"
                     value={label}
                     onChange={(event) => setLabel(event.target.value)}
                     inputProps={{ "aria-label": "Control label" }}
                  />
               )}
               {editing && control.origin === "model" && (
                  <Alert severity="info" sx={{ py: 0.5 }}>
                     Declared in the model, so its label and options are set
                     there. This dashboard decides which tiles it drives, and
                     removing it here takes it off every tile.
                  </Alert>
               )}

               <Divider />

               <Stack sx={{ gap: 0.5 }}>
                  <Stack
                     direction="row"
                     sx={{ alignItems: "baseline", gap: 1 }}
                  >
                     <Typography variant="subtitle2">
                        Tiles to update
                     </Typography>
                     <Typography
                        variant="caption"
                        sx={{ color: theme.tileTitle, opacity: 0.8, flex: 1 }}
                     >
                        Ticked tiles move when the control does.
                     </Typography>
                     <Button
                        size="small"
                        onClick={() =>
                           setRows((previous) =>
                              previous.map((row, i) => ({
                                 ...row,
                                 include: canBind(document.tiles[i]),
                              })),
                           )
                        }
                     >
                        All
                     </Button>
                     <Button
                        size="small"
                        onClick={() =>
                           setRows((previous) =>
                              previous.map((row) => ({
                                 ...row,
                                 include: false,
                              })),
                           )
                        }
                     >
                        None
                     </Button>
                  </Stack>

                  {document.tiles.map((tile, index) => {
                     const row = rows[index];
                     const bindable = canBind(tile);
                     const title = tile.label ?? tile.name;
                     return (
                        <Stack
                           key={`${tile.source}.${tile.name}`}
                           direction="row"
                           sx={{
                              gap: 1,
                              alignItems: "center",
                              opacity: bindable ? 1 : 0.6,
                           }}
                        >
                           <Checkbox
                              size="small"
                              disabled={!bindable}
                              checked={(row?.include ?? false) && bindable}
                              inputProps={{ "aria-label": `Filter ${title}` }}
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
                           {bindable ? (
                              <>
                                 {valueTyped && (
                                    <TextField
                                       select
                                       size="small"
                                       value={row?.op ?? ">="}
                                       disabled={!row?.include}
                                       onChange={(event) =>
                                          setRow(index, {
                                             op: event.target.value,
                                          })
                                       }
                                       inputProps={{
                                          "aria-label": `Comparison for ${title}`,
                                       }}
                                       sx={{ width: 120 }}
                                    >
                                       {OPERATORS.filter(
                                          (o) => o.op !== "~",
                                       ).map((o) => (
                                          <MenuItem key={o.op} value={o.op}>
                                             {o.label}
                                          </MenuItem>
                                       ))}
                                    </TextField>
                                 )}
                                 <TextField
                                    size="small"
                                    label="Field"
                                    value={row?.field ?? ""}
                                    disabled={!row?.include}
                                    inputProps={{
                                       "aria-label": `Field for ${title}`,
                                    }}
                                    onChange={(event) => {
                                       setTouched((previous) =>
                                          previous.map((was, i) =>
                                             i === index ? true : was,
                                          ),
                                       );
                                       setRow(index, {
                                          field: event.target.value,
                                       });
                                    }}
                                    sx={{ width: 170 }}
                                 />
                              </>
                           ) : (
                              <Tooltip
                                 title={
                                    tile.declaration.kind === "inherited"
                                       ? "Declared on its source, which this dashboard does not write."
                                       : "Its query is written out here rather than named, so a filter cannot be added to it."
                                 }
                              >
                                 <Typography
                                    variant="caption"
                                    sx={{ width: 170, color: theme.tileTitle }}
                                 >
                                    {tile.declaration.kind === "inherited"
                                       ? "From the model"
                                       : "Inline query"}
                                 </Typography>
                              </Tooltip>
                           )}
                        </Stack>
                     );
                  })}
               </Stack>
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
            <Tooltip
               title={
                  anyBound
                     ? ""
                     : "No tile is ticked, so this control will not appear."
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
