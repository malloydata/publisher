// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { segmentedSx } from "./BuilderToolbar";
import { reportClasses } from "../../theme/classes";
import ArrowDropDownCircleOutlinedIcon from "@mui/icons-material/ArrowDropDownCircleOutlined";
import ChecklistRtlIcon from "@mui/icons-material/ChecklistRtl";
import DeleteOutlineIcon from "@mui/icons-material/DeleteOutline";
import EventOutlinedIcon from "@mui/icons-material/EventOutlined";
import ExpandMoreIcon from "@mui/icons-material/ExpandMore";
import LinearScaleIcon from "@mui/icons-material/LinearScale";
import ShortTextIcon from "@mui/icons-material/ShortText";
import ViewQuiltOutlinedIcon from "@mui/icons-material/ViewQuiltOutlined";
import {
   Autocomplete,
   Box,
   Button,
   ButtonBase,
   Checkbox,
   Collapse,
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
import {
   useEffect,
   useMemo,
   useRef,
   useState,
   type ReactNode,
   type Ref,
} from "react";
import { titleCase, useFilterForm } from "./useFilterForm";
import { dangerTextColor, MOTION_FAST } from "../../theme/motion";
import { edgeColor, hoverEdgeColor } from "../controlTokens";
import { usePublisherTheme } from "../../theme/ThemeContext";
import type { CatalogField } from "./catalog";
import {
   acceptsField,
   CONTROL_KINDS,
   newLocalGiven,
   OPERATORS,
   typeLabel,
   type BuilderControl,
   type ControlKind,
   type MappingRow,
} from "./controls";
import {
   isQueryTile,
   type DashboardDocument,
   type DashboardTile,
   type LocalGiven,
   type QueryTile,
} from "./document";
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
   /** Names a new filter must not take, beyond the page's own and `available`'s. */
   takenNames?: readonly string[];
   /**
    * The fields a binding on THIS tile may name: the dimensions of the source
    * it reads. Per tile, because a composite dashboard exists to span sources,
    * and a field one source has another may not. Undefined for a tile whose
    * source the host has no catalog for, in which case any name is accepted
    * for it and nothing is searched.
    */
   fieldsFor?: (tile: QueryTile) => readonly CatalogField[] | undefined;
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
   inputRef,
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
   inputRef?: Ref<HTMLInputElement>;
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
               <TypeBadge type={types.get(option)} />
               <Typography variant="body2" sx={{ flex: 1, ml: 1 }}>
                  {option}
               </Typography>
            </li>
         )}
         renderInput={(params) => (
            <TextField
               {...params}
               label={label}
               placeholder={placeholder}
               autoFocus={autoFocus}
               {...(inputRef ? { inputRef } : {})}
               error={error}
               helperText={helperText}
               InputLabelProps={{ ...params.InputLabelProps, shrink: true }}
               InputProps={{
                  ...params.InputProps,
                  startAdornment: types.get(value.trim()) ? (
                     <Box sx={{ display: "flex", ml: 0.5, mr: -0.25 }}>
                        <TypeBadge type={types.get(value.trim())} />
                     </Box>
                  ) : undefined,
               }}
               inputProps={{ ...params.inputProps, "aria-label": ariaLabel }}
            />
         )}
         sx={sx}
      />
   );
}

/**
 * Why a tile cannot take a binding. A tile declared on its source and one
 * declared HERE in a body with no single block for a `where:` are different
 * situations, and saying "from the model" about the second one is untrue.
 */
const unbindable = (tile: DashboardTile | undefined) =>
   tile && isQueryTile(tile) && tile.declaration.kind === "opaque"
      ? {
           label: "Not bindable",
           reason: `Its body is ${tile.declaration.why}, so a filter has no single place to go.`,
        }
      : {
           label: "From the model",
           reason: "Declared on its source, which this report does not write.",
        };

export function FilterDialog({
   open,
   document,
   control,
   available,
   takenNames,
   fieldsFor,
   onClose,
   onApply,
   onRemove,
}: FilterDialogProps) {
   const { theme } = usePublisherTheme();
   const ui = theme.ui;
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
      ...(takenNames ? { takenNames } : {}),
      fieldsFor,
      onApply,
   });

   const fieldRef = useRef<HTMLInputElement>(null);
   const edge = edgeColor(theme.cardBorder);
   const hoverEdge = hoverEdgeColor(theme.cardBorder, theme.tileTitle);
   // The tiles are a summary until asked for: most filters apply to every
   // tile, and a list of checkboxes on every open was most of the window. It
   // opens by itself on a control that does not apply to them all.
   const [tilesOpen, setTilesOpen] = useState(false);
   // Decided once per opening, on the render after the form has read the
   // control's mapping: in the opening render itself the rows are still the
   // last window's, and the list opened (or not) for the wrong control.
   const [deciding, setDeciding] = useState(false);
   useEffect(() => {
      if (open) setDeciding(true);
      else setTilesOpen(false);
   }, [open]);
   useEffect(() => {
      if (!deciding) return;
      setDeciding(false);
      setTilesOpen(editing && included < bindableCount);
      // Only when deciding: ticking a tile must not fold the list away.
      // eslint-disable-next-line react-hooks/exhaustive-deps
   }, [deciding]);
   useEffect(() => {
      if (perTile) setTilesOpen(true);
   }, [perTile]);
   // Why a kind cannot filter the picked field, when it cannot.
   const fieldType = commonFields?.find((f) => f.name === field.trim())?.type;
   const kindReason = (candidate: ControlKind): string | undefined => {
      if (!fieldType) return undefined;
      const wants = newLocalGiven({
         name: "X",
         label,
         kind: candidate,
         field,
      }).type;
      return acceptsField(wants, fieldType)
         ? undefined
         : `${field.trim()} is ${typeLabel(fieldType)}, which this control cannot filter.`;
   };
   const previewLabel =
      label.trim() || (newName ? titleCase(newName) : "Filter");
   const previewValue =
      kind === "number"
         ? "Any"
         : kind === "date"
           ? dateDefault
              ? new Date(`${dateDefault}T00:00:00`).toLocaleDateString([], {
                   month: "short",
                   day: "numeric",
                   year: "numeric",
                })
              : "Any"
           : "All";
   // The builder's own segmented control, as the tile picker's is.
   const segmentSx = {
      ...segmentedSx(edge, ui),
      alignSelf: "flex-start",
      "& .MuiToggleButton-root": {
         ...segmentedSx(edge, ui)["& .MuiToggleButton-root"],
         textTransform: "none",
      },
   };

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
         className={`${reportClasses.dialog} ${reportClasses.filterDialog}`}
         open={open}
         onClose={onClose}
         {...(!editing ? { initialFocus: fieldRef } : {})}
         title={editing ? (shown.label ?? shown.name) : "Add a filter"}
         description={
            editing ? (
               <>
                  <Box component="span" sx={{ fontFamily: ui.font.mono }}>
                     ${shown.name}
                  </Box>
                  {fromModel
                     ? " · declared in the model, so its label and options are set there"
                     : " · declared in this report"}
               </>
            ) : (
               "A filter on the page, and the tiles it filters."
            )
         }
         actions={
            <>
               {editing && (
                  <Button
                     color="error"
                     startIcon={<DeleteOutlineIcon />}
                     onClick={() => onRemove(shown.name)}
                     sx={(muiTheme) => ({
                        mr: "auto",
                        color: dangerTextColor(muiTheme),
                     })}
                  >
                     Remove filter
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
                  sx={segmentSx}
               >
                  <ToggleButton value="new">New in this report</ToggleButton>
                  <ToggleButton value="existing">From the model</ToggleButton>
               </ToggleButtonGroup>
            )}

            {!editing && source.kind === "existing" ? (
               <TextField
                  select
                  size="small"
                  label="Filter"
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

            <Box
               sx={{
                  display: "grid",
                  gridTemplateColumns: {
                     xs: "1fr",
                     sm: fromModel ? "1fr" : "minmax(0, 1.4fr) minmax(0, 1fr)",
                  },
                  gap: 1.5,
                  alignItems: "start",
               }}
            >
               {/* Hidden while per-tile fields are open: they speak for
                   themselves then, and a box above them that applied to
                   nothing would read as one more thing to fill. */}
               {!perTile && (
                  <FieldPicker
                     value={field}
                     onChange={pickField}
                     fields={commonFields}
                     accepts={accepts}
                     label="Field"
                     ariaLabel="Field to filter"
                     placeholder="category"
                     inputRef={fieldRef}
                     error={commonProblem !== undefined}
                     helperText={
                        commonProblem ??
                        (!editing && newName
                           ? `Declared as $${newName}`
                           : undefined)
                     }
                  />
               )}
               {!fromModel && (
                  <TextField
                     size="small"
                     label="Label"
                     placeholder={newName ? titleCase(newName) : ""}
                     value={label}
                     onChange={(event) => setLabel(event.target.value)}
                     InputLabelProps={{ shrink: true }}
                     inputProps={{ "aria-label": "Filter label" }}
                  />
               )}
            </Box>
            {valueTyped &&
               !perTile &&
               operatorField(commonOp, setCommonOp, "Comparison")}

            {!editing && source.kind === "new" && (
               <Box>
                  <SectionLabel>Control</SectionLabel>
                  <Box
                     role="radiogroup"
                     aria-label="Filter kind"
                     sx={{
                        display: "grid",
                        gridTemplateColumns: {
                           xs: "repeat(3, minmax(0, 1fr))",
                           sm: "repeat(5, minmax(0, 1fr))",
                        },
                        gap: 1,
                     }}
                  >
                     {CONTROL_KINDS.map((option) => {
                        const checked = option.kind === kind;
                        const reason = kindReason(option.kind);
                        return (
                           <Tooltip
                              key={option.kind}
                              title={reason ?? option.hint}
                              describeChild
                           >
                              <ButtonBase
                                 role="radio"
                                 aria-checked={checked}
                                 aria-label={option.label}
                                 aria-disabled={reason !== undefined}
                                 onClick={() => {
                                    if (reason === undefined)
                                       pickKind(option.kind);
                                 }}
                                 sx={(mui) => ({
                                    flexDirection: "column",
                                    gap: 0.5,
                                    px: 1,
                                    py: 1.25,
                                    borderRadius: ui.radius.tile,
                                    border: `1px solid ${checked ? theme.accent : edge}`,
                                    boxShadow: checked
                                       ? `0 0 0 1px ${theme.accent}`
                                       : "none",
                                    bgcolor: checked
                                       ? `color-mix(in srgb, ${theme.accent} 6%, ${mui.palette.background.paper})`
                                       : mui.palette.background.paper,
                                    color: checked
                                       ? theme.accent
                                       : mui.palette.text.secondary,
                                    opacity: reason ? 0.45 : 1,
                                    cursor: reason ? "default" : "pointer",
                                    transition: `border-color ${MOTION_FAST}, background-color ${MOTION_FAST}`,
                                    "&:hover":
                                       reason || checked
                                          ? {}
                                          : { borderColor: hoverEdge },
                                 })}
                              >
                                 <KindGlyph kind={option.kind} />
                                 <Box
                                    component="span"
                                    sx={{
                                       ...ui.type.caption,
                                       fontWeight: checked ? 600 : 500,
                                       color: checked
                                          ? theme.accent
                                          : "text.primary",
                                       textAlign: "center",
                                    }}
                                 >
                                    {SHORT_KIND[option.kind]}
                                 </Box>
                              </ButtonBase>
                           </Tooltip>
                        );
                     })}
                  </Box>
                  {kind === "date" && (
                     <TextField
                        size="small"
                        type="date"
                        label="Starts at"
                        value={dateDefault}
                        onChange={(event) => setDateDefault(event.target.value)}
                        InputLabelProps={{ shrink: true }}
                        sx={{ mt: 1.5, width: 200 }}
                     />
                  )}
               </Box>
            )}

            {/* WHICH tiles: a summary, opened into the list on request. */}
            <Box
               sx={{
                  border: `1px solid ${edge}`,
                  borderRadius: ui.radius.tile,
                  overflow: "hidden",
               }}
            >
               <Stack
                  direction="row"
                  sx={{ alignItems: "center", gap: 1.25, px: 1.5, py: 1 }}
               >
                  <ViewQuiltOutlinedIcon
                     aria-hidden
                     sx={{ fontSize: 18, color: "text.secondary" }}
                  />
                  <Typography variant="body2" sx={{ flex: 1, minWidth: 0 }}>
                     <Box
                        component="span"
                        sx={{ fontWeight: ui.type.label.fontWeight }}
                     >
                        {included === bindableCount && bindableCount > 0
                           ? `Applies to all ${included} tile${included === 1 ? "" : "s"}`
                           : `Applies to ${included} of ${bindableCount} tile${bindableCount === 1 ? "" : "s"}`}
                     </Box>
                     {!perTile && field.trim() !== "" && included > 0 && (
                        <Box component="span" sx={{ color: "text.secondary" }}>
                           {" · each filters on "}
                           <Box
                              component="span"
                              sx={{
                                 ...ui.type.code,
                              }}
                           >
                              {field.trim()}
                           </Box>
                        </Box>
                     )}
                  </Typography>
                  {/* Not folded while each tile has a field of its own: a
                      problem with one is the only thing holding Apply, and
                      folded it would be out of sight. */}
                  {!perTile && (
                     <Button
                        size="small"
                        aria-expanded={tilesOpen}
                        onClick={() => setTilesOpen((was) => !was)}
                     >
                        {tilesOpen ? "Done choosing" : "Choose tiles"}
                     </Button>
                  )}
               </Stack>
               <Collapse in={tilesOpen} unmountOnExit>
                  <Box sx={{ borderTop: `1px solid ${edge}`, px: 0.5, pb: 1 }}>
                     <Stack
                        direction="row"
                        sx={{ alignItems: "center", gap: 0.5, pt: 0.5 }}
                     >
                        <Checkbox
                           size="small"
                           checked={
                              bindableCount > 0 && included === bindableCount
                           }
                           indeterminate={
                              included > 0 && included < bindableCount
                           }
                           disabled={bindableCount === 0}
                           onChange={(event) => setAll(event.target.checked)}
                           inputProps={{ "aria-label": "All tiles" }}
                        />
                        <Typography
                           variant="body2"
                           sx={{ flex: 1, color: "text.secondary" }}
                        >
                           All tiles
                        </Typography>
                        <Button
                           size="small"
                           onClick={() => {
                              // Opening per-tile starts every row from the
                              // common field; closing it goes back to one box.
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
                           if (!isQueryTile(tile)) return null;
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
                                    alignItems: perTile
                                       ? "flex-start"
                                       : "center",
                                    opacity: bindable[index] ? 1 : 0.6,
                                 }}
                              >
                                 <Checkbox
                                    size="small"
                                    disabled={!bindable[index]}
                                    checked={
                                       (row?.include ?? false) &&
                                       bindable[index]
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
                                    <Typography
                                       variant="caption"
                                       sx={{
                                          color: theme.tileTitle,
                                          pr: 1,
                                          maxWidth: 260,
                                          textAlign: "right",
                                       }}
                                    >
                                       {
                                          unbindable(document.tiles[index])
                                             .reason
                                       }
                                    </Typography>
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
                                             sx={{ width: 220, pr: 1 }}
                                          />
                                       </>
                                    )
                                 )}
                              </Stack>
                           );
                        })}
                     </Stack>
                  </Box>
               </Collapse>
            </Box>
            {included === 0 && (
               <Typography
                  variant="caption"
                  sx={{ color: "warning.main", display: "block", mt: -1.5 }}
               >
                  No tile is ticked, so this control will not appear on the
                  report.
               </Typography>
            )}

            {/* What a reader will see: the filter's button as it will sit on
                the page, before it does. */}
            {!fromModel && (
               <Box>
                  <SectionLabel>Preview</SectionLabel>
                  <Box
                     sx={(mui) => ({
                        display: "flex",
                        alignItems: "center",
                        gap: 1,
                        p: 1.5,
                        borderRadius: ui.radius.tile,
                        bgcolor: mui.palette.action.hover,
                     })}
                  >
                     <Box
                        aria-hidden
                        sx={(mui) => ({
                           display: "inline-flex",
                           alignItems: "center",
                           gap: 0.75,
                           height: 30,
                           px: 1.25,
                           borderRadius: ui.radius.control,
                           border: `1px solid ${edge}`,
                           bgcolor: mui.palette.background.paper,
                           ...ui.type.control,
                        })}
                     >
                        <Box component="span" sx={{ color: "text.secondary" }}>
                           {previewLabel}
                        </Box>
                        <Box
                           component="span"
                           sx={{ fontWeight: ui.type.label.fontWeight }}
                        >
                           {previewValue}
                        </Box>
                        <ExpandMoreIcon
                           sx={{ fontSize: 16, color: "text.secondary" }}
                        />
                     </Box>
                  </Box>
               </Box>
            )}
         </Stack>
      </AppDialog>
   );
}

/** A small heading over a part of the window. */
function SectionLabel({ children }: { children: ReactNode }) {
   const ui = usePublisherTheme().theme.ui;
   return (
      <Typography
         component="div"
         sx={{
            ...ui.type.label,
            color: "text.secondary",
            mb: 0.75,
         }}
      >
         {children}
      </Typography>
   );
}

/** The words on a control kind's card: short, since five sit in a row. */
const SHORT_KIND: Record<ControlKind, string> = {
   select: "Pick one",
   multiselect: "Pick several",
   text: "Text",
   number: "Range",
   date: "Date",
};

/** A control kind's picture, so the five read at a glance. */
function KindGlyph({ kind }: { kind: ControlKind }) {
   const sx = { fontSize: 22 };
   switch (kind) {
      case "select":
         return <ArrowDropDownCircleOutlinedIcon sx={sx} />;
      case "multiselect":
         return <ChecklistRtlIcon sx={sx} />;
      case "text":
         return <ShortTextIcon sx={sx} />;
      case "number":
         return <LinearScaleIcon sx={sx} />;
      case "date":
         return <EventOutlinedIcon sx={sx} />;
   }
}

/** A field's type, as a small badge: Abc, #, a date. */
function TypeBadge({ type }: { type: string | undefined }) {
   const scalar = type?.replace(/_type$/, "");
   const text =
      scalar === "string"
         ? "Abc"
         : scalar === "number"
           ? "#"
           : scalar === "date" || scalar === "timestamp"
             ? "Date"
             : scalar === "boolean"
               ? "T/F"
               : undefined;
   const ui = usePublisherTheme().theme.ui;
   if (!text) return null;
   return (
      <Box
         component="span"
         aria-hidden
         sx={(mui) => ({
            fontFamily: ui.font.mono,
            fontSize: 10,
            fontWeight: ui.type.label.fontWeight,
            color: mui.palette.text.secondary,
            bgcolor: mui.palette.action.hover,
            borderRadius: "4px",
            px: 0.5,
            py: 0.25,
            minWidth: 22,
            textAlign: "center",
            flex: "none",
         })}
      >
         {text}
      </Box>
   );
}
