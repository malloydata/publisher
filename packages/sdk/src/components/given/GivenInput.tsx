import ClearIcon from "@mui/icons-material/Clear";
import {
   Autocomplete,
   Box,
   Checkbox,
   CircularProgress,
   FormControl,
   FormControlLabel,
   FormHelperText,
   IconButton,
   InputAdornment,
   Slider,
   Stack,
   TextField,
   Typography,
} from "@mui/material";
import { AdapterDayjs } from "@mui/x-date-pickers/AdapterDayjs";
import { DatePicker } from "@mui/x-date-pickers/DatePicker";
import { LocalizationProvider } from "@mui/x-date-pickers/LocalizationProvider";
import dayjs from "dayjs";
import utc from "dayjs/plugin/utc";
import { Given } from "../../client";
import { GivenValue } from "../../hooks/givenValue";
import {
   decodeAtLeast,
   decodeFilterList,
   encodeAtLeast,
   encodeFilterList,
   filterInnerType,
   isFilterType,
} from "./filterValue";
import { renderGivenDefault } from "./utils";

dayjs.extend(utc);

/**
 * Metrics of MUI's small outlined input, which the box-less controls match so a
 * row of mixed widgets lines up: the horizontal inset of an outlined field's
 * content and helper text, and the height of the field itself.
 */
const INPUT_CONTENT_INSET = 14;
const SLIDER_FIELD_HEIGHT = 42;

export interface GivenInputProps {
   /**
    * The declaration, which carries its own presentation (`label`, `control`,
    * `rangeMin`, `rangeMax`) from the control tags on it
    * (`# label="Brand" control=select range_min=0`).
    *
    * Presentation never changes what the given means, just which widget stands
    * in for typing the value by hand. An untagged given falls back to the
    * widget its type implies, which is what notebooks have always rendered.
    */
   given: Given;
   value: GivenValue | undefined;
   onChange: (next: GivenValue) => void;
   /**
    * Options for a `select`/`multiselect`, already resolved from the given's
    * `suggest` query. Resolved by the caller rather than fetched here so this
    * stays a presentational component: the suggest query is an ordinary query
    * against the host's model, which the widget has no business knowing about.
    */
   options?: string[];
   optionsLoading?: boolean;
}

/**
 * Distill a given's `#(...)` annotation list into helper text for the UI.
 * If an annotation includes `description="..."` (a Malloy convention), the
 * quoted value is surfaced verbatim. Otherwise the annotation contents
 * inside `#(...)` are joined as-is so model authors still see something
 * recognizable. Returns undefined when nothing is renderable.
 */
function annotationHelperText(given: Given): string | undefined {
   const visible = (given.annotations ?? []).filter((a) =>
      a.trim().startsWith("#("),
   );
   if (visible.length === 0) return undefined;

   const rendered: string[] = [];
   for (const raw of visible) {
      const trimmed = raw.trim();
      const descriptionMatch = trimmed.match(/description="([^"]*)"/);
      if (descriptionMatch) {
         rendered.push(descriptionMatch[1]);
         continue;
      }
      // Strip leading `#(` and trailing `)`, then push the inner content
      const inner = trimmed
         .replace(/^#\(/, "")
         .replace(/\)\s*$/, "")
         .trim();
      if (inner) rendered.push(inner);
   }
   return rendered.length > 0 ? rendered.join("\n") : undefined;
}

/**
 * Renders an input widget appropriate for the declared given type.
 * Unknown / unrecognized types fall back to a plain text input.
 *
 * Three states, distinguished so a deliberate empty/false override is not
 * confused with "use the model default":
 *   - unset (`value === undefined`) → the given is omitted from the request and
 *     the server applies the model default. Text widgets show the default as a
 *     ghost placeholder; the boolean checkbox reflects the default's value.
 *   - explicit override (any concrete value, INCLUDING `""` and `false`) → sent
 *     verbatim. A clear (×) affordance appears whenever a value is overridden —
 *     including an empty string — so typing the field empty (a deliberate `""`)
 *     is distinguishable from unset by the × being present.
 *   - revert → the × affordance calls `onChange(null)`, which drops the override
 *     (useGivensState deletes the key) and returns the widget to its unset state.
 *
 * A given's model default (if any) is also surfaced as an always-visible
 * `Default: …` helper line on every widget — including the boolean checkbox,
 * which gets a wrapping FormControl for the slot.
 */
export function GivenInput({
   given,
   value,
   onChange,
   options,
   optionsLoading,
}: GivenInputProps) {
   const label = given.label ?? given.name ?? "";
   const type = given.type ?? "string";
   const helperText = annotationHelperText(given);
   const defaultDisplay = renderGivenDefault(type, given.default);
   // Always-visible default caption. Test `=== undefined`, not truthiness: an
   // explicit empty-string default (`is ''`) renders as "" and must still show
   // (as `(empty)`), not be mistaken for "no default".
   const defaultLine =
      defaultDisplay !== undefined
         ? `Default: ${defaultDisplay === "" ? "(empty)" : defaultDisplay}`
         : undefined;
   // Render annotation and default on separate lines via an explicit <br/>
   // rather than a \n + `white-space: pre-line`: the latter doesn't reach the
   // TextField nested inside MUI's DatePicker, so the date helper ran together.
   // A ReactNode helperText works uniformly across every widget.
   const helperNode =
      helperText || defaultLine ? (
         <>
            {helperText}
            {helperText && defaultLine ? <br /> : null}
            {defaultLine}
         </>
      ) : undefined;

   // A picker, when the declaration asked for one. Placed ahead of the
   // type branches because `control=` is an explicit instruction and the type
   // is only an inference.
   if (given.control === "select" || given.control === "multiselect") {
      const multiple = given.control === "multiselect";
      const filtered = isFilterType(type);
      // A filter carries its selection as one string ("Nike, Levi's"); a plain
      // array-typed given carries a real array.
      const selected: string[] = filtered
         ? typeof value === "string"
            ? decodeFilterList(value)
            : []
         : Array.isArray(value)
           ? value.map(String)
           : typeof value === "string" && value !== ""
             ? [value]
             : [];

      const commit = (next: string[]) => {
         if (next.length === 0) {
            // No selection is "All", which for a filter is the empty filter and
            // for anything else is simply no override.
            onChange(filtered ? "" : null);
            return;
         }
         if (filtered) {
            onChange(encodeFilterList(next));
         } else if (multiple) {
            onChange(next);
         } else {
            onChange(next[0]);
         }
      };

      return (
         <Autocomplete
            multiple={multiple}
            // Picking one value out of several should not put the list away;
            // MUI closes on select by default, which makes "and also Aurora"
            // cost a second click to reopen.
            disableCloseOnSelect={multiple}
            // Free text keeps the control from being a cage: the suggest query
            // returns the common values, not necessarily every legal one.
            freeSolo
            options={options ?? []}
            loading={optionsLoading}
            value={multiple ? selected : (selected[0] ?? null)}
            onChange={(_event, next) =>
               commit(
                  next === null
                     ? []
                     : Array.isArray(next)
                       ? (next as string[])
                       : [next as string],
               )
            }
            renderInput={(params) => (
               <TextField
                  {...params}
                  label={label}
                  size="small"
                  placeholder={selected.length === 0 ? "All" : undefined}
                  helperText={helperNode}
                  slotProps={{
                     input: {
                        ...params.InputProps,
                        endAdornment: (
                           <>
                              {optionsLoading && <CircularProgress size={16} />}
                              {params.InputProps.endAdornment}
                           </>
                        ),
                     },
                  }}
               />
            )}
            fullWidth
         />
      );
   }

   // A slider, when the declaration bounded the range. Only a lower bound is
   // expressible as a filter here (see `encodeAtLeast`), so this reads as
   // "at least", which is what a threshold control is for.
   const numericFilter =
      isFilterType(type) && filterInnerType(type) === "number";
   const { rangeMin, rangeMax } = given;
   if (
      (numericFilter || type === "number") &&
      rangeMin !== undefined &&
      rangeMax !== undefined
   ) {
      const current = numericFilter
         ? typeof value === "string"
            ? decodeAtLeast(value)
            : undefined
         : typeof value === "number"
           ? value
           : undefined;
      // An unset control rests at the low end, which is the no-op threshold.
      const position = current ?? rangeMin;
      const isOverridden = current !== undefined;
      return (
         <FormControl fullWidth>
            {/* A slider is the one control with no box around it, so it has to
                borrow the outlined inputs' metrics to sit in a row with them:
                their content is inset 14px and their field is 42px tall, and
                a control that matches both puts its helper text on the same
                line as its neighbours'. Left to itself the label started at
                the column edge and the helper sat 10px lower than the rest,
                which is most of what made a mixed control row look crooked. */}
            <Box
               sx={{
                  height: SLIDER_FIELD_HEIGHT,
                  px: `${INPUT_CONTENT_INSET}px`,
                  display: "flex",
                  flexDirection: "column",
                  justifyContent: "center",
               }}
            >
               <Stack direction="row" alignItems="center" spacing={1}>
                  <Typography variant="body2" color="text.secondary" noWrap>
                     {label}
                  </Typography>
                  <Typography variant="body2" sx={{ fontWeight: 600 }}>
                     {isOverridden ? `≥ ${position}` : "Any"}
                  </Typography>
                  {isOverridden && (
                     <IconButton
                        size="small"
                        aria-label="clear value"
                        onClick={() => onChange(numericFilter ? "" : null)}
                        sx={{ p: 0 }}
                     >
                        <ClearIcon fontSize="small" />
                     </IconButton>
                  )}
               </Stack>
               <Slider
                  size="small"
                  min={rangeMin}
                  max={rangeMax}
                  value={position}
                  valueLabelDisplay="auto"
                  aria-label={label}
                  // MUI pads a slider vertically for a touch target, which is
                  // 26px this control cannot spare; the row above already
                  // gives the thumb somewhere to be.
                  sx={{ py: 0, mt: 0.5 }}
                  onChange={(_event, next) => {
                     const picked = Array.isArray(next) ? next[0] : next;
                     // Back at the floor is no threshold at all, not `>= min`,
                     // so dragging left the whole way clears rather than
                     // leaving a filter that reads as a constraint.
                     if (picked <= rangeMin) {
                        onChange(numericFilter ? "" : null);
                        return;
                     }
                     onChange(numericFilter ? encodeAtLeast(picked) : picked);
                  }}
               />
            </Box>
            {helperNode && (
               <FormHelperText sx={{ mx: `${INPUT_CONTENT_INSET}px` }}>
                  {helperNode}
               </FormHelperText>
            )}
         </FormControl>
      );
   }

   // A date-typed filter gets the same picker a plain date given gets. The
   // value is filter syntax, but a bare ISO date is valid filter syntax for
   // "on that day", so the round trip is direct.
   const dateFilterInner = isFilterType(type)
      ? filterInnerType(type)
      : undefined;
   if (
      dateFilterInner === "date" ||
      dateFilterInner === "timestamp" ||
      dateFilterInner === "timestamptz"
   ) {
      const parsed =
         typeof value === "string" && value !== "" ? dayjs.utc(value) : null;
      return (
         <LocalizationProvider dateAdapter={AdapterDayjs}>
            <DatePicker
               label={label}
               value={parsed?.isValid() ? parsed : null}
               onChange={(next) =>
                  onChange(next ? next.format("YYYY-MM-DD") : "")
               }
               slotProps={{
                  textField: {
                     fullWidth: true,
                     size: "small",
                     helperText: helperNode,
                  },
                  field: { clearable: true, onClear: () => onChange("") },
               }}
            />
         </LocalizationProvider>
      );
   }

   if (type === "boolean") {
      // Three states for a boolean. When unset, reflect the model DEFAULT so the
      // box shows what the query will actually run with (not a misleading
      // unchecked). A toggle is an explicit true/false override; the revert (×)
      // — shown only when overridden — drops the override back to the default.
      const isOverridden = typeof value === "boolean";
      const defaultChecked = given.default?.trim() === "true";
      const checked = isOverridden ? value : defaultChecked;
      return (
         <FormControl>
            <Stack direction="row" alignItems="center">
               <FormControlLabel
                  control={
                     <Checkbox
                        checked={checked}
                        onChange={(e) => onChange(e.target.checked)}
                     />
                  }
                  label={label}
               />
               {isOverridden && (
                  <IconButton
                     size="small"
                     aria-label="clear value"
                     onClick={() => onChange(null)}
                     edge="end"
                  >
                     <ClearIcon fontSize="small" />
                  </IconButton>
               )}
            </Stack>
            {helperNode && <FormHelperText>{helperNode}</FormHelperText>}
         </FormControl>
      );
   }

   if (type === "number") {
      const num = typeof value === "number" ? value : "";
      return (
         <TextField
            label={label}
            type="number"
            value={num}
            onChange={(e) => {
               const v = e.target.value;
               onChange(v === "" ? null : Number(v));
            }}
            placeholder={defaultDisplay}
            helperText={helperNode}
            slotProps={{
               input: {
                  endAdornment: num !== "" && (
                     <ClearAdornment onClear={() => onChange(null)} />
                  ),
               },
            }}
            fullWidth
            size="small"
         />
      );
   }

   if (type === "date" || type === "timestamp" || type === "timestamptz") {
      const dateValue = value instanceof Date ? dayjs.utc(value) : null;
      // The date picker shows a format mask, not a placeholder, so the default
      // rides on the shared helper line.
      return (
         <LocalizationProvider dateAdapter={AdapterDayjs}>
            <DatePicker
               label={label}
               value={dateValue}
               onChange={(next) => onChange(next ? next.toDate() : null)}
               slotProps={{
                  textField: {
                     fullWidth: true,
                     size: "small",
                     helperText: helperNode,
                  },
                  field: { clearable: true, onClear: () => onChange(null) },
               }}
            />
         </LocalizationProvider>
      );
   }

   if (type.startsWith("array<")) {
      const list = Array.isArray(value) ? value.map(String) : [];
      return (
         <Autocomplete
            multiple
            freeSolo
            options={[]}
            value={list}
            onChange={(_event, next) =>
               onChange(next.length === 0 ? null : (next as string[]))
            }
            renderInput={(params) => (
               <TextField
                  {...params}
                  label={label}
                  size="small"
                  placeholder={list.length === 0 ? defaultDisplay : undefined}
                  helperText={helperNode}
               />
            )}
            fullWidth
         />
      );
   }

   // Default: string, filter<...>, or unknown types — plain text input.
   // An empty field is a deliberate `""` override, NOT a revert: typing the
   // field empty sends "" (so gates like `$region != ''` are expressible). Only
   // the × reverts to the model default. The default ghost shows just for the
   // unset state, so an empty override (× present, no ghost) reads differently.
   const str = typeof value === "string" ? value : "";
   const isOverridden = value !== undefined && value !== null;
   return (
      <TextField
         label={label}
         value={str}
         onChange={(e) => onChange(e.target.value)}
         placeholder={
            isOverridden
               ? undefined
               : (defaultDisplay ??
                 (type.startsWith("filter<") ? type : undefined))
         }
         helperText={helperNode}
         slotProps={{
            input: {
               endAdornment: isOverridden && (
                  <ClearAdornment onClear={() => onChange(null)} />
               ),
            },
         }}
         fullWidth
         size="small"
      />
   );
}

function ClearAdornment({ onClear }: { onClear: () => void }) {
   return (
      <InputAdornment position="end">
         <IconButton
            size="small"
            aria-label="clear value"
            onClick={onClear}
            edge="end"
         >
            <ClearIcon fontSize="small" />
         </IconButton>
      </InputAdornment>
   );
}
