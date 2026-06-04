import ClearIcon from "@mui/icons-material/Clear";
import {
   Autocomplete,
   Checkbox,
   FormControl,
   FormControlLabel,
   FormHelperText,
   IconButton,
   InputAdornment,
   TextField,
} from "@mui/material";
import { AdapterDayjs } from "@mui/x-date-pickers/AdapterDayjs";
import { DatePicker } from "@mui/x-date-pickers/DatePicker";
import { LocalizationProvider } from "@mui/x-date-pickers/LocalizationProvider";
import dayjs from "dayjs";
import utc from "dayjs/plugin/utc";
import { Given } from "../../client";
import { GivenValue } from "../../hooks/useGivensForm";
import { renderGivenDefault } from "./utils";

dayjs.extend(utc);

export interface GivenInputProps {
   given: Given;
   value: GivenValue | undefined;
   onChange: (next: GivenValue) => void;
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
 * For text-based inputs (string, number, filter, default), a clear (×)
 * adornment appears when the field has a value. DatePicker, Checkbox, and
 * multi-Autocomplete have their own native clear affordances.
 *
 * A given's model default (if any) is surfaced as an always-visible
 * `Default: …` helper line on every widget — including the boolean checkbox,
 * which gets a wrapping FormControl for the slot — plus a ghost placeholder on
 * the text widgets (a bonus MUI only reveals on focus, since the floating label
 * sits in the placeholder's spot at rest). The value itself stays empty, so
 * leaving the field blank still means "use the model default".
 */
export function GivenInput({ given, value, onChange }: GivenInputProps) {
   const label = given.name ?? "";
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

   if (type === "boolean") {
      const checked = value === true;
      // A checkbox has no helperText slot of its own and no "unset" visual, so
      // wrap it in a FormControl to carry the annotation + `Default: …` line.
      // This matters most for a `boolean is true` given: the box reads unchecked
      // when untouched, but the query runs with the default, so the caption is
      // what tells the user that. (The deeper "no unset state" checkbox quirk is
      // a pre-existing givens limitation, not specific to defaults.)
      return (
         <FormControl>
            <FormControlLabel
               control={
                  <Checkbox
                     checked={checked}
                     onChange={(e) => onChange(e.target.checked)}
                  />
               }
               label={label}
            />
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

   // Default: string, filter<...>, or unknown types — plain text input
   const str = typeof value === "string" ? value : "";
   return (
      <TextField
         label={label}
         value={str}
         onChange={(e) => {
            const v = e.target.value;
            onChange(v === "" ? null : v);
         }}
         placeholder={
            defaultDisplay ?? (type.startsWith("filter<") ? type : undefined)
         }
         helperText={helperNode}
         slotProps={{
            input: {
               endAdornment: str !== "" && (
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
