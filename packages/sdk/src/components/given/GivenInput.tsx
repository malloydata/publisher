import ClearIcon from "@mui/icons-material/Clear";
import {
   Autocomplete,
   Checkbox,
   FormControlLabel,
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
 * A given's model default (if any) is surfaced as a ghost placeholder on the
 * text-based widgets and as a `Default: …` helper line on the date picker
 * (which has no usable placeholder). The value itself stays empty, so leaving
 * the field blank still means "use the model default". Boolean givens render
 * as a checkbox with no helper slot, so their default isn't surfaced.
 */
export function GivenInput({ given, value, onChange }: GivenInputProps) {
   const label = given.name ?? "";
   const type = given.type ?? "string";
   const helperText = annotationHelperText(given);
   const defaultDisplay = renderGivenDefault(type, given.default);

   if (type === "boolean") {
      const checked = value === true;
      // Checkbox wrapped in FormControlLabel — no helperText slot available.
      return (
         <FormControlLabel
            control={
               <Checkbox
                  checked={checked}
                  onChange={(e) => onChange(e.target.checked)}
               />
            }
            label={label}
         />
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
            helperText={helperText}
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
      // The date picker has no usable placeholder (it shows a format mask), so
      // surface the default as a helper line instead of a ghost value.
      const dateHelper = defaultDisplay
         ? [helperText, `Default: ${defaultDisplay}`].filter(Boolean).join("\n")
         : helperText;
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
                     helperText: dateHelper,
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
                  helperText={helperText}
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
         helperText={helperText}
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
