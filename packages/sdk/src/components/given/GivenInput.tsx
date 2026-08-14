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
import { paramToGiven, pickedDayToUtc } from "./paramCodec";
import { GivenValue } from "../../hooks/givenValue";
import {
   decodeAtLeast,
   decodeFilterList,
   encodeAtLeast,
   encodeFilterList,
   filterInnerType,
   isFilterType,
   isPlainFilterList,
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
   /**
    * The `suggest` query for this given failed. Distinguished from an empty
    * `options` because a dropdown looks identical either way, and the two mean
    * opposite things: no values in the data, versus no answer from the server.
    * Free text still works, so the control degrades rather than locking up.
    */
   optionsFailed?: boolean;
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
   optionsFailed,
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
   //
   // But this picker only speaks two languages: plain strings, and the STRING
   // filter grammar. It commits through `encodeFilterList`, which is
   // `StringFilterExpression.unparse`. So `control=select` on anything else is
   // refused rather than honoured, because honouring it corrupts the value:
   // a `filter<date>` or `filter<number>` would receive string-grammar escaping
   // that the temporal and number parsers reject outright, and a `number`,
   // `date` or `boolean` given would receive a string that `givensToRequest`
   // forwards verbatim for Malloy to refuse. Falling through gives the given the
   // widget its type implies, which is the behaviour before `control=` existed.
   // Nothing populates `control` today, so this closes the hole before the slice
   // that opens it rather than after.
   const pickableType = type === "string" || filterInnerType(type) === "string";
   const filterIsPickable =
      !isFilterType(type) ||
      typeof value !== "string" ||
      isPlainFilterList(value);
   // And multi-pick only for a `filter<…>`, which is the one type that can carry
   // several values in one string. A plain `string` given would receive a real
   // array, and the URL codec cannot round-trip one: `givenToParam` joins on
   // `,` while reading it back yields the joined string, not the list. So a
   // `multiselect` on a plain string given renders as a single-pick instead of
   // emitting something that cannot survive the address bar.
   /**
    * A value that is in force but that this control cannot render.
    *
    * Every typed branch has the same hazard: the control substitutes a blank
    * for a value of the wrong shape, so the value goes on filtering every cell
    * while the box looks empty. Shown here rather than per branch, because
    * handling it three different ways is how the date branch ended up with a
    * revert and the number branch without one.
    *
    * Read-only on purpose: an editable version swapped itself for the real
    * control the moment the typed text first parsed, taking the cursor with it.
    */
   const unrepresentable = (what: string) => (
      <TextField
         label={label}
         value={String(value)}
         error
         inputProps={{ readOnly: true }}
         helperText={
            <>
               {`Not a ${what}. Clear it to pick one.`}
               {helperNode ? <br /> : null}
               {helperNode}
            </>
         }
         slotProps={{
            input: {
               endAdornment: <ClearAdornment onClear={() => onChange(null)} />,
            },
         }}
         fullWidth
         size="small"
      />
   );

   const multiplePickable = isFilterType(type);
   const multiple = given.control === "multiselect" && multiplePickable;
   const filtered = isFilterType(type);
   const pickerControl =
      given.control === "select" || given.control === "multiselect";
   // A filter carries its selection as one string ("Nike, Levi's"); a plain
   // array-typed given carries a real array.
   //
   // Guarded by `pickerControl` because it is read only by the picker below and
   // by the condition guarding it. Computing it for every given would decode a
   // filter on every render of a control that never shows one.
   const selected: string[] = !pickerControl
      ? []
      : filtered
        ? typeof value === "string"
           ? decodeFilterList(value)
           : []
        : typeof value === "string" && value !== ""
          ? [value]
          : [];

   if (
      pickerControl &&
      pickableType &&
      // A picker that cannot represent the current filter would silently
      // rewrite it on the next edit: `-Nike` decodes to one opaque chip and
      // re-encodes as a literal, inverting what the filter means. Fall through
      // to the text box instead, where the author's filter stays visible and
      // editable as written.
      filterIsPickable &&
      // Same reasoning, different cause: a single-pick control has room for one
      // value and the model's filter holds several. It rendered the first and
      // dropped the rest as soon as the reader picked anything.
      !(!multiple && selected.length > 1)
   ) {
      const commit = (next: string[]) => {
         // ASK the encoder what survived; do not predict it from `next.length`.
         // `encodeFilterList` drops any value with no non-whitespace character,
         // so a single blank-looking pick (a run of spaces, a tab, an NBSP, all
         // ordinary in scraped data and passed through untouched by
         // `readOptionValues`) has length 1, skipped the revert arm below, and
         // encoded to `""`: the EMPTY filter, sent as an explicit "match
         // everything" override while the control redrew showing the model
         // default as a ghost. This is the identical drift that made a blank
         // cell drill to every row, at a second call site: `drillValueToFilter`
         // was fixed to ask the encoder and this one still predicted it.
         const encoded = filtered ? encodeFilterList(next) : undefined;
         if (next.length === 0 || encoded === "") {
            // `null` for every type, which is what the × means everywhere else
            // in this file: drop the override and let the model's own value
            // stand. This arm used to send `""` for a filter, the empty filter,
            // meaning "match everything" as an explicit override. Same button,
            // same given, opposite result set from the text box's ×, and
            // nothing on screen said which one you were about to get.
            //
            // FORECLOSED by choosing revert: a reader cannot override a
            // DECLARED default with "match everything", because the only
            // gesture for it now means revert. Unreachable today (nothing
            // populates `control`, so no picker renders), and the honest fix is
            // a separate affordance rather than two meanings for one ×. Worth
            // settling before `control=` is wired up server-side.
            onChange(null);
            return;
         }
         // One value for a non-filter given, never the array. `GivenValue`
         // admits no array at all now, and the URL codec could not round-trip
         // one anyway: `givenToParam` refuses it rather than joining on `,`,
         // because reading it back yields the joined string, not the list.
         onChange(filtered ? (encoded as string) : next[0]);
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
            // Keeps text the reader typed but did not pick from the list. A
            // `freeSolo` Autocomplete without this discards it on blur, so a
            // value that is legal but absent from `suggest` could be typed and
            // then silently vanish.
            autoSelect
            options={options ?? []}
            loading={optionsLoading}
            noOptionsText={
               optionsFailed ? "Options unavailable" : "No matching values"
            }
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
                  // The model's own default when it has one, as a ghost, the
                  // way every other unset control shows it. A hardcoded "All"
                  // told the reader nothing would be filtered while the query
                  // was about to run on the declared default.
                  placeholder={
                     selected.length === 0
                        ? (defaultDisplay ?? "All")
                        : undefined
                  }
                  error={optionsFailed}
                  helperText={
                     optionsFailed ? (
                        <>
                           Could not load the options for this control. Type a
                           value to filter anyway.
                           {helperNode ? <br /> : null}
                           {helperNode}
                        </>
                     ) : (
                        helperNode
                     )
                  }
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
   // A slider can only represent `>= N`, so a `filter<number>` carrying anything
   // else (a range, an upper bound, a negation) falls through to the text box.
   // Otherwise `decodeAtLeast` returns undefined, the slider claims "Any" while a
   // filter is in force, and the first drag silently replaces the author's filter
   // with a threshold. Same principle as the picker and the date guards above.
   const sliderIsRepresentable =
      !numericFilter ||
      value === undefined ||
      value === null ||
      value === "" ||
      (typeof value === "string" && decodeAtLeast(value) !== undefined);
   if (
      (numericFilter || type === "number") &&
      rangeMin !== undefined &&
      rangeMax !== undefined &&
      sliderIsRepresentable
   ) {
      const current = numericFilter
         ? typeof value === "string"
            ? decodeAtLeast(value)
            : undefined
         : typeof value === "number"
           ? value
           : undefined;
      // Same hazard as the number and date branches: a value that cannot be
      // placed on the track would otherwise read as "Any" while still being
      // sent. Latent today, because nothing populates `rangeMin`/`rangeMax` yet,
      // which is exactly why it is worth closing before anything does.
      if (
         current === undefined &&
         value !== undefined &&
         value !== null &&
         value !== ""
      ) {
         return unrepresentable("number");
      }
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
                        // `null`, like every other × in this file: revert the
                        // override. It used to send `""` for a filter, which
                        // `decodeAtLeast` reads back as undefined, so the
                        // control redrew as "Any" and hid this button while the
                        // given was still explicitly set to the empty filter.
                        // The reader could neither see the override nor undo it.
                        onClick={() => onChange(null)}
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
                     // Back at the floor is no threshold at all for a FILTER,
                     // not `>= min`, so dragging left the whole way clears
                     // rather than leaving a filter that reads as a constraint.
                     //
                     // A plain `number` given is not a threshold, so its floor
                     // is an ordinary value and clearing there made the minimum
                     // the one number on the scale the reader could not pick.
                     if (numericFilter && picked <= rangeMin) {
                        onChange(null);
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
   const isDateFilter =
      dateFilterInner === "date" ||
      dateFilterInner === "timestamp" ||
      dateFilterInner === "timestamptz";
   // Only a bare `YYYY-MM-DD` is the picker's own spelling; anything else is a
   // filter expression the picker cannot represent. Unset and the empty filter
   // are both fine, since the picker simply shows nothing.
   const bareDate =
      typeof value === "string" && /^\d{4}-\d{2}-\d{2}$/.test(value);
   const dateIsPickable =
      value === undefined || value === null || value === "" || bareDate;
   // An unrepresentable date filter falls through to the text box below, for the
   // same reason a non-list string filter does: a range like
   // `2024-01-01 to 2024-02-01` handed whole to `dayjs.utc` parses as its
   // leading date, so the picker would claim a single wrong day while a range
   // was in force. The text box shows the filter as written, and editable.
   if (isDateFilter && dateIsPickable) {
      const parsed = bareDate ? dayjs.utc(value as string) : null;
      return (
         <LocalizationProvider dateAdapter={AdapterDayjs}>
            <DatePicker
               label={label}
               value={parsed?.isValid() ? parsed : null}
               // Cleared means UNSET, `null`, not an empty filter expression.
               // `""` is a value for a filter given (the empty string is
               // meaningful there), so it reached the wire as a filter of no
               // characters instead of dropping the override.
               onChange={(next) =>
                  onChange(next ? next.format("YYYY-MM-DD") : null)
               }
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

   if (type === "boolean") {
      // Three states for a boolean. When unset, reflect the model DEFAULT so the
      // box shows what the query will actually run with (not a misleading
      // unchecked). A toggle is an explicit true/false override; the revert (×)
      // — shown only when overridden — drops the override back to the default.
      const isOverridden = typeof value === "boolean";
      // A link carrying `?FLAG=yes` reaches here as a string, which `paramToGiven`
      // passes through on purpose. Without this the checkbox rendered the model
      // DEFAULT for it, so the box said one thing while `givensToRequest` sent
      // "yes" and every cell failed, with nothing on screen to explain why.
      if (
         !isOverridden &&
         value !== undefined &&
         value !== null &&
         value !== ""
      ) {
         return unrepresentable("true or false");
      }
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
      // Track whether a value is OVERRIDDEN, the way the string branch below
      // does, rather than whether the box happens to render non-empty. A
      // non-number renders blank (see `num` above), and keying the revert off
      // that blank left the user no way to clear it.
      const isOverridden = value !== undefined && value !== null;
      // `""` is an empty box, not an unreadable value. `paramToGiven` now reads
      // `?N=` as UNSET so it no longer arrives here at all, but a host passing
      // values in directly still can, and calling that "Not a number" would put
      // a red error over a field showing nothing.
      if (isOverridden && value !== "" && typeof value !== "number") {
         return unrepresentable("number");
      }
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

   if (type === "date" || type === "timestamp" || type === "timestamptz") {
      // A string is read as a date first. `# drill { to=self }` onto a date
      // given hands over a bare `YYYY-MM-DD` string, not a Date, so treating
      // any non-Date as unreadable painted an error over a value the product
      // itself had just produced.
      const asDate =
         value instanceof Date
            ? value
            : typeof value === "string"
              ? ((parsed) => (parsed instanceof Date ? parsed : null))(
                   paramToGiven(type, value),
                )
              : null;
      const dateValue = asDate ? dayjs.utc(asDate) : null;
      // Same lesson as `number` above, and it bit here too. A value the codec
      // cannot read (a shared link carrying `?ORDER_DATE=last month`) is still
      // filtering every cell, but a DatePicker can only render a date, so it
      // showed blank, and the picker's own `clearable` appears only when it
      // holds one: the state was invisible AND had no revert.
      //
      // `""` is an empty control, not an unreadable value; see the number
      // branch above for the same distinction.
      const unreadable =
         value !== undefined &&
         value !== null &&
         value !== "" &&
         asDate === null;

      // Its own field rather than a badged DatePicker, which was tried twice.
      // Putting the revert in `slotProps.textField.InputProps` REPLACES the
      // picker's own "Choose date" button, because MUI merges that object
      // shallowly and ours wins outright. And passing `error` through the same
      // slot pins it: MUI's `useField` returns any defined `error` ahead of its
      // own validation, so `error: false` suppressed the picker's real errors.
      if (unreadable) return unrepresentable("date");

      // The date picker shows a format mask, not a placeholder, so the default
      // rides on the shared helper line.
      return (
         <LocalizationProvider dateAdapter={AdapterDayjs}>
            <DatePicker
               label={label}
               value={dateValue}
               // The clicked day rebuilt in UTC, never `next.toDate()`, which
               // is local-mode and lands a day out. See `pickedDayToUtc`.
               onChange={(next) =>
                  onChange(
                     next ? pickedDayToUtc(next, asDate ?? undefined) : null,
                  )
               }
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

   // No `array<…>` branch on purpose. `malloyGivenToApi` renders a non-filter
   // given as the bare `type.type`, so the widest thing the server could ever
   // send is `"array"`; `"array<…>"` is a spelling nothing produces. Malloy's
   // grammar emits only scalar parameter types for givens today anyway
   // (`service/given.ts`), so the case is doubly unreachable.

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
