import Autocomplete from "@mui/material/Autocomplete";
import Box from "@mui/material/Box";
import CircularProgress from "@mui/material/CircularProgress";
import FormControl from "@mui/material/FormControl";
import InputLabel from "@mui/material/InputLabel";
import MenuItem from "@mui/material/MenuItem";
import Select, { SelectChangeEvent } from "@mui/material/Select";
import TextField from "@mui/material/TextField";
import Typography from "@mui/material/Typography";
import { AdapterDayjs } from "@mui/x-date-pickers/AdapterDayjs";
import { DatePicker } from "@mui/x-date-pickers/DatePicker";
import { LocalizationProvider } from "@mui/x-date-pickers/LocalizationProvider";
import dayjs, { Dayjs } from "dayjs";
import utc from "dayjs/plugin/utc";
import { useEffect, useRef, useState } from "react";
import {
   DimensionSpec,
   DimensionValue,
} from "../../hooks/useDimensionalFilterRangeData";
import {
   FilterSelection,
   FilterValue,
   FilterValuePrimitive,
   MatchType,
} from "../../hooks/useDimensionFilters";

dayjs.extend(utc);

/**
 * Type for the retrieval function used by Retrieval filter type
 */
export type RetrievalFunction = (
   query: string,
   spec: DimensionSpec,
) => Promise<DimensionValue[]>;

/**
 * Props for DimensionFilter component
 */
export interface DimensionFilterProps {
   /** The dimension specification */
   spec: DimensionSpec;
   /** Available values for this dimension */
   values: DimensionValue[];
   /** Current filter selection (optional) */
   selection?: FilterSelection | null;
   /** Callback when filter changes */
   onChange: (selection: FilterSelection | null) => void;
   /** Retrieval function for semantic search (required for Retrieval filter type) */
   retrievalFn?: RetrievalFunction;
}

/**
 * Get available match types based on filter type
 */
function getAvailableMatchTypes(
   filterType: DimensionSpec["filterType"],
): MatchType[] {
   switch (filterType) {
      case "Star":
         return ["Equals", "Contains"];
      case "MinMax":
         return ["Equals", "Less Than", "Greater Than", "Between"];
      case "DateMinMax":
         return ["Equals", "Before", "After", "Between"];
      case "Retrieval":
         return ["Semantic Search"];
      case "Boolean":
         return ["Equals"];
      case "NONE":
      default:
         return [];
   }
}

/**
 * Determines if the filter type uses date values
 */
function isDateFilter(filterType: DimensionSpec["filterType"]): boolean {
   return filterType === "DateMinMax";
}

/**
 * Determines if the match type requires two values
 */
function requiresTwoValues(matchType: MatchType): boolean {
   return matchType === "Between";
}

/**
 * DimensionFilter component - Renders appropriate filter UI based on dimension type
 *
 * This component dynamically renders the appropriate filter UI based on the dimension's
 * filter type and selected match type. It supports:
 * - Star filters: Dropdown selection or text search
 * - MinMax filters: Numeric input with comparison operators
 * - DateMinMax filters: Date pickers with date comparison operators
 *
 * @param props - Component props including spec, values, selection, and onChange callback
 *
 * @example
 * ```tsx
 * <DimensionFilter
 *   spec={{ dimensionName: "category", filterType: "Star" }}
 *   values={categoryValues}
 *   selection={currentSelection}
 *   onChange={(selection) => updateFilter("category", selection)}
 * />
 * ```
 */
export function DimensionFilter({
   spec,
   values,
   selection,
   onChange,
   retrievalFn,
}: DimensionFilterProps) {
   // Default to "Between" for date filters, otherwise use first available match type
   const getDefaultMatchType = () => {
      if (selection?.matchType) return selection.matchType;
      if (spec.filterType === "DateMinMax") return "Between";
      return getAvailableMatchTypes(spec.filterType)[0] || "Equals";
   };

   const [matchType, setMatchType] = useState<MatchType>(getDefaultMatchType());
   const [value1, setValue1] = useState<FilterValue | "">(
      selection?.value ?? "",
   );
   const [value2, setValue2] = useState<FilterValuePrimitive | "">(
      selection?.value2 ?? "",
   );

   // Retrieval state
   const [retrievalOptions, setRetrievalOptions] = useState<DimensionValue[]>(
      [],
   );
   const [retrievalLoading, setRetrievalLoading] = useState(false);
   const [retrievalInputValue, setRetrievalInputValue] = useState("");
   const [retrievalSearched, setRetrievalSearched] = useState(false);
   const [retrievalFocused, setRetrievalFocused] = useState(false);
   const retrievalTimerRef = useRef<ReturnType<typeof setTimeout> | null>(null);
   const latestRequestIdRef = useRef(0);

   // MinMax focus state for showing helper text
   const [showMinMaxHelper, setShowMinMaxHelper] = useState(false);

   // Effect to trigger retrieval with debounce
   useEffect(() => {
      if (spec.filterType !== "Retrieval" || !retrievalFn) return;

      const query = retrievalInputValue.trim();
      if (query.length <= 2) {
         setRetrievalOptions([]);
         setRetrievalSearched(false);
         return;
      }

      // Cancel any existing timer
      if (retrievalTimerRef.current) {
         clearTimeout(retrievalTimerRef.current);
      }

      // Set a new 300ms timer
      retrievalTimerRef.current = setTimeout(async () => {
         // Increment and capture request ID to track this request
         latestRequestIdRef.current += 1;
         const thisRequestId = latestRequestIdRef.current;

         setRetrievalLoading(true);

         try {
            const results = await retrievalFn(query, spec);
            // Only update state if this is still the latest request
            if (thisRequestId === latestRequestIdRef.current) {
               setRetrievalOptions(results);
            }
         } catch (e) {
            // Only update state if this is still the latest request
            if (thisRequestId === latestRequestIdRef.current) {
               console.error("Retrieval failed", e);
               setRetrievalOptions([]);
            }
         } finally {
            // Only update loading/searched state if this is still the latest request
            if (thisRequestId === latestRequestIdRef.current) {
               setRetrievalLoading(false);
               setRetrievalSearched(true);
            }
         }
      }, 500);

      // Cleanup: cancel timer on unmount or when dependencies change
      return () => {
         if (retrievalTimerRef.current) {
            clearTimeout(retrievalTimerRef.current);
         }
      };
   }, [retrievalInputValue, spec, retrievalFn]);

   // Sync internal state with selection prop changes (e.g., when filter is cleared externally)
   useEffect(() => {
      if (selection === null) {
         // Clear internal state when selection is cleared externally
         setValue1("");
         setValue2("");
         // Reset to default match type (Between for dates, first available for others)
         setMatchType(
            spec.filterType === "DateMinMax"
               ? "Between"
               : getAvailableMatchTypes(spec.filterType)[0] || "Equals",
         );
      } else if (selection) {
         // Update internal state when selection changes externally
         setMatchType(selection.matchType);
         // Use nullish coalescing to preserve false/0 values
         setValue1(selection.value ?? "");
         setValue2(selection.value2 ?? "");
      }
   }, [selection, spec.filterType]);

   const availableMatchTypes = getAvailableMatchTypes(spec.filterType);
   const isDate = isDateFilter(spec.filterType);
   const needsTwoValues = requiresTwoValues(matchType);

   // Extract min/max values for range filters (use UTC to avoid timezone shifts)
   const minDate =
      isDate && values.length >= 2 && values[0].value instanceof Date
         ? dayjs.utc(values[0].value)
         : undefined;
   const maxDate =
      isDate && values.length >= 2 && values[1].value instanceof Date
         ? dayjs.utc(values[1].value)
         : undefined;

   // Extract min/max numbers for MinMax filters
   const minNumber =
      spec.filterType === "MinMax" &&
      values.length >= 2 &&
      typeof values[0].value === "number"
         ? values[0].value
         : undefined;
   const maxNumber =
      spec.filterType === "MinMax" &&
      values.length >= 2 &&
      typeof values[1].value === "number"
         ? values[1].value
         : undefined;

   // Don't render if no match types available
   if (availableMatchTypes.length === 0) {
      return null;
   }

   // Handle match type change
   const handleMatchTypeChange = (event: SelectChangeEvent<MatchType>) => {
      const newMatchType = event.target.value as MatchType;
      setMatchType(newMatchType);

      // Clear value2 if not needed
      if (!requiresTwoValues(newMatchType)) {
         setValue2("");
      }

      // Update selection
      if (value1) {
         onChange({
            dimensionName: spec.dimensionName,
            matchType: newMatchType,
            value: value1,
            ...(requiresTwoValues(newMatchType) && value2 && { value2 }),
         });
      }
   };

   // Handle value change
   const handleValueChange = (
      newValue1: FilterValue | "" | null,
      newValue2?: FilterValuePrimitive | "" | null,
   ) => {
      setValue1(newValue1 ?? "");
      if (newValue2 !== undefined) {
         setValue2(newValue2 ?? "");
      }

      // check for empty array (multi-select cleared)
      const isEmptyArray = Array.isArray(newValue1) && newValue1.length === 0;

      // Only call onChange if we have a valid value
      if (
         !isEmptyArray &&
         newValue1 !== "" &&
         newValue1 !== null &&
         newValue1 !== undefined
      ) {
         onChange({
            dimensionName: spec.dimensionName,
            matchType,
            value: newValue1,
            ...(needsTwoValues && newValue2 && { value2: newValue2 }),
         });
      } else {
         onChange(null);
      }
   };

   // Handle clear
   const handleClear = () => {
      setValue1("");
      setValue2("");
      onChange(null);
   };

   return (
      <Box sx={{ display: "flex", flexDirection: "column", gap: 2 }}>
         {/* Dimension Name */}
         <Box sx={{ fontWeight: 600, fontSize: "0.875rem" }}>
            {spec.dimensionName}
         </Box>

         {/* Match Type Selector */}
         {spec.filterType !== "Boolean" && (
            <FormControl size="small" fullWidth>
               <InputLabel>Match Type</InputLabel>
               <Select
                  value={matchType}
                  label="Match Type"
                  onChange={handleMatchTypeChange}
                  disabled={availableMatchTypes.length === 1}
               >
                  {availableMatchTypes.map((type) => (
                     <MenuItem key={type} value={type}>
                        {type}
                     </MenuItem>
                  ))}
               </Select>
            </FormControl>
         )}

         {/* Value Input - varies by filter type */}
         {spec.filterType === "Star" && matchType === "Equals" && (
            <Autocomplete
               multiple
               size="small"
               options={values}
               getOptionLabel={(option) => {
                  if (typeof option === "string") {
                     return option;
                  }
                  if (
                     typeof option === "object" &&
                     option !== null &&
                     "value" in option
                  ) {
                     return String((option as DimensionValue).value);
                  }
                  return String(option);
               }}
               value={
                  Array.isArray(value1)
                     ? value1.map(
                          (v: string) =>
                             values.find((opt) => opt.value === v) || v,
                       )
                     : value1
                       ? [values.find((v) => v.value === value1) || value1]
                       : []
               }
               onChange={(_, newValue) => {
                  const newValues = newValue.map((item) => {
                     if (typeof item === "string") return item;
                     if (item && typeof item === "object" && "value" in item) {
                        return (item as DimensionValue).value;
                     }
                     return item;
                  }) as FilterValuePrimitive[];
                  handleValueChange(newValues);
               }}
               noOptionsText="No matches found"
               renderInput={(params) => (
                  <TextField
                     {...params}
                     label="Values"
                     placeholder="Select values..."
                  />
               )}
               freeSolo={!spec.values || spec.values.length === 0}
            />
         )}

         {spec.filterType === "Star" && matchType === "Contains" && (
            <TextField
               size="small"
               label="Search Text"
               value={value1}
               onChange={(e) => handleValueChange(e.target.value)}
               placeholder="Enter text to search..."
               fullWidth
            />
         )}

         {spec.filterType === "Boolean" && (
            <FormControl size="small" fullWidth>
               <InputLabel>Value</InputLabel>
               <Select
                  value={
                     value1 === true ? "true" : value1 === false ? "false" : ""
                  }
                  label="Value"
                  onChange={(e) => {
                     const val = e.target.value;
                     if (val === "true") handleValueChange(true);
                     else if (val === "false") handleValueChange(false);
                     else handleClear();
                  }}
               >
                  <MenuItem value="">
                     <em>Blank</em>
                  </MenuItem>
                  <MenuItem value="true">True</MenuItem>
                  <MenuItem value="false">False</MenuItem>
               </Select>
            </FormControl>
         )}

         {spec.filterType === "Retrieval" &&
            matchType === "Semantic Search" && (
               <Autocomplete
                  multiple
                  size="small"
                  options={retrievalOptions}
                  loading={retrievalLoading}
                  getOptionLabel={(option) => {
                     if (typeof option === "string") {
                        return option;
                     }
                     if (
                        typeof option === "object" &&
                        option !== null &&
                        "value" in option
                     ) {
                        return String((option as DimensionValue).value);
                     }
                     return String(option);
                  }}
                  value={
                     Array.isArray(value1)
                        ? value1.map((v: string) => {
                             const found = retrievalOptions.find(
                                (opt) => opt.value === v,
                             );
                             return found || { value: v };
                          })
                        : value1
                          ? [{ value: value1 }]
                          : []
                  }
                  onInputChange={(_, newInputValue) => {
                     setRetrievalInputValue(newInputValue);
                  }}
                  onChange={(_, newValue) => {
                     const newValues = newValue.map((item) => {
                        if (typeof item === "string") return item;
                        if (
                           item &&
                           typeof item === "object" &&
                           "value" in item
                        ) {
                           return (item as DimensionValue).value;
                        }
                        return item;
                     }) as FilterValuePrimitive[];
                     handleValueChange(newValues);
                  }}
                  noOptionsText={
                     retrievalInputValue.trim().length <= 2
                        ? "Type at least 3 characters to search"
                        : "No matches found"
                  }
                  renderInput={(params) => (
                     <TextField
                        {...params}
                        label="Search Values"
                        placeholder="Type to search..."
                        onFocus={() => setRetrievalFocused(true)}
                        onBlur={() => setRetrievalFocused(false)}
                        helperText={
                           retrievalFocused &&
                           !retrievalLoading &&
                           retrievalSearched &&
                           retrievalOptions.length === 0
                              ? "No matches found"
                              : undefined
                        }
                        InputProps={{
                           ...params.InputProps,
                           endAdornment: (
                              <>
                                 {retrievalLoading ? (
                                    <CircularProgress
                                       color="inherit"
                                       size={20}
                                    />
                                 ) : null}
                                 {params.InputProps.endAdornment}
                              </>
                           ),
                        }}
                     />
                  )}
                  freeSolo
                  filterOptions={(x) => x}
               />
            )}

         {spec.filterType === "MinMax" && !needsTwoValues && (
            <TextField
               size="small"
               type="number"
               label="Value"
               value={value1}
               onChange={(e) =>
                  handleValueChange(parseFloat(e.target.value) || "")
               }
               onFocus={() => setShowMinMaxHelper(true)}
               onBlur={() => setShowMinMaxHelper(false)}
               placeholder="Enter number..."
               fullWidth
               inputProps={{
                  min: minNumber,
                  max: maxNumber,
               }}
               helperText={
                  showMinMaxHelper &&
                  minNumber !== undefined &&
                  maxNumber !== undefined
                     ? `Valid range: [${minNumber} - ${maxNumber}]`
                     : undefined
               }
            />
         )}

         {spec.filterType === "MinMax" && needsTwoValues && (
            <Box sx={{ display: "flex", flexDirection: "column", gap: 1 }}>
               <Box sx={{ display: "flex", gap: 1, alignItems: "center" }}>
                  <TextField
                     size="small"
                     type="number"
                     label="From"
                     value={value1}
                     onChange={(e) =>
                        handleValueChange(
                           parseFloat(e.target.value) || "",
                           value2,
                        )
                     }
                     onFocus={() => setShowMinMaxHelper(true)}
                     onBlur={() => setShowMinMaxHelper(false)}
                     placeholder="Min..."
                     fullWidth
                     inputProps={{
                        min: minNumber,
                        max: value2 || maxNumber,
                     }}
                  />
                  <Box>to</Box>
                  <TextField
                     size="small"
                     type="number"
                     label="To"
                     value={value2}
                     onChange={(e) =>
                        handleValueChange(
                           value1,
                           parseFloat(e.target.value) || "",
                        )
                     }
                     onFocus={() => setShowMinMaxHelper(true)}
                     onBlur={() => setShowMinMaxHelper(false)}
                     placeholder="Max..."
                     fullWidth
                     inputProps={{
                        min: value1 || minNumber,
                        max: maxNumber,
                     }}
                  />
               </Box>
               {showMinMaxHelper &&
                  minNumber !== undefined &&
                  maxNumber !== undefined && (
                     <Typography
                        variant="caption"
                        color="text.secondary"
                        sx={{ mt: -0.5, ml: 1.5 }}
                     >
                        Valid range: [{minNumber} - {maxNumber}]
                     </Typography>
                  )}
            </Box>
         )}

         {isDate && !needsTwoValues && (
            <LocalizationProvider dateAdapter={AdapterDayjs}>
               <DatePicker
                  label="Date"
                  value={value1 instanceof Date ? dayjs.utc(value1) : null}
                  onChange={(newValue: Dayjs | null) => {
                     // Only call onChange if value is null (cleared) or a valid date
                     if (newValue === null) {
                        handleValueChange(null);
                     } else if (newValue.isValid()) {
                        handleValueChange(newValue.utc().toDate());
                     }
                     // Invalid dates are ignored - don't update state
                  }}
                  timezone="UTC"
                  minDate={value1 instanceof Date ? undefined : minDate}
                  maxDate={value1 instanceof Date ? undefined : maxDate}
                  referenceDate={
                     matchType === "After" && minDate ? minDate : undefined
                  }
                  slotProps={{
                     textField: {
                        size: "small",
                        fullWidth: true,
                        sx: { minWidth: 150 },
                        onFocus: () => setShowMinMaxHelper(true),
                        onBlur: () => setShowMinMaxHelper(false),
                        helperText:
                           showMinMaxHelper && minDate && maxDate
                              ? `Valid range: [${minDate.format("YYYY-MM-DD")} - ${maxDate.format("YYYY-MM-DD")}]`
                              : undefined,
                     },
                     openPickerButton: {
                        sx: { backgroundColor: "transparent" },
                     },
                  }}
               />
            </LocalizationProvider>
         )}

         {isDate && needsTwoValues && (
            <LocalizationProvider dateAdapter={AdapterDayjs}>
               <Box sx={{ display: "flex", flexDirection: "column", gap: 1 }}>
                  <Box sx={{ display: "flex", gap: 1, alignItems: "center" }}>
                     <Box sx={{ flex: 1, minWidth: 0 }}>
                        <DatePicker
                           label="From"
                           value={
                              value1 instanceof Date ? dayjs.utc(value1) : null
                           }
                           onChange={(newValue: Dayjs | null) => {
                              // Only call onChange if value is null (cleared) or a valid date
                              if (newValue === null) {
                                 handleValueChange(null, value2);
                              } else if (newValue.isValid()) {
                                 handleValueChange(
                                    newValue.utc().toDate(),
                                    value2,
                                 );
                              }
                              // Invalid dates are ignored - don't update state
                           }}
                           timezone="UTC"
                           minDate={
                              value1 instanceof Date ? undefined : minDate
                           }
                           maxDate={
                              value2 instanceof Date
                                 ? dayjs.utc(value2)
                                 : value1 instanceof Date
                                   ? undefined
                                   : maxDate
                           }
                           referenceDate={minDate}
                           slotProps={{
                              textField: {
                                 size: "small",
                                 fullWidth: true,
                                 onFocus: () => setShowMinMaxHelper(true),
                                 onBlur: () => setShowMinMaxHelper(false),
                              },
                              openPickerButton: {
                                 sx: { backgroundColor: "transparent" },
                              },
                           }}
                        />
                     </Box>
                     <Box>to</Box>
                     <Box sx={{ flex: 1, minWidth: 0 }}>
                        <DatePicker
                           label="To"
                           value={
                              value2 instanceof Date ? dayjs.utc(value2) : null
                           }
                           onChange={(newValue: Dayjs | null) => {
                              // Only call onChange if value is null (cleared) or a valid date
                              if (newValue === null) {
                                 handleValueChange(value1, null);
                              } else if (newValue.isValid()) {
                                 handleValueChange(
                                    value1,
                                    newValue.utc().toDate(),
                                 );
                              }
                              // Invalid dates are ignored - don't update state
                           }}
                           timezone="UTC"
                           minDate={
                              value1 instanceof Date
                                 ? dayjs.utc(value1)
                                 : value2 instanceof Date
                                   ? undefined
                                   : minDate
                           }
                           maxDate={
                              value2 instanceof Date ? undefined : maxDate
                           }
                           referenceDate={
                              value1 instanceof Date
                                 ? dayjs.utc(value1)
                                 : minDate
                           }
                           slotProps={{
                              textField: {
                                 size: "small",
                                 fullWidth: true,
                                 onFocus: () => setShowMinMaxHelper(true),
                                 onBlur: () => setShowMinMaxHelper(false),
                              },
                              openPickerButton: {
                                 sx: { backgroundColor: "transparent" },
                              },
                           }}
                        />
                     </Box>
                  </Box>
                  {showMinMaxHelper && minDate && maxDate && (
                     <Typography
                        variant="caption"
                        color="text.secondary"
                        sx={{ mt: -0.5, ml: 1.5 }}
                     >
                        Valid range: [{minDate.format("YYYY-MM-DD")} -{" "}
                        {maxDate.format("YYYY-MM-DD")}]
                     </Typography>
                  )}
               </Box>
            </LocalizationProvider>
         )}
      </Box>
   );
}
