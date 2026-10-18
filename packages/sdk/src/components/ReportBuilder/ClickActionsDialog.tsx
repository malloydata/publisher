// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { reportClasses } from "../../theme/classes";
import {
   Box,
   Button,
   Checkbox,
   FormControlLabel,
   MenuItem,
   TextField,
   Typography,
} from "@mui/material";
import type * as Malloy from "@malloydata/malloy-interfaces";
import { useEffect, useMemo, useState } from "react";
import { usePublisherTheme } from "../../theme/ThemeContext";
import { AppDialog } from "../AppDialog";
import { edgeColor } from "../controlTokens";
import type { EntryDrill } from "../drill/resolveDrill";
import { drillValueLabel } from "../drill/resolveDrill";
import {
   clickDimensions,
   clickMenuPreview,
   currentChoice,
   entryFor,
   filtersFor,
   selfFilter,
   linkTargetsFor,
   type ClickChoice,
   type ClickDimension,
   type LinkTarget,
   type ParseTags,
   type ReportFilter,
} from "./clickActions";
import { loadMalloyTag } from "./loadMalloy";

/** The click menu's word for filtering this report, as the report's own reader says it. */
const SELF_LABEL = "Filter this report";

export interface ClickActionsDialogProps {
   open: boolean;
   /** The tile's title, as its card shows it. */
   tileTitle: string;
   /** The tile's result as the builder's preview of it ran (`QueryResult.result`): where its dimensions and first row come from. */
   result: string | undefined;
   /** The tile entry's drill as written now. */
   drill: Record<string, EntryDrill> | undefined;
   /** This report's filters, with their declared types. */
   controls: readonly ReportFilter[];
   /** The package's other reports. */
   reports: readonly LinkTarget[];
   onClose: () => void;
   /** The entry's new drill: undefined when every dimension does what the model says. */
   onApply: (drill: Record<string, EntryDrill> | undefined) => void;
}

/**
 * "When someone clicks a value in <tile>": what the menu a click opens offers
 * besides Show rows, per dimension the tile groups by, written on the tile's
 * own entry in `tiles=[…]` (`drill { … }`). The model's `# drill` tag is the
 * starting point, marked as such; a choice that matches it writes nothing.
 * The preview at the bottom is the menu itself, built with the reader's own
 * labels from the tile's first row.
 */
export function ClickActionsDialog({
   open,
   tileTitle,
   result,
   drill,
   controls,
   reports,
   onClose,
   onApply,
}: ClickActionsDialogProps) {
   const { theme } = usePublisherTheme();
   const ui = theme.ui;
   const edge = edgeColor(theme.cardBorder);
   const [parse, setParse] = useState<ParseTags | undefined>();
   useEffect(() => {
      let live = true;
      void loadMalloyTag().then(({ parseAnnotation }) => {
         if (live) setParse(() => parseAnnotation as unknown as ParseTags);
      });
      return () => {
         live = false;
      };
   }, []);

   const dimensions = useMemo<ClickDimension[]>(() => {
      if (!result || !parse) return [];
      try {
         return clickDimensions(JSON.parse(result) as Malloy.Result, parse);
      } catch {
         return [];
      }
   }, [result, parse]);

   const [choices, setChoices] = useState<Record<string, ClickChoice>>({});
   const [focus, setFocus] = useState<string | undefined>();
   useEffect(() => {
      if (!open) return;
      setChoices(
         Object.fromEntries(
            dimensions.map((d) => [d.name, currentChoice(d, drill?.[d.name])]),
         ),
      );
      setFocus(dimensions[0]?.name);
   }, [open, dimensions, drill]);

   const set = (name: string, next: Partial<ClickChoice>) => {
      setFocus(name);
      setChoices((all) => ({
         ...all,
         [name]: { ...(all[name] ?? { self: false, extra: [] }), ...next },
      }));
   };

   const entries = (): Record<string, EntryDrill> | undefined => {
      // Dimensions this dialog does not list (not in the result) keep what the entry says.
      const listed = new Set(dimensions.map((d) => d.name));
      const out: Record<string, EntryDrill> = Object.fromEntries(
         Object.entries(drill ?? {}).filter(([name]) => !listed.has(name)),
      );
      for (const d of dimensions) {
         const choice = choices[d.name];
         if (!choice) continue;
         const entry = entryFor(d, choice, controls, reports);
         if (entry) out[d.name] = entry;
      }
      return Object.keys(out).length > 0 ? out : undefined;
   };

   const focused = dimensions.find((d) => d.name === focus) ?? dimensions[0];
   const preview =
      focused && choices[focused.name]
         ? clickMenuPreview(
              focused,
              entryFor(focused, choices[focused.name], controls, reports) ??
                 focused.modelDrill,
              { selfLabel: SELF_LABEL, rows: true },
           )
         : [];

   return (
      <AppDialog
         className={`${reportClasses.dialog} ${reportClasses.clickableCellsDialog}`}
         open={open}
         onClose={onClose}
         title={`When someone clicks a value in ${tileTitle}`}
         description="Every value offers Show rows. Add more choices to the menu that opens."
         actions={
            <>
               <Button onClick={onClose}>Cancel</Button>
               <Button
                  variant="contained"
                  disabled={dimensions.length === 0}
                  onClick={() => {
                     onApply(entries());
                     onClose();
                  }}
               >
                  Apply
               </Button>
            </>
         }
      >
         {dimensions.length === 0 ? (
            <Typography variant="body2" sx={{ color: theme.tileTitle, pt: 1 }}>
               {result === undefined
                  ? "This tile hasn't run yet. Once it shows its values, its choices appear here."
                  : "This tile shows no value a click acts on."}
            </Typography>
         ) : (
            <>
               <Box role="list" aria-label="Dimensions">
                  {dimensions.map((d, index) => {
                     const choice = choices[d.name] ?? {
                        self: false,
                        extra: [],
                     };
                     const selfOptions = filtersFor(
                        d,
                        controls,
                        choice.selfGiven,
                     );
                     const selfSets = selfFilter(d, choice, controls);
                     const targets = linkTargetsFor(d, reports);
                     const fromModel =
                        d.modelDrill !== undefined &&
                        entryFor(d, choice, controls, reports) === undefined;
                     return (
                        <Box
                           key={d.name}
                           role="listitem"
                           aria-label={d.label}
                           onFocusCapture={() => setFocus(d.name)}
                           sx={{
                              py: 1.25,
                              borderTop:
                                 index === 0 ? "none" : `1px solid ${edge}`,
                           }}
                        >
                           <Box
                              sx={{
                                 display: "flex",
                                 alignItems: "baseline",
                                 gap: 1,
                                 ...ui.type.control,
                                 fontWeight: ui.type.label.fontWeight,
                              }}
                           >
                              {d.label}
                              {fromModel && (
                                 <Typography
                                    component="span"
                                    variant="caption"
                                    sx={{ color: "text.secondary" }}
                                 >
                                    From the model
                                 </Typography>
                              )}
                           </Box>
                           <Box
                              sx={{
                                 display: "flex",
                                 alignItems: "center",
                                 gap: 1,
                                 flexWrap: "wrap",
                              }}
                           >
                              <FormControlLabel
                                 sx={{ mr: 0 }}
                                 control={
                                    <Checkbox
                                       size="small"
                                       checked={choice.self}
                                       disabled={selfOptions.length === 0}
                                       onChange={(event) =>
                                          set(d.name, {
                                             self: event.target.checked,
                                             ...(event.target.checked &&
                                             selfSets !== undefined
                                                ? { selfGiven: selfSets }
                                                : {}),
                                          })
                                       }
                                    />
                                 }
                                 label={`Filter this report by ${d.label}`}
                              />
                              {(selfOptions.length > 1 ||
                                 (selfOptions.length === 1 &&
                                    selfSets === undefined)) && (
                                 <TextField
                                    select
                                    size="small"
                                    value={selfSets ?? ""}
                                    disabled={!choice.self}
                                    slotProps={{
                                       htmlInput: {
                                          "aria-label": `Filter ${d.label} sets`,
                                       },
                                       select: {
                                          displayEmpty: true,
                                          renderValue: (value) =>
                                             value === ""
                                                ? "Choose a filter"
                                                : (selfOptions.find(
                                                     (filter) =>
                                                        filter.name === value,
                                                  )?.label ?? String(value)),
                                       },
                                    }}
                                    onChange={(event) =>
                                       set(d.name, {
                                          selfGiven: event.target.value,
                                       })
                                    }
                                    sx={{ minWidth: 160 }}
                                 >
                                    {selfOptions.map((filter) => (
                                       <MenuItem
                                          key={filter.name}
                                          value={filter.name}
                                       >
                                          {filter.label ?? filter.name}
                                       </MenuItem>
                                    ))}
                                 </TextField>
                              )}
                           </Box>
                           {choice.self &&
                              selfOptions.length > 0 &&
                              selfSets === undefined && (
                                 <Typography
                                    variant="caption"
                                    component="div"
                                    sx={{ color: "text.secondary", pl: 4 }}
                                 >
                                    Choose the filter a {d.label} value sets.
                                 </Typography>
                              )}
                           {selfOptions.length === 0 && (
                              <Typography
                                 variant="caption"
                                 component="div"
                                 sx={{ color: "text.secondary", pl: 4 }}
                              >
                                 Add a {d.label} filter to this report first
                              </Typography>
                           )}
                           <Box
                              sx={{
                                 display: "flex",
                                 alignItems: "center",
                                 gap: 1,
                                 flexWrap: "wrap",
                              }}
                           >
                              <FormControlLabel
                                 sx={{ mr: 0 }}
                                 control={
                                    <Checkbox
                                       size="small"
                                       checked={choice.link !== undefined}
                                       disabled={targets.length === 0}
                                       onChange={(event) =>
                                          set(d.name, {
                                             link: event.target.checked
                                                ? targets[0]?.path
                                                : undefined,
                                          })
                                       }
                                    />
                                 }
                                 label="Link to another report:"
                              />
                              <TextField
                                 select
                                 size="small"
                                 value={choice.link ?? ""}
                                 disabled={
                                    targets.length === 0 ||
                                    choice.link === undefined
                                 }
                                 onChange={(event) =>
                                    set(d.name, { link: event.target.value })
                                 }
                                 slotProps={{
                                    htmlInput: {
                                       "aria-label": `Report to open for ${d.label}`,
                                    },
                                 }}
                                 sx={{ minWidth: 180 }}
                              >
                                 {targets.map((target) => (
                                    <MenuItem
                                       key={target.path}
                                       value={target.path}
                                    >
                                       {target.title}
                                    </MenuItem>
                                 ))}
                                 {choice.link !== undefined &&
                                    !targets.some(
                                       (t) => t.path === choice.link,
                                    ) && (
                                       <MenuItem value={choice.link}>
                                          {choice.link}
                                       </MenuItem>
                                    )}
                              </TextField>
                           </Box>
                           <Typography
                              variant="caption"
                              component="div"
                              sx={{ color: "text.secondary", pl: 4 }}
                           >
                              {targets.length === 0
                                 ? `No other report has a ${d.label} filter.`
                                 : `Opens it with ${d.label} set to the clicked value.`}
                           </Typography>
                        </Box>
                     );
                  })}
               </Box>
               {focused && (
                  <Box
                     aria-label="Preview"
                     sx={{
                        mt: 1.5,
                        pt: 1.25,
                        borderTop: `1px solid ${edge}`,
                     }}
                  >
                     <Typography variant="body2" sx={{ mb: 0.5 }}>
                        {focused.clickable
                           ? `Clicking "${drillValueLabel(focused.value)}" shows:`
                           : `This tile's first ${focused.label} is empty, so clicking it does nothing.`}
                     </Typography>
                     <Box component="ul" sx={{ m: 0, pl: 2.5 }}>
                        {preview.map((item) => (
                           <Typography
                              key={item}
                              component="li"
                              variant="body2"
                              sx={{ color: "text.secondary" }}
                           >
                              {item}
                           </Typography>
                        ))}
                     </Box>
                  </Box>
               )}
            </>
         )}
      </AppDialog>
   );
}
