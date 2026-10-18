// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { reportClasses } from "../../theme/classes";
import AddIcon from "@mui/icons-material/Add";
import CheckIcon from "@mui/icons-material/Check";
import CloseIcon from "@mui/icons-material/Close";
import ExpandMoreIcon from "@mui/icons-material/ExpandMore";
import {
   Box,
   Button,
   ButtonBase,
   IconButton,
   Popover,
   Tooltip,
   Typography,
} from "@mui/material";
import type { ReactNode } from "react";
import { useState } from "react";
import { MOTION_FAST, reducedMotionSx } from "../../theme/motion";
import { usePublisherTheme } from "../../theme/ThemeContext";
import { edgeColor, hoverEdgeColor } from "../controlTokens";
import { RemoveBadge, removeBadgeRevealSx } from "../RemoveBadge";
import { useFilterEditing } from "./filterEditing";
import { isRequiredGiven } from "./required";
import { summarizeGivenValue } from "./filterSummary";
import { GivenInput, type GivenInputProps } from "./GivenInput";
import { givenToParam } from "./paramCodec";

/**
 * One filter in a report's control row: a 32px button that names the filter
 * and says what it is set to (`Category  All`, `Ordered since  Jan 1, 2023`),
 * and opens the filter's full control in a popover.
 *
 * The row reads at a glance and costs one line, where a form field per filter
 * cost two (the field, then a `Default:` caption under it) and wrapped. Every
 * kind of control (picker, range, time range, date, boolean, text) opens the
 * same way, so the row has one shape whatever the filters are.
 *
 * Set to a value of its own, the button is tinted in the accent and carries a
 * clear (×), which drops the value back to the model default, as the control's
 * own × does. The default itself reads as a value, not as a hint.
 *
 * Inside the builder ({@link useFilterEditing}) the same button is where the
 * filter is configured: a remove badge on its corner while it is hovered or
 * focused, and its settings from the popover. Distinct from the clear (×):
 * that one empties the value, the badge takes the filter off the report.
 */
export function FilterButton(props: GivenInputProps) {
   const { given, value, onChange } = props;
   const { theme } = usePublisherTheme();
   const ui = theme.ui;
   const editing = useFilterEditing();
   const [anchor, setAnchor] = useState<HTMLElement | null>(null);
   const name = given.name ?? "";
   const label = given.label ?? name;
   const set = value !== null && value !== undefined;
   const summary = summarizeGivenValue(given, value);
   const open = anchor !== null;
   const state = editing?.stateOf(name);
   const problem = state?.problem;
   // Read through the model: set here as in the reader, changed only there.
   const fixed = state?.fixed;
   const button = (
      <FilterShell
         name={name}
         set={set}
         open={open}
         problem={problem}
         fixed={fixed !== undefined}
         required={!set && isRequiredGiven(given)}
      >
         <ButtonBase
            aria-haspopup="dialog"
            aria-expanded={open}
            aria-label={`${label}: ${summary}`}
            onClick={(event) => setAnchor(event.currentTarget)}
            sx={(mui) => ({
               height: "100%",
               borderRadius: ui.radius.control,
               pl: "10px",
               pr: set ? "4px" : "8px",
               gap: "6px",
               fontFamily: "inherit",
               fontSize: ui.type.control.fontSize,
               lineHeight: 1,
               minWidth: 0,
               color: mui.palette.text.primary,
            })}
         >
            <Box
               component="span"
               sx={(mui) => ({
                  color: set ? theme.accent : mui.palette.text.secondary,
                  whiteSpace: "nowrap",
               })}
            >
               {label}
            </Box>
            <Box
               component="span"
               sx={{
                  fontWeight: ui.type.control.fontWeight,
                  whiteSpace: "nowrap",
                  overflow: "hidden",
                  textOverflow: "ellipsis",
               }}
            >
               {summary}
            </Box>
            {!set && (
               <ExpandMoreIcon
                  aria-hidden
                  sx={(mui) => ({
                     fontSize: 16,
                     color: mui.palette.text.secondary,
                     transform: open ? "rotate(180deg)" : "none",
                  })}
               />
            )}
         </ButtonBase>
         {set && (
            <IconButton
               aria-label={`Clear ${label}`}
               onClick={() => onChange(null)}
               sx={{
                  width: 22,
                  height: 22,
                  mr: "4px",
                  borderRadius: "4px",
                  color: theme.accent,
               }}
            >
               <CloseIcon sx={{ fontSize: 14 }} />
            </IconButton>
         )}
      </FilterShell>
   );
   return (
      <>
         {problem ? <Tooltip title={problem}>{button}</Tooltip> : button}
         <Popover
            className={reportClasses.filterPopover}
            open={open}
            anchorEl={anchor}
            onClose={() => setAnchor(null)}
            anchorOrigin={{ vertical: "bottom", horizontal: "left" }}
            transformOrigin={{ vertical: "top", horizontal: "left" }}
            slotProps={{
               paper: {
                  role: "dialog",
                  "aria-label": `${label} filter`,
                  sx: {
                     mt: 0.75,
                     p: 1.5,
                     width: 300,
                     maxWidth: "calc(100vw - 32px)",
                     boxSizing: "border-box",
                     borderRadius: ui.radius.tile,
                     border: theme.cardBorder,
                     boxShadow: theme.shadow.lift,
                  },
               },
            }}
         >
            <GivenInput {...props} />
            {editing && (
               <Box
                  sx={(mui) => ({
                     mt: 1.5,
                     pt: 1,
                     borderTop: `1px solid ${mui.palette.divider}`,
                     display: "flex",
                     flexDirection: "column",
                     alignItems: "flex-start",
                     gap: 0.5,
                  })}
               >
                  {problem && (
                     <Typography
                        sx={{ ...ui.type.caption, color: "warning.main" }}
                     >
                        {problem}
                     </Typography>
                  )}
                  {fixed !== undefined && (
                     <Typography
                        sx={(mui) => ({
                           ...ui.type.caption,
                           color: mui.palette.text.secondary,
                        })}
                     >
                        {fixed}
                     </Typography>
                  )}
                  <StartingValue
                     name={name}
                     type={given.type}
                     text={startingTextOf(value, given.type)}
                  />
                  {fixed === undefined ? (
                     <Button
                        size="small"
                        aria-label={`Edit filter ${name}`}
                        aria-haspopup="dialog"
                        onClick={() => {
                           setAnchor(null);
                           editing.onEdit(name);
                        }}
                        sx={{ px: 0.75, minWidth: 0, ...ui.type.control }}
                     >
                        Filter settings…
                     </Button>
                  ) : (
                     state?.openModel && (
                        <Button
                           size="small"
                           onClick={() => {
                              setAnchor(null);
                              state.openModel?.();
                           }}
                           sx={{ px: 0.75, minWidth: 0, ...ui.type.control }}
                        >
                           Open the model
                        </Button>
                     )
                  )}
               </Box>
            )}
         </Popover>
      </>
   );
}

/**
 * A filter's value as the report's `givens { … }` writes it: as the query
 * endpoint spells it, the URL's spelling. For a `filter<…>` that is the filter
 * text itself, not an `f'…'` literal: the server unwraps a literal without
 * un-escaping it, so `f'O\'Brien'` would start the report at `O\'Brien`.
 * Undefined when there is no value to start from.
 */
function startingTextOf(
   value: GivenInputProps["value"],
   type: string | undefined,
): string | undefined {
   if (value === undefined || value === null) return undefined;
   return givenToParam(value, type);
}

/** A starting value as it compares: a filter's `f'…'` literal, as a file may write it, unwrapped. */
function startingComparable(
   text: string | undefined,
   type: string | undefined,
): string | undefined {
   if (text === undefined || !type?.startsWith("filter<")) return text;
   return /^f(['"])([\s\S]*)\1$/.exec(text)?.[2] ?? text;
}

/**
 * The report's default for this filter, in the builder: the value the report
 * opens with (`givens { … }`). "Make this the report's default" saves the
 * current value as it; once saved, a quiet "Report default" with a check says
 * so, and the clearing button goes back to the default the filter is declared
 * with: "Use the model's default" for a model's filter, "Use the filter's
 * default" for one this report declares, whose fallback is the default written
 * in the report. Any filter has one, the model's included, since where a
 * report opens is the report's to say.
 */
function StartingValue({
   name,
   type,
   text,
}: {
   name: string;
   type: string | undefined;
   /** The current value as the file would spell it; undefined when unset. */
   text: string | undefined;
}) {
   const editing = useFilterEditing();
   const ui = usePublisherTheme().theme.ui;
   if (!editing?.setStartingValue) return null;
   const set = editing.setStartingValue;
   const start = startingComparable(editing.startingValueOf?.(name), type);
   const declaredHere = editing.stateOf(name)?.origin === "dashboard";
   const buttonSx = { px: 0.75, minWidth: 0, ...ui.type.control };
   return (
      <>
         {start !== undefined && start === text && (
            <Typography
               sx={(mui) => ({
                  ...ui.type.caption,
                  color: mui.palette.text.secondary,
                  px: 0.75,
                  display: "flex",
                  alignItems: "center",
                  gap: 0.5,
               })}
            >
               <CheckIcon aria-hidden sx={{ fontSize: 14 }} />
               Report default
            </Typography>
         )}
         {text !== undefined && text !== start && (
            <Button size="small" onClick={() => set(name, text)} sx={buttonSx}>
               Make this the report&apos;s default
            </Button>
         )}
         {start !== undefined && (
            <Button
               size="small"
               onClick={() => set(name, undefined)}
               sx={buttonSx}
            >
               {declaredHere
                  ? "Use the filter's default"
                  : "Use the model's default"}
            </Button>
         )}
      </>
   );
}

/**
 * The button's box: its edge, its tint when set, and in the builder the
 * remove badge on its corner. Shared with {@link UnusedFilterButton} so a
 * filter no tile reads yet sits in the row in the same shape.
 */
function FilterShell({
   name,
   set = false,
   open = false,
   unused = false,
   problem,
   fixed = false,
   required = false,
   children,
}: {
   name: string;
   set?: boolean;
   open?: boolean;
   unused?: boolean;
   problem?: string;
   /** The model's filter: no remove badge, since the builder cannot take it off. */
   fixed?: boolean;
   /** Required and unset: edged in the warning colour, since nothing that reads it runs. */
   required?: boolean;
   children: ReactNode;
}) {
   const { theme } = usePublisherTheme();
   const ui = theme.ui;
   const editing = useFilterEditing();
   return (
      <Box
         className={reportClasses.filter}
         data-filter={name}
         data-set={set || undefined}
         data-problem={problem ? true : undefined}
         data-required={required || undefined}
         sx={(mui) => ({
            position: "relative",
            display: "inline-flex",
            alignItems: "center",
            height: ui.size.control,
            boxSizing: "border-box",
            borderRadius: ui.radius.control,
            border: "1px solid",
            borderStyle: unused ? "dashed" : "solid",
            borderColor:
               problem || required
                  ? mui.palette.warning.main
                  : set
                    ? `color-mix(in srgb, ${theme.accent} 45%, transparent)`
                    : open
                      ? theme.accent
                      : edgeColor(theme.cardBorder),
            bgcolor: set
               ? `color-mix(in srgb, ${theme.accent} 8%, ${mui.palette.background.paper})`
               : mui.palette.background.paper,
            opacity: unused ? 0.75 : 1,
            transition: `border-color ${MOTION_FAST}`,
            ...reducedMotionSx,
            "&:hover": {
               borderColor:
                  set || open || problem
                     ? undefined
                     : hoverEdgeColor(theme.cardBorder, theme.tileTitle),
            },
            maxWidth: "100%",
            ...removeBadgeRevealSx,
         })}
      >
         {children}
         {editing && !fixed && (
            <RemoveBadge
               label={`Remove filter ${name}`}
               onRemove={() => editing.onRemove(name)}
            />
         )}
      </Box>
   );
}

/**
 * A filter the row has no live control for, as a button that opens its
 * settings. Mostly a filter the file declares that no tile reads yet
 * (`unused`): dashed and faint, saying so in words, and its settings are where
 * it is bound to tiles. Also every filter when the builder has no live row to
 * draw (no server to run one), so filters can still be edited.
 */
export function FilterSettingsButton({
   name,
   label,
   unused = true,
}: {
   name: string;
   label: string;
   unused?: boolean;
}) {
   const editing = useFilterEditing();
   const ui = usePublisherTheme().theme.ui;
   if (!editing) return null;
   const problem = editing.stateOf(name)?.problem;
   const shell = (
      <FilterShell name={name} unused={unused} problem={problem}>
         <ButtonBase
            aria-label={`Edit filter ${name}`}
            aria-haspopup="dialog"
            onClick={() => editing.onEdit(name)}
            sx={(mui) => ({
               height: "100%",
               borderRadius: ui.radius.control,
               px: "10px",
               gap: "6px",
               fontFamily: "inherit",
               fontSize: ui.type.control.fontSize,
               lineHeight: 1,
               color: mui.palette.text.secondary,
               whiteSpace: "nowrap",
            })}
         >
            <span>{label}</span>
            {unused && (
               <Box component="span" sx={{ fontStyle: "italic" }}>
                  Unused
               </Box>
            )}
         </ButtonBase>
      </FilterShell>
   );
   return problem ? <Tooltip title={problem}>{shell}</Tooltip> : shell;
}

/** The row's last slot in the builder: declare or bind another filter. */
export function AddFilterButton() {
   const { theme } = usePublisherTheme();
   const ui = theme.ui;
   const editing = useFilterEditing();
   if (!editing) return null;
   const disabled = editing.addDisabledReason !== undefined;
   return (
      // A disabled button takes no pointer events, so the tooltip saying why
      // sits on a wrapper.
      <Tooltip title={editing.addDisabledReason ?? ""}>
         <span>
            <Button
               variant="outlined"
               startIcon={<AddIcon />}
               aria-label="Add filter"
               aria-haspopup="dialog"
               disabled={disabled}
               onClick={() => editing.onAdd()}
               sx={(mui) => ({
                  height: ui.size.control,
                  minHeight: ui.size.control,
                  px: 1.25,
                  borderRadius: ui.radius.control,
                  borderStyle: "dashed",
                  borderColor: edgeColor(theme.cardBorder),
                  color: mui.palette.text.secondary,
                  boxShadow: "none",
                  ...ui.type.control,
                  "& .MuiButton-startIcon": { mr: 0.5 },
                  "& .MuiButton-startIcon svg": { fontSize: 16 },
               })}
            >
               Filter
            </Button>
         </span>
      </Tooltip>
   );
}
