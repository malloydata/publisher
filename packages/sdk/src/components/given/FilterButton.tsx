// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import AddIcon from "@mui/icons-material/Add";
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
import {
   CONTROL_HEIGHT_PX,
   CONTROL_RADIUS,
   edgeColor,
   hoverEdgeColor,
   TILE_RADIUS,
   TYPE,
} from "../controlTokens";
import { useFilterEditing } from "./filterEditing";
import { summarizeGivenValue } from "./filterSummary";
import { GivenInput, type GivenInputProps } from "./GivenInput";

/** Class on the remove badge, shown while its filter is hovered or focused. */
const REMOVE_BADGE = "filter-remove-badge";

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
   const editing = useFilterEditing();
   const [anchor, setAnchor] = useState<HTMLElement | null>(null);
   const name = given.name ?? "";
   const label = given.label ?? name;
   const set = value !== null && value !== undefined;
   const summary = summarizeGivenValue(given, value);
   const open = anchor !== null;
   const problem = editing?.stateOf(name)?.problem;
   const button = (
      <FilterShell name={name} set={set} open={open} problem={problem}>
         <ButtonBase
            aria-haspopup="dialog"
            aria-expanded={open}
            aria-label={`${label}: ${summary}`}
            onClick={(event) => setAnchor(event.currentTarget)}
            sx={(mui) => ({
               height: "100%",
               borderRadius: CONTROL_RADIUS,
               pl: "10px",
               pr: set ? "4px" : "8px",
               gap: "6px",
               fontFamily: "inherit",
               fontSize: 13,
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
                  fontWeight: 500,
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
                     borderRadius: TILE_RADIUS,
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
                        sx={{ ...TYPE.caption, color: "warning.main" }}
                     >
                        {problem}
                     </Typography>
                  )}
                  <Button
                     size="small"
                     aria-label={`Edit filter ${name}`}
                     aria-haspopup="dialog"
                     onClick={() => {
                        setAnchor(null);
                        editing.onEdit(name);
                     }}
                     sx={{ px: 0.75, minWidth: 0, ...TYPE.control }}
                  >
                     Filter settings…
                  </Button>
               </Box>
            )}
         </Popover>
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
   children,
}: {
   name: string;
   set?: boolean;
   open?: boolean;
   unused?: boolean;
   problem?: string;
   children: ReactNode;
}) {
   const { theme } = usePublisherTheme();
   const editing = useFilterEditing();
   return (
      <Box
         data-filter={name}
         data-set={set || undefined}
         data-problem={problem ? true : undefined}
         sx={(mui) => ({
            position: "relative",
            display: "inline-flex",
            alignItems: "center",
            height: CONTROL_HEIGHT_PX,
            boxSizing: "border-box",
            borderRadius: CONTROL_RADIUS,
            border: "1px solid",
            borderStyle: unused ? "dashed" : "solid",
            borderColor: problem
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
            [`& .${REMOVE_BADGE}`]: { opacity: 0 },
            [`&:hover .${REMOVE_BADGE}, &:focus-within .${REMOVE_BADGE}`]: {
               opacity: 1,
            },
            "@media (hover: none)": {
               [`& .${REMOVE_BADGE}`]: { opacity: 1 },
            },
         })}
      >
         {children}
         {editing && (
            <IconButton
               className={REMOVE_BADGE}
               aria-label={`Remove filter ${name}`}
               onClick={() => editing.onRemove(name)}
               sx={(mui) => ({
                  position: "absolute",
                  top: -8,
                  right: -8,
                  width: 18,
                  height: 18,
                  p: 0,
                  borderRadius: "50%",
                  border: `1px solid ${edgeColor(theme.cardBorder)}`,
                  bgcolor: mui.palette.background.paper,
                  color: mui.palette.text.secondary,
                  transition: `opacity ${MOTION_FAST}`,
                  ...reducedMotionSx,
                  "&:hover": {
                     bgcolor: mui.palette.background.paper,
                     color: mui.palette.text.primary,
                     borderColor: hoverEdgeColor(
                        theme.cardBorder,
                        theme.tileTitle,
                     ),
                  },
               })}
            >
               <CloseIcon sx={{ fontSize: 12 }} />
            </IconButton>
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
               borderRadius: CONTROL_RADIUS,
               px: "10px",
               gap: "6px",
               fontFamily: "inherit",
               fontSize: 13,
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
                  height: CONTROL_HEIGHT_PX,
                  minHeight: CONTROL_HEIGHT_PX,
                  px: 1.25,
                  borderRadius: CONTROL_RADIUS,
                  borderStyle: "dashed",
                  borderColor: edgeColor(theme.cardBorder),
                  color: mui.palette.text.secondary,
                  boxShadow: "none",
                  ...TYPE.control,
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
