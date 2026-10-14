// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import {
   Box,
   Button,
   Checkbox,
   ListItemText,
   MenuItem,
   Stack,
   Switch,
   TextField,
   Typography,
} from "@mui/material";
import { useEffect, useState } from "react";
import { usePublisherTheme } from "../../theme/ThemeContext";
import { AppDialog } from "../AppDialog";
import { edgeColor, TYPE } from "../controlTokens";
import type {
   DashboardDocument,
   DashboardDrill,
   DashboardSource,
} from "./document";

/**
 * Which cells click, and where a click goes.
 *
 * The format's drill is a `# drill { to=… given=… }` tag on a dimension, and
 * the builder writes tags on declarations this file owns — so what is offered
 * here is every dimension the tile's source declares IN THIS FILE, each with
 * its destinations (this dashboard, other dashboards in the package) and the
 * control the clicked value is written into. A dimension the model declares
 * is tagged in the model, and the dialog says so rather than offering a tag it
 * cannot write.
 *
 * Committed once, on Apply, as one history entry.
 */
const SELF = "self";

export interface DrillDialogProps {
   open: boolean;
   document: DashboardDocument;
   /** The tile's source: the extension whose dimensions are offered. */
   source: DashboardSource | undefined;
   /** The controls a click can write into: the dashboard's and the model's. */
   givenNames: string[];
   /** Other dashboards in the package, by slug, as `to=` destinations. */
   dashboards: string[];
   /** The tile the window was opened from, named in its description. */
   tileLabel?: string;
   onClose: () => void;
   /** Every drill on `source`, replacing what the document had for it. */
   onApply: (drills: DashboardDrill[]) => void;
}

interface Row {
   name: string;
   expression: string;
   to: string[];
   given: string;
   /** Where it went before it was switched off. */
   lastTo?: string[];
}

export function DrillDialog({
   open,
   document,
   source,
   givenNames,
   dashboards,
   tileLabel,
   onClose,
   onApply,
}: DrillDialogProps) {
   const { theme } = usePublisherTheme();
   const [rows, setRows] = useState<Row[]>([]);

   useEffect(() => {
      if (!open || !source) return;
      const declared = (document.drills ?? []).filter(
         (d) => d.source === source.name,
      );
      setRows(
         (source.dimensions ?? []).map((dimension) => {
            const drill = declared.find((d) => d.name === dimension.name);
            return {
               name: dimension.name,
               expression: dimension.expression,
               to: drill?.to ?? [],
               given: drill?.given ?? "",
            };
         }),
      );
   }, [open, source, document]);

   const patch = (index: number, change: (row: Row) => void) =>
      setRows((previous) =>
         previous.map((row, i) => {
            if (i !== index) return row;
            const next = { ...row };
            change(next);
            return next;
         }),
      );

   const destinations = [SELF, ...dashboards.filter((d) => d !== SELF)];
   const labelFor = (destination: string) =>
      destination === SELF ? "This report" : destination;

   const apply = () => {
      if (!source) return;
      onApply(
         rows
            .filter((row) => row.to.length > 0)
            .map((row) => ({
               source: source.name,
               name: row.name,
               expression: row.expression,
               to: row.to,
               ...(row.given ? { given: row.given } : {}),
            })),
      );
      onClose();
   };

   const edge = edgeColor(theme.cardBorder);
   // A row switched off keeps where it went, so switching it back on is not
   // starting over.
   const toggle = (index: number, on: boolean) =>
      patch(index, (row) => {
         if (on) row.to = row.lastTo?.length ? row.lastTo : [SELF];
         else {
            row.lastTo = row.to;
            row.to = [];
         }
      });

   return (
      <AppDialog
         open={open}
         onClose={onClose}
         title="Clickable cells"
         description={
            <>
               {tileLabel && (
                  <Box component="span" sx={{ fontWeight: 600 }}>
                     {tileLabel} ·{" "}
                  </Box>
               )}
               A click on a cell sets a filter to its value, on this report or
               on the one it opens.
            </>
         }
         actions={
            <>
               <Button onClick={onClose}>Cancel</Button>
               <Button
                  variant="contained"
                  onClick={apply}
                  disabled={rows.length === 0}
               >
                  Apply
               </Button>
            </>
         }
      >
         {rows.length === 0 ? (
            <Typography variant="body2" sx={{ color: theme.tileTitle, pt: 1 }}>
               {source ? (
                  <>
                     <Box component="code">{source.name}</Box> declares no
                     dimensions of its own in this file. A click is a tag on the
                     dimension&apos;s declaration, so a dimension the model
                     declares is made clickable in the model; to make one
                     clickable here, declare it in{" "}
                     <Box component="code">{source.name}</Box> and group by it.
                  </>
               ) : (
                  "This tile's source is not declared in this file."
               )}
            </Typography>
         ) : (
            <Box role="list" aria-label="Dimensions">
               {rows.map((row, index) => {
                  const on = row.to.length > 0;
                  return (
                     <Box
                        key={row.name}
                        role="listitem"
                        sx={{
                           display: "grid",
                           gridTemplateColumns: {
                              xs: "auto minmax(0, 1fr)",
                              sm: "auto minmax(0, 0.9fr) minmax(0, 1.6fr)",
                           },
                           alignItems: "center",
                           columnGap: 1.5,
                           rowGap: 1,
                           py: 1.25,
                           borderTop:
                              index === 0 ? "none" : `1px solid ${edge}`,
                        }}
                     >
                        <Switch
                           size="small"
                           checked={on}
                           onChange={(event) =>
                              toggle(index, event.target.checked)
                           }
                           slotProps={{
                              input: {
                                 "aria-label": `${row.name} is clickable`,
                              },
                           }}
                        />
                        <Box sx={{ minWidth: 0 }}>
                           <Box
                              sx={{
                                 ...TYPE.control,
                                 fontWeight: 500,
                                 whiteSpace: "nowrap",
                                 overflow: "hidden",
                                 textOverflow: "ellipsis",
                              }}
                           >
                              {titleCase(row.name)}
                           </Box>
                           <Box
                              title={row.expression}
                              sx={{
                                 fontFamily:
                                    "ui-monospace, SFMono-Regular, Menlo, monospace",
                                 fontSize: 11.5,
                                 color: "text.secondary",
                                 whiteSpace: "nowrap",
                                 overflow: "hidden",
                                 textOverflow: "ellipsis",
                              }}
                           >
                              {row.name} is {row.expression}
                           </Box>
                        </Box>
                        {on ? (
                           <Stack
                              direction="row"
                              sx={{
                                 gridColumn: { xs: "1 / -1", sm: "auto" },
                                 alignItems: "center",
                                 gap: 1,
                                 justifyContent: "flex-end",
                                 ...TYPE.control,
                              }}
                           >
                              <Box
                                 component="span"
                                 sx={{ color: "text.secondary" }}
                              >
                                 Opens
                              </Box>
                              <TextField
                                 select
                                 size="small"
                                 value={row.to}
                                 inputProps={{
                                    "aria-label": `Where ${row.name} goes`,
                                 }}
                                 SelectProps={{
                                    multiple: true,
                                    renderValue: (selected) =>
                                       (selected as string[])
                                          .map(labelFor)
                                          .join(", "),
                                 }}
                                 onChange={(event) =>
                                    patch(index, (r) => {
                                       const value = event.target
                                          .value as unknown;
                                       const next = Array.isArray(value)
                                          ? (value as string[])
                                          : String(value)
                                               .split(",")
                                               .filter(Boolean);
                                       // Unticking the last destination is
                                       // switching the row off.
                                       if (next.length === 0) r.lastTo = r.to;
                                       r.to = next;
                                    })
                                 }
                                 sx={{
                                    flex: "1 1 150px",
                                    minWidth: 0,
                                    maxWidth: 200,
                                 }}
                              >
                                 {destinations.map((destination) => (
                                    <MenuItem
                                       key={destination}
                                       value={destination}
                                    >
                                       <Checkbox
                                          size="small"
                                          checked={row.to.includes(destination)}
                                          sx={{ p: 0, mr: 1 }}
                                       />
                                       <ListItemText
                                          primary={labelFor(destination)}
                                       />
                                    </MenuItem>
                                 ))}
                              </TextField>
                              <Box
                                 component="span"
                                 sx={{ color: "text.secondary" }}
                              >
                                 sets
                              </Box>
                              <TextField
                                 select
                                 size="small"
                                 value={row.given}
                                 inputProps={{
                                    "aria-label": `Control ${row.name} sets`,
                                 }}
                                 SelectProps={{
                                    displayEmpty: true,
                                    renderValue: (value) =>
                                       value === "" ? row.name : String(value),
                                 }}
                                 onChange={(event) =>
                                    patch(index, (r) => {
                                       r.given = event.target.value;
                                    })
                                 }
                                 sx={{
                                    flex: "1 1 120px",
                                    minWidth: 0,
                                    maxWidth: 170,
                                 }}
                              >
                                 <MenuItem value="">
                                    <ListItemText
                                       primary={row.name}
                                       secondary="Named like the dimension"
                                    />
                                 </MenuItem>
                                 {givenNames.map((name) => (
                                    <MenuItem key={name} value={name}>
                                       {name}
                                    </MenuItem>
                                 ))}
                              </TextField>
                           </Stack>
                        ) : (
                           <Typography
                              variant="body2"
                              sx={{
                                 color: "text.secondary",
                                 textAlign: "right",
                                 display: { xs: "none", sm: "block" },
                              }}
                           >
                              Not clickable
                           </Typography>
                        )}
                     </Box>
                  );
               })}
            </Box>
         )}
      </AppDialog>
   );
}

/** `product_category` -> `Product category`. */
function titleCase(name: string): string {
   const words = name.replace(/_+/g, " ").trim();
   return words.charAt(0).toUpperCase() + words.slice(1);
}
