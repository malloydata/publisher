// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import {
   Box,
   Button,
   Checkbox,
   Dialog,
   DialogActions,
   DialogContent,
   DialogTitle,
   ListItemText,
   MenuItem,
   Stack,
   TextField,
   Typography,
} from "@mui/material";
import { useEffect, useState } from "react";
import { usePublisherTheme } from "../../theme/ThemeContext";
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
   onClose: () => void;
   /** Every drill on `source`, replacing what the document had for it. */
   onApply: (drills: DashboardDrill[]) => void;
}

interface Row {
   name: string;
   expression: string;
   to: string[];
   given: string;
}

export function DrillDialog({
   open,
   document,
   source,
   givenNames,
   dashboards,
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
      destination === SELF ? "This dashboard" : destination;

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

   return (
      <Dialog open={open} onClose={onClose} maxWidth="sm" fullWidth>
         <DialogTitle sx={{ pb: 0.5 }}>
            Clickable cells
            <Typography
               component="div"
               variant="caption"
               sx={{ color: theme.tileTitle, mt: 0.25 }}
            >
               A cell in a column that groups by one of these dimensions becomes
               a link. Clicking it writes the value into a control — here, or on
               the dashboard it opens.
            </Typography>
         </DialogTitle>
         <DialogContent>
            <Stack sx={{ gap: 2, pt: 1 }}>
               {rows.length === 0 ? (
                  <Typography variant="body2" sx={{ color: theme.tileTitle }}>
                     {source ? (
                        <>
                           <Box component="code">{source.name}</Box> declares no
                           dimensions of its own in this file. A drill is a tag
                           on the dimension&apos;s declaration, so a dimension
                           the model declares is made clickable in the model; to
                           make one clickable here, declare it in{" "}
                           <Box component="code">{source.name}</Box> and group
                           by it.
                        </>
                     ) : (
                        "This tile's source is not declared in this file."
                     )}
                  </Typography>
               ) : (
                  rows.map((row, index) => (
                     <Box
                        key={row.name}
                        sx={{
                           display: "grid",
                           gridTemplateColumns: {
                              xs: "1fr",
                              sm: "minmax(0, 1.2fr) minmax(0, 1fr) minmax(0, 1fr)",
                           },
                           gap: 1.5,
                           alignItems: "start",
                        }}
                     >
                        <Box sx={{ minWidth: 0, pt: 0.75 }}>
                           <Typography
                              variant="body2"
                              sx={{
                                 fontFamily: "ui-monospace, monospace",
                                 fontSize: 13,
                              }}
                           >
                              {row.name}
                           </Typography>
                           <Typography
                              variant="caption"
                              sx={{
                                 color: theme.tileTitle,
                                 display: "block",
                                 overflow: "hidden",
                                 textOverflow: "ellipsis",
                                 whiteSpace: "nowrap",
                              }}
                              title={row.expression}
                           >
                              is {row.expression}
                           </Typography>
                        </Box>
                        <TextField
                           select
                           size="small"
                           label="Clicks go to"
                           value={row.to}
                           inputProps={{
                              "aria-label": `Where ${row.name} goes`,
                           }}
                           SelectProps={{
                              multiple: true,
                              renderValue: (selected) =>
                                 (selected as string[]).length === 0
                                    ? "Not clickable"
                                    : (selected as string[])
                                         .map(labelFor)
                                         .join(", "),
                              displayEmpty: true,
                           }}
                           InputLabelProps={{ shrink: true }}
                           onChange={(event) =>
                              patch(index, (r) => {
                                 const value = event.target.value as unknown;
                                 r.to = Array.isArray(value)
                                    ? (value as string[])
                                    : String(value).split(",").filter(Boolean);
                              })
                           }
                        >
                           {destinations.map((destination) => (
                              <MenuItem key={destination} value={destination}>
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
                        <TextField
                           select
                           size="small"
                           label="Sets the control"
                           value={row.given}
                           disabled={row.to.length === 0}
                           inputProps={{
                              "aria-label": `Control ${row.name} sets`,
                           }}
                           SelectProps={{ displayEmpty: true }}
                           InputLabelProps={{ shrink: true }}
                           helperText={
                              row.given === ""
                                 ? `Named ${row.name}, like the dimension`
                                 : undefined
                           }
                           onChange={(event) =>
                              patch(index, (r) => {
                                 r.given = event.target.value;
                              })
                           }
                        >
                           <MenuItem value="">
                              <em>Same name as the dimension</em>
                           </MenuItem>
                           {givenNames.map((name) => (
                              <MenuItem key={name} value={name}>
                                 {name}
                              </MenuItem>
                           ))}
                        </TextField>
                     </Box>
                  ))
               )}
            </Stack>
         </DialogContent>
         <DialogActions sx={{ px: 3, py: 1.5 }}>
            <Button onClick={onClose}>Cancel</Button>
            <Button
               variant="contained"
               disabled={rows.length === 0}
               onClick={apply}
            >
               Apply
            </Button>
         </DialogActions>
      </Dialog>
   );
}
