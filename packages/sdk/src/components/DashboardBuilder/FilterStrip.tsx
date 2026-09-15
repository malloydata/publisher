// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import AddIcon from "@mui/icons-material/Add";
import FilterListIcon from "@mui/icons-material/FilterList";
import { Button, Chip, Stack, Tooltip, Typography } from "@mui/material";
import type { ReactNode } from "react";
import { usePublisherTheme } from "../../theme/ThemeContext";
import type { BuilderControl } from "./controls";

/**
 * The strip under the header where the dashboard's controls are configured —
 * the ONE place: a chip per control opens its window, and "Add filter"
 * declares a new one. The live control row the host renders sits under it.
 */
export function FilterStrip({
   controls: controlList,
   tileCount,
   unknownFieldsOf,
   onEdit,
   onAdd,
   children,
}: {
   controls: BuilderControl[];
   tileCount: number;
   /** Bindings a control has that its tiles' sources cannot take. */
   unknownFieldsOf: (name: string, type: string | undefined) => string[];
   onEdit: (control: BuilderControl) => void;
   onAdd: () => void;
   /** The host's live control row. */
   children?: ReactNode;
}) {
   const { theme } = usePublisherTheme();
   return (
      <>
         {/* The filter band — Looker's, for this format. The header is the
       dashboard's controls as this FILE has them: a chip per control,
       which opens its window — the one place a control is edited, bound
       or removed, so the consequences are in view when it happens. A ×
       on the chip was a second place, with none of them. The live control row the caller
       passes in sits directly under, showing the same controls as a
       reader gets them — from the saved file. */}
         <Stack sx={{ gap: 1 }}>
            <Stack
               direction="row"
               aria-label="Filters"
               sx={{
                  gap: 1,
                  alignItems: "center",
                  flexWrap: "wrap",
                  minHeight: 32,
               }}
            >
               <FilterListIcon
                  sx={{ fontSize: 18, color: theme.tileTitle, opacity: 0.7 }}
               />
               <Typography
                  variant="subtitle2"
                  sx={{ color: theme.tileTitle, mr: 0.5 }}
               >
                  Filters
               </Typography>
               {controlList.length === 0 && (
                  <Typography
                     variant="body2"
                     sx={{ color: theme.tileTitle, opacity: 0.8 }}
                  >
                     None yet.
                  </Typography>
               )}
               {controlList.map((control) => {
                  const unknown = unknownFieldsOf(control.name, control.type);
                  return (
                     <Tooltip
                        key={control.name}
                        title={
                           unknown.length > 0
                              ? `$${control.name} · cannot filter on: ${unknown.join(", ")}`
                              : `$${control.name} · ${
                                   control.origin === "dashboard"
                                      ? "declared here"
                                      : "from the model"
                                } · ${control.boundTiles} of ${tileCount} tiles`
                        }
                     >
                        <Chip
                           size="small"
                           label={control.label ?? control.name}
                           aria-label={`Edit filter ${control.name}`}
                           // Warning where a binding names a field the source
                           // does not have: the package would refuse the file.
                           color={unknown.length > 0 ? "warning" : "default"}
                           variant={
                              control.origin === "dashboard"
                                 ? "filled"
                                 : "outlined"
                           }
                           onClick={() => onEdit(control)}
                           sx={{
                              // Faint when nothing binds it: declared, but not yet a
                              // control a reader would see.
                              opacity: control.boundTiles === 0 ? 0.6 : 1,
                              cursor: "pointer",
                              transition: "opacity 120ms",
                           }}
                        />
                     </Tooltip>
                  );
               })}
               <Button
                  size="small"
                  variant="outlined"
                  startIcon={<AddIcon fontSize="small" />}
                  onClick={() => onAdd()}
                  sx={{ ml: "auto" }}
               >
                  Add filter
               </Button>
            </Stack>

            {children}
         </Stack>
      </>
   );
}
