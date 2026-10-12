// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import {
   Box,
   Chip,
   List,
   ListItemButton,
   ListItemText,
   MenuItem,
   TextField,
   Typography,
} from "@mui/material";
import { usePublisherTheme } from "../../theme/ThemeContext";
import type { CatalogSource } from "./catalog";

/** A source select and the list of its views, which is what both "add a tile" and "add a query" pick from. */
export function SourceViewPicker({
   sources,
   source,
   view,
   onSource,
   onView,
}: {
   sources: CatalogSource[];
   source: string;
   view: string;
   onSource: (name: string) => void;
   onView: (name: string) => void;
}) {
   const { theme } = usePublisherTheme();
   const picked = sources.find((s) => s.name === source);
   return (
      <>
         <TextField
            select
            size="small"
            label="Source"
            value={source}
            onChange={(event) => onSource(event.target.value)}
            inputProps={{ "aria-label": "Source" }}
         >
            {sources.map((s) => (
               <MenuItem
                  key={s.name}
                  value={s.name}
                  disabled={s.views.length === 0}
               >
                  {s.name}
                  {s.views.length === 0 && (
                     <Typography
                        component="span"
                        variant="caption"
                        sx={{ ml: 1 }}
                     >
                        declares no views
                     </Typography>
                  )}
                  {s.description && (
                     <Typography
                        component="span"
                        variant="caption"
                        sx={{ ml: 1, opacity: 0.6 }}
                     >
                        {s.description}
                     </Typography>
                  )}
               </MenuItem>
            ))}
         </TextField>
         <Box>
            <Typography variant="subtitle2" sx={{ mb: 0.5 }}>
               View
            </Typography>
            <List
               dense
               disablePadding
               aria-label="Views"
               sx={{
                  maxHeight: "min(280px, 30vh)",
                  overflowY: "auto",
                  border: theme.border,
                  borderRadius: 1,
               }}
            >
               {(picked?.views ?? []).map((v) => (
                  <ListItemButton
                     key={v.name}
                     selected={view === v.name}
                     onClick={() => onView(v.name)}
                     aria-label={`View ${v.name}`}
                  >
                     <ListItemText
                        primary={v.name}
                        secondary={v.description}
                        primaryTypographyProps={{
                           fontFamily: "ui-monospace, monospace",
                           fontSize: 13,
                        }}
                     />
                     {v.chart && (
                        <Chip
                           size="small"
                           variant="outlined"
                           label={v.chart.replace(/_chart$/, "")}
                        />
                     )}
                  </ListItemButton>
               ))}
               {picked && picked.views.length === 0 && (
                  <Typography
                     variant="body2"
                     sx={{ p: 1.5, color: theme.tileTitle }}
                  >
                     This source declares no views.
                  </Typography>
               )}
            </List>
         </Box>
      </>
   );
}
