// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import {
   FormControlLabel,
   MenuItem,
   Popover,
   Stack,
   Switch,
   TextField,
   ToggleButton,
   ToggleButtonGroup,
   Typography,
} from "@mui/material";
import { useDraft } from "./useDraft";
import { usePublisherTheme } from "../../theme/ThemeContext";
import type { DashboardDocument, DocumentKind } from "./document";

/**
 * The page's own settings, off the edit bar: what it shows as, the grid's
 * width, and whether controls run as they change or behind an Apply button.
 * Its title and description are edited on the page. Edits commit on close
 * (`useDraft`).
 */
export interface PageSettings {
   /** Absent reads as a dashboard. */
   kind?: DocumentKind;
   columns?: number;
   autorun?: boolean;
}

export const settingsOf = (document: DashboardDocument): PageSettings => ({
   ...(document.kind === undefined ? {} : { kind: document.kind }),
   ...(document.columns === undefined ? {} : { columns: document.columns }),
   ...(document.autorun === undefined ? {} : { autorun: document.autorun }),
});

/** The widths a grid is usually given; the file may say any other. */
const WIDTHS = [2, 3, 4, 6, 8, 12, 16, 24];

export function SettingsPopover({
   anchor,
   settings,
   onClose,
   onCommit,
}: {
   anchor: HTMLElement | null;
   settings: PageSettings;
   onClose: () => void;
   /** Called once, on close, only when something changed. */
   onCommit: (next: PageSettings) => void;
}) {
   const { theme } = usePublisherTheme();
   const { draft, patch, close } = useDraft(
      settings,
      anchor !== null,
      onCommit,
      onClose,
   );

   const notebook = draft?.kind === "notebook";
   const widths =
      draft?.columns !== undefined && !WIDTHS.includes(draft.columns)
         ? [...WIDTHS, draft.columns].sort((a, b) => a - b)
         : WIDTHS;

   return (
      <Popover
         open={anchor !== null && draft !== undefined}
         anchorEl={anchor}
         onClose={close}
         anchorOrigin={{ vertical: "bottom", horizontal: "right" }}
         transformOrigin={{ vertical: "top", horizontal: "right" }}
         slotProps={{ paper: { sx: { width: 380, p: 2 } } }}
      >
         {draft && (
            <Stack sx={{ gap: 1.5 }}>
               <Typography
                  variant="overline"
                  sx={{ color: theme.tileTitle, lineHeight: 1.5 }}
               >
                  {notebook ? "Notebook" : "Dashboard"}
               </Typography>
               {/* A notebook is the same file with one column, so switching is a tag edit, not a move. */}
               <Stack
                  direction="row"
                  sx={{ gap: 1, alignItems: "center", flexWrap: "wrap" }}
               >
                  <Typography variant="caption" sx={{ color: theme.tileTitle }}>
                     Show as
                  </Typography>
                  <ToggleButtonGroup
                     size="small"
                     exclusive
                     value={notebook ? "notebook" : "dashboard"}
                     aria-label="Show as"
                     onChange={(_, next: DocumentKind | null) => {
                        if (next === null) return;
                        patch((s) => {
                           if (next === "notebook") {
                              s.kind = "notebook";
                              delete s.columns;
                           } else delete s.kind;
                        });
                     }}
                  >
                     <ToggleButton value="dashboard" sx={{ px: 1.5 }}>
                        Dashboard
                     </ToggleButton>
                     <ToggleButton value="notebook" sx={{ px: 1.5 }}>
                        Notebook
                     </ToggleButton>
                  </ToggleButtonGroup>
               </Stack>
               {!notebook && (
                  <TextField
                     select
                     size="small"
                     label="Grid width"
                     value={draft.columns ?? ""}
                     helperText="Columns across the page. Tiles are placed in these."
                     inputProps={{ "aria-label": "Grid width" }}
                     onChange={(event) =>
                        patch((s) => {
                           const v = Number(event.target.value);
                           if (!v) delete s.columns;
                           else s.columns = v;
                        })
                     }
                  >
                     <MenuItem value="">Default (2)</MenuItem>
                     {widths.map((w) => (
                        <MenuItem key={w} value={w}>
                           {w}
                        </MenuItem>
                     ))}
                  </TextField>
               )}
               <FormControlLabel
                  control={
                     <Switch
                        size="small"
                        checked={draft.autorun !== false}
                        slotProps={{
                           input: { "aria-label": "Run as controls change" },
                        }}
                        onChange={(event) =>
                           patch((s) => {
                              if (event.target.checked) delete s.autorun;
                              else s.autorun = false;
                           })
                        }
                     />
                  }
                  label={
                     <Typography variant="body2">
                        Run as controls change
                        <Typography
                           component="span"
                           variant="caption"
                           sx={{ display: "block", opacity: 0.7 }}
                        >
                           Off, readers press Apply. For a page whose queries
                           are slow.
                        </Typography>
                     </Typography>
                  }
               />
            </Stack>
         )}
      </Popover>
   );
}
