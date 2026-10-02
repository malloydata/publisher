// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import CancelIcon from "@mui/icons-material/Cancel";
import {
   Chip,
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
import type { PackageCatalog } from "./catalog";
import type {
   DashboardDocument,
   DashboardImport,
   DocumentKind,
} from "./document";

/**
 * The page's own settings, off the edit bar: what it shows as, the grid's
 * width, whether controls run as they change or behind an Apply button, and
 * the sources it imports. Its title and description are edited on the page. Edits commit on close
 * (`useDraft`).
 */
export interface PageSettings {
   /** Absent reads as a dashboard. */
   kind?: DocumentKind;
   columns?: number;
   autorun?: boolean;
   /** A `{ names }` import is edited as sources; a whole-file import is shown and left alone. */
   imports: DashboardImport[];
}

export const settingsOf = (document: DashboardDocument): PageSettings => ({
   imports: document.imports,
   ...(document.kind === undefined ? {} : { kind: document.kind }),
   ...(document.columns === undefined ? {} : { columns: document.columns }),
   ...(document.autorun === undefined ? {} : { autorun: document.autorun }),
});

/** Where a model is imported from: documents sit one folder below the package root. */
const importPathOf = (modelPath: string) => `../${modelPath}`;

/** `settings` with `name` from `modelPath` imported. */
export function withSource(
   imports: DashboardImport[],
   name: string,
   modelPath: string,
): DashboardImport[] {
   const from = importPathOf(modelPath);
   if (imports.some((i) => i.kind === "names" && i.names.includes(name)))
      return imports;
   const at = imports.findIndex((i) => i.kind === "names" && i.from === from);
   if (at < 0) return [...imports, { kind: "names", names: [name], from }];
   return imports.map((i, index) =>
      index === at && i.kind === "names"
         ? { ...i, names: [...i.names, name] }
         : i,
   );
}

/** `imports` without the source `name`; its statement goes with its last name. */
export const withoutSource = (
   imports: DashboardImport[],
   name: string,
): DashboardImport[] =>
   imports.flatMap((i) => {
      if (i.kind !== "names" || !i.names.includes(name)) return [i];
      const names = i.names.filter((n) => n !== name);
      return names.length === 0 ? [] : [{ ...i, names }];
   });

/** The widths a grid is usually given; the file may say any other. */
const WIDTHS = [2, 3, 4, 6, 8, 12, 16, 24];

export function SettingsPopover({
   anchor,
   settings,
   catalog,
   inUse,
   onClose,
   onCommit,
}: {
   anchor: HTMLElement | null;
   settings: PageSettings;
   /** What the package offers to import; without it the sources are shown, not edited. */
   catalog?: PackageCatalog | undefined;
   /** Sources a tile or extension reads, which cannot be taken off. */
   inUse: ReadonlySet<string>;
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
   const imported = new Set(
      (draft?.imports ?? []).flatMap((i) =>
         i.kind === "names" ? i.names : [],
      ),
   );
   const offered = (catalog?.sources ?? []).filter(
      (source) =>
         !imported.has(source.name) &&
         !source.modelPath.startsWith("dashboards/") &&
         !source.modelPath.startsWith("notebooks/"),
   );
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
               <Stack sx={{ gap: 0.75 }}>
                  <Typography variant="caption" sx={{ color: theme.tileTitle }}>
                     Sources
                  </Typography>
                  <Stack
                     direction="row"
                     aria-label="Sources"
                     sx={{ gap: 0.75, flexWrap: "wrap" }}
                  >
                     {draft.imports.length === 0 && (
                        <Typography variant="body2" sx={{ opacity: 0.7 }}>
                           None imported.
                        </Typography>
                     )}
                     {draft.imports.flatMap((entry) =>
                        entry.kind === "all"
                           ? [
                                <Chip
                                   key={`all ${entry.from}`}
                                   size="small"
                                   variant="outlined"
                                   label={entry.from}
                                   title="Every declaration in this file; edit it in the file"
                                />,
                             ]
                           : entry.names.map((name) => (
                                <Chip
                                   key={`${entry.from} ${name}`}
                                   size="small"
                                   label={name}
                                   title={
                                      inUse.has(name)
                                         ? `${name} from ${entry.from}; a tile reads it`
                                         : `${name} from ${entry.from}`
                                   }
                                   {...(inUse.has(name)
                                      ? {}
                                      : {
                                           onDelete: () =>
                                              patch((s) => {
                                                 s.imports = withoutSource(
                                                    s.imports,
                                                    name,
                                                 );
                                              }),
                                           deleteIcon: (
                                              <CancelIcon
                                                 aria-label={`Remove source ${name}`}
                                              />
                                           ),
                                        })}
                                />
                             )),
                     )}
                  </Stack>
                  {catalog && offered.length > 0 && (
                     <TextField
                        select
                        size="small"
                        label="Add a source"
                        value=""
                        inputProps={{ "aria-label": "Add a source" }}
                        onChange={(event) => {
                           const picked = offered.find(
                              (source) =>
                                 `${source.modelPath} ${source.name}` ===
                                 event.target.value,
                           );
                           if (!picked) return;
                           patch((s) => {
                              s.imports = withSource(
                                 s.imports,
                                 picked.name,
                                 picked.modelPath,
                              );
                           });
                        }}
                     >
                        {offered.map((source) => (
                           <MenuItem
                              key={`${source.modelPath} ${source.name}`}
                              value={`${source.modelPath} ${source.name}`}
                           >
                              {source.name}
                              <Typography
                                 component="span"
                                 variant="caption"
                                 sx={{ ml: 1, opacity: 0.7 }}
                              >
                                 {source.modelPath}
                              </Typography>
                           </MenuItem>
                        ))}
                     </TextField>
                  )}
               </Stack>
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
