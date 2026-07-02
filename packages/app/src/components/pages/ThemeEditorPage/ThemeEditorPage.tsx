import { useServer, type Theme, type ThemeMode } from "@malloy-publisher/sdk";
import DarkModeIcon from "@mui/icons-material/DarkMode";
import LightModeIcon from "@mui/icons-material/LightMode";
import RestartAltIcon from "@mui/icons-material/RestartAlt";
import {
   Alert,
   Box,
   Button,
   Card,
   CardContent,
   CardHeader,
   Dialog,
   DialogActions,
   DialogContent,
   DialogContentText,
   DialogTitle,
   Snackbar,
   Stack,
   ToggleButton,
   ToggleButtonGroup,
   Typography,
} from "@mui/material";
import { useMutation, useQuery, useQueryClient } from "@tanstack/react-query";
import { useEffect, useMemo, useRef, useState } from "react";
import { MapsSection } from "./sections/MapsSection";
import { SeriesColorsSection } from "./sections/SeriesColorsSection";
import { TablesSection } from "./sections/TablesSection";
import { TypographySection } from "./sections/TypographySection";

const AUTO_SAVE_DELAY_MS = 600;

/**
 * Settings → Visualization theme. Operator picks colors / fonts for
 * the Malloy renderer's output (charts, tables, dashboards) that apply
 * to every viewer of this Publisher instance. Distinct from the app
 * shell light/dark toggle in the header, which controls the MUI
 * palette. Auto-saves on debounced edit.
 */
export default function ThemeEditorPage() {
   const { apiClients } = useServer();
   const queryClient = useQueryClient();

   const themeQuery = useQuery({
      queryKey: ["theme"],
      queryFn: async () => {
         const res = await apiClients.publisher.getTheme();
         return (res.data ?? {}) as Theme;
      },
   });
   const statusQuery = useQuery({
      queryKey: ["status"],
      queryFn: async () => {
         const res = await apiClients.publisher.getStatus();
         return res.data;
      },
   });

   const savedTheme: Theme = useMemo(
      () => themeQuery.data ?? {},
      [themeQuery.data],
   );
   const frozen = Boolean(
      (statusQuery.data as { frozenConfig?: boolean } | undefined)
         ?.frozenConfig,
   );

   const [draft, setDraft] = useState<Theme>(savedTheme);
   const [snackbar, setSnackbar] = useState<string | null>(null);
   const debounceRef = useRef<NodeJS.Timeout | null>(null);
   // Bumped whenever Reset or an auto-save fires. Each save closure
   // captures the value at issue time and aborts (or ignores its
   // onSuccess) if the ref has advanced past it. This prevents an
   // in-flight save from stomping a subsequent reset's setQueryData.
   const saveGenRef = useRef(0);
   // Serializes overlapping auto-saves so their PUTs settle in issue
   // order. Each save chains off the previous one's promise; because
   // every PUT is a full snapshot, chaining makes the last-issued draft
   // the last to land, so the DB can't end up behind the editor cache
   // even if the network would otherwise deliver two in-flight PUTs out
   // of order.
   const saveChainRef = useRef<Promise<void>>(Promise.resolve());
   // Reset goes through a confirm dialog so the operator doesn't wipe
   // hand-tuned colours with an accidental click.
   const [confirmResetOpen, setConfirmResetOpen] = useState(false);
   // Guard the auto-save effect against firing during the brief
   // pending window of themeQuery — without this, an edit made before
   // the initial /api/v0/theme response lands ends up auto-saving the
   // empty-state draft (resolved-defaults snapshot + the edit) over
   // the real saved theme. Flips to true on first successful resync.
   const hasSyncedOnceRef = useRef(false);

   // Tracks the savedTheme value that the draft last agreed with. The
   // resync effect uses this to tell external updates (initial load,
   // reset, another tab) apart from echoes of our own save: if the draft
   // matches the previously-synced saved value, the user has no pending
   // edits and we adopt the new saved as the baseline. If the draft has
   // diverged, the user is mid-edit and we keep their changes.
   const lastSyncedSavedKeyRef = useRef<string | null>(null);

   useEffect(() => {
      // Wait until the theme query actually resolves before treating
      // the savedTheme as authoritative. Without this, the resync
      // effect's first-load branch runs against the empty `{}` from
      // the pending query and unlocks auto-save before the real saved
      // theme arrives.
      if (!themeQuery.isSuccess) return;
      const savedKey = JSON.stringify(savedTheme);
      const draftKey = JSON.stringify(draft);
      if (lastSyncedSavedKeyRef.current === null) {
         // First successful load: adopt saved as draft baseline.
         setDraft(savedTheme);
         lastSyncedSavedKeyRef.current = savedKey;
         hasSyncedOnceRef.current = true;
         return;
      }
      if (savedKey === lastSyncedSavedKeyRef.current) return;
      const draftMatchedPrevSaved = draftKey === lastSyncedSavedKeyRef.current;
      if (draftMatchedPrevSaved) {
         setDraft(savedTheme);
      }
      // Either way, advance the baseline so future external updates are
      // detected from the latest server state, not the stale one.
      lastSyncedSavedKeyRef.current = savedKey;
      // draft is intentionally read inside the effect via the closure but
      // not in the dep array — re-running on every keystroke would defeat
      // the divergence check.
      // eslint-disable-next-line react-hooks/exhaustive-deps
   }, [savedTheme, themeQuery.isSuccess]);

   const saveMutation = useMutation({
      mutationFn: async (theme: Theme) => {
         const res = await apiClients.publisher.putTheme(theme);
         return res.data as Theme;
      },
      // onSuccess / onError handled per-call so the closure can compare
      // against the generation it was issued in. Reset bumps the
      // generation; any save whose generation is stale silently drops
      // its response instead of stomping the reset's setQueryData.
   });

   const resetMutation = useMutation({
      mutationFn: async () => {
         const res = await apiClients.publisher.resetTheme();
         return res.data as Theme;
      },
      onMutate: () => {
         // Cancel any pending debounce and invalidate any in-flight
         // save's onSuccess before the reset request hits the wire.
         saveGenRef.current++;
         if (debounceRef.current) {
            clearTimeout(debounceRef.current);
            debounceRef.current = null;
         }
      },
      onSuccess: (reseeded) => {
         queryClient.setQueryData(["theme"], reseeded);
         setDraft(reseeded);
         setSnackbar("Reset to defaults");
      },
      onError: (err: unknown) => {
         setSnackbar(
            err instanceof Error
               ? `Reset failed: ${err.message}`
               : "Reset failed",
         );
      },
   });

   // Auto-save debounce: any change to the draft triggers a save after
   // AUTO_SAVE_DELAY_MS of inactivity. Skip when the draft equals the
   // saved value (no-op edits), when the instance is frozen, or when
   // the initial /status resync hasn't landed (otherwise we'd PUT a
   // draft built off the empty pre-query state).
   const draftKey = useMemo(() => JSON.stringify(draft), [draft]);
   const savedKey = useMemo(() => JSON.stringify(savedTheme), [savedTheme]);
   useEffect(() => {
      if (frozen) return;
      if (!themeQuery.isSuccess) return;
      if (!hasSyncedOnceRef.current) return;
      if (draftKey === savedKey) return;
      if (debounceRef.current) clearTimeout(debounceRef.current);
      debounceRef.current = setTimeout(() => {
         const myGen = ++saveGenRef.current;
         // Serialize saves: chain each PUT after the previous one settles
         // so two overlapping auto-saves reach the server in issue order.
         // The gen guard below still protects the client cache, but
         // ThemeStore.set full-replaces in arrival order, so without this
         // chain a reordered PUT could leave the DB on the older draft
         // while the editor cache shows the newer one and says "Saved".
         const themeToSave = draft;
         saveChainRef.current = saveChainRef.current
            .catch(() => {})
            .then(async () => {
               try {
                  const saved = await saveMutation.mutateAsync(themeToSave);
                  if (saveGenRef.current !== myGen) return;
                  queryClient.setQueryData(["theme"], saved);
                  queryClient.invalidateQueries({ queryKey: ["status"] });
                  setSnackbar("Saved");
               } catch (err: unknown) {
                  if (saveGenRef.current !== myGen) return;
                  setSnackbar(
                     err instanceof Error
                        ? `Save failed: ${err.message}`
                        : "Save failed",
                  );
               }
            });
      }, AUTO_SAVE_DELAY_MS);
      return () => {
         if (debounceRef.current) clearTimeout(debounceRef.current);
      };
      // saveMutation intentionally excluded — including it would re-arm the
      // debounce on every mutation state change and double-save.
      // eslint-disable-next-line react-hooks/exhaustive-deps
   }, [draftKey, savedKey, frozen, themeQuery.isSuccess]);

   // Which per-mode variant the color pickers below edit. This is purely
   // an editor-side concern; it doesn't change which mode VIEWERS see.
   // Both light and dark variants live in the same Theme and are saved
   // together — the toggle just decides which slot the pickers write.
   const [editingMode, setEditingMode] = useState<ThemeMode>("light");

   const sectionProps = {
      theme: draft,
      onChange: setDraft,
      disabled: frozen,
      mode: editingMode,
   };

   return (
      <Box sx={{ p: 4, maxWidth: 980, mx: "auto" }}>
         <Stack
            direction="row"
            justifyContent="space-between"
            alignItems="flex-start"
            sx={{ mb: 3 }}
         >
            <Box>
               <Typography variant="h4" sx={{ fontWeight: 600, mb: 1 }}>
                  Visualization theme
               </Typography>
               <Typography variant="body2" color="text.secondary">
                  Pick colors and fonts for charts, tables, and dashboards. The
                  app shell light/dark toggle lives in the header. Changes save
                  automatically and apply to every viewer on next page load.
               </Typography>
            </Box>
            <Button
               variant="outlined"
               size="small"
               startIcon={<RestartAltIcon />}
               disabled={frozen || resetMutation.isPending}
               onClick={() => setConfirmResetOpen(true)}
            >
               Reset to defaults
            </Button>
         </Stack>

         {frozen && (
            <Alert severity="warning" sx={{ mb: 3 }}>
               This Publisher instance has <code>{`"frozenConfig": true`}</code>{" "}
               set, so the theme can&apos;t be edited from this page. Edit{" "}
               <code>publisher.config.json</code> directly to change the theme.
            </Alert>
         )}

         <Box sx={{ mb: 3, display: "flex", alignItems: "center", gap: 2 }}>
            <Typography variant="body2" color="text.secondary">
               Editing colors for
            </Typography>
            <ToggleButtonGroup
               value={editingMode}
               exclusive
               size="small"
               onChange={(_, next) => {
                  // Don't allow deselecting; ToggleButtonGroup fires
                  // onChange with null when the active button is clicked.
                  if (next === "light" || next === "dark") {
                     setEditingMode(next);
                  }
               }}
               disabled={frozen}
               aria-label="Edit colors for mode"
            >
               <ToggleButton value="light" aria-label="Light mode">
                  <LightModeIcon fontSize="small" sx={{ mr: 0.75 }} />
                  Light
               </ToggleButton>
               <ToggleButton value="dark" aria-label="Dark mode">
                  <DarkModeIcon fontSize="small" sx={{ mr: 0.75 }} />
                  Dark
               </ToggleButton>
            </ToggleButtonGroup>
            <Typography variant="caption" color="text.secondary">
               Each mode has its own colors. Pickers below edit the active
               mode&apos;s values.
            </Typography>
         </Box>

         <Stack spacing={3}>
            <Card>
               <CardHeader title="Charts" />
               <CardContent>
                  <SeriesColorsSection {...sectionProps} />
               </CardContent>
            </Card>

            <Card>
               <CardHeader title="Maps" />
               <CardContent>
                  <MapsSection {...sectionProps} />
               </CardContent>
            </Card>

            <Card>
               <CardHeader title="Tables" />
               <CardContent>
                  <TablesSection {...sectionProps} />
               </CardContent>
            </Card>

            <Card>
               <CardHeader title="Typography" />
               <CardContent>
                  <TypographySection {...sectionProps} />
               </CardContent>
            </Card>
         </Stack>

         <Snackbar
            open={snackbar !== null}
            autoHideDuration={2500}
            onClose={() => setSnackbar(null)}
            message={snackbar ?? ""}
            anchorOrigin={{ vertical: "bottom", horizontal: "right" }}
         />

         <Dialog
            open={confirmResetOpen}
            onClose={() => setConfirmResetOpen(false)}
            aria-labelledby="reset-confirm-title"
         >
            <DialogTitle id="reset-confirm-title">
               Reset to defaults?
            </DialogTitle>
            <DialogContent>
               <DialogContentText>
                  Every customized color and font for the visualization theme
                  will be cleared, restoring the publisher.config.json boot seed
                  (or the built-in defaults). This applies immediately to every
                  viewer.
               </DialogContentText>
            </DialogContent>
            <DialogActions>
               <Button
                  onClick={() => setConfirmResetOpen(false)}
                  sx={{ color: "text.primary" }}
               >
                  Cancel
               </Button>
               <Button
                  color="error"
                  onClick={() => {
                     resetMutation.mutate();
                     setConfirmResetOpen(false);
                  }}
                  autoFocus
               >
                  Reset
               </Button>
            </DialogActions>
         </Dialog>
      </Box>
   );
}
