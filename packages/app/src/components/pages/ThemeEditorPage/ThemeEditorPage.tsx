import { useServer, type Theme, type ThemeMode } from "@malloy-publisher/sdk";
import DarkModeIcon from "@mui/icons-material/DarkMode";
import LightModeIcon from "@mui/icons-material/LightMode";
import RestartAltIcon from "@mui/icons-material/RestartAlt";
import UndoIcon from "@mui/icons-material/Undo";
import {
   Alert,
   Box,
   Button,
   Card,
   CardContent,
   CardHeader,
   Snackbar,
   Stack,
   ToggleButton,
   ToggleButtonGroup,
   Typography,
} from "@mui/material";
import { useMutation, useQuery, useQueryClient } from "@tanstack/react-query";
import { useEffect, useMemo, useRef, useState } from "react";
import { SeriesColorsSection } from "./sections/SeriesColorsSection";
import { TablesSection } from "./sections/TablesSection";
import { TypographySection } from "./sections/TypographySection";

const AUTO_SAVE_DELAY_MS = 600;

/**
 * Settings → Theme. Operator picks colors / fonts that apply to every
 * viewer of this Publisher instance. Auto-saves on debounced edit.
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

   // Tracks the savedTheme value that the draft last agreed with. The
   // resync effect uses this to tell external updates (initial load,
   // reset, another tab) apart from echoes of our own save: if the draft
   // matches the previously-synced saved value, the user has no pending
   // edits and we adopt the new saved as the baseline. If the draft has
   // diverged, the user is mid-edit and we keep their changes.
   const lastSyncedSavedKeyRef = useRef<string | null>(null);

   useEffect(() => {
      const savedKey = JSON.stringify(savedTheme);
      const draftKey = JSON.stringify(draft);
      if (lastSyncedSavedKeyRef.current === null) {
         // First successful load: adopt saved as draft baseline.
         setDraft(savedTheme);
         lastSyncedSavedKeyRef.current = savedKey;
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
   }, [savedTheme]);

   const saveMutation = useMutation({
      mutationFn: async (theme: Theme) => {
         const res = await apiClients.publisher.putTheme(theme);
         return res.data as Theme;
      },
      onSuccess: (saved) => {
         queryClient.setQueryData(["theme"], saved);
         queryClient.invalidateQueries({ queryKey: ["status"] });
         setSnackbar("Saved");
      },
      onError: (err: unknown) => {
         setSnackbar(
            err instanceof Error
               ? `Save failed: ${err.message}`
               : "Save failed",
         );
      },
   });

   const resetMutation = useMutation({
      mutationFn: async () => {
         const res = await apiClients.publisher.resetTheme();
         return res.data as Theme;
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
   // saved value (no-op edits) or when the instance is frozen.
   const draftKey = useMemo(() => JSON.stringify(draft), [draft]);
   const savedKey = useMemo(() => JSON.stringify(savedTheme), [savedTheme]);
   useEffect(() => {
      if (frozen) return;
      if (!themeQuery.isSuccess) return;
      if (draftKey === savedKey) return;
      if (debounceRef.current) clearTimeout(debounceRef.current);
      debounceRef.current = setTimeout(() => {
         saveMutation.mutate(draft);
      }, AUTO_SAVE_DELAY_MS);
      return () => {
         if (debounceRef.current) clearTimeout(debounceRef.current);
      };
      // saveMutation intentionally excluded — including it would re-arm the
      // debounce on every mutation state change and double-save.
      // eslint-disable-next-line react-hooks/exhaustive-deps
   }, [draftKey, savedKey, frozen, themeQuery.isSuccess]);

   const dirty = draftKey !== savedKey;

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
                  Theme
               </Typography>
               <Typography variant="body2" color="text.secondary">
                  Pick colors and fonts for the Malloy renderer. Changes save
                  automatically and apply to every viewer on next page load.
               </Typography>
            </Box>
            <Stack direction="row" spacing={1}>
               <Button
                  variant="outlined"
                  size="small"
                  startIcon={<UndoIcon />}
                  disabled={frozen || !dirty}
                  onClick={() => setDraft(savedTheme)}
               >
                  Discard changes
               </Button>
               <Button
                  variant="outlined"
                  size="small"
                  startIcon={<RestartAltIcon />}
                  disabled={frozen || resetMutation.isPending}
                  onClick={() => resetMutation.mutate()}
               >
                  Reset to defaults
               </Button>
            </Stack>
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
      </Box>
   );
}
