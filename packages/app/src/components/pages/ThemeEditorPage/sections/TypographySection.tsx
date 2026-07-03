import {
   resolveTheme,
   type Theme,
   type ThemeMode,
} from "@malloy-publisher/sdk";
import { Box, MenuItem, Stack, TextField, Typography } from "@mui/material";
import { TypographyPreview } from "../previews/TypographyPreview";

interface TypographySectionProps {
   theme: Theme;
   onChange: (next: Theme) => void;
   disabled: boolean;
   /** Active editor mode. Only used to render the preview accurately. */
   mode: ThemeMode;
}

const FONT_OPTIONS: Array<{ label: string; value: string }> = [
   { label: "Inter (default)", value: "Inter, system-ui, sans-serif" },
   { label: "Roboto", value: "Roboto, system-ui, sans-serif" },
   { label: "System UI sans", value: "system-ui, -apple-system, sans-serif" },
   {
      label: "Monospace (JetBrains/Menlo)",
      value: '"JetBrains Mono", Menlo, monospace',
   },
];

/**
 * Edits `font.family` and `font.size`. Both fields are shared across
 * light and dark modes because font choice is a brand decision, not a
 * mode-specific one. The Light/Dark toggle at the editor level still
 * changes the preview background so the operator can see the font on
 * both surfaces.
 */
export function TypographySection({
   theme,
   onChange,
   disabled,
   mode,
}: TypographySectionProps) {
   const resolved = resolveTheme([theme], mode);
   // Defensive: an old per-mode font shape from a previous Publisher
   // version would deserialise as an object here. Fall back to the
   // resolved default so the picker still mounts.
   const family =
      typeof theme.font?.family === "string"
         ? theme.font.family
         : resolved.font.family;
   const size =
      typeof theme.font?.size === "number"
         ? theme.font.size
         : resolved.font.size;

   // Guard against legacy non-object `theme.font` values (e.g. an
   // operator-edited config that put a stray string there). Same
   // shape concern as the per-mode color spreads in TablesSection.
   const fontBase = () =>
      theme.font && typeof theme.font === "object" && !Array.isArray(theme.font)
         ? theme.font
         : {};
   const setFamily = (next: string) => {
      onChange({ ...theme, font: { ...fontBase(), family: next } });
   };
   const setSize = (next: number) => {
      onChange({ ...theme, font: { ...fontBase(), size: next } });
   };

   const matchingOption = FONT_OPTIONS.find((o) => o.value === family);

   return (
      <Box>
         <Typography
            variant="caption"
            color="text.secondary"
            sx={{ mb: 1, display: "block" }}
         >
            Sample text
         </Typography>
         <Box sx={{ mb: 2 }}>
            <TypographyPreview fontFamily={family} fontSize={size} />
         </Box>
         <Stack spacing={2} sx={{ maxWidth: 360 }}>
            <TextField
               select
               size="small"
               label="Font family"
               value={matchingOption ? family : "__custom"}
               disabled={disabled}
               onChange={(e) => {
                  const v = e.target.value;
                  if (v !== "__custom") setFamily(v);
               }}
            >
               {FONT_OPTIONS.map((opt) => (
                  <MenuItem key={opt.value} value={opt.value}>
                     {opt.label}
                  </MenuItem>
               ))}
               {!matchingOption && (
                  <MenuItem value="__custom" disabled>
                     Custom: {family}
                  </MenuItem>
               )}
            </TextField>
            <TextField
               size="small"
               type="number"
               label="Base font size (px)"
               value={size}
               disabled={disabled}
               inputProps={{ min: 8, max: 24, step: 1 }}
               onChange={(e) => {
                  const v = Number(e.target.value);
                  if (Number.isFinite(v) && v >= 8 && v <= 24) setSize(v);
               }}
            />
         </Stack>
      </Box>
   );
}
