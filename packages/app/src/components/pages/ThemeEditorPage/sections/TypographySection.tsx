import { resolveTheme, type Theme } from "@malloy-publisher/sdk";
import { Box, MenuItem, Stack, TextField, Typography } from "@mui/material";
import { TypographyPreview } from "../previews/TypographyPreview";

interface TypographySectionProps {
   theme: Theme;
   onChange: (next: Theme) => void;
   disabled: boolean;
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
 * Edits `font.family` and `font.size`. Family is a curated dropdown
 * (custom stacks can still be entered as raw CSS — we just don't
 * provide a builder for them in v1).
 */
export function TypographySection({
   theme,
   onChange,
   disabled,
}: TypographySectionProps) {
   const resolved = resolveTheme([theme], "light");
   const family = theme.font?.family ?? resolved.font.family;
   const size = theme.font?.size ?? resolved.font.size;

   const setFamily = (next: string) => {
      onChange({ ...theme, font: { ...theme.font, family: next } });
   };
   const setSize = (next: number) => {
      onChange({ ...theme, font: { ...theme.font, size: next } });
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
