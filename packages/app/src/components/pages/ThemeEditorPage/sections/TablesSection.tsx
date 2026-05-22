import { resolveTheme, type Theme } from "@malloy-publisher/sdk";
import { Alert, Box, Stack, Typography } from "@mui/material";
import { ColorPickerField } from "../ColorPickerField";
import { TablePreview } from "../previews/TablePreview";

interface TablesSectionProps {
   theme: Theme;
   onChange: (next: Theme) => void;
   disabled: boolean;
}

/**
 * Edits `palette.background.light` and `palette.tableHeader.light` —
 * the only table-related tokens the current Theme schema exposes.
 * Body color, borders, and pinned background still come from the
 * renderer's hardcoded defaults; they'll become editable once the
 * renderer's `theme` prop ships upstream.
 */
export function TablesSection({
   theme,
   onChange,
   disabled,
}: TablesSectionProps) {
   const resolved = resolveTheme([theme], "light");
   const headerColor =
      theme.palette?.tableHeader?.light ?? resolved.tableHeader;
   const background = theme.palette?.background?.light ?? resolved.background;

   const setHeader = (hex: string) => {
      onChange({
         ...theme,
         palette: {
            ...theme.palette,
            tableHeader: { ...theme.palette?.tableHeader, light: hex },
         },
      });
   };
   const setBackground = (hex: string) => {
      onChange({
         ...theme,
         palette: {
            ...theme.palette,
            background: { ...theme.palette?.background, light: hex },
         },
      });
   };

   return (
      <Box>
         <Typography
            variant="caption"
            color="text.secondary"
            sx={{ mb: 1, display: "block" }}
         >
            Sample table
         </Typography>
         <Box sx={{ mb: 2 }}>
            <TablePreview
               background={background}
               headerColor={headerColor}
               bodyColor="#727883"
               border="1px solid #e5e7eb"
               pinnedBackground={background}
               fontFamily={resolved.font.family}
               fontSize={resolved.font.size}
            />
         </Box>
         <Stack spacing={2}>
            <ColorPickerField
               label="Header color"
               value={headerColor}
               onChange={setHeader}
               disabled={disabled}
            />
            <ColorPickerField
               label="Background"
               value={background}
               onChange={setBackground}
               disabled={disabled}
            />
         </Stack>
         <Alert severity="info" sx={{ mt: 2 }}>
            Body text, borders, and dashboard tile colors come from the
            renderer&apos;s defaults today. Editing those requires the upstream
            renderer&apos;s <code>theme</code> prop, which is tracked as a
            follow-up.
         </Alert>
      </Box>
   );
}
