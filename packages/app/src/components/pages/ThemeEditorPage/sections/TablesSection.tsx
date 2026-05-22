import {
   resolveTheme,
   type Theme,
   type ThemeMode,
} from "@malloy-publisher/sdk";
import { Box, Stack, Typography } from "@mui/material";
import { ColorPickerField } from "../ColorPickerField";
import { TablePreview } from "../previews/TablePreview";

interface TablesSectionProps {
   theme: Theme;
   onChange: (next: Theme) => void;
   disabled: boolean;
   mode: ThemeMode;
}

type PerModeKey =
   | "tableHeader"
   | "tableHeaderBackground"
   | "tableBody"
   | "tile"
   | "tileTitle";

/**
 * Edits the per-mode table tokens: header text, header background, body
 * text, the dashboard tile (padding around the table), and the tile
 * title text. All are stored as { light, dark } variants on the Theme;
 * the active variant is chosen by the editor-level Light/Dark toggle.
 *
 * Header background and tile background are separate because the
 * operator's mental model is "the padding around the table" (tile) vs
 * "the band at the top of the table" (header background) — even
 * though the renderer historically reused one value for both.
 */
export function TablesSection({
   theme,
   onChange,
   disabled,
   mode,
}: TablesSectionProps) {
   const resolved = resolveTheme([theme], mode);

   // Read the active variant from the saved theme if set; otherwise fall
   // back to the resolved value so the picker shows the colour that will
   // actually render rather than an empty input.
   const valueFor = (key: PerModeKey): string => {
      const fromTheme = theme.palette?.[key]?.[mode];
      if (typeof fromTheme === "string") return fromTheme;
      return resolved[key];
   };

   const setColor = (key: PerModeKey) => (hex: string) => {
      onChange({
         ...theme,
         palette: {
            ...theme.palette,
            [key]: { ...theme.palette?.[key], [mode]: hex },
         },
      });
   };

   const headerColor = valueFor("tableHeader");
   const headerBackground = valueFor("tableHeaderBackground");
   const bodyColor = valueFor("tableBody");
   const tile = valueFor("tile");
   const tileTitle = valueFor("tileTitle");

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
               background={resolved.background}
               headerColor={headerColor}
               headerBackground={headerBackground}
               bodyColor={bodyColor}
               border={resolved.border}
               tileBackground={tile}
               fontFamily={resolved.font.family}
               fontSize={resolved.font.size}
            />
         </Box>
         <Stack spacing={2}>
            <ColorPickerField
               label="Header text color"
               value={headerColor}
               onChange={setColor("tableHeader")}
               disabled={disabled}
            />
            <ColorPickerField
               label="Header background"
               value={headerBackground}
               onChange={setColor("tableHeaderBackground")}
               disabled={disabled}
            />
            <ColorPickerField
               label="Body text color"
               value={bodyColor}
               onChange={setColor("tableBody")}
               disabled={disabled}
            />
            <ColorPickerField
               label="Tile background (around the table)"
               value={tile}
               onChange={setColor("tile")}
               disabled={disabled}
            />
            <ColorPickerField
               label="Tile title color"
               value={tileTitle}
               onChange={setColor("tileTitle")}
               disabled={disabled}
            />
         </Stack>
      </Box>
   );
}
