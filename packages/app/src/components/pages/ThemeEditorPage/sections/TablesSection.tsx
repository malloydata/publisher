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

type PerModeKey = "tableHeader" | "tableBody" | "tile" | "tileTitle";

/**
 * Edits the per-mode table tokens: header color, body text color, the
 * dashboard tile (padded container) background, and the tile title text
 * color. All four are stored as { light, dark } variants on the Theme
 * and the active variant is chosen by the editor-level Light/Dark
 * toggle (passed in via `mode`).
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
               bodyColor={bodyColor}
               border={
                  mode === "dark" ? "1px solid #334155" : "1px solid #e5e7eb"
               }
               pinnedBackground={tile}
               fontFamily={resolved.font.family}
               fontSize={resolved.font.size}
            />
         </Box>
         <Stack spacing={2}>
            <ColorPickerField
               label="Header color"
               value={headerColor}
               onChange={setColor("tableHeader")}
               disabled={disabled}
            />
            <ColorPickerField
               label="Body text color"
               value={bodyColor}
               onChange={setColor("tableBody")}
               disabled={disabled}
            />
            <ColorPickerField
               label="Tile background"
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
