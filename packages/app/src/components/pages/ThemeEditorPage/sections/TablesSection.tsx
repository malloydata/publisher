// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import {
   resolveTheme,
   type PerModeColorKey,
   type Theme,
   type ThemeMode,
} from "@malloy-publisher/sdk";
import { Box, Typography } from "@mui/material";
import { ColorPickerField } from "../ColorPickerField";
import { perModeColor, withPerModeColor } from "../perModeColor";
import { TablePreview } from "../previews/TablePreview";

interface TablesSectionProps {
   theme: Theme;
   onChange: (next: Theme) => void;
   disabled: boolean;
   mode: ThemeMode;
}

/**
 * Edits the per-mode table tokens: header text, header background, body
 * text, the dashboard tile (padding around the table), the tile title
 * text, the gridlines between rows, and the card edge (which also rules
 * off a pinned header row). All are stored as { light, dark } variants on the Theme;
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

   const valueFor = (key: PerModeColorKey) => perModeColor(theme, key, mode);
   const setColor = (key: PerModeColorKey) => (hex: string) =>
      onChange(withPerModeColor(theme, key, mode, hex));

   const headerColor = valueFor("tableHeader");
   const headerBackground = valueFor("tableHeaderBackground");
   const bodyColor = valueFor("tableBody");
   const tile = valueFor("tile");
   const tileTitle = valueFor("tileTitle");
   const border = valueFor("border");
   const cardBorder = valueFor("cardBorder");

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
               cardBorder={resolved.cardBorder}
               pinnedBorder={resolved.pinnedBorder}
               tileBackground={tile}
               fontFamily={resolved.font.family}
               fontSize={resolved.font.size}
            />
         </Box>
         <Box
            sx={{
               display: "grid",
               // Tiles section labels are longer ("Tile background
               // (around the table)") so the cell minimum sits a bit
               // wider than the series grid.
               gridTemplateColumns: "repeat(auto-fill, minmax(260px, 1fr))",
               columnGap: 2,
               rowGap: 2,
            }}
         >
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
            <ColorPickerField
               label="Gridline color (between rows)"
               value={border}
               onChange={setColor("border")}
               disabled={disabled}
            />
            <ColorPickerField
               label="Card border (tile and header edge)"
               value={cardBorder}
               onChange={setColor("cardBorder")}
               disabled={disabled}
            />
         </Box>
      </Box>
   );
}
