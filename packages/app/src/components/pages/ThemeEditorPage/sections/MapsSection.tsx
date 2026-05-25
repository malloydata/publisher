import {
   resolveTheme,
   type Theme,
   type ThemeMode,
} from "@malloy-publisher/sdk";
import { Box, Typography } from "@mui/material";
import { ColorPickerField } from "../ColorPickerField";
import { MapPreview } from "../previews/MapPreview";

interface MapsSectionProps {
   theme: Theme;
   onChange: (next: Theme) => void;
   disabled: boolean;
   mode: ThemeMode;
}

/**
 * Edits `palette.mapColor` — the saturated end of the gradient used
 * by choropleth maps (sales_by_state etc.). The renderer pairs this
 * with a neutral grey low end to generate the full ramp via
 * `getColorScale` in `@malloydata/render`.
 *
 * Lives in its own section rather than alongside chart series because
 * the operator's mental model is "one knob for maps", not "another
 * thing in the Charts grid". Maps consume a sequential / quantitative
 * scale, not the categorical series palette.
 */
export function MapsSection({
   theme,
   onChange,
   disabled,
   mode,
}: MapsSectionProps) {
   const resolved = resolveTheme([theme], mode);

   const mapColor = theme.palette?.mapColor?.[mode] ?? resolved.mapColor;
   const setMapColor = (hex: string) => {
      // Legacy-shape guard mirrors the other section writers.
      const existing = theme.palette?.mapColor;
      const base =
         existing && typeof existing === "object" && !Array.isArray(existing)
            ? existing
            : {};
      onChange({
         ...theme,
         palette: {
            ...theme.palette,
            mapColor: { ...base, [mode]: hex },
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
            Sample choropleth
         </Typography>
         <Box sx={{ mb: 2 }}>
            <MapPreview theme={resolved} />
         </Box>
         <ColorPickerField
            label="Map color"
            value={mapColor}
            onChange={setMapColor}
            disabled={disabled}
         />
         <Typography
            variant="caption"
            color="text.secondary"
            sx={{ mt: 1, display: "block" }}
         >
            Saturated end of the gradient. Choropleths ramp from a neutral grey
            to this colour.
         </Typography>
      </Box>
   );
}
