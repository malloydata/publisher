import {
   DEFAULT_THEME,
   resolveTheme,
   type Theme,
   type ThemeMode,
} from "@malloy-publisher/sdk";
import AddIcon from "@mui/icons-material/Add";
import DeleteOutlineIcon from "@mui/icons-material/DeleteOutline";
import {
   Box,
   Button,
   IconButton,
   Stack,
   Tooltip,
   Typography,
} from "@mui/material";
import { useRef } from "react";
import { ColorPickerField } from "../ColorPickerField";
import { BarChartPreview } from "../previews/BarChartPreview";
import { LineChartPreview } from "../previews/LineChartPreview";

let __seriesRowCounter = 0;
const nextRowId = () => `series-row-${++__seriesRowCounter}`;

interface SeriesColorsSectionProps {
   theme: Theme;
   onChange: (next: Theme) => void;
   disabled: boolean;
   /**
    * Active editor mode. Drives the per-mode background picker below
    * and the bar chart preview. The series palette itself is shared
    * across modes so brand identity stays consistent on toggle.
    */
   mode: ThemeMode;
}

const DEFAULT_NEW_COLOR = "#1877f2";

/**
 * Edits the chart-side theme tokens: `palette.background` (per mode,
 * paints the chart canvas via Vega's background config) and
 * `palette.series` (shared, Vega's `range.category`). Tables get their
 * own section because they touch a different surface.
 */
export function SeriesColorsSection({
   theme,
   onChange,
   disabled,
   mode,
}: SeriesColorsSectionProps) {
   const resolved = resolveTheme([theme], mode);

   // Background picker is per-mode (chart canvas in light vs dark).
   const background = theme.palette?.background?.[mode] ?? resolved.background;
   const setBackground = (hex: string) => {
      // Same legacy-shape guard as TablesSection.setColor: a string in
      // theme.palette.background (pre-per-mode shape) would spread into
      // character-indexed garbage.
      const existing = theme.palette?.background;
      const base =
         existing && typeof existing === "object" && !Array.isArray(existing)
            ? existing
            : {};
      onChange({
         ...theme,
         palette: {
            ...theme.palette,
            background: { ...base, [mode]: hex },
         },
      });
   };

   // Defensive: if a stale schema shape sneaked past the server-side
   // sanitiser (e.g. an old per-mode `series` object from a previous
   // version of Publisher), fall back to the resolved default rather
   // than crashing on `.map`.
   const series = Array.isArray(theme.palette?.series)
      ? theme.palette.series
      : resolved.series;

   // Stable per-row ids so React keys survive insertions and deletions
   // in the middle of the list. Without them the popover state and any
   // in-flight text edits would re-attach to the wrong color when a
   // row is removed.
   const idsRef = useRef<string[]>([]);
   while (idsRef.current.length < series.length) {
      idsRef.current.push(nextRowId());
   }
   if (idsRef.current.length > series.length) {
      idsRef.current.length = series.length;
   }

   const setSeries = (next: string[]) => {
      // If the operator's array equals the SDK default palette
      // exactly, drop the explicit `series` so the cascade keeps
      // resolving dynamically — that way a future Publisher release
      // with new brand defaults still reaches this user. As soon as
      // they pick a non-default colour, the field re-materialises.
      // (Compared against DEFAULT_THEME rather than the resolved
      // theme so collapse means 'fall back to SDK defaults', not
      // 'self-reference what's already saved'.)
      const defaults =
         (DEFAULT_THEME.palette?.series as string[] | undefined) ?? [];
      const matchesDefaults =
         next.length === defaults.length &&
         next.every((c, i) => c === defaults[i]);
      const nextPalette: NonNullable<Theme["palette"]> = { ...theme.palette };
      if (matchesDefaults) {
         delete nextPalette.series;
      } else {
         nextPalette.series = next;
      }
      onChange({ ...theme, palette: nextPalette });
   };

   const addColor = () => {
      idsRef.current.push(nextRowId());
      setSeries([...series, DEFAULT_NEW_COLOR]);
   };
   const removeAt = (idx: number) => {
      idsRef.current.splice(idx, 1);
      setSeries(series.filter((_, i) => i !== idx));
   };
   const setAt = (idx: number, hex: string) =>
      setSeries(series.map((c, i) => (i === idx ? hex : c)));

   return (
      <Box>
         <Typography
            variant="caption"
            color="text.secondary"
            sx={{ mb: 1, display: "block" }}
         >
            Sample charts
         </Typography>
         <Stack
            direction="row"
            spacing={2}
            useFlexGap
            flexWrap="wrap"
            sx={{ mb: 2 }}
         >
            <BarChartPreview theme={resolved} />
            <LineChartPreview theme={resolved} />
         </Stack>

         <Box sx={{ mb: 3 }}>
            <ColorPickerField
               label="Chart background"
               value={background}
               onChange={setBackground}
               disabled={disabled}
            />
         </Box>

         <Typography variant="subtitle2" sx={{ mb: 1, mt: 1, fontWeight: 600 }}>
            Series colors
         </Typography>
         <Typography
            variant="caption"
            color="text.secondary"
            sx={{ mb: 2, display: "block" }}
         >
            Cycling palette for multi-series charts. Shared between light and
            dark so brand stays consistent.
         </Typography>
         <Box
            sx={{
               display: "grid",
               // auto-fill packs as many ~240px pickers per row as fit,
               // so wide viewports show 3-4 swatches side by side and
               // narrow ones fall back to a single column.
               gridTemplateColumns: "repeat(auto-fill, minmax(240px, 1fr))",
               columnGap: 2,
               rowGap: 1.5,
               mb: 2,
            }}
         >
            {series.map((color, i) => (
               <Stack
                  direction="row"
                  spacing={1}
                  alignItems="flex-end"
                  key={idsRef.current[i]}
               >
                  <ColorPickerField
                     value={color}
                     onChange={(hex) => setAt(i, hex)}
                     disabled={disabled}
                     label={`Series ${i + 1}`}
                  />
                  <Tooltip title="Remove">
                     <span>
                        <IconButton
                           size="small"
                           onClick={() => removeAt(i)}
                           disabled={disabled || series.length <= 1}
                           aria-label={`Remove series ${i + 1}`}
                        >
                           <DeleteOutlineIcon fontSize="small" />
                        </IconButton>
                     </span>
                  </Tooltip>
               </Stack>
            ))}
         </Box>
         <Box>
            <Button
               variant="outlined"
               size="small"
               startIcon={<AddIcon />}
               onClick={addColor}
               disabled={disabled}
            >
               Add color
            </Button>
         </Box>
      </Box>
   );
}
