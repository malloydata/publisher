import {
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
      onChange({
         ...theme,
         palette: {
            ...theme.palette,
            background: { ...theme.palette?.background, [mode]: hex },
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
      onChange({
         ...theme,
         palette: { ...theme.palette, series: next },
      });
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
            Sample bar chart
         </Typography>
         <Box sx={{ mb: 2 }}>
            <BarChartPreview theme={resolved} />
         </Box>

         <Stack spacing={2} sx={{ mb: 3 }}>
            <ColorPickerField
               label="Chart background"
               value={background}
               onChange={setBackground}
               disabled={disabled}
            />
         </Stack>

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
         <Stack spacing={1.5}>
            {series.map((color, i) => (
               <Stack
                  direction="row"
                  spacing={1}
                  alignItems="center"
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
         </Stack>
      </Box>
   );
}
