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
    * The mode the preview is rendered in. The series palette itself is
    * shared across modes (there's no .light/.dark variant for series),
    * so the pickers don't change behavior — but the preview needs to
    * pick up the active mode's background so the bars look right.
    */
   mode: ThemeMode;
}

const DEFAULT_NEW_COLOR = "#1877f2";

/**
 * Edits `palette.series` — the cycling color palette used for
 * multi-series charts (Vega's `range.category`).
 */
export function SeriesColorsSection({
   theme,
   onChange,
   disabled,
   mode,
}: SeriesColorsSectionProps) {
   const resolved = resolveTheme([theme], mode);
   // The series array is per-mode; pickers below operate on whichever
   // mode the editor toggle is currently on, and the other mode's
   // palette is untouched.
   const series = theme.palette?.series?.[mode] ?? resolved.series;

   // Stable per-row ids so React keys survive insertions and deletions in
   // the middle of the list. Without them the Popover state and any
   // in-flight text edits would re-attach to the wrong color when a row
   // is removed (the next row shifts up into the deleted row's key slot).
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
         palette: {
            ...theme.palette,
            series: { ...theme.palette?.series, [mode]: next },
         },
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
