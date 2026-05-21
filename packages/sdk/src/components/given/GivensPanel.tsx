import { Box, Button, Paper, Stack, Typography } from "@mui/material";
import { Given } from "../../client";
import { GivenValue } from "../../hooks/useGivensForm";
import { GivenInput } from "./GivenInput";

export interface GivensPanelProps {
   givens: Given[];
   values: Map<string, GivenValue>;
   onChange: (name: string, value: GivenValue) => void;
   onClearAll: () => void;
}

/**
 * Renders the "Parameters" panel — one input per declared `given:`.
 * Returns null when the model declares no givens, so callers can drop
 * `<GivensPanel ... />` unconditionally without a length guard.
 *
 * A "Reset" button appears in the panel header when at least one value
 * is set; it fires `onClearAll` which the parent should wire to
 * `useGivensForm.clearAll`.
 */
export function GivensPanel({
   givens,
   values,
   onChange,
   onClearAll,
}: GivensPanelProps) {
   if (givens.length === 0) return null;
   const hasValues = values.size > 0;

   return (
      <Paper
         elevation={0}
         sx={{
            p: 3,
            backgroundColor: "transparent",
            border: "none",
            boxShadow: "none",
         }}
      >
         <Stack
            direction="row"
            alignItems="center"
            justifyContent="space-between"
            sx={{ mb: 2 }}
         >
            <Typography
               variant="subtitle2"
               sx={{ fontWeight: 600, color: "#333" }}
            >
               Parameters
            </Typography>
            {hasValues && (
               <Button
                  variant="text"
                  size="small"
                  onClick={onClearAll}
                  sx={{ textTransform: "none" }}
               >
                  Reset
               </Button>
            )}
         </Stack>
         <Box
            sx={{
               display: "grid",
               gridTemplateColumns: "repeat(auto-fill, minmax(250px, 1fr))",
               gap: 3,
            }}
         >
            {givens.map((given) => (
               <Box key={given.name}>
                  <GivenInput
                     given={given}
                     value={given.name ? values.get(given.name) : undefined}
                     onChange={(next) =>
                        given.name && onChange(given.name, next)
                     }
                  />
               </Box>
            ))}
         </Box>
      </Paper>
   );
}
