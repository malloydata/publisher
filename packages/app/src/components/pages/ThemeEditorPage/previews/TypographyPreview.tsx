import { Box, Typography } from "@mui/material";

interface TypographyPreviewProps {
   fontFamily: string;
   fontSize: number;
}

/**
 * Heading + body sample showing the editor's draft font choices. Renders
 * mode-agnostic on a neutral background so the font choice is the only
 * variable on screen.
 */
export function TypographyPreview(props: TypographyPreviewProps) {
   return (
      <Box
         sx={{
            fontFamily: props.fontFamily,
            display: "inline-flex",
            flexDirection: "column",
            gap: 0.5,
            px: 2,
            py: 1.5,
         }}
         aria-label="Typography preview"
      >
         <Typography
            sx={{
               fontFamily: "inherit",
               fontWeight: 600,
               fontSize: props.fontSize + 6,
            }}
         >
            Sample heading
         </Typography>
         <Typography sx={{ fontFamily: "inherit", fontSize: props.fontSize }}>
            The quick brown fox jumps over the lazy dog · 12,345.67
         </Typography>
      </Box>
   );
}
