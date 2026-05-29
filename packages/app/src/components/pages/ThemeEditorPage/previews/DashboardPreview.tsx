import { Box, Typography } from "@mui/material";

interface DashboardPreviewProps {
   tileBackground: string;
   tileBorder: string;
   titleColor: string;
   valueColor: string;
   fontFamily: string;
}

/**
 * Two-tile dashboard mock: a label + big value, matching the renderer's
 * `.dashboard-item` / `.dashboard-item-title` / `.dashboard-item-value`
 * shape. Paints with the editor's draft dashboard tokens.
 */
export function DashboardPreview(props: DashboardPreviewProps) {
   const tiles = [
      { label: "total_sales", value: "$12.5M" },
      { label: "order_count", value: "264.1k" },
   ];

   return (
      <Box
         sx={{
            display: "inline-flex",
            gap: 1,
            fontFamily: props.fontFamily,
         }}
         aria-label="Dashboard preview"
      >
         {tiles.map((tile) => (
            <Box
               key={tile.label}
               sx={{
                  backgroundColor: props.tileBackground,
                  border: props.tileBorder,
                  borderRadius: 0.5,
                  px: 2,
                  py: 1.5,
                  minWidth: 140,
                  display: "flex",
                  flexDirection: "column",
                  gap: 0.5,
               }}
            >
               <Typography
                  variant="caption"
                  sx={{ color: props.titleColor, fontWeight: 400 }}
               >
                  {tile.label}
               </Typography>
               <Typography
                  variant="h6"
                  sx={{ color: props.valueColor, fontWeight: 500 }}
               >
                  {tile.value}
               </Typography>
            </Box>
         ))}
      </Box>
   );
}
