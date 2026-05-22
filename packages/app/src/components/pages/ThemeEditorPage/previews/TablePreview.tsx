import { Box } from "@mui/material";

interface TablePreviewProps {
   background: string;
   headerColor: string;
   bodyColor: string;
   border: string;
   pinnedBackground: string;
   fontFamily: string;
   fontSize: number;
}

/**
 * Static 4-column 3-row sample table that paints with the editor's draft
 * table tokens. Mirrors the renderer's `.malloy-table` structure in
 * spirit (header band + body cells) without depending on a real result.
 */
export function TablePreview(props: TablePreviewProps) {
   const cells = [
      ["Q1", "$118,420", "+12%", "Levi's"],
      ["Q2", "$132,990", "+18%", "Carhartt"],
      ["Q3", "$104,210", "+ 7%", "Dockers"],
   ];
   const headers = ["quarter", "total_sales", "growth", "top_brand"];

   return (
      <Box
         sx={{
            backgroundColor: props.background,
            borderRadius: 1,
            border: props.border,
            fontFamily: props.fontFamily,
            fontSize: props.fontSize,
            color: props.bodyColor,
            overflow: "hidden",
            display: "inline-block",
         }}
         aria-label="Table preview"
      >
         <Box
            sx={{
               display: "grid",
               gridTemplateColumns: "repeat(4, minmax(0, 1fr))",
            }}
         >
            {headers.map((h) => (
               <Box
                  key={h}
                  sx={{
                     backgroundColor: props.pinnedBackground,
                     color: props.headerColor,
                     fontWeight: 600,
                     px: 1.5,
                     py: 1,
                     borderBottom: props.border,
                  }}
               >
                  {h}
               </Box>
            ))}
            {cells.flatMap((row, rIdx) =>
               row.map((cell, cIdx) => (
                  <Box
                     key={`${rIdx}-${cIdx}`}
                     sx={{
                        px: 1.5,
                        py: 1,
                        borderBottom:
                           rIdx < cells.length - 1 ? props.border : "none",
                     }}
                  >
                     {cell}
                  </Box>
               )),
            )}
         </Box>
      </Box>
   );
}
