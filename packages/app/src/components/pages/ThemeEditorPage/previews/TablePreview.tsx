// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { Box } from "@mui/material";

interface TablePreviewProps {
   /** Table interior background (cells body). */
   background: string;
   /** Header text color. */
   headerColor: string;
   /** Header row background (the band at the top of the table). */
   headerBackground: string;
   /** Body cell text color. */
   bodyColor: string;
   /** Gridline between rows, as a border shorthand. */
   border: string;
   /** The card's edge, as a border shorthand. */
   cardBorder: string;
   /** The rule under the (pinned) header row, as a border shorthand. */
   pinnedBorder: string;
   /** Padding/wrapper around the table (the dashboard tile colour). */
   tileBackground: string;
   fontFamily: string;
   fontSize: number;
}

/**
 * Static 4-column 3-row sample table that paints with the editor's draft
 * table tokens. Mirrors the actual rendered structure: a tile wrapper
 * (padding) surrounds an inner table with its own header band.
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
            // Outer wrapper paints the tile colour around the table to
            // mirror what the operator sees on the actual package page.
            backgroundColor: props.tileBackground,
            border: props.cardBorder,
            padding: 1.5,
            borderRadius: 1.5,
            display: "inline-block",
         }}
         aria-label="Table preview"
      >
         <Box
            sx={{
               backgroundColor: props.background,
               borderRadius: 1,
               border: props.border,
               fontFamily: props.fontFamily,
               fontSize: props.fontSize,
               color: props.bodyColor,
               overflow: "hidden",
            }}
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
                        backgroundColor: props.headerBackground,
                        color: props.headerColor,
                        fontWeight: 600,
                        px: 1.5,
                        py: 1,
                        borderBottom: props.pinnedBorder,
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
      </Box>
   );
}
