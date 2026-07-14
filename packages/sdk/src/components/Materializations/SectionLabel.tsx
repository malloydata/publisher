import { Typography } from "@mui/material";
import { ReactNode } from "react";

/**
 * Small uppercase section heading for consistent hierarchy across the
 * materialization views (detail dialog, manifest view). One source of truth for
 * the style so the headings can't drift.
 */
export default function SectionLabel({ children }: { children: ReactNode }) {
   return (
      <Typography
         variant="caption"
         sx={{
            display: "block",
            textTransform: "uppercase",
            letterSpacing: "0.06em",
            fontWeight: 600,
            color: "text.secondary",
            mb: 1,
         }}
      >
         {children}
      </Typography>
   );
}
