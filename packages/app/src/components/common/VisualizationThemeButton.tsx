// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import PaletteOutlinedIcon from "@mui/icons-material/PaletteOutlined";
import { IconButton, Tooltip } from "@mui/material";
import { useLocation, useNavigate } from "react-router-dom";

const LABEL = "Visualization theme";

/**
 * The way to the visualization theme editor, as a palette in the header beside
 * the light/dark toggle.
 *
 * It belongs with the mode toggle rather than in the sidebar: both are viewer
 * preferences about how the same data is drawn, and the sidebar is for
 * navigating the data itself. As an icon it needs no label taking up the width
 * — the tooltip names it on hover, and `aria-label` names it the rest of the
 * time.
 */
export function VisualizationThemeButton() {
   const navigate = useNavigate();
   const location = useLocation();
   const selected = location.pathname.startsWith("/settings/theme");

   return (
      <Tooltip title={LABEL}>
         <IconButton
            aria-label={LABEL}
            onClick={() => navigate("/settings/theme")}
            size="small"
            // Lit while its page is open, the way a selected sidebar row was.
            color={selected ? "primary" : "default"}
         >
            <PaletteOutlinedIcon fontSize="small" />
         </IconButton>
      </Tooltip>
   );
}
