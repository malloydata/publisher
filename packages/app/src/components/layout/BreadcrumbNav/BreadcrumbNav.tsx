// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { useRouterClickHandler } from "@malloy-publisher/sdk";
import Box from "@mui/material/Box";
import Breadcrumbs from "@mui/material/Breadcrumbs";
import Chip from "@mui/material/Chip";
import { MouseEvent } from "react";
import { useParams } from "react-router-dom";

interface BreadcrumbChipProps {
   label: string;
   onClick: (event: MouseEvent) => void;
   /** The page you are on: full-strength text, where the path above it is muted. */
   current?: boolean;
}

function BreadcrumbChip({
   label,
   onClick,
   current = false,
}: BreadcrumbChipProps) {
   return (
      <Chip
         clickable
         onClick={onClick}
         label={label}
         size="small"
         aria-label={`Navigate to ${label}`}
         aria-current={current ? "page" : undefined}
         sx={(theme) => ({
            // Text, not a row of outlined boxes: a breadcrumb is a path, and
            // each crumb drawn as a control made the top of every page a row
            // of buttons. The hover fill still says each one is a link.
            backgroundColor: "transparent",
            border: "none",
            color: current ? "text.primary" : "text.secondary",
            fontWeight: current ? 500 : 400,
            fontSize: 13,
            height: 28,
            cursor: "pointer",
            borderRadius: "6px",
            maxWidth: 320,
            "& .MuiChip-label": {
               px: 0.75,
               overflow: "hidden",
               textOverflow: "ellipsis",
               whiteSpace: "nowrap",
            },
            "&:hover": {
               // grey.100 in our palette is a near-white #F4F3F1, which
               // matches the light-mode text colour at hover and makes
               // the label invisible in dark mode. Use a translucent
               // white in dark and the off-white grey in light so the
               // chip stays legible in both modes.
               backgroundColor:
                  theme.palette.mode === "dark"
                     ? "rgba(255, 255, 255, 0.08)"
                     : "grey.100",
            },
         })}
      />
   );
}

export default function BreadcrumbNav() {
   const params = useParams();
   // The builder is `?edit`, a mode, not a place: the path names the document
   // in either mode, and a report may be named `edit`.
   const modelPath = params["*"] ? params["*"].replace(/\/$/, "") : undefined;
   const navigate = useRouterClickHandler();

   if (!params.environmentName && !params.packageName && !modelPath) {
      return null;
   }

   return (
      <Box sx={{ display: "flex", alignItems: "center", minWidth: 0 }}>
         <Breadcrumbs
            aria-label="breadcrumb"
            separator={
               <Box
                  component="span"
                  aria-hidden
                  sx={{ color: "text.disabled" }}
               >
                  /
               </Box>
            }
         >
            {params.environmentName && (
               <BreadcrumbChip
                  label={params.environmentName}
                  current={!params.packageName && !modelPath}
                  onClick={(event) =>
                     navigate(`/${params.environmentName}/`, event)
                  }
               />
            )}

            {params.packageName && (
               <BreadcrumbChip
                  label={params.packageName}
                  current={!modelPath}
                  onClick={(event) =>
                     navigate(
                        `/${params.environmentName}/${params.packageName}/`,
                        event,
                     )
                  }
               />
            )}

            {modelPath && (
               <BreadcrumbChip
                  label={modelPath}
                  current
                  onClick={(event) =>
                     navigate(
                        `/${params.environmentName}/${params.packageName}/${modelPath}`,
                        event,
                     )
                  }
               />
            )}
         </Breadcrumbs>
      </Box>
   );
}
