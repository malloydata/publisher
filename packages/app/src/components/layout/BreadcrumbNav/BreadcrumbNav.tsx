import { useRouterClickHandler } from "@malloy-publisher/sdk";
import ChevronRight from "@mui/icons-material/ChevronRight";
import Box from "@mui/material/Box";
import Breadcrumbs from "@mui/material/Breadcrumbs";
import Chip from "@mui/material/Chip";
import { MouseEvent } from "react";
import { useParams } from "react-router-dom";

interface BreadcrumbChipProps {
   label: string;
   onClick: (event: MouseEvent) => void;
}

function BreadcrumbChip({ label, onClick }: BreadcrumbChipProps) {
   return (
      <Chip
         clickable
         onClick={onClick}
         label={label}
         size="small"
         aria-label={`Navigate to ${label}`}
         sx={{
            backgroundColor: "background.paper",
            color: "text.primary",
            fontWeight: 500,
            fontSize: "0.875rem",
            height: 32,
            cursor: "pointer",
            borderRadius: "4px",
            maxWidth: 320,
            "& .MuiChip-label": {
               overflow: "hidden",
               textOverflow: "ellipsis",
               whiteSpace: "nowrap",
            },
            "&:hover": {
               backgroundColor: "grey.100",
            },
         }}
      />
   );
}

export default function BreadcrumbNav() {
   const params = useParams();
   const modelPath = params["*"];
   const navigate = useRouterClickHandler();

   if (!params.projectName && !params.packageName && !modelPath) {
      return null;
   }

   return (
      <Box sx={{ display: "flex", alignItems: "center", minWidth: 0 }}>
         <Breadcrumbs
            aria-label="breadcrumb"
            separator={
               <ChevronRight sx={{ fontSize: 14, color: "text.secondary" }} />
            }
         >
            {params.projectName && (
               <BreadcrumbChip
                  label={params.projectName}
                  onClick={(event) =>
                     navigate(`/${params.projectName}/`, event)
                  }
               />
            )}

            {params.packageName && (
               <BreadcrumbChip
                  label={params.packageName}
                  onClick={(event) =>
                     navigate(
                        `/${params.projectName}/${params.packageName}/`,
                        event,
                     )
                  }
               />
            )}

            {modelPath && (
               <BreadcrumbChip
                  label={modelPath}
                  onClick={(event) =>
                     navigate(
                        `/${params.projectName}/${params.packageName}/${modelPath}`,
                        event,
                     )
                  }
               />
            )}
         </Breadcrumbs>
      </Box>
   );
}
