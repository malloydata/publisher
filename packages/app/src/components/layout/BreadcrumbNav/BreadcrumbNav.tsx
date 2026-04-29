import { useRouterClickHandler } from "@malloy-publisher/sdk";
import { ChevronRight } from "@mui/icons-material";
import { Box, Chip } from "@mui/material";
import Breadcrumbs from "@mui/material/Breadcrumbs";
import { useParams } from "react-router-dom";

export default function BreadcrumbNav() {
   const params = useParams();
   const modelPath = params["*"];
   const navigate = useRouterClickHandler();

   return (
      <Box sx={{ display: "flex", alignItems: "center" }}>
         <Breadcrumbs
            aria-label="breadcrumb"
            separator={
               <ChevronRight sx={{ fontSize: 14, color: "text.secondary" }} />
            }
            sx={{
               "& .MuiBreadcrumbs-separator": {
                  margin: "0 6px",
               },
            }}
         >
            {params.projectName && (
               <Chip
                  onClick={(event) =>
                     navigate(`/${params.projectName}/`, event)
                  }
                  label={params.projectName}
                  size="medium"
                  sx={{
                     backgroundColor: "white",
                     color: "primary.main",
                     fontWeight: 500,
                     height: "32px",
                     fontSize: "1rem",
                     cursor: "pointer",
                     "&:hover": {
                        backgroundColor: "primary.100",
                     },
                  }}
               />
            )}

            {params.packageName && (
               <Chip
                  onClick={(event) =>
                     navigate(
                        `/${params.projectName}/${params.packageName}/`,
                        event,
                     )
                  }
                  label={params.packageName}
                  size="medium"
                  sx={{
                     backgroundColor: "white",
                     color: "primary.main",
                     fontWeight: 500,
                     height: "32px",
                     fontSize: "1rem",
                     cursor: "pointer",
                     "&:hover": {
                        backgroundColor: "secondary.100",
                     },
                  }}
               />
            )}

            {modelPath && (
               <Chip
                  onClick={(event) =>
                     navigate(
                        `/${params.projectName}/${params.packageName}/${modelPath}`,
                        event,
                     )
                  }
                  label={modelPath}
                  size="medium"
                  sx={{
                     backgroundColor: "white",
                     color: "primary.main",
                     fontWeight: 500,
                     height: "32px",
                     fontSize: "1rem",
                     cursor: "pointer",
                     "&:hover": {
                        backgroundColor: "grey.200",
                     },
                  }}
               />
            )}
         </Breadcrumbs>
      </Box>
   );
}
