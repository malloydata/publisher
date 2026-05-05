import { MoreVert } from "@mui/icons-material";
import Inventory2OutlinedIcon from "@mui/icons-material/Inventory2Outlined";
import {
   Box,
   Card,
   CardContent,
   Grid,
   IconButton,
   Menu,
   Tooltip,
   Typography,
} from "@mui/material";
import { useState } from "react";
import { Package } from "../../client";
import { useQueryWithApiError } from "../../hooks/useQueryWithApiError";
import { encodeResourceUri, parseResourceUri } from "../../utils/formatting";
import { ApiErrorDisplay } from "../ApiErrorDisplay";
import { Loading } from "../Loading";
import { useServer } from "../ServerProvider";
import DeletePackageDialog from "./DeletePackageDialog";
import EditPackageDialog from "./EditPackageDialog";

interface PackagesProps {
   onSelectPackage: (to: string, event?: React.MouseEvent) => void;
   resourceUri: string;
}

export default function Packages({
   onSelectPackage,
   resourceUri,
}: PackagesProps) {
   const { apiClients } = useServer();
   const { projectName } = parseResourceUri(resourceUri);
   const { data, isSuccess, isError, error } = useQueryWithApiError({
      queryKey: ["packages", projectName],
      queryFn: () => apiClients.packages.listPackages(projectName),
   });

   if (isError) {
      return (
         <ApiErrorDisplay error={error} context={`${projectName} > Packages`} />
      );
   }

   if (!isSuccess) {
      return <Loading text="Fetching Packages..." />;
   }

   const packages = [...data.data].sort((a, b) => a.name.localeCompare(b.name));

   return (
      <Grid container spacing={2}>
         {packages.map((pkg) => {
            const packageResourceUri = encodeResourceUri({
               projectName,
               packageName: pkg.name,
            });
            return (
               <Grid size={{ xs: 12, sm: 6, md: 4 }} key={pkg.name}>
                  <PackageCard
                     pkg={pkg}
                     packageResourceUri={packageResourceUri}
                     onSelectPackage={onSelectPackage}
                  />
               </Grid>
            );
         })}
      </Grid>
   );
}

function PackageCard({
   pkg,
   packageResourceUri,
   onSelectPackage,
}: {
   pkg: Package;
   packageResourceUri: string;
   onSelectPackage: (to: string, event?: React.MouseEvent) => void;
}) {
   const { mutable } = useServer();
   const [menuAnchorEl, setMenuAnchorEl] = useState<null | HTMLElement>(null);
   const menuOpen = Boolean(menuAnchorEl);

   const handleClick = (event: React.MouseEvent) => {
      onSelectPackage(pkg.name, event);
   };

   const handleMenuClick = (event: React.MouseEvent<HTMLElement>) => {
      event.stopPropagation();
      setMenuAnchorEl(event.currentTarget);
   };

   const handleMenuClose = () => {
      setMenuAnchorEl(null);
   };

   const description = pkg.description ?? "";

   return (
      <Card
         variant="outlined"
         onClick={handleClick}
         sx={{
            height: "100%",
            cursor: "pointer",
            borderRadius: 3,
            borderColor: "divider",
            boxShadow: "none",
            transition: "all 0.2s ease-in-out",
            "&:hover": { boxShadow: 2, borderColor: "primary.main" },
         }}
      >
         <CardContent sx={{ p: 2.5, "&:last-child": { pb: 2.5 } }}>
            <Box sx={{ display: "flex", alignItems: "flex-start", gap: 1.5 }}>
               <Box
                  sx={{
                     width: 36,
                     height: 36,
                     borderRadius: 1.5,
                     bgcolor: "grey.100",
                     display: "flex",
                     alignItems: "center",
                     justifyContent: "center",
                     flexShrink: 0,
                     color: "text.primary",
                  }}
               >
                  <Inventory2OutlinedIcon sx={{ fontSize: 20 }} />
               </Box>
               <Box sx={{ flex: 1, minWidth: 0 }}>
                  <Typography
                     variant="subtitle1"
                     noWrap
                     sx={{ fontWeight: 600, mb: 0.5 }}
                  >
                     {pkg.name}
                  </Typography>
                  <Tooltip title={description} followCursor enterDelay={1000}>
                     <Typography
                        variant="body2"
                        color="text.secondary"
                        sx={{
                           overflow: "hidden",
                           textOverflow: "ellipsis",
                           display: "-webkit-box",
                           WebkitLineClamp: 2,
                           WebkitBoxOrient: "vertical",
                           lineHeight: 1.5,
                        }}
                     >
                        {description}
                     </Typography>
                  </Tooltip>
               </Box>
               {mutable && (
                  <>
                     <IconButton
                        size="small"
                        onClick={handleMenuClick}
                        aria-label="Package options"
                        sx={{ flexShrink: 0, mt: -0.5, mr: -0.5 }}
                     >
                        <MoreVert fontSize="small" />
                     </IconButton>
                     <Menu
                        anchorEl={menuAnchorEl}
                        open={menuOpen}
                        onClose={handleMenuClose}
                        onClick={(e) => e.stopPropagation()}
                        anchorOrigin={{
                           vertical: "bottom",
                           horizontal: "right",
                        }}
                        transformOrigin={{
                           vertical: "top",
                           horizontal: "right",
                        }}
                     >
                        <EditPackageDialog
                           package={pkg}
                           resourceUri={packageResourceUri}
                           onCloseDialog={handleMenuClose}
                        />
                        <DeletePackageDialog
                           resourceUri={packageResourceUri}
                           onCloseDialog={handleMenuClose}
                        />
                     </Menu>
                  </>
               )}
            </Box>
         </CardContent>
      </Card>
   );
}
