import { MoreVert } from "@mui/icons-material";
import {
   Box,
   Divider,
   Grid,
   IconButton,
   Menu,
   Typography,
} from "@mui/material";
import { useState } from "react";
import { Package } from "../../client";
import { useQueryWithApiError } from "../../hooks/useQueryWithApiError";
import { encodeResourceUri, parseResourceUri } from "../../utils/formatting";
import { ApiErrorDisplay } from "../ApiErrorDisplay";
import { Loading } from "../Loading";
import { useServer } from "../ServerProvider";
import { PackageCard, PackageCardContent } from "../styles";
import DeletePackageDialog from "./DeletePackageDialog";
import EditPackageDialog from "./EditPackageDialog";

interface PackagesProps {
   onSelectPackage: (to: string, event?: React.MouseEvent) => void;
   resourceUri: string;
}

const PackageMenu = ({
   package: p,
   resourceUri: packageResourceUri,
}: {
   package: Package;
   resourceUri: string;
}) => {
   const { mutable } = useServer();
   const [menuAnchorEl, setMenuAnchorEl] = useState<null | HTMLElement>(null);
   const isMenuOpen = Boolean(menuAnchorEl);
   const openMenu = (event: React.MouseEvent<HTMLElement>) => {
      setMenuAnchorEl(event.currentTarget);
   };
   const closeMenu = () => {
      setMenuAnchorEl(null);
   };

   return (
      <>
         {mutable && (
            <>
               <IconButton
                  onClick={(event) => {
                     event.stopPropagation();
                     openMenu(event);
                  }}
                  aria-controls={isMenuOpen ? "package-menu" : undefined}
                  aria-haspopup="true"
                  aria-expanded={isMenuOpen ? "true" : undefined}
               >
                  <MoreVert fontSize="small" />
               </IconButton>
               <Menu
                  id="package-menu"
                  aria-haspopup="true"
                  aria-expanded={isMenuOpen ? "true" : undefined}
                  open={isMenuOpen}
                  anchorEl={menuAnchorEl}
                  onClose={closeMenu}
                  disableRestoreFocus
                  anchorOrigin={{
                     vertical: "top",
                     horizontal: "left",
                  }}
                  transformOrigin={{
                     vertical: "top",
                     horizontal: "right",
                  }}
                  onClick={(event) => {
                     event.stopPropagation();
                  }}
               >
                  <EditPackageDialog
                     package={p}
                     resourceUri={packageResourceUri}
                     onCloseDialog={closeMenu}
                  />
                  <DeletePackageDialog
                     resourceUri={packageResourceUri}
                     onCloseDialog={closeMenu}
                  />
               </Menu>
            </>
         )}
      </>
   );
};

export default function Packages({
   onSelectPackage,
   resourceUri,
}: PackagesProps) {
   const { apiClients } = useServer();
   const { projectName: projectName } = parseResourceUri(resourceUri);
   const { data, isSuccess, isError, error } = useQueryWithApiError({
      queryKey: ["packages", projectName],
      queryFn: () => apiClients.packages.listPackages(projectName),
   });

   return (
      <>
         {!isSuccess && !isError && <Loading text="Fetching Packages..." />}
         {isSuccess && (
            <Grid container spacing={3} columns={12}>
               {data.data
                  .sort((a, b) => {
                     return a.name.localeCompare(b.name);
                  })
                  .map((p) => {
                     const packageResourceUri = encodeResourceUri({
                        projectName,
                        packageName: p.name,
                     });
                     return (
                        <Grid
                           size={{ xs: 12, sm: 12, md: 12, lg: 4 }}
                           key={p.name}
                        >
                           <PackageCard
                              sx={{
                                 cursor: "pointer",
                                 transition: "all 0.2s ease-in-out",
                                 paddingTop: "8px",
                                 "&:hover": {
                                    boxShadow: "0 2px 6px rgba(0, 0, 0, 0.08)",
                                    transform: "translateY(-1px)",
                                 },
                              }}
                              onClick={(event) => {
                                 onSelectPackage(p.name, event);
                              }}
                           >
                              <PackageCardContent>
                                 <Box
                                    display="flex"
                                    alignItems="center"
                                    justifyContent="space-between"
                                    sx={{
                                       marginBottom: "8px",
                                    }}
                                 >
                                    <Typography
                                       variant="overline"
                                       sx={{
                                          fontSize: "12px",
                                          fontWeight: "600",
                                          color: "primary.main",
                                          textTransform: "uppercase",
                                          letterSpacing: "0.5px",
                                       }}
                                    >
                                       {p.name}
                                    </Typography>
                                    <PackageMenu
                                       package={p}
                                       resourceUri={packageResourceUri}
                                    />
                                 </Box>
                                 <Divider sx={{ mb: 2 }} />
                                 <Box
                                    sx={{
                                       maxHeight: "120px",
                                       overflowY: "auto",
                                       mt: 2,
                                    }}
                                 >
                                    <Typography variant="body2">
                                       {p.description}
                                    </Typography>
                                 </Box>
                              </PackageCardContent>
                           </PackageCard>
                        </Grid>
                     );
                  })}
            </Grid>
         )}
         {isError && (
            <ApiErrorDisplay
               error={error}
               context={`${projectName} > Packages`}
            />
         )}
      </>
   );
}
