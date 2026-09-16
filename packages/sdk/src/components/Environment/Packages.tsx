// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { MoreVert } from "@mui/icons-material";
import Inventory2OutlinedIcon from "@mui/icons-material/Inventory2Outlined";
import { IconButton, Menu, Stack } from "@mui/material";
import { useState } from "react";
import { Package } from "../../client";
import { useQueryWithApiError } from "../../hooks/useQueryWithApiError";
import { encodeResourceUri, parseResourceUri } from "../../utils/formatting";
import { ApiErrorDisplay } from "../ApiErrorDisplay";
import { ItemRow } from "../ItemRow";
import { SURFACE_TINT } from "../styles";
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
   const { environmentName } = parseResourceUri(resourceUri);
   const { data, isSuccess, isError, error } = useQueryWithApiError({
      queryKey: ["packages", environmentName],
      queryFn: () => apiClients.packages.listPackages(environmentName),
   });

   if (isError) {
      return (
         <ApiErrorDisplay
            error={error}
            context={`${environmentName} > Packages`}
         />
      );
   }

   if (!isSuccess) {
      return <Loading text="Fetching Packages..." />;
   }

   const packages = [...data.data].sort((a, b) => a.name.localeCompare(b.name));

   return (
      <Stack>
         {packages.map((pkg) => {
            const packageResourceUri = encodeResourceUri({
               environmentName,
               packageName: pkg.name,
            });
            return (
               <PackageRow
                  key={pkg.name}
                  pkg={pkg}
                  packageResourceUri={packageResourceUri}
                  onSelectPackage={onSelectPackage}
               />
            );
         })}
      </Stack>
   );
}

function PackageRow({
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

   const handleMenuClick = (event: React.MouseEvent<HTMLElement>) => {
      event.stopPropagation();
      setMenuAnchorEl(event.currentTarget);
   };
   const handleMenuClose = () => setMenuAnchorEl(null);

   return (
      <ItemRow
         icon={<Inventory2OutlinedIcon sx={{ fontSize: 18 }} />}
         tint={SURFACE_TINT.package}
         label={pkg.name}
         ariaLabel={pkg.name}
         {...(pkg.description ? { description: pkg.description } : {})}
         onClick={(event) => onSelectPackage(pkg.name, event)}
         {...(mutable
            ? {
                 trailingAction: (
                    <>
                       <IconButton
                          size="small"
                          onClick={handleMenuClick}
                          aria-label={`Package actions for ${pkg.name}`}
                       >
                          <MoreVert fontSize="small" />
                       </IconButton>
                       <Menu
                          anchorEl={menuAnchorEl}
                          open={menuOpen}
                          onClose={handleMenuClose}
                          onClick={(event) => event.stopPropagation()}
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
                 ),
              }
            : {})}
      />
   );
}
