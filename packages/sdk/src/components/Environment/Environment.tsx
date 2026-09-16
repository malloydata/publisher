// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { Box, Container, Typography } from "@mui/material";
import { useEffect } from "react";
import { useQueryWithApiError } from "../../hooks/useQueryWithApiError";
import { parseResourceUri } from "../../utils/formatting";
import { BackLink } from "../BackLink";
import { PackageSection } from "../PackageSection";
import { useServer } from "../ServerProvider";
import About from "./About";
import AddPackageDialog from "./AddPackageDialog";
import Connections from "./Connections";
import Packages from "./Packages";

interface EnvironmentProps {
   onSelectPackage: (to: string, event?: React.MouseEvent) => void;
   resourceUri: string;
}

export default function Environment({
   onSelectPackage,
   resourceUri,
}: EnvironmentProps) {
   const { apiClients, mutable } = useServer();
   const { environmentName } = parseResourceUri(resourceUri);
   // Only for the heading's count: the list below runs the same query under the
   // same key, so this shares its cache rather than fetching twice.
   const packages = useQueryWithApiError({
      queryKey: ["packages", environmentName],
      queryFn: () => apiClients.packages.listPackages(environmentName),
   });

   useEffect(() => {
      window.scrollTo({ top: 0, behavior: "auto" });
   }, []);

   return (
      <Container
         maxWidth={false}
         sx={{ maxWidth: 1024, mx: "auto", px: 3, py: 6 }}
      >
         <Box sx={{ mb: 4 }}>
            {/* The environment's parent is the server itself, which is what
                home lists. `onSelectPackage` is this component's one navigation
                hook; the name is narrow but the job is not. */}
            <Box>
               <BackLink
                  label="Publisher"
                  onClick={(event) => onSelectPackage("/", event)}
               />
            </Box>
            <Typography
               variant="h4"
               component="h1"
               sx={{ fontWeight: 600, letterSpacing: "-0.025em", mb: 0.5 }}
            >
               {environmentName}
            </Typography>
            <Typography variant="body2" color="text.secondary">
               Manage packages and database connections in this environment.
               Open a package to explore its models and notebooks.
            </Typography>
         </Box>

         <PackageSection
            title="Packages"
            {...(packages.isSuccess
               ? { count: packages.data.data.length }
               : {})}
            description="Published packages available for use in this environment"
            {...(mutable
               ? { action: <AddPackageDialog resourceUri={resourceUri} /> }
               : {})}
         >
            <Packages
               onSelectPackage={onSelectPackage}
               resourceUri={resourceUri}
            />
         </PackageSection>

         <Connections resourceUri={resourceUri} />

         <About resourceUri={resourceUri} />
      </Container>
   );
}
