import { Box, Grid, Typography } from "@mui/material";
import { parseResourceUri } from "../../utils/formatting";
import { useServer } from "../ServerProvider";
import { PackageContainer } from "../styles";
import About from "./About";
import AddPackageDialog from "./AddPackageDialog";
import Packages from "./Packages";
import { useEffect } from "react";

interface ProjectProps {
   onSelectPackage: (to: string, event?: React.MouseEvent) => void;
   resourceUri: string;
}

export default function Project({
   onSelectPackage,
   resourceUri,
}: ProjectProps) {
   const { mutable } = useServer();
   const { projectName } = parseResourceUri(resourceUri);

   useEffect(() => {
      window.scrollTo({ top: 0, behavior: "auto" });
   }, []);

   return (
      <>
         <PackageContainer>
            <Box
               display="flex"
               justifyContent="space-between"
               alignItems="center"
               mb={2}
            >
               <Typography variant="h6">{projectName} packages</Typography>
               {mutable && <AddPackageDialog resourceUri={resourceUri} />}
            </Box>
            <Grid container spacing={3} columns={12}>
               <Grid size={{ xs: 12, md: 12 }}>
                  <Packages
                     onSelectPackage={onSelectPackage}
                     resourceUri={resourceUri}
                  />
               </Grid>
               <Grid size={{ xs: 12, md: 12 }}>
                  <About resourceUri={resourceUri} />
               </Grid>
            </Grid>
         </PackageContainer>
      </>
   );
}
