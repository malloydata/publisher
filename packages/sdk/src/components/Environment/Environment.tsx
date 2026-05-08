import { Box, Container, Stack, Typography } from "@mui/material";
import { useEffect } from "react";
import { parseResourceUri } from "../../utils/formatting";
import { useServer } from "../ServerProvider";
import About from "./About";
import AddPackageDialog from "./AddPackageDialog";
import Packages from "./Packages";

interface EnvironmentProps {
   onSelectPackage: (to: string, event?: React.MouseEvent) => void;
   resourceUri: string;
}

export default function Environment({
   onSelectPackage,
   resourceUri,
}: EnvironmentProps) {
   const { mutable } = useServer();
   const { environmentName } = parseResourceUri(resourceUri);

   useEffect(() => {
      window.scrollTo({ top: 0, behavior: "auto" });
   }, []);

   return (
      <Container
         maxWidth={false}
         sx={{ maxWidth: 1024, mx: "auto", px: 4, py: 3 }}
      >
         <Box sx={{ mb: 5 }}>
            <Typography
               variant="h4"
               component="h1"
               sx={{ fontWeight: 600, letterSpacing: "-0.025em", mb: 0.5 }}
            >
               {environmentName}
            </Typography>
            <Typography variant="body2" color="text.secondary">
               Manage packages in this environment. Open a package to explore
               its models, notebooks, and connections.
            </Typography>
         </Box>

         <Box sx={{ mb: 5 }}>
            <Stack
               direction="row"
               justifyContent="space-between"
               alignItems="flex-start"
               sx={{ mb: 3 }}
            >
               <Box>
                  <Typography
                     variant="h6"
                     sx={{ fontWeight: 600, letterSpacing: "-0.025em" }}
                  >
                     Packages
                  </Typography>
                  <Typography variant="body2" color="text.secondary">
                     Published packages available for use in this environment
                  </Typography>
               </Box>
               {mutable && <AddPackageDialog resourceUri={resourceUri} />}
            </Stack>
            <Packages
               onSelectPackage={onSelectPackage}
               resourceUri={resourceUri}
            />
         </Box>

         <About resourceUri={resourceUri} />
      </Container>
   );
}
