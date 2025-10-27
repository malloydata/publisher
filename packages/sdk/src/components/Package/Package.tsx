import { Box, Grid } from "@mui/material";
import { encodeResourceUri, parseResourceUri } from "../../utils/formatting";
import { Notebook } from "../Notebook";
import {
   PackageCard,
   PackageCardContent,
   PackageSectionTitle,
} from "../styles";
import Config from "./Config";
import Connections from "./Connections";
import Databases from "./Databases";
import Models from "./Models";
import Notebooks from "./Notebooks";

const README_NOTEBOOK = "README.malloynb";

interface PackageProps {
   onClickPackageFile?: (to: string, event?: React.MouseEvent) => void;
   resourceUri: string;
}

export default function Package({
   onClickPackageFile,
   resourceUri,
}: PackageProps) {
   if (!onClickPackageFile) {
      onClickPackageFile = (to: string) => {
         window.location.href = to;
      };
   }

   const readmeResourceUri = encodeResourceUri({
      ...parseResourceUri(resourceUri),
      modelPath: README_NOTEBOOK,
   });

   return (
      <>
         <Grid container spacing={3} columns={12}>
            <Grid size={{ xs: 12, md: 4 }}>
               <Config resourceUri={resourceUri} />
            </Grid>
            <Grid size={{ xs: 12, md: 4 }}>
               <Notebooks
                  onClickNotebookFile={onClickPackageFile}
                  resourceUri={resourceUri}
               />
            </Grid>
            <Grid size={{ xs: 12, md: 4 }}>
               <Models
                  onClickModelFile={onClickPackageFile}
                  resourceUri={resourceUri}
               />
            </Grid>
            <Grid size={{ xs: 12, md: 6 }}>
               <Databases resourceUri={resourceUri} />
            </Grid>
            <Grid size={{ xs: 12, md: 6 }}>
               <Connections resourceUri={resourceUri} />
            </Grid>
            <Grid size={{ xs: 12, md: 12 }}>
               <PackageCard>
                  <PackageCardContent>
                     <PackageSectionTitle>README</PackageSectionTitle>
                     <Box sx={{ mt: 1 }}>
                        <Notebook resourceUri={readmeResourceUri} />
                     </Box>
                  </PackageCardContent>
               </PackageCard>
            </Grid>
         </Grid>
      </>
   );
}
