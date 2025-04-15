import { Grid2 } from "@mui/material";
import { Notebook } from "../Notebook";
import Config from "./Config";
import Models from "./Models";
import Databases from "./Databases";
import Schedules from "./Schedules";

const README_NOTEBOOK = "README.malloynb";

interface PackageProps {
   server?: string;
   projectName: string;
   packageName: string;
   versionId?: string;
   navigate?: (to: string) => void;
   accessToken?: string;
}

export default function Package({
   server,
   projectName,
   packageName,
   versionId,
   navigate,
   accessToken,
}: PackageProps) {
   if (!navigate) {
      navigate = (to: string) => {
         window.location.href = to;
      };
   }

   return (
      <Grid2
         container
         spacing={2}
         columns={12}
         sx={{ mb: (theme) => theme.spacing(2) }}
      >
         <Grid2 size={{ xs: 12, md: 12, lg: 4 }}>
            <Config
               server={server}
               projectName={projectName}
               packageName={packageName}
               versionId={versionId}
               accessToken={accessToken}
            />
         </Grid2>
         <Grid2 size={{ xs: 12, md: 12, lg: 4 }}>
            <Models
               server={server}
               projectName={projectName}
               packageName={packageName}
               versionId={versionId}
               navigate={navigate}
               accessToken={accessToken}
            />
         </Grid2>
         <Grid2 size={{ xs: 12, md: 12, lg: 4 }}>
            <Databases
               server={server}
               projectName={projectName}
               packageName={packageName}
               versionId={versionId}
               accessToken={accessToken}
            />
         </Grid2>
         <Grid2 size={{ xs: 12, md: 12 }}>
            <Schedules
               server={server}
               projectName={projectName}
               packageName={packageName}
               versionId={versionId}
               accessToken={accessToken}
            />
         </Grid2>
         <Grid2 size={{ xs: 12, md: 12 }}>
            <Notebook
               server={server}
               projectName={projectName}
               packageName={packageName}
               notebookPath={README_NOTEBOOK}
               versionId={versionId}
               expandCodeCells={true}
               accessToken={accessToken}
            />
         </Grid2>
      </Grid2>
   );
}
