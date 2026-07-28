import { DataAppViewer, encodeResourceUri, Model } from "@malloy-publisher/sdk";
import Box from "@mui/material/Box";
import { useParams } from "react-router-dom";
import DashboardPage from "../DashboardPage/DashboardPage";
import NotebookPage from "../NotebookPage/NotebookPage";

function ModelPage() {
   const params = useParams();
   const modelPath = params["*"];
   if (!params.environmentName) {
      return (
         <div>
            <h2>Missing environment name</h2>
         </div>
      );
   }
   if (!params.packageName) {
      return (
         <div>
            <h2>Missing package name</h2>
         </div>
      );
   }

   const wrapperSx = { p: 3, maxWidth: 1200, mx: "auto" } as const;

   // Dashboard viewer. `dashboards/overview` is the dashboard; the file it is
   // declared in, `dashboards/overview.malloy`, keeps opening in the Model
   // view, which is the author's "view the Malloy" path. Branching on the
   // extension here rather than declaring a literal `dashboards/:name` route
   // is what keeps those two apart: a route param matches any single segment,
   // dots included, so the literal route would have swallowed the file path
   // too. Same reasoning as the `data-apps/` branch below.
   if (
      modelPath?.startsWith("dashboards/") &&
      !modelPath.endsWith(".malloy") &&
      !modelPath.endsWith(".malloynb")
   ) {
      return (
         <DashboardPage
            environmentName={params.environmentName}
            packageName={params.packageName}
            dashboardName={modelPath.slice("dashboards/".length)}
         />
      );
   }

   // In-package HTML data app (embedded view). The Data Apps section in
   // <Package> routes clicks to `data-apps/<file>` so this branch picks them
   // up. <DataAppViewer> iframes the standalone Publisher URL and resizes
   // via the publisher.js postMessage protocol. Real models that live under
   // a `data-apps/` subdirectory (e.g. `data-apps/x.malloy`) are excluded so
   // they still open in the Model/Notebook viewer.
   if (
      modelPath?.startsWith("data-apps/") &&
      !modelPath.endsWith(".malloy") &&
      !modelPath.endsWith(".malloynb")
   ) {
      const dataAppPath = modelPath.slice("data-apps/".length);
      const dataAppResourceUri = encodeResourceUri({
         environmentName: params.environmentName,
         packageName: params.packageName,
         modelPath: dataAppPath,
      });
      return <DataAppViewer resourceUri={dataAppResourceUri} />;
   }

   const resourceUri = encodeResourceUri({
      environmentName: params.environmentName,
      packageName: params.packageName,
      modelPath,
   });

   if (modelPath?.endsWith(".malloy")) {
      return (
         <Box sx={wrapperSx}>
            <Model
               resourceUri={resourceUri}
               runOnDemand={true}
               maxResultSize={512 * 1024}
            />
         </Box>
      );
   }
   if (modelPath?.endsWith(".malloynb")) {
      return (
         <NotebookPage
            environmentName={params.environmentName}
            packageName={params.packageName}
            notebookPath={modelPath}
         />
      );
   }
   return (
      <Box sx={wrapperSx}>
         <h2>Unrecognized file type: {modelPath}</h2>
      </Box>
   );
}

export default ModelPage;
