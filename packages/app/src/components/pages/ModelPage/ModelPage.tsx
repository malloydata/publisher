import {
   encodeResourceUri,
   Model,
   Notebook,
   PageViewer,
   useRouterClickHandler,
} from "@malloy-publisher/sdk";
import Box from "@mui/material/Box";
import { useParams } from "react-router-dom";

function ModelPage() {
   const params = useParams();
   const modelPath = params["*"];
   const navigate = useRouterClickHandler();
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

   // In-package HTML page (embedded view). The Pages section in
   // <Package> routes clicks to `pages/<file>` so this branch picks them
   // up. <PageViewer> iframes the standalone Publisher URL and resizes
   // via the publisher.js postMessage protocol. Real models that live under
   // a `pages/` subdirectory (e.g. `pages/x.malloy`) are excluded so they
   // still open in the Model/Notebook viewer.
   if (
      modelPath?.startsWith("pages/") &&
      !modelPath.endsWith(".malloy") &&
      !modelPath.endsWith(".malloynb")
   ) {
      const pagePath = modelPath.slice("pages/".length);
      const pageResourceUri = encodeResourceUri({
         environmentName: params.environmentName,
         packageName: params.packageName,
         modelPath: pagePath,
      });
      return <PageViewer resourceUri={pageResourceUri} />;
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
         <Box sx={wrapperSx}>
            <Notebook
               resourceUri={resourceUri}
               maxResultSize={1024 * 1024}
               onNavigate={navigate}
            />
         </Box>
      );
   }
   return (
      <Box sx={wrapperSx}>
         <h2>Unrecognized file type: {modelPath}</h2>
      </Box>
   );
}

export default ModelPage;
