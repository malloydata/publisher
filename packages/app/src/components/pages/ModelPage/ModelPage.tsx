import {
   DataAppViewer,
   encodeResourceUri,
   Model,
   Notebook,
   packageFileUrl,
   useRouterClickHandler,
   useServer,
} from "@malloy-publisher/sdk";
import Box from "@mui/material/Box";
import Link from "@mui/material/Link";
import Typography from "@mui/material/Typography";
import { useParams } from "react-router-dom";
import { MONO_FONT_FAMILY } from "../../../theme/colors";

function ModelPage() {
   const params = useParams();
   const modelPath = params["*"];
   const navigate = useRouterClickHandler();
   const { server } = useServer();
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
         <Box sx={wrapperSx}>
            <Notebook
               resourceUri={resourceUri}
               maxResultSize={1024 * 1024}
               onNavigate={navigate}
            />
         </Box>
      );
   }
   // This route is `/:environmentName/:packageName/*`, so it matches any path
   // under a package, and everything that is not a model, notebook, or data app
   // arrives here. Naming the file's type was the wrong diagnosis: the path is
   // usually what is wrong, and blaming the file sent a reader looking for a
   // problem in a file that was fine. Say what was served and what does serve it.
   const looksLikeFile = /\.[^./]+$/.test(modelPath ?? "");
   const staticUrl = packageFileUrl({
      server,
      environmentName: params.environmentName,
      packageName: params.packageName,
      path: modelPath ?? "",
   });
   return (
      <Box sx={wrapperSx}>
         <Typography variant="h6" sx={{ fontWeight: 600 }}>
            Nothing to open at this path
         </Typography>
         <Typography variant="body2" color="text.secondary" sx={{ mt: 1 }}>
            <Box component="span" sx={{ fontFamily: MONO_FONT_FAMILY }}>
               {modelPath}
            </Box>{" "}
            does not name a{" "}
            <Box component="span" sx={{ fontFamily: MONO_FONT_FAMILY }}>
               .malloy
            </Box>{" "}
            or{" "}
            <Box component="span" sx={{ fontFamily: MONO_FONT_FAMILY }}>
               .malloynb
            </Box>{" "}
            file in package{" "}
            <Box component="span" sx={{ fontFamily: MONO_FONT_FAMILY }}>
               {params.packageName}
            </Box>
            . This address opens{" "}
            <Box component="span" sx={{ fontFamily: MONO_FONT_FAMILY }}>
               .malloy
            </Box>{" "}
            and{" "}
            <Box component="span" sx={{ fontFamily: MONO_FONT_FAMILY }}>
               .malloynb
            </Box>{" "}
            files.
         </Typography>
         {looksLikeFile && (
            <Typography variant="body2" color="text.secondary" sx={{ mt: 1 }}>
               A file from the package&apos;s{" "}
               <Box component="span" sx={{ fontFamily: MONO_FONT_FAMILY }}>
                  public/
               </Box>{" "}
               directory is served at <Link href={staticUrl}>{staticUrl}</Link>.
            </Typography>
         )}
         <Typography variant="body2" color="text.secondary" sx={{ mt: 1 }}>
            <Link
               href={`/${encodeURIComponent(params.environmentName)}/${encodeURIComponent(
                  params.packageName,
               )}`}
            >
               Back to {params.packageName}
            </Link>{" "}
            lists this package&apos;s models, notebooks, and data apps.
         </Typography>
      </Box>
   );
}

export default ModelPage;
