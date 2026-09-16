// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import {
   BackLink,
   DataAppViewer,
   encodeResourceUri,
   Model,
   packageFileUrl,
   useRouterClickHandler,
   useServer,
} from "@malloy-publisher/sdk";
import Box from "@mui/material/Box";
import Link from "@mui/material/Link";
import Typography from "@mui/material/Typography";
import { useParams } from "react-router-dom";
import { MONO_FONT_FAMILY } from "../../../theme/colors";
import DashboardPage from "../DashboardPage/DashboardPage";
import DashboardEditPage from "../DashboardEditPage/DashboardEditPage";
import NotebookPage from "../NotebookPage/NotebookPage";

function ModelPage() {
   const params = useParams();
   const modelPath = params["*"];
   const { server } = useServer();
   // Every branch below has the same parent, the package, so the way up is
   // built once here.
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

   // Dashboard viewer. `dashboards/overview` is the dashboard; the file it is
   // declared in, `dashboards/overview.malloy`, keeps opening in the Model
   // view, which is the author's "view the Malloy" path. Branching on the
   // extension here rather than declaring a literal `dashboards/:name` route
   // is what keeps those two apart: a route param matches any single segment,
   // dots included, so the literal route would have swallowed the file path
   // too. Same reasoning as the `data-apps/` branch below.
   //
   // The cost, stated the way `spa_fallback.ts` states its own: a slug is a
   // filename with `.malloy` removed, so `dashboards/report.malloy.malloy`
   // publishes the slug `report.malloy`, and this branch sends that URL to the
   // Model viewer instead of the dashboard, which then finds no such file. That
   // dashboard is listed and served by the API but has no working address here.
   // Unhandled deliberately: `/{env}/{pkg}/dashboards/report.malloy` is
   // genuinely ambiguous between the dashboard and a real model file of that
   // path, and guessing would break the author's "view the Malloy" path, which
   // is the commoner case by far. `report.csv` has no such ambiguity, which is
   // why the server-side entry in SPA_OWNED_SEGMENTS can fix that one.
   if (
      modelPath?.startsWith("dashboards/") &&
      !modelPath.endsWith(".malloy") &&
      !modelPath.endsWith(".malloynb")
   ) {
      const slug = modelPath.slice("dashboards/".length);
      // `dashboards/<slug>/edit` opens the same dashboard in the builder. A
      // slug never contains a slash (nested dashboard directories are not
      // discovered), so the one segment can only be this.
      if (slug.endsWith("/edit")) {
         return (
            <DashboardEditPage
               environmentName={params.environmentName}
               packageName={params.packageName}
               dashboardName={slug.slice(0, -"/edit".length)}
            />
         );
      }
      return (
         <DashboardPage
            environmentName={params.environmentName}
            packageName={params.packageName}
            dashboardName={slug}
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
      return (
         <Box sx={wrapperSx}>
            <BackLink
               label={params.packageName}
               href={`/${params.environmentName}/${params.packageName}`}
               onClick={(event) =>
                  navigate(
                     `/${params.environmentName}/${params.packageName}`,
                     event,
                  )
               }
            />
            <DataAppViewer resourceUri={dataAppResourceUri} />
         </Box>
      );
   }

   const resourceUri = encodeResourceUri({
      environmentName: params.environmentName,
      packageName: params.packageName,
      modelPath,
   });

   if (modelPath?.endsWith(".malloy")) {
      return (
         <Box sx={wrapperSx}>
            <BackLink
               label={params.packageName}
               href={`/${params.environmentName}/${params.packageName}`}
               onClick={(event) =>
                  navigate(
                     `/${params.environmentName}/${params.packageName}`,
                     event,
                  )
               }
            />
            <Model
               resourceUri={resourceUri}
               runOnDemand={true}
               maxResultSize={512 * 1024}
            />
         </Box>
      );
   }
   if (modelPath?.endsWith(".malloynb")) {
      // Handed to a host of its own rather than rendered inline, because a
      // notebook's parameters live in the URL now and this component reads
      // nothing from the router beyond its path params.
      return (
         <NotebookPage
            environmentName={params.environmentName}
            packageName={params.packageName}
            notebookPath={modelPath}
         />
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
         <BackLink
            label={params.packageName}
            href={`/${params.environmentName}/${params.packageName}`}
            onClick={(event) =>
               navigate(
                  `/${params.environmentName}/${params.packageName}`,
                  event,
               )
            }
         />
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
            The package lists its models, notebooks, and data apps.
         </Typography>
      </Box>
   );
}

export default ModelPage;
