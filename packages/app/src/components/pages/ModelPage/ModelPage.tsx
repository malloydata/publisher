// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import {
   DataAppViewer,
   encodeResourceUri,
   Model,
   packageFileUrl,
   useServer,
} from "@malloy-publisher/sdk";
import Box from "@mui/material/Box";
import Link from "@mui/material/Link";
import Typography from "@mui/material/Typography";
import { Navigate, useLocation, useParams } from "react-router-dom";
import { MONO_FONT_FAMILY } from "../../../theme/colors";
import DashboardPage from "../DashboardPage/DashboardPage";
import NotebookPage from "../NotebookPage/NotebookPage";

function ModelPage() {
   const params = useParams();
   const modelPath = params["*"];
   const { search, hash } = useLocation();
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

   // A data app used to be addressed as `pages/<file>`, which this release
   // renames to `data-apps/<file>`. Redirect instead of letting the old form
   // fail, so a bookmark or a shared link self-corrects in the address bar, and
   // `replace` so the dead URL does not stay in history. The query string and
   // fragment come along so a rewrite never silently discards state the caller
   // supplied, matching the server's asset redirect, which rebuilds them through
   // `withRequestQuery` (server.ts) rather than in the classifier. Note this does
   // NOT deliver an `embed_token`: that belongs on the standalone URL,
   // which <DataAppViewer> builds itself via packageFileUrl and which carries no
   // query string, so an embedded page reads its token from that URL rather than
   // from this one.
   //
   // A model or notebook that legitimately lives in a `pages/` directory is
   // excluded on the same test the data-apps branch below uses, since the old
   // data-app URL never named one.
   //
   // DEPRECATED. Remove one release after the release that ships this rename,
   // together with `pages` in SPA_OWNED_SEGMENTS (packages/server/src/
   // spa_fallback.ts), which is what lets this URL reach the app at all. The
   // newest tag when this was written was v0.0.240.
   if (
      modelPath?.startsWith("pages/") &&
      !modelPath.endsWith(".malloy") &&
      !modelPath.endsWith(".malloynb")
   ) {
      // Both names are encoded, matching the "Back to" link below. Today it can
      // only be a no-op, because a name that reaches a loaded package has passed
      // assertSafePackageName and holds nothing worth encoding. But these two come
      // from the URL rather than from anything validated, so a typed name need not
      // be safe, and the encoding keeps a stray character in one segment from
      // reading as structure in the path. The rest is NOT encoded: it carries the
      // remaining slashes, which have to stay separators.
      const renamed = `data-apps/${modelPath.slice("pages/".length)}`;
      const env = encodeURIComponent(params.environmentName);
      const pkg = encodeURIComponent(params.packageName);
      return (
         <Navigate to={`/${env}/${pkg}/${renamed}${search}${hash}`} replace />
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
