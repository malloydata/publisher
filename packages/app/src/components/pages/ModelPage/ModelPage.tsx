// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import {
   DataAppViewer,
   encodeResourceUri,
   Model,
   packageFileUrl,
   useGivenUrlParams,
   useServer,
} from "@malloy-publisher/sdk";
import Box from "@mui/material/Box";
import Link from "@mui/material/Link";
import Typography from "@mui/material/Typography";
import { useLocation, useParams } from "react-router-dom";
import { MONO_FONT_FAMILY } from "../../../theme/colors";
import { isEditSearch } from "../../common/documentRoutes";
import ReportPage from "../ReportPage/ReportPage";
import RoutedDocument from "./RoutedDocument";

function ModelPage() {
   const params = useParams();
   const { search } = useLocation();
   const modelPath = params["*"];
   const { server } = useServer();
   // Every branch below has the same parent, the package, so the way up is
   // built once here.
   // Parameter values ride in the query string, so a Model's Run is a
   // shareable link, the same as a report's.
   const { params: givens, onGivensChange } = useGivenUrlParams();
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

   // A report. Its route is its package path without `.malloy`, so
   // `sales/q3` is `sales/q3.malloy`, and `?edit` on it opens the builder;
   // the file itself, `sales/q3.malloy`, keeps opening in the Model view,
   // which is the author's "view the Malloy" path. The package's report
   // listing decides whether a path is a report, not a directory name: an old
   // `dashboards/<name>` or `notebooks/<name>` link with no file there is
   // redirected by name, and a legacy `<path>/edit` link is redirected to
   // `<path>?edit` only when no report is at `<path>/edit.malloy`.
   //
   // The report lookup comes FIRST, ahead of the `data-apps/` viewer, so a
   // report under `data-apps/` opens as a report. A path there that is not a
   // report is an in-package HTML data app: the Apps section in <Package>
   // routes clicks to `data-apps/<file>`, and <DataAppViewer> iframes the
   // standalone Publisher URL and resizes via the publisher.js postMessage
   // protocol.
   //
   // The cost, stated the way `spa_fallback.ts` states its own: the route
   // drops `.malloy`, so a report at `report.malloy.malloy` has the route
   // `report.malloy`, which opens the Model view of `report.malloy` instead.
   // Unhandled deliberately: that URL is genuinely ambiguous, and guessing
   // would break the far commoner "view the Malloy" path.
   if (
      modelPath !== undefined &&
      modelPath !== "" &&
      !modelPath.endsWith(".malloy") &&
      !modelPath.endsWith(".malloynb")
   ) {
      const routePath = modelPath.replace(/\/$/, "");
      const dataApp = routePath.startsWith("data-apps/");
      const notFound = dataApp ? (
         // No wrapper: the viewer lays itself out at the report's width and
         // edges, and a second padded box around it would push it 24px further in.
         <DataAppViewer
            resourceUri={encodeResourceUri({
               environmentName: params.environmentName,
               packageName: params.packageName,
               modelPath: routePath.slice("data-apps/".length),
            })}
         />
      ) : (
         <NothingToOpen
            environmentName={params.environmentName}
            packageName={params.packageName}
            modelPath={modelPath}
            staticUrl={packageFileUrl({
               server,
               environmentName: params.environmentName,
               packageName: params.packageName,
               path: modelPath,
            })}
         />
      );
      return (
         <RoutedDocument
            environmentName={params.environmentName}
            packageName={params.packageName}
            routePath={routePath}
            edit={isEditSearch(search)}
            notFound={notFound}
            // A data app does not depend on the report listing: when it cannot be read, show the app.
            notFoundOnError={dataApp}
         />
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
            <Model
               resourceUri={resourceUri}
               runOnDemand={true}
               maxResultSize={512 * 1024}
               givens={givens}
               onGivensChange={onGivensChange}
            />
         </Box>
      );
   }
   if (modelPath?.endsWith(".malloynb")) {
      // A legacy `.malloynb` is a linear report, opened by its own path.
      return (
         <ReportPage
            environmentName={params.environmentName}
            packageName={params.packageName}
            path={modelPath}
            editable={false}
         />
      );
   }
   return (
      <NothingToOpen
         environmentName={params.environmentName}
         packageName={params.packageName}
         modelPath={modelPath ?? ""}
         staticUrl={packageFileUrl({
            server,
            environmentName: params.environmentName,
            packageName: params.packageName,
            path: modelPath ?? "",
         })}
      />
   );
}

/**
 * This route is `/:environmentName/:packageName/*`, so it matches any path
 * under a package, and everything that is not a model, report, or data app
 * arrives here. Naming the file's type was the wrong diagnosis: the path is
 * usually what is wrong, and blaming the file sent a reader looking for a
 * problem in a file that was fine. Say what was served and what does serve it.
 */
function NothingToOpen({
   packageName,
   modelPath,
   staticUrl,
}: {
   environmentName: string;
   packageName: string;
   modelPath: string;
   staticUrl: string;
}) {
   const looksLikeFile = /\.[^./]+$/.test(modelPath);
   const mono = { fontFamily: MONO_FONT_FAMILY };
   return (
      <Box sx={{ p: 3, maxWidth: 1200, mx: "auto" }}>
         <Typography variant="h6" sx={{ fontWeight: "fontWeightMedium" }}>
            Nothing to open at this path
         </Typography>
         <Typography variant="body2" color="text.secondary" sx={{ mt: 1 }}>
            <Box component="span" sx={mono}>
               {modelPath}
            </Box>{" "}
            does not name a report, a{" "}
            <Box component="span" sx={mono}>
               .malloy
            </Box>{" "}
            or a{" "}
            <Box component="span" sx={mono}>
               .malloynb
            </Box>{" "}
            file in package{" "}
            <Box component="span" sx={mono}>
               {packageName}
            </Box>
            . This address opens reports by their path without{" "}
            <Box component="span" sx={mono}>
               .malloy
            </Box>
            , and{" "}
            <Box component="span" sx={mono}>
               .malloy
            </Box>{" "}
            and{" "}
            <Box component="span" sx={mono}>
               .malloynb
            </Box>{" "}
            files.
         </Typography>
         {looksLikeFile && (
            <Typography variant="body2" color="text.secondary" sx={{ mt: 1 }}>
               A file from the package&apos;s{" "}
               <Box component="span" sx={mono}>
                  public/
               </Box>{" "}
               directory is served at <Link href={staticUrl}>{staticUrl}</Link>.
            </Typography>
         )}
         <Typography variant="body2" color="text.secondary" sx={{ mt: 1 }}>
            The package lists its reports, models, and data apps.
         </Typography>
      </Box>
   );
}

export default ModelPage;
