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
import { useLocation, useParams, useSearchParams } from "react-router-dom";
import { MONO_FONT_FAMILY } from "../../../theme/colors";
import { isEditSearch } from "../../common/documentRoutes";
import ReportPage from "../ReportPage/ReportPage";
import RoutedDocument from "./RoutedDocument";
import { PAGE_FRAME_SX } from "../pageFrame";

function ModelPage() {
   const params = useParams();
   const { search } = useLocation();
   const [searchParams, setSearchParams] = useSearchParams();
   const modelPath = params["*"];
   const { server } = useServer();
   // Given values ride in the query string, beside `?source=`, so a link to a
   // model (and its Copy link) opens with the values the page was running with.
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
   // report is an in-package HTML data app, drawn without waiting for the
   // listing (reports still win when it arrives): the Apps section in <Package>
   // routes clicks to `data-apps/<file>`, and <DataAppViewer> iframes the
   // standalone Publisher URL and resizes via the publisher.js postMessage
   // protocol.
   //
   // The cost: the route drops `.malloy`, so a report at
   // `report.malloy.malloy` has the route `report.malloy`, which opens the
   // Model view of `report.malloy` instead.
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
            // A data app does not wait on the report listing: it opens at once,
            // and gives way only if the listing names a report at this path.
            notFoundUntilListed={dataApp}
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
         <Box sx={PAGE_FRAME_SX}>
            <Model
               resourceUri={resourceUri}
               // The source on screen is in the address, so a link to one opens
               // on it. Replaced, not pushed: picking a source is a choice
               // within this page, and Back should leave the model.
               sourceName={searchParams.get("source") ?? undefined}
               onSourceChange={(source) =>
                  setSearchParams(
                     (current) => {
                        const next = new URLSearchParams(current);
                        next.set("source", source);
                        return next;
                     },
                     { replace: true },
                  )
               }
               givens={givens}
               onGivensChange={onGivensChange}
               maxResultSize={512 * 1024}
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
 * arrives here. The path is usually what is wrong, not the file, so say what
 * was served and what does serve it.
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
      <Box sx={PAGE_FRAME_SX}>
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
