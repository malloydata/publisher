// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import {
   encodeResourceUri,
   Notebook,
   useGivenUrlParams,
   useRouterClickHandler,
} from "@malloy-publisher/sdk";
import Box from "@mui/material/Box";
import { useDrillNavigate } from "../../common/useDrillNavigate";

export interface NotebookPageProps {
   environmentName: string;
   packageName: string;
   /** Package-relative path, e.g. `storefront.malloynb`. */
   notebookPath: string;
}

/**
 * The Console's host for the SDK `Notebook`.
 *
 * The component reads nothing from the router; the URL sync that makes a
 * parameterized notebook a shareable link lands here.
 */
export default function NotebookPage({
   environmentName,
   packageName,
   notebookPath,
}: NotebookPageProps) {
   // Parameter values ride in the query string, so a link reproduces the view;
   // a drill pushes a route. Both are shared with DashboardPage.
   const { params: givens, onGivensChange } = useGivenUrlParams();
   const onDrillNavigate = useDrillNavigate(environmentName, packageName);
   // Ordinary links inside the notebook's markdown, routed in-app.
   const navigate = useRouterClickHandler();

   return (
      <Box sx={{ p: 3, maxWidth: 1200, mx: "auto" }}>
         <Notebook
            resourceUri={encodeResourceUri({
               environmentName,
               packageName,
               modelPath: notebookPath,
            })}
            maxResultSize={1024 * 1024}
            givens={givens}
            onGivensChange={onGivensChange}
            onNavigate={navigate}
            onDrillNavigate={onDrillNavigate}
         />
      </Box>
   );
}
