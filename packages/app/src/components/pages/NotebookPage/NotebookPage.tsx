// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import {
   encodeResourceUri,
   Notebook,
   useGivenUrlParams,
   useNarrowScreen,
   useRouterClickHandler,
} from "@malloy-publisher/sdk";
import Box from "@mui/material/Box";
import { useEffect } from "react";
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
   // A legacy `.malloynb` is never authored; the tag gate is the server listing only tagged notebooks, this is just a suffix check.
   const editable = notebookPath.endsWith(".malloy");
   // Below 600px the editor steps aside: the header shows no Edit, and there is no builder chunk to warm.
   const narrow = useNarrowScreen();

   // Fetch the builder chunk (it carries the Malloy parser) while idle so Edit is a re-render, not a spinner.
   useEffect(() => {
      if (!editable || narrow) return;
      const warm = () => void import("@malloy-publisher/sdk/builder");
      const idle = window.requestIdleCallback;
      if (idle) {
         const handle = idle(warm);
         return () => window.cancelIdleCallback?.(handle);
      }
      const timer = setTimeout(warm, 1500);
      return () => clearTimeout(timer);
   }, [editable, narrow]);

   return (
      // The dashboard's width and edges, so a package's pages line up.
      <Box sx={{ p: 3, maxWidth: 1600, mx: "auto" }}>
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
