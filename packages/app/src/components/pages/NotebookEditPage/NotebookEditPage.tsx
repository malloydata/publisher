// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import {
   BackLink,
   encodeResourceUri,
   Loading,
   NarrowEditGate,
} from "@malloy-publisher/sdk";
import { Box } from "@mui/material";
import React, { Suspense, useMemo } from "react";
import { useLocation, useNavigate } from "react-router-dom";
import { logNotebookEvent } from "../../../utils/consoleTelemetry";

/** Lazy: the builder entry carries the Malloy parser, which no other page should pay for. */
const NotebookEditor = React.lazy(() =>
   import("@malloy-publisher/sdk/builder").then((module) => ({
      default: module.NotebookEditor,
   })),
);

export interface NotebookEditPageProps {
   environmentName: string;
   packageName: string;
   /** The notebook's slug: `tour`, not `notebooks/tour.malloy`. */
   notebookName: string;
}

/**
 * The Console's host for the SDK `NotebookEditor`: `/<env>/<pkg>/notebooks/<slug>/edit`.
 * Done returns to the notebook itself, one segment up.
 */
export default function NotebookEditPage({
   environmentName,
   packageName,
   notebookName,
}: NotebookEditPageProps) {
   const navigate = useNavigate();
   const { pathname } = useLocation();
   const notebookPath = pathname.replace(/\/edit\/?$/, "");
   const onEvent = useMemo(
      () => logNotebookEvent({ environmentName, packageName, notebookName }),
      [environmentName, packageName, notebookName],
   );
   return (
      <Box sx={{ p: 3, maxWidth: 1200, mx: "auto" }}>
         <BackLink
            label={packageName}
            href={`/${environmentName}/${packageName}`}
            onClick={() => navigate(`/${environmentName}/${packageName}`)}
         />
         <NarrowEditGate>
            <Suspense fallback={<Loading text="Opening the editor…" />}>
               <NotebookEditor
                  // Remounts on a route change so another notebook starts from a fresh read.
                  key={`${environmentName}/${packageName}/${notebookName}`}
                  resourceUri={encodeResourceUri({
                     environmentName,
                     packageName,
                  })}
                  notebook={notebookName}
                  onExit={() => navigate(notebookPath)}
                  onEvent={onEvent}
               />
            </Suspense>
         </NarrowEditGate>
      </Box>
   );
}
