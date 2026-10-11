// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { Box } from "@mui/material";
import type { DashboardManifest } from "../../client";
import { DashboardProse, DashboardView } from "../Dashboard/DashboardView";
import type { DrillNavigation } from "../drill";
import { CleanNotebookContainer, CleanNotebookSection } from "../styles";

/**
 * A linear report written as tiles: a one-column grid, its prose drawn bare
 * above the tiles and its controls as a panel.
 */
export function LinearReport({
   manifest,
   environmentName,
   packageName,
   versionId,
   documentName,
   givens,
   onGivensChange,
   onDrillNavigate,
   maxResultSize,
}: {
   /** A report manifest, or the tile layout a deprecated notebook read carries. */
   manifest: DashboardManifest;
   environmentName: string;
   packageName: string;
   versionId: string | undefined;
   documentName: string;
   givens?: Record<string, string>;
   onGivensChange?: (
      givens: Record<string, string>,
      managed: readonly string[],
   ) => void;
   onDrillNavigate?: (target: DrillNavigation, event?: MouseEvent) => void;
   maxResultSize?: number;
}) {
   const { title, description } = manifest;
   return (
      <CleanNotebookContainer>
         <CleanNotebookSection>
            {/* chrome="none" drops the view's own header, so the prose is drawn here. */}
            {(title || description) && (
               <Box sx={{ mb: 2 }}>
                  <DashboardProse
                     // A linear report's text tiles have no card; its description matches them.
                     chrome="none"
                     title={title ?? ""}
                     {...(description ? { description } : {})}
                  />
               </Box>
            )}
            <DashboardView
               manifest={manifest}
               environmentName={environmentName}
               packageName={packageName}
               versionId={versionId}
               documentName={documentName}
               givens={givens}
               onGivensChange={onGivensChange}
               onNavigate={onDrillNavigate}
               maxResultSize={maxResultSize}
               chrome="none"
               // A linear report's control panel, not a grid's bar.
               controlsLayout="panel"
            />
         </CleanNotebookSection>
      </CleanNotebookContainer>
   );
}
