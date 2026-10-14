// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import {
   draftStorageAvailable,
   encodeResourceUri,
   NarrowEditGate,
   Report,
   type ReportLayout,
} from "@malloy-publisher/sdk";
import type { BuilderControls } from "@malloy-publisher/sdk/builder";
import { Box, Button } from "@mui/material";
import React, { Suspense, useEffect, useMemo, useRef, useState } from "react";
import { createPortal } from "react-dom";
import { useLocation, useNavigate } from "react-router-dom";
import { withoutEdit } from "../../common/documentRoutes";
import { logReportEvent } from "../../../utils/consoleTelemetry";
import { useLeaveGuard } from "../useLeaveGuard";
import { useMarkEditPageMounted } from "./editPageMounted";
import { useDrawn } from "../useDrawn";
import { loadBuilder } from "./loadBuilder";
import { TITLED_PAGE_FRAME_SX } from "../pageFrame";

/**
 * The builder's entry is loaded here and nowhere else. It carries the Malloy
 * parser (440 KB gzipped), which most Console users never need, so it stays out
 * of every other page's chunk and downloads the first time someone opens a
 * report for editing.
 */
const ReportEditor = React.lazy(() =>
   loadBuilder().then((module) => ({
      default: module.ReportEditor,
   })),
);

/** The builder's controls for the header, from the same chunk as the editor. */
const BuilderBar = React.lazy(() =>
   loadBuilder().then((module) => ({
      default: module.BuilderBar,
   })),
);

/** The header's slot for a page's own actions (`MainPage`), once it is in the document. */
function useHeaderActionsSlot(): HTMLElement | null {
   const [slot, setSlot] = useState<HTMLElement | null>(null);
   useEffect(() => {
      setSlot(document.getElementById("header-actions-portal"));
   }, []);
   return slot;
}

export interface DashboardEditPageProps {
   environmentName: string;
   packageName: string;
   /** The report's package path, `sales/q3.malloy`: what the editor is keyed by. */
   path: string;
   /** The layout the report opens with, from the listing or the host's draft; default grid. */
   layout?: ReportLayout;
   /**
    * Told once the builder is drawn: it is open, and the tiles in view show
    * their results (or after a few seconds, whatever they show). For a host
    * that keeps the reader on screen until then, so switching to edit swaps
    * one drawn page for another rather than blanking in between.
    */
   onReady?: () => void;
   /**
    * Draw nothing of its own while the builder loads, because the host keeps
    * the reader up underneath. Default false: the reader's own copy of the
    * report stands in, as on a direct `?edit` link.
    */
   quietLoading?: boolean;
   /**
    * The page is not the one on screen: it is being drawn out of sight, or is
    * on its way out. Its controls stay out of the header meanwhile.
    */
   offstage?: boolean;
}

/**
 * The Console's host for the SDK `ReportEditor`: `/<env>/<pkg>/<path>?edit`,
 * either layout. The builder's controls are drawn in the header, where the
 * View / Edit switch is in the reader, and Done there saves the edits and
 * returns to the report itself: the same route without `?edit`.
 *
 * Unsaved edits are kept in this browser as they are made, so leaving any
 * other way loses nothing and asks nothing: opening the report for editing
 * again puts them back. The leave guard still asks for text being typed into
 * an open field, which is not an edit yet, and for everything where the
 * browser keeps nothing (a private window, storage refused).
 */
export default function DashboardEditPage({
   environmentName,
   packageName,
   path,
   layout = "grid",
   onReady,
   quietLoading = false,
   offstage = false,
}: DashboardEditPageProps) {
   const guard = useLeaveGuard();
   // The header's View steps aside while this page draws the bar there.
   useMarkEditPageMounted();
   const [keepDraft] = useState(draftStorageAvailable);
   const navigate = useNavigate();
   const { pathname, search } = useLocation();
   const slot = useHeaderActionsSlot();
   const [controls, setControls] = useState<BuilderControls | undefined>();
   const surface = useRef<HTMLDivElement>(null);
   // The builder hands its controls over once it is open, which is the first
   // half of being drawn; the tiles' results are the second.
   useDrawn(surface, controls !== undefined, onReady);
   const done = () =>
      navigate(`${pathname.replace(/\/$/, "")}${withoutEdit(search)}`);
   // Each event names the report by path and carries its layout as it stands.
   const onEvent = useMemo(
      () => logReportEvent({ environmentName, packageName }),
      [environmentName, packageName],
   );
   // While the builder's code and data load, the report as the reader drew
   // it rather than a spinner: the builder then replaces it in place, drawing
   // the same page from the same cached results, so switching to edit does not
   // blank the page. No `onGivensChange`: this copy does not write the URL.
   const reading = (
      <Report
         resourceUri={encodeResourceUri({ environmentName, packageName })}
         path={path}
         // Drawn twice (while the builder's code loads, then while it opens),
         // and read again by the builder: one read serves all three, as the
         // builder's own reuse window does.
         manifestStaleTime={10_000}
      />
   );
   return (
      // The reader's page width and edges, the same for either layout, so the
      // margins do not move between modes.
      <Box ref={surface} sx={TITLED_PAGE_FRAME_SX}>
         <NarrowEditGate>
            <Suspense fallback={quietLoading ? null : reading}>
               <ReportEditor
                  // Remounts on a route change so another report starts from a fresh read.
                  key={`${environmentName}/${packageName}/${path}`}
                  resourceUri={encodeResourceUri({
                     environmentName,
                     packageName,
                  })}
                  path={path}
                  layout={layout}
                  keepDraft={keepDraft}
                  // With a draft kept, dirty means only text still being typed
                  // into an open field, which no draft holds yet.
                  onDirtyChange={guard.onDirtyChange}
                  onExit={done}
                  onEvent={onEvent}
                  loading={quietLoading ? null : reading}
                  onControlsChange={setControls}
               />
            </Suspense>
         </NarrowEditGate>
         {guard.dialog}
         {slot &&
            !offstage &&
            createPortal(
               // Done is there from the first frame, before the builder's
               // code arrives, so there is always a way back to the report.
               <Suspense
                  fallback={
                     <Button
                        variant="contained"
                        disableElevation
                        onClick={done}
                     >
                        Done
                     </Button>
                  }
               >
                  <BuilderBar controls={controls} onDone={done} />
               </Suspense>,
               slot,
            )}
      </Box>
   );
}
