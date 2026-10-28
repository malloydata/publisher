// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import LinkOutlinedIcon from "@mui/icons-material/LinkOutlined";
import { IconButton, Snackbar, Tooltip } from "@mui/material";
import React, { useState } from "react";
import { ModelViewer, type ModelViewerProps } from "../ModelViewer";

export interface ModelProps extends ModelViewerProps {
   /**
    * @deprecated Every view and named query runs when it is opened and never on
    * render, so there is nothing left for this to turn on.
    */
   runOnDemand?: boolean;
}

/** Copies the page's address: a model, at its source and given values, is shareable. */
function CopyLinkButton() {
   const [copyMessage, setCopyMessage] = useState("");
   const copyToClipboard = () => {
      navigator.clipboard
         .writeText(window.location.href)
         .then(() => setCopyMessage("URL copied to clipboard!"))
         .catch(() => setCopyMessage("Failed to copy URL"));
   };
   return (
      <>
         <Tooltip title="Copy link">
            <IconButton
               size="small"
               aria-label="Copy link"
               onClick={copyToClipboard}
               sx={{ color: "text.secondary" }}
            >
               <LinkOutlinedIcon fontSize="small" />
            </IconButton>
         </Tooltip>
         <Snackbar
            open={copyMessage !== ""}
            autoHideDuration={6000}
            onClose={() => setCopyMessage("")}
            message={copyMessage}
         />
      </>
   );
}

/**
 * A model's page: the model viewer ({@link ModelViewer}) with a Copy link
 * action on its heading. Each source's views, measures, calculations,
 * dimensions and joins, the model's named queries, and the givens they read.
 *
 * It replaces the Malloy Explorer this page used to embed, which wrote
 * structured queries back to Malloy text through `@malloydata/malloy-interfaces`,
 * whose string escaping is a TODO: a value holding a double quote rewrote the
 * query it was drilled into. The viewer writes no value into Malloy text.
 */
export default function Model({
   runOnDemand: _runOnDemand,
   headerActions,
   ...props
}: ModelProps) {
   return (
      <ModelViewer
         {...props}
         headerActions={headerActions ?? <CopyLinkButton />}
      />
   );
}

export type { ModelViewerProps };
