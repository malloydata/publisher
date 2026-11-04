// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import LinkOutlinedIcon from "@mui/icons-material/LinkOutlined";
import { IconButton, Snackbar, Tooltip } from "@mui/material";
import React, { useState } from "react";
import { Loading } from "../Loading";
import { ModelViewer, type ModelViewerProps } from "../ModelViewer";
import { useServer } from "../ServerProvider";
import { ExplorerPage } from "./ExplorerPage";
import type { QueryExplorerResult } from "./SourcesExplorer";

export interface ModelProps extends ModelViewerProps {
   /**
    * The Explorer page only: called with the query the Explorer builds. The
    * model viewer builds none.
    */
   onChange?: (query: QueryExplorerResult) => void;
   /**
    * The Explorer page only: a named query waits for its Run rather than
    * running on render. The model viewer runs a view or a query only when it
    * is opened, so there it turns nothing on.
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
 * A model's page, the one the server's `PUBLISHER_MODEL_PAGE` names (see
 * `useServer().modelPage`). Nothing shows until `/status` answers, so a
 * `viewer` server never flashes the Explorer.
 *
 * - `explorer`, the default: the Malloy Explorer over the model's sources
 *   ({@link ExplorerPage}), and its named queries. The Explorer writes the
 *   queries it builds back to Malloy text through
 *   `@malloydata/malloy-interfaces`, whose string escaping is a TODO, so a
 *   drilled value holding a double quote rewrites its query.
 * - `viewer`: the model viewer ({@link ModelViewer}) with a Copy link action
 *   on its heading. Each source's views, measures, calculations, dimensions
 *   and joins, the model's named queries, and the givens they read. It writes
 *   no value into Malloy text.
 */
export default function Model(props: ModelProps) {
   const { modelPage, isLoadingStatus } = useServer();
   if (isLoadingStatus) {
      return <Loading text="Fetching Model..." />;
   }
   if (modelPage !== "viewer") {
      return <ExplorerPage {...props} />;
   }
   const {
      onChange: _onChange,
      runOnDemand: _runOnDemand,
      headerActions,
      ...viewerProps
   } = props;
   return (
      <ModelViewer
         {...viewerProps}
         headerActions={headerActions ?? <CopyLinkButton />}
      />
   );
}

export type { ModelViewerProps };
