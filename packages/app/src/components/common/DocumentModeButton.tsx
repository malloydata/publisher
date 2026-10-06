// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { SecondaryButton, useNarrowScreen } from "@malloy-publisher/sdk";
import EditOutlinedIcon from "@mui/icons-material/EditOutlined";
import VisibilityOutlinedIcon from "@mui/icons-material/VisibilityOutlined";
import { useLocation, useNavigate } from "react-router-dom";

/** `/:env/:pkg/(dashboards|notebooks)/:slug`, with `/edit` when the builder is open. */
const DOCUMENT_ROUTE =
   /^(\/[^/]+\/[^/]+\/(?:dashboards|notebooks)\/([^/]+?))(\/edit)?\/?$/;

/**
 * A slug ending in a model suffix is a FILE route, not a document: ModelPage
 * opens `dashboards/x.malloy` in the Model view and a legacy `.malloynb` by
 * path, and neither has a builder (a `.malloynb` is never authored).
 */
const FILE_ROUTE = /\.malloy(nb)?$/;

/**
 * The switch between reading a dashboard or notebook and editing it, in the
 * header. One place in both modes, so the page under the header is the same
 * page in two states: Edit opens the builder, View returns to the read-only
 * page. Leaving the builder with unsaved
 * edits is the edit page's leave guard to ask about.
 *
 * Absent on every other route, and below 600px, where the builder steps aside.
 */
export function DocumentModeButton() {
   const { pathname } = useLocation();
   const navigate = useNavigate();
   const narrow = useNarrowScreen();
   const match = DOCUMENT_ROUTE.exec(pathname);
   if (!match || narrow) return null;
   const [, page, slug, edit] = match;
   if (FILE_ROUTE.test(slug)) return null;
   return edit ? (
      <SecondaryButton
         label="View"
         icon={<VisibilityOutlinedIcon />}
         onClick={() => navigate(page)}
      />
   ) : (
      <SecondaryButton
         label="Edit"
         icon={<EditOutlinedIcon />}
         onClick={() => navigate(`${page}/edit`)}
      />
   );
}
