// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import {
   locateReport,
   SecondaryButton,
   useNarrowScreen,
   useReportListing,
} from "@malloy-publisher/sdk";
import EditOutlinedIcon from "@mui/icons-material/EditOutlined";
import VisibilityOutlinedIcon from "@mui/icons-material/VisibilityOutlined";
import { useLocation, useNavigate, useParams } from "react-router-dom";
import { editPathOf, splitEdit } from "./documentRoutes";

/**
 * A route ending in a model suffix is a FILE route, not a report: ModelPage
 * opens `sales/q3.malloy` in the Model view and a legacy `.malloynb` by path,
 * and neither has a builder (a `.malloynb` is never authored).
 */
const FILE_ROUTE = /\.malloy(nb)?$/;

/**
 * The switch between reading a report and editing it, in the header. One
 * place in both modes, so the page under the header is the same page in two
 * states: Edit opens the builder, View returns to the read-only page. Leaving
 * the builder with unsaved edits is the edit page's leave guard to ask about.
 *
 * Shown for the route of a report the package lists as editable, resolved by
 * its path; in the builder, View is always offered, since a draft the package
 * does not list yet still needs a way back. Absent on every other route, and
 * below 600px, where the builder steps aside.
 */
export function DocumentModeButton() {
   const { pathname } = useLocation();
   const params = useParams();
   const navigate = useNavigate();
   const narrow = useNarrowScreen();
   const environmentName = params.environmentName ?? "";
   const packageName = params.packageName ?? "";
   const splat = params["*"] ?? "";
   const routeShaped =
      environmentName !== "" &&
      packageName !== "" &&
      splat !== "" &&
      !FILE_ROUTE.test(splat) &&
      !splat.startsWith("data-apps/");
   const { reports, settled } = useReportListing(
      environmentName,
      packageName,
      undefined,
      routeShaped,
   );
   if (!routeShaped || narrow) return null;
   const { path, edit } = splitEdit(splat);
   const page = edit
      ? pathname.replace(/\/$/, "").replace(/\/edit$/, "")
      : pathname.replace(/\/$/, "");
   if (edit)
      return (
         <SecondaryButton
            label="View"
            icon={<VisibilityOutlinedIcon />}
            onClick={() => navigate(page)}
         />
      );
   const report = settled ? locateReport(`${path}.malloy`, reports) : undefined;
   if (!report?.editable) return null;
   return (
      <SecondaryButton
         label="Edit"
         icon={<EditOutlinedIcon />}
         onClick={() => navigate(editPathOf(page))}
      />
   );
}
