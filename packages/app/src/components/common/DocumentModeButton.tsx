// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import {
   locateReport,
   SecondaryButton,
   useNarrowScreen,
   useReportListing,
} from "@malloy-publisher/sdk";
import EditOutlinedIcon from "@mui/icons-material/EditOutlined";
import { useLocation, useNavigate, useParams } from "react-router-dom";
import { loadBuilder } from "../pages/DashboardEditPage/loadBuilder";
import { isEditSearch, withEdit, withoutEdit } from "./documentRoutes";
import { usePackageRoute } from "./usePackageRoute";
import { useEditPageMounted } from "../pages/DashboardEditPage/editPageMounted";

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
 * its path. In the builder the edit page draws the builder's bar in this slot
 * instead, with Done as the way back. Absent on every other route, and below
 * 600px, where the builder steps aside.
 */
export function DocumentModeButton() {
   const { pathname, search } = useLocation();
   const params = useParams();
   const navigate = useNavigate();
   const narrow = useNarrowScreen();
   const editPageMounted = useEditPageMounted();
   const environmentName = params.environmentName ?? "";
   const packageName = params.packageName ?? "";
   // A published version is immutable: its reports are read, never edited.
   const route = usePackageRoute();
   const splat = route.path ?? "";
   const routeShaped =
      environmentName !== "" &&
      packageName !== "" &&
      splat !== "" &&
      !route.versioned &&
      !FILE_ROUTE.test(splat);
   const { reports, settled } = useReportListing(
      environmentName,
      packageName,
      undefined,
      routeShaped,
   );
   if (!routeShaped || narrow) return null;
   const path = splat.replace(/\/$/, "");
   const edit = isEditSearch(search);
   const page = pathname.replace(/\/$/, "");
   // In the builder the edit page draws the builder's bar here, Done included.
   // On an edit route that never got that far (the listing still loading, or
   // failed), View stays: it is the way back.
   if (edit)
      return editPageMounted ? null : (
         <SecondaryButton
            label="View"
            onClick={() => navigate(`${page}${withoutEdit(search)}`)}
         />
      );
   const report = settled ? locateReport(`${path}.malloy`, reports) : undefined;
   if (!report?.editable) return null;
   return (
      // The builder's code starts downloading when the pointer reaches Edit or
      // it takes focus, so it is usually in hand by the click: see `loadBuilder`.
      <span
         onPointerEnter={() => void loadBuilder()}
         onFocus={() => void loadBuilder()}
      >
         <SecondaryButton
            label="Edit"
            icon={<EditOutlinedIcon />}
            onClick={() => navigate(`${page}${withEdit(search)}`)}
         />
      </span>
   );
}
