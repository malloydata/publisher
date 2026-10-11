// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

export { default as Package, type OpenDocumentRequest } from "./Package";
export {
   documentRoute,
   documentSlug,
   kindOfLayout,
   layoutOfKind,
   locateDocument,
   locateReport,
   locateReportByName,
   reportRoute,
   reportRoutePath,
   resolveDrillTarget,
   type DocumentKind,
   type DrillTarget,
   type ListedReport,
   type LocatedDocument,
   type NameLookup,
   type ReportLayout,
} from "./documentLocation";
export {
   reportsQueryKey,
   useDocumentLocation,
   useReportListing,
} from "./useDocumentLocation";
export { findDraftType, useDraftType } from "./useDraftType";
export {
   NewDocumentDialog,
   type NewDocumentDialogProps,
} from "./NewDocumentDialog";
// Exported so a host decides Retry on its own failed requests as the dialog does.
export { canRetryRequest } from "../DocumentCreate/canRetry";
