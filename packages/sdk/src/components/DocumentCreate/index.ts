// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

export {
   createDocument,
   createRoute,
   MAX_SLUG_ATTEMPTS,
   reportPathProblem,
   type CreatedDocument,
   type CreateDocumentOptions,
   type CreateTarget,
} from "./createDocument";
export {
   locatorFor,
   newDashboardSource,
   reportPathFor,
   reportPathForTitle,
   slugFor,
   slugOrFallback,
} from "./documentPath";
export type {
   DashboardCreatedEvent,
   DocumentCreatedEvent,
   NotebookCreatedEvent,
} from "./events";
export { newDocumentProblem, type NewDocument } from "./guards";
export { newNotebookSource } from "./newNotebook";
export { useDocumentChoices, type DocumentChoice } from "./useDocumentChoices";
