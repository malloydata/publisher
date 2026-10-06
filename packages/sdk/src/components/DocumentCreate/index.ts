// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

export {
   createDocument,
   createRoute,
   MAX_SLUG_ATTEMPTS,
   type CreatedDocument,
   type CreateDocumentOptions,
   type CreateTarget,
} from "./createDocument";
export {
   documentPathFor,
   documentPathForTitle,
   locatorFor,
   newDashboardSource,
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
