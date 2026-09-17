// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

export type {
   DocumentLocator,
   DocumentStorage,
   DocumentType,
   Workspace,
} from "./DocumentStorage";
export { DocumentNotFoundError, isDocumentNotFound } from "./DocumentStorage";
export {
   DocumentStorageProvider,
   useDocumentStorage,
   useOptionalDocumentStorage,
   type DocumentStorageProviderProps,
} from "./DocumentStorageProvider";
export { BrowserDocumentStorage } from "./BrowserDocumentStorage";
