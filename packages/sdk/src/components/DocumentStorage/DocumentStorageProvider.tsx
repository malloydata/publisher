// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import React, { createContext, useContext, useMemo } from "react";
import type { DocumentStorage } from "./DocumentStorage";

export interface DocumentStorageProviderProps {
   children: React.ReactNode;
   documentStorage: DocumentStorage;
}

interface DocumentStorageContextValue {
   documentStorage: DocumentStorage;
}

const DocumentStorageContext = createContext<
   DocumentStorageContextValue | undefined
>(undefined);

/**
 * Hands a {@link DocumentStorage} to every SDK component beneath it. The host
 * constructs the storage; the SDK only ever reads it through
 * {@link useDocumentStorage}.
 */
export function DocumentStorageProvider({
   children,
   documentStorage,
}: DocumentStorageProviderProps) {
   const value = useMemo(() => ({ documentStorage }), [documentStorage]);
   return (
      <DocumentStorageContext.Provider value={value}>
         {children}
      </DocumentStorageContext.Provider>
   );
}

/** The host's storage, or a thrown error outside a provider: there is no default. */
export function useDocumentStorage(): DocumentStorageContextValue {
   const context = useContext(DocumentStorageContext);
   if (!context) {
      throw new Error(
         "useDocumentStorage must be used within a DocumentStorageProvider",
      );
   }
   return context;
}
