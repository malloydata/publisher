// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import React, { createContext, useContext, useMemo } from "react";
import type { WorkbookStorage } from "./WorkbookStorage";

export interface WorkbookStorageProviderProps {
   children: React.ReactNode;
   workbookStorage: WorkbookStorage;
}

interface WorkbookStorageContextValue {
   workbookStorage: WorkbookStorage;
}

const WorkbookStorageContext = createContext<
   WorkbookStorageContextValue | undefined
>(undefined);

export function WorkbookStorageProvider({
   children,
   workbookStorage,
}: WorkbookStorageProviderProps) {
   const value = useMemo(() => ({ workbookStorage }), [workbookStorage]);
   return (
      <WorkbookStorageContext.Provider value={value}>
         {children}
      </WorkbookStorageContext.Provider>
   );
}

export function useWorkbookStorage() {
   const context = useContext(WorkbookStorageContext);
   if (!context) {
      throw new Error(
         "useWorkbookStorage must be used within a WorkbookStorageProvider",
      );
   }
   return context;
}
