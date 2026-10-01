// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { useQueries } from "@tanstack/react-query";
import { useMemo } from "react";
import { useServer } from "../ServerProvider";
import { exportedSources } from "./exportedSources";

export interface DocumentChoice {
   source: string;
   view: string;
}

/**
 * What a new dashboard or notebook can start from: model -> the (source, view)
 * pairs it declares. One `getModel` per model under its own key, read only
 * while `enabled` (the dialog is open) and cached after, so opening it costs
 * nothing until it is opened.
 */
export function useDocumentChoices({
   environmentName,
   packageName,
   versionId,
   models,
   enabled,
}: {
   environmentName: string;
   packageName: string;
   /** The package version the model list was read at. */
   versionId?: string;
   /** The package's model files, relative to its root. */
   models: readonly string[];
   enabled: boolean;
}): {
   choices: Map<string, DocumentChoice[]>;
   isLoading: boolean;
   isSuccess: boolean;
} {
   const { apiClients, server } = useServer();
   const results = useQueries({
      queries: models.map((path) => ({
         // `useQueries` bypasses `useQueryWithApiError`, so the server is added here to keep two servers' caches apart.
         queryKey: [
            "new-document-model",
            environmentName,
            packageName,
            versionId,
            path,
            server,
         ],
         queryFn: async () =>
            (
               await apiClients.models.getModel(
                  environmentName,
                  packageName,
                  path,
                  versionId,
               )
            ).data,
         enabled,
         retry: false,
         staleTime: 5 * 60 * 1000,
         refetchOnWindowFocus: false,
      })),
   });
   // `useQueries` returns a new array each render; its data changes when `dataUpdatedAt` does.
   const version = results.map((q) => q.dataUpdatedAt).join();
   const choices = useMemo(() => {
      const out = new Map<string, DocumentChoice[]>();
      results.forEach((result, i) => {
         const pairs: DocumentChoice[] = [];
         const exported = exportedSources(result.data?.modelInfo);
         for (const source of result.data?.sources ?? []) {
            if (typeof source.name !== "string" || !exported.has(source.name))
               continue;
            for (const view of source.views ?? [])
               if (typeof view.name === "string")
                  pairs.push({ source: source.name, view: view.name });
         }
         if (pairs.length > 0) out.set(models[i], pairs);
      });
      return out;
      // eslint-disable-next-line react-hooks/exhaustive-deps
   }, [version, models]);
   return {
      choices,
      isLoading: results.some((q) => q.isLoading),
      // A model that will not load drops out of the choices rather than failing the list.
      isSuccess: results.length > 0 && results.every((q) => !q.isPending),
   };
}
