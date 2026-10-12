// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { useQueries } from "@tanstack/react-query";
import { useMemo } from "react";
import type { CompiledModel } from "../../client";
import { useQueryWithApiError } from "../../hooks/useQueryWithApiError";
import type { ListedReport } from "../Package/documentLocation";
import { useServer } from "../ServerProvider";
import { buildCatalog, type PackageCatalog } from "./catalog";

/**
 * The catalog a report's builder offers: every source the package publishes,
 * less the reports (offering a report's own tile views would be circular) and
 * this file. A tile runs against this file and may read only the package
 * surface; files off it are not readable anyway (their model GET is 404).
 *
 * Nothing cached depends on the listing or on `modelPath`: the model listing
 * is cached per package version, and each model under its own path, so the
 * exclusions are applied on render and always follow the current listing and
 * path. A listing that failed and then recovered, or a second report opened
 * from the same cache, never inherits what a first read left out. The models
 * excluded now are not fetched, so the builder never re-reads its own file or
 * another report. Undefined until the listing and every model it needs have
 * answered.
 */
export function useEditorCatalog({
   environmentName,
   packageName,
   versionId,
   modelPath,
   reports,
   reportsSettled,
}: {
   environmentName: string;
   packageName: string;
   versionId?: string;
   modelPath: string;
   reports: readonly ListedReport[];
   reportsSettled: boolean;
}): PackageCatalog | undefined {
   const { apiClients, server } = useServer();
   const { data: listed } = useQueryWithApiError<string[]>({
      queryKey: [
         "report-editor-models",
         environmentName,
         packageName,
         versionId,
      ],
      queryFn: async () =>
         (
            await apiClients.models.listModels(
               environmentName,
               packageName,
               versionId,
            )
         ).data.flatMap((m) => (m.path && !m.error ? [m.path] : [])),
   });
   const excluded = useMemo(
      () =>
         new Set([
            modelPath,
            ...reports.flatMap((r) => (r.path ? [r.path] : [])),
         ]),
      [reports, modelPath],
   );
   const wanted = useMemo(
      () =>
         reportsSettled
            ? (listed ?? []).filter((path) => !excluded.has(path))
            : [],
      [listed, excluded, reportsSettled],
   );
   const results = useQueries({
      queries: wanted.map((path) => ({
         // `useQueries` bypasses `useQueryWithApiError`, so the server is added here to keep two servers' caches apart.
         queryKey: [
            "report-editor-model",
            environmentName,
            packageName,
            path,
            versionId,
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
         retry: false,
      })),
   });
   // One model that fails to load (a reload racing this fetch, say) costs the
   // catalog that model's sources, not every suggestion.
   const settled =
      listed !== undefined &&
      reportsSettled &&
      results.every((r) => r.isSuccess || r.isError);
   // `useQueries` returns a new array each render; its data changes when `dataUpdatedAt` does.
   const version = results.map((r) => r.dataUpdatedAt).join();
   return useMemo(
      () =>
         settled
            ? buildCatalog(
                 results.flatMap((r) =>
                    r.data ? [r.data as CompiledModel] : [],
                 ),
                 excluded,
              )
            : undefined,
      // `results` is read through `wanted` and `version`, which change when the set of models or any one's data does.
      // eslint-disable-next-line react-hooks/exhaustive-deps
      [settled, wanted, version, excluded],
   );
}
