// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import type { CompiledDocument, CompileResult } from "../client";
import { useServer } from "../components/ServerProvider";
import { useQueryWithApiError } from "./useQueryWithApiError";

export interface CompiledDocumentSpec {
   environmentName: string;
   packageName: string;
   /** The model the text is compiled on top of: what it may name is what this model offers the viewer. */
   modelPath: string;
   source: string;
   /** The values the host sets for the givens a source's gate reads; a gate sent none of its givens denies, so every tile would come back `restricted`. */
   givens?: Record<string, string>;
}

/**
 * The document a text compiles to, read for the viewer who asks: the manifest
 * and cells the server would serve, with the tiles and cells that viewer may
 * not read marked `restricted`. Compile-only; nothing runs.
 */
export function useCompiledDocument(
   spec: CompiledDocumentSpec,
   { enabled = true }: { enabled?: boolean } = {},
) {
   const { apiClients } = useServer();
   return useQueryWithApiError<{
      document?: CompiledDocument;
      result: CompileResult;
   }>({
      queryKey: [
         "compiled-document",
         spec.environmentName,
         spec.packageName,
         spec.modelPath,
         spec.source,
         spec.givens ?? null,
      ],
      queryFn: async () => {
         const result = (
            await apiClients.models.compileModelSource(
               spec.environmentName,
               spec.packageName,
               spec.modelPath,
               {
                  source: spec.source,
                  scope: "append",
                  ...(spec.givens && Object.keys(spec.givens).length > 0
                     ? { givens: spec.givens }
                     : {}),
               },
            )
         ).data;
         return { document: result.document, result };
      },
      enabled,
   });
}
