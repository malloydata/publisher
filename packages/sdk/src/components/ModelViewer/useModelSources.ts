// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { useQuery } from "@tanstack/react-query";
import type * as Malloy from "@malloydata/malloy-interfaces";
import { extractCompiledModelGivens, type GivenInfo } from "./modelGivens";
import { extractSourceAccess, type SourceAccess } from "./sourceAccess";
import { getModelDocString } from "./docAnnotations";
import { useServer } from "../ServerProvider";

/** A model's named queries, as the publisher lists them on `CompiledModel.queries`. */
export interface ModelNamedQuery {
   name?: string;
   sourceName?: string;
   annotations?: string[];
}

const EMPTY_QUERIES: ModelNamedQuery[] = [];
const EMPTY_GIVENS: GivenInfo[] = [];
const EMPTY_ACCESS: ReadonlyMap<string, SourceAccess> = new Map();

/** What the viewer keeps of a compiled model. */
export interface CompiledModelData {
   sourceInfos: Malloy.SourceInfo[];
   queries: ModelNamedQuery[];
   /** The model file's Malloy text. Newer publishers return it on the compiled
    *  model (`sourceText`); older ones don't, and the SDK's type predates it,
    *  so it is read structurally and stays undefined when absent — the code
    *  view says so rather than showing an empty file. A SHAPE check, not a
    *  cast: through the router a declared-but-unsent field arrives as `null`,
    *  which must read as absent too. */
   sourceText?: string;
   /** The model's declared givens (shape-checked by extractCompiledModelGivens),
    *  so a run can say which defaults it used, or that it can't run without a
    *  value. */
   givens: GivenInfo[];
   /** Source name → its `#(authorize)` / `#(access_filter)` declarations (the
    *  compiled model's `sources[]`), for every source that declares either. */
   access: ReadonlyMap<string, SourceAccess>;
   /** The model file's own description (`##(doc)` / `##"`), from `modelInfo`. */
   modelDoc?: string;
   /** The model imports other files: an import-only model's page says why it
    *  is empty rather than reading as broken. */
   hasImports: boolean;
}

/**
 * Whether the compiled model imports other files. `modelDef` is returned by the
 * server but absent from the typed `CompiledModel` (an internal compiler blob),
 * so it is read by shape.
 */
function importsOf(data: unknown): boolean {
   const modelDef = (data as { modelDef?: unknown }).modelDef;
   if (typeof modelDef !== "string") return false;
   try {
      const def = JSON.parse(modelDef) as { imports?: unknown[] };
      return Array.isArray(def.imports) && def.imports.length > 0;
   } catch {
      return false;
   }
}

/**
 * The model's own doc: from the compiled model's `modelInfo` (a JSON string of
 * Malloy's `ModelInfo`) where it carries the model's annotations, else from the
 * model file's text. The publisher's `modelInfo` leaves `annotations` out today,
 * and a `##` line is a model annotation wherever it sits at the top level, so
 * the text answers the same question. Undefined when neither has one.
 */
export function modelDocOf(
   modelInfo: unknown,
   sourceText: string | undefined,
): string | undefined {
   if (typeof modelInfo === "string") {
      try {
         const doc = getModelDocString(
            (JSON.parse(modelInfo) as Malloy.ModelInfo).annotations,
         );
         if (doc) return doc;
      } catch {
         // Unreadable: fall through to the text.
      }
   }
   if (!sourceText) return undefined;
   const lines = sourceText
      .split("\n")
      .filter((line) => /^##(\(doc\)|")/.test(line.trim()))
      .map((value) => ({ value }));
   return getModelDocString(lines);
}

/**
 * The one shaping of a compiled-model response, for both hooks below: they
 * share a cache key, so they must store one shape, and a function is a
 * guarantee where a comment was not.
 */
export async function fetchCompiledModel(
   apiClients: ReturnType<typeof useServer>["apiClients"],
   projectName: string,
   packageName: string,
   modelPath: string,
   versionId?: string,
   includeHiddenFilesAndSources?: boolean,
): Promise<CompiledModelData> {
   const response = await apiClients.models.getModel(
      projectName,
      packageName,
      modelPath,
      versionId,
      includeHiddenFilesAndSources,
   );
   const sourceText = readSourceText(response.data);
   return {
      sourceInfos: (response.data.sourceInfos ?? []).map(
         (s) => JSON.parse(s) as Malloy.SourceInfo,
      ),
      queries: (response.data.queries ?? []) as ModelNamedQuery[],
      sourceText,
      givens: extractCompiledModelGivens(modelPath, response.data),
      access: extractSourceAccess(response.data),
      modelDoc: modelDocOf(response.data.modelInfo, sourceText),
      hasImports: importsOf(response.data),
   };
}

/** `sourceText` off a compiled-model response, by shape (see {@link CompiledModelData}). */
function readSourceText(data: unknown): string | undefined {
   const text = (data as { sourceText?: unknown }).sourceText;
   return typeof text === "string" ? text : undefined;
}

/**
 * The viewer's cache key for a compiled model, for a host that reads the same
 * model (and shares the one compile), or refreshes it after a republish.
 * A hidden file's read is its own entry: the curated read's 404 must never be
 * served in its place, nor its text to a reader who can't edit the package.
 */
export function modelQueryKey(
   projectName: string,
   packageName: string,
   modelPath: string,
   versionId?: string,
   includeHiddenFilesAndSources = false,
): readonly unknown[] {
   return [
      "model-viewer-sources",
      projectName,
      packageName,
      modelPath,
      versionId ?? "",
      ...(includeHiddenFilesAndSources ? ["off-surface"] : []),
   ];
}

/**
 * A compiled model's query: its one cache key and fetch, for every reader of a
 * compiled model. Compiling is server work, so
 * a pinned version — which can't change — is kept for the page's life; `latest`
 * goes stale like any other read.
 */
export function compiledModelQuery(
   apiClients: ReturnType<typeof useServer>["apiClients"],
   projectName: string,
   packageName: string,
   modelPath: string,
   versionId?: string,
   includeHiddenFilesAndSources = false,
) {
   return {
      queryKey: modelQueryKey(
         projectName,
         packageName,
         modelPath,
         versionId,
         includeHiddenFilesAndSources,
      ),
      queryFn: () =>
         fetchCompiledModel(
            apiClients,
            projectName,
            packageName,
            modelPath,
            versionId,
            includeHiddenFilesAndSources,
         ),
      ...(versionId && versionId !== "latest" ? { staleTime: Infinity } : {}),
   };
}

/**
 * Fetch the compiled sources for a published model and parse them into
 * `Malloy.SourceInfo` objects (`sourceInfos` is an array of JSON strings). The same
 * response lists the model's named queries, so they ride along rather than
 * costing a second fetch of the compiled model.
 */
export function useModelSources(
   projectName: string,
   packageName: string,
   modelPath: string,
   versionId?: string,
   /** False holds the read, e.g. until the page has resolved `latest` to the
    *  version it will read everything at. */
   enabled = true,
   /** Read a file the package's `index.malloy` hides, for a reader who can
    *  edit the package; anyone else is refused. */
   includeHiddenFilesAndSources = false,
) {
   const { apiClients } = useServer();

   const query = useQuery({
      ...compiledModelQuery(
         apiClients,
         projectName,
         packageName,
         modelPath,
         versionId,
         includeHiddenFilesAndSources,
      ),
      enabled: enabled && Boolean(projectName && packageName && modelPath),
   });

   return {
      sources: query.data?.sourceInfos ?? [],
      queries: query.data?.queries ?? EMPTY_QUERIES,
      sourceText: query.data?.sourceText,
      givens: query.data?.givens ?? EMPTY_GIVENS,
      access: query.data?.access ?? EMPTY_ACCESS,
      modelDoc: query.data?.modelDoc,
      hasImports: query.data?.hasImports ?? false,
      isLoading: query.isLoading,
      isError: query.isError,
      error: query.error as Error | null,
      // Exposed so the model pane's failure can offer a Retry: it replaces the
      // whole surface, which guide §6a says owns its error and its way out.
      refetch: query.refetch,
   };
}
