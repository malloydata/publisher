// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * One view of a compiled author model for the code that reads its text: its
 * sources by name (hidden dependencies an import leaves out of the namespace
 * included), a sourceID index, and readers for a declaration's verbatim text
 * or a whole file.
 *
 * Read by the build (the stored upstreams a source reaches, and how a strict
 * miss is named) and by the served copy and storage bindings. The caller
 * hands it the file reader, and a model's context is built once: a compiled
 * model never changes.
 *
 * Pure apart from that memo.
 */

export interface DerivedSourceDef {
   type?: unknown;
   /** The sourceID of the source this one extends, when it extends one. */
   extends?: unknown;
   /**
    * A query-derived source's query. Malloy sets no `extends` on one, so
    * `structRef` is the only link to the source it reads.
    */
   query?: { structRef?: unknown; pipeline?: unknown };
   /** Where the author declared the source, for lifting its text verbatim. */
   location?: SourceLocation;
   fields?: unknown[];
   filterList?: unknown[];
   /** The compiled model's own annotation record, read for `#@ -persist`. */
   annotations?: unknown;
   /** Malloy's own flag: this source carries `#@ persist`, inherited or not. */
   persistent?: unknown;
}

/**
 * The compiled-model facts a lift reads: `contents` by source name, a sourceID
 * index, and readers for a declaration's verbatim text or a whole file. Shared
 * by the serve path and the chained build so both read ONE view of the model.
 */
type AuthorModelLiftContext = {
   contents: Record<string, DerivedSourceDef & { sourceID?: unknown }>;
   sourceNameById: Map<string, string>;
   liftText: (location: SourceLocation) => string | undefined;
   fileText: (url: string) => string | undefined;
};

/**
 * One context per compiled model, which is immutable (a republish compiles a
 * new one), so author files are read once rather than per persist source.
 */
const liftContextByModel = new WeakMap<object, AuthorModelLiftContext>();

export function authorModelLiftContext(
   modelDef: unknown,
   readFile: (url: string) => string | undefined,
): AuthorModelLiftContext {
   const cacheable = modelDef !== null && typeof modelDef === "object";
   if (cacheable) {
      const cached = liftContextByModel.get(modelDef);
      if (cached) return cached;
   }
   const built = buildAuthorModelLiftContext(modelDef, readFile);
   if (cacheable) liftContextByModel.set(modelDef, built);
   return built;
}

function buildAuthorModelLiftContext(
   modelDef: unknown,
   readFile: (url: string) => string | undefined,
): AuthorModelLiftContext {
   type Def = DerivedSourceDef & { sourceID?: unknown };
   const md = modelDef as
      | {
           contents?: Record<string, Def>;
           sourceRegistry?: Record<
              string,
              { entry?: (Def & { type?: unknown; as?: unknown }) | undefined }
           >;
        }
      | undefined;
   const namespace = md?.contents ?? {};
   // The namespace plus hidden dependencies from `sourceRegistry`:
   // `import { weekly } from "orders.malloy"` leaves `daily`, which `weekly` is
   // built from, out of `contents`. Including them lets a stored parent reached
   // only through an import be found. The namespace wins a name clash.
   const contents: Record<string, Def> = {};
   for (const value of Object.values(md?.sourceRegistry ?? {})) {
      const entry = value?.entry;
      if (!entry || entry.type === "source_registry_reference") continue;
      const name = typeof entry.as === "string" ? entry.as : undefined;
      if (!name || name in namespace || name in contents) continue;
      contents[name] = entry;
   }
   Object.assign(contents, namespace);
   const sourceNameById = new Map<string, string>();
   for (const [name, def] of Object.entries(contents)) {
      if (typeof def?.sourceID === "string") {
         sourceNameById.set(def.sourceID, name);
      }
   }
   const fileCache = new Map<string, string | null>();
   const fileText = (url: string): string | undefined => {
      if (!url.startsWith("file:")) return undefined;
      if (!fileCache.has(url)) fileCache.set(url, readFile(url) ?? null);
      return fileCache.get(url) ?? undefined;
   };
   const liftText = (location: SourceLocation): string | undefined => {
      if (!location?.url) return undefined;
      const text = fileText(location.url);
      return text ? sliceSourceRange(text, location.range) : undefined;
   };
   return { contents, sourceNameById, liftText, fileText };
}

interface SourceRange {
   start: { line: number; character: number };
   end: { line: number; character: number };
}

export interface SourceLocation {
   url: string;
   range: SourceRange;
}

/**
 * Slice the text covered by a zero-based Malloy `location.range`. Undefined
 * when out of bounds (stale or mismatched source), so the caller skips the
 * refinement rather than emitting garbage.
 */
export function sliceSourceRange(
   text: string,
   range: SourceRange,
): string | undefined {
   const lines = text.split("\n");
   const { start, end } = range;
   if (start.line < 0 || start.line > end.line || end.line >= lines.length) {
      return undefined;
   }
   if (start.line === end.line) {
      return lines[start.line].slice(start.character, end.character);
   }
   let out = lines[start.line].slice(start.character);
   for (let i = start.line + 1; i < end.line; i++) {
      out += "\n" + lines[i];
   }
   out += "\n" + lines[end.line].slice(0, end.character);
   return out;
}
