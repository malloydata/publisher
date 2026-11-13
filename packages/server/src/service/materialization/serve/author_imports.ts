// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * The `import` lines a generated variant model writes so a caller's query
 * text compiles against it unchanged: every name of the author's model (its
 * sources, queries and givens), each imported from the file that declares it,
 * under the same name. Shared by the router legs that compile a caller's text
 * in a model of their own: the persisted-view leg (`view_variants.ts`) and
 * the rollup leg (#1651, `rollup_variants.ts`).
 *
 * A name the model exports is imported from the model itself; any other from
 * the file its `sourceID` (or, for a query, its location) names, under its
 * declared name. Every name goes through `malloy_text`.
 */

import * as path from "path";
import { fileURLToPath } from "url";
import { doubleQuotedString, identifier } from "../../malloy_text";
import { overlayKey } from "../../overlay_key";
import { splitSourceID } from "../plan/persisted_view";

/** `url` relative to `from`'s directory, for an `import`, or undefined. */
export function relativeImport(from: URL, url: string): string | undefined {
   try {
      return path
         .relative(path.dirname(fileURLToPath(from)), fileURLToPath(url))
         .split(path.sep)
         .join("/");
   } catch {
      return undefined;
   }
}

export interface AuthorImportOptions {
   /** Names the variant declares itself, never imported. */
   skip?: ReadonlySet<string>;
   /**
    * Names declared in `from` (an overlay key) other than `keep` are imported
    * from `to` instead: a name a file imports is not exported again, so when
    * the variant overlays the declaring file, only `keep` stays there.
    */
   redirect?: { from: string; to: string; keep: string };
}

/**
 * The import lines for the author's model's names, as seen from
 * `queryModelURL` (`modelURL` is the author's model), one per file.
 */
export function authorNameImports(
   modelDef: unknown,
   queryModelURL: URL,
   modelURL: string,
   options: AuthorImportOptions = {},
): string {
   const model = modelDef as {
      contents?: Record<string, unknown>;
      exports?: string[];
      givens?: Record<string, { name?: unknown; location?: { url?: unknown } }>;
   };
   const exported = new Set(model.exports ?? []);
   const byFile = new Map<string, string[]>();
   const { skip, redirect } = options;
   const add = (url: string, item: string, declaredName: string) => {
      const target =
         redirect &&
         overlayKey(url) === redirect.from &&
         declaredName !== redirect.keep
            ? redirect.to
            : url;
      byFile.set(target, [...(byFile.get(target) ?? []), item]);
   };
   for (const [name, entry] of Object.entries(model.contents ?? {})) {
      if (skip?.has(name)) continue;
      if (exported.has(name)) {
         add(modelURL, identifier(name), name);
         continue;
      }
      const e = entry as {
         sourceID?: unknown;
         name?: unknown;
         location?: { url?: unknown };
      };
      const declared =
         typeof e.sourceID === "string" ? splitSourceID(e.sourceID) : undefined;
      const url =
         declared?.url ??
         (typeof e.location?.url === "string" ? e.location.url : undefined);
      const declaredName =
         declared?.name ?? (typeof e.name === "string" ? e.name : name);
      if (!url) continue;
      add(
         url,
         declaredName === name
            ? identifier(name)
            : `${identifier(name)} is ${identifier(declaredName)}`,
         declaredName,
      );
   }
   // Givens are not in `contents`; each comes from the file declaring it.
   for (const given of Object.values(model.givens ?? {})) {
      const url = given.location?.url;
      if (typeof given.name !== "string" || typeof url !== "string") continue;
      if (skip?.has(given.name)) continue;
      add(url, identifier(given.name), given.name);
   }
   return [...byFile.entries()]
      .map(([url, items]) => {
         const from = relativeImport(queryModelURL, url);
         if (from === undefined) return "";
         // malloy-text: each item is identifier(name) or identifier(name) is identifier(declared)
         return `import { ${items.join(", ")} } from ${doubleQuotedString(from)}`;
      })
      .filter((line) => line !== "")
      .join("\n");
}
