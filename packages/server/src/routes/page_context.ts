// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * The context Publisher hands a data-app page: which environment, package and
 * version it was served from, where the API lives, and which hosts may embed
 * it. Injected into the page's HTML as a JSON data block, so the runtime reads
 * it instead of guessing from the URL.
 */

import { hostOriginPattern } from "../frame_ancestors";

/** The request header a gateway sets to override the context, when trusted. */
export const PAGE_CONTEXT_HEADER = "x-publisher-page-context";

export const PAGE_CONTEXT_ELEMENT_ID = "publisher-context";

export interface PageContext {
   v: 1;
   environment: string;
   package: string;
   versionId: string | null;
   apiBase: string;
   /** Host patterns the runtime may talk to; `'self'` stands for the page's own origin. */
   hostOrigins: string[];
}

/** The fields a gateway may override; each is optional. */
export type PageContextOverride = Partial<
   Pick<PageContext, "environment" | "package" | "apiBase" | "hostOrigins">
>;

export type PageContextHeader =
   | { override: PageContextOverride }
   | { invalid: string };

const MAX_NAME_CHARS = 256;
const MAX_API_BASE_CHARS = 1024;
const MAX_HOST_ORIGINS = 32;
// eslint-disable-next-line no-control-regex -- the point is to refuse control characters
const CONTROL_CHARS = /[\u0000-\u001f\u007f]/;

/**
 * The override a request's page-context header carries, undefined when it
 * carries none. All or nothing: one bad field makes the whole header invalid,
 * so a half-trusted context never reaches a page. Unknown keys are ignored.
 */
export function parsePageContextHeader(
   raw: string | undefined,
): PageContextHeader | undefined {
   if (raw === undefined) return undefined;
   let parsed: unknown;
   try {
      parsed = JSON.parse(raw);
   } catch {
      return { invalid: "not JSON" };
   }
   if (parsed === null || typeof parsed !== "object" || Array.isArray(parsed)) {
      return { invalid: "not a JSON object" };
   }
   const fields = parsed as Record<string, unknown>;
   const override: PageContextOverride = {};
   for (const key of ["environment", "package"] as const) {
      const value = fields[key];
      if (value === undefined) continue;
      if (
         typeof value !== "string" ||
         value === "" ||
         value.length > MAX_NAME_CHARS ||
         CONTROL_CHARS.test(value)
      ) {
         return { invalid: `${key} is not a name` };
      }
      override[key] = value;
   }
   if (fields.apiBase !== undefined) {
      const { apiBase } = fields;
      if (
         typeof apiBase !== "string" ||
         !/^\/(?!\/)/.test(apiBase) ||
         apiBase.length > MAX_API_BASE_CHARS ||
         apiBase.includes("\\") ||
         CONTROL_CHARS.test(apiBase)
      ) {
         return { invalid: "apiBase is not a path" };
      }
      override.apiBase = apiBase;
   }
   if (fields.hostOrigins !== undefined) {
      const { hostOrigins } = fields;
      if (
         !Array.isArray(hostOrigins) ||
         hostOrigins.length > MAX_HOST_ORIGINS
      ) {
         return { invalid: "hostOrigins is not a short list" };
      }
      const patterns: string[] = [];
      for (const source of hostOrigins) {
         const pattern =
            typeof source === "string" ? hostOriginPattern(source) : undefined;
         if (pattern === undefined) {
            return { invalid: "hostOrigins has an unusable origin" };
         }
         patterns.push(pattern);
      }
      override.hostOrigins = [...new Set(patterns)];
   }
   return { override };
}

/**
 * The context as a `<script type="application/json">` element, ASCII only.
 * `<`, `>`, `&`, U+2028/9 and every non-ASCII character are written as
 * `\uXXXX`, so the element survives any charset the page is decoded in and no
 * value can close the element or open a comment inside it.
 */
export function pageContextScript(context: PageContext): string {
   const json = JSON.stringify(context).replace(
      /[<>&\u2028\u2029]|[^\x20-\x7e]/g,
      (char) => `\\u${char.charCodeAt(0).toString(16).padStart(4, "0")}`,
   );
   return `<script type="application/json" id="${PAGE_CONTEXT_ELEMENT_ID}">${json}</script>`;
}
