// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { type ModelDef } from "@malloydata/malloy";
import { components } from "../api";
import { logger } from "../logger";
import {
   type AuthorizeMap,
   type AuthorizeOwnNotesMap,
   type MisplacedAuthorizeAnnotation,
} from "./authorize";
import { type FilterDefinition } from "./filter";
import {
   extractQueriesFromModelDef,
   extractSourcesFromModelDef,
} from "./source_extraction";

type ApiSource = components["schemas"]["Source"];
type ApiGiven = components["schemas"]["Given"];
type ApiQuery = components["schemas"]["Query"];

export function extractApiQueries(modelDef: ModelDef): {
   queries: ApiQuery[];
   misplacedAuthorize: MisplacedAuthorizeAnnotation[];
} {
   // Shared with the package-load worker — see service/source_extraction.ts.
   const { queries, misplacedAuthorize } = extractQueriesFromModelDef(modelDef);
   return { queries: queries as unknown as ApiQuery[], misplacedAuthorize };
}

export function extractApiSources(
   modelDef: ModelDef,
   givens?: ApiGiven[],
): {
   sources: ApiSource[];
   filterMap: Map<string, FilterDefinition[]>;
   authorizeMap: AuthorizeMap;
   misplacedAuthorize: MisplacedAuthorizeAnnotation[];
   authorizeOwnNotes: AuthorizeOwnNotesMap;
   attributedAuthorizeOwnNotes: AuthorizeOwnNotesMap;
} {
   // Shared with the package-load worker — see service/source_extraction.ts.
   // The service path logs filter parse failures; the worker stays silent.
   const {
      sources,
      filterMap,
      authorizeMap,
      misplacedAuthorize,
      authorizeOwnNotes,
      attributedAuthorizeOwnNotes,
   } = extractSourcesFromModelDef(modelDef, givens, (sourceName, err) =>
      logger.warn(
         `Failed to parse filter annotations on source "${sourceName}"`,
         { error: err },
      ),
   );
   return {
      sources: sources as unknown as ApiSource[],
      filterMap,
      authorizeMap,
      misplacedAuthorize,
      authorizeOwnNotes,
      attributedAuthorizeOwnNotes,
   };
}

/** Whether a top-level `run:` reads a source named in `published`. */
export function runReadsPublished(
   query: unknown,
   published: ReadonlySet<string>,
): boolean {
   const ref = (query as { structRef?: unknown }).structRef;
   const name =
      typeof ref === "string"
         ? ref
         : ((ref as { as?: string; name?: string } | undefined)?.as ??
           (ref as { name?: string } | undefined)?.name);
   return name !== undefined && published.has(name);
}

/** The keys of a compiled struct that hold a `name@file` identity of another
 *  source: what it extends, and what a join points at. */
const SOURCE_IDENTITY_KEYS = new Set(["extends", "sourceID", "referenceID"]);

/**
 * A copy of `def` without identities of sources outside `published`. A
 * published source built on a hidden one (`source: pub is hidden extend
 * {...}`) records `extends: "hidden@file"`, and a join records the joined
 * source's identity even when the join is renamed.
 *
 * A join to a hidden source also carries that source's whole compiled
 * definition: its table path or SQL, its connection, and its fields. It is cut
 * down to what the published source's field paths need, the join's own name
 * and the names and types of the fields reached through it.
 */
export function scrubHiddenIds<T>(def: T, published: ReadonlySet<string>): T {
   const hidden = (id: unknown) =>
      typeof id === "string" && !published.has(id.split("@")[0]);
   const walk = (value: unknown): unknown => {
      if (Array.isArray(value)) return value.map(walk);
      if (!value || typeof value !== "object") return value;
      const record = value as Record<string, unknown>;
      if (
         typeof record.join === "string" &&
         (hidden(record.sourceID) || hidden(record.referenceID))
      ) {
         return joinOutline(record);
      }
      const out: Record<string, unknown> = {};
      for (const [key, inner] of Object.entries(record)) {
         if (SOURCE_IDENTITY_KEYS.has(key) && hidden(inner)) continue;
         out[key] = walk(inner);
      }
      return out;
   };
   return walk(def) as T;
}

/** A field reached through a hidden join, as its name and type only, and a
 *  join nested inside one the same way. */
function joinOutline(field: Record<string, unknown>): Record<string, unknown> {
   const name = field.as ?? field.name;
   if (typeof field.join !== "string") {
      return { type: field.type, name };
   }
   const fields = Array.isArray(field.fields) ? field.fields : [];
   return {
      type: field.type,
      join: field.join,
      name,
      fields: fields.map((inner) =>
         inner && typeof inner === "object"
            ? joinOutline(inner as Record<string, unknown>)
            : inner,
      ),
   };
}
