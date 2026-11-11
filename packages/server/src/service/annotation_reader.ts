// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * How Publisher reads the annotation routes it owns: which route a note is on,
 * its payload, each route's closed key set, and one diagnostic shape.
 *
 * Route detection is Malloy's own (`routeOf` / `payloadOf`), never a regex, so
 * a block note (`#|(doc) … |#`) is read like a line note and a malformed
 * prefix declares nothing, as the compiler decides. `annotations.ts` keeps the
 * helpers for annotation bundles (own notes, model notes, positions); this
 * module is about routes.
 *
 * Materialization v2's routes (`#(materialize)`, `#(preaggregate)`) join the
 * registry in the phases that make them work (3 and 4), so nothing here
 * recognizes a spelling that still does nothing.
 */

import {
   payloadOf,
   routeOf,
   type Annotations,
   type DocumentLocation,
} from "@malloydata/malloy";
import { parseAnnotation, type Tag } from "@malloydata/malloy-tag";
import { parseRouteTag } from "./motly";

/** Malloy's routing for one note: its route, `""` for a MOTLY tag, `undefined` when the prefix is malformed. */
export function routeOfNote(text: string): string | undefined {
   return routeOf({ value: text.trimStart() } as Parameters<typeof routeOf>[0]);
}

/** A note's payload: everything after its prefix, dedented for a block note. */
export function payloadOfNote(text: string): string {
   return (
      payloadOf({ value: text.trimStart() } as Parameters<
         typeof payloadOf
      >[0]) ?? ""
   );
}

/** The payloads of every note in `texts` that is on `route`, in order. */
export function payloadsOnRoute(
   texts: ReadonlyArray<string>,
   route: string,
): string[] {
   return texts
      .filter((text) => routeOfNote(text) === route)
      .map(payloadOfNote);
}

/** How a route's payload reads. */
export type RoutePayload =
   /** A Malloy expression: `#(authorize) org_id = $ORG`. */
   | "expression"
   /** A quoted string or quoted list: `#(classification) ["phi.x"]`. */
   | "list"
   /** MOTLY tag properties, checked against `keys`. */
   | "tag"
   /** Free text for a person: `#(doc) …`. */
   | "prose"
   /** No payload: the route alone says it, `#(secure)`. */
   | "none";

export interface RouteSpec {
   payload: RoutePayload;
   /** For a `tag` payload: every top-level key the route accepts. */
   keys?: ReadonlySet<string>;
   /** Keys the route no longer accepts, each with what to write instead. */
   retiredKeys?: Readonly<Record<string, string>>;
}

/**
 * The `#@` persistence route's keys. `persist` and its directives, and the
 * pre-aggregation directive and its keys, which share the route.
 */
const PERSIST_KEYS: ReadonlySet<string> = new Set([
   "persist",
   "name",
   "storage",
   "realization",
   "refresh",
   "watermark",
   "merge_key",
   "freshness",
   "queryMetadata",
   "partition",
]);

/** The pre-aggregation directive's keys, on the same `#@` route. */
const PREAGGREGATE_KEYS: ReadonlySet<string> = new Set([
   "preaggregate",
   "grain",
   "namespace",
]);

/**
 * The routes Publisher reads, keyed by route: the bracketed app routes it
 * owns, and the Malloy routes it reads directives from (`@`, `!`, and `""`
 * for MOTLY tags).
 */
export const PUBLISHER_ROUTES: Readonly<Record<string, RouteSpec>> = {
   authorize: { payload: "expression" },
   access_filter: { payload: "expression" },
   classification: { payload: "list" },
   carries: { payload: "list" },
   secure: { payload: "none" },
   index: { payload: "tag" },
   dbmeta: { payload: "tag" },
   doc: { payload: "prose" },
   markdown: { payload: "prose" },
   "@": {
      payload: "tag",
      keys: new Set([...PERSIST_KEYS, ...PREAGGREGATE_KEYS]),
      retiredKeys: {
         sharing:
            "set materialization.scope in publisher.json; a source no longer chooses its own sharing",
         schedule:
            "set materialization.schedule in publisher.json; a source no longer sets its own schedule",
      },
   },
   "!": { payload: "tag" },
   "": { payload: "tag" },
};

/**
 * Every key a `#@ persist` line may carry, retired ones included so they get
 * their own message rather than a vaguer "unknown key". Anything else is a
 * load-time warning, never a refusal, so a new directive needs no Publisher
 * change.
 */
export const RECOGNIZED_PERSIST_KEYS: ReadonlySet<string> = new Set([
   ...PERSIST_KEYS,
   ...Object.keys(PUBLISHER_ROUTES["@"]?.retiredKeys ?? {}),
]);

/** One diagnostic about an annotation, with its tier from the outcome rule. */
export interface AnnotationFinding {
   route: string;
   /**
    * `malformed`: it cannot be read as written, and is refused at publish.
    * `unservable`: well-formed but not honoured here; a coded warning, served
    * live. `deprecated`: still read, with its rewrite.
    */
   tier: "malformed" | "unservable" | "deprecated";
   /** Stable, for counting and for clients: `persist-unknown-key`. */
   code: string;
   message: string;
   /** The fix, when there is one. */
   rewrite?: string;
   /** The source or field the annotation is on. */
   subject?: string;
   at?: DocumentLocation;
}

/**
 * A route's tag, read without the server's environment (see `parseRouteTag`),
 * and a finding for each top-level key the route does not accept or has
 * retired. The findings are reported at the tier the callers already use:
 * an unknown key is `unservable` (a warning), a retired one `malformed`.
 */
export function readRouteTag(
   annotations: Annotations,
   route: string,
): { tag: Tag; findings: AnnotationFinding[] } {
   const { tag } = parseRouteTag(annotations, route);
   const spec = PUBLISHER_ROUTES[route];
   const findings: AnnotationFinding[] = [];
   if (spec?.keys) {
      for (const [key] of tag.entries()) {
         const retired = spec.retiredKeys?.[key];
         if (retired !== undefined) {
            findings.push({
               route,
               tier: "malformed",
               code: `${route === "@" ? "persist" : route}-retired-key`,
               message: `\`${key}=\` is no longer read on \`#${route}\``,
               rewrite: retired,
            });
         } else if (!spec.keys.has(key)) {
            findings.push({
               route,
               tier: "unservable",
               code: `${route === "@" ? "persist" : route}-unknown-key`,
               message: `\`${key}=\` is not a key \`#${route}\` reads, so it does nothing`,
            });
         }
      }
   }
   return { tag, findings };
}

/**
 * Read a list-route payload as a tag value: a quoted string (`"x"`) or a
 * quoted, comma-separated list (`["x", "y"]`), the form every list-valued
 * annotation in Publisher uses (`tiles=[…]`, `# drill { to=[…] }`). Parsed by
 * Malloy's own tag parser, so a malformed list is a parse error with its
 * reason, never a silent split. `values` is a sorted set; `malformed` says
 * what is wrong in the words the load uses.
 */
export function readRouteList(
   route: string,
   content: string,
   { list }: { list: "required" | "optional" },
): { values: string[]; malformed?: string } {
   const trimmed = content.trim();
   const example =
      route === "carries"
         ? '["phi.clinical", "none"]'
         : '"phi.direct_identifier" or ["phi.direct_identifier", "gdpr.personal"]';
   const fault = (why: string) => ({
      values: [] as string[],
      malformed: `has a #(${route}) annotation that ${why}; write ${list === "required" ? "a quoted, comma-separated list" : "a quoted string or a quoted, comma-separated list"}, such as #(${route}) ${example}`,
   });
   if (trimmed === "") return fault("names nothing");
   // An environment reference would be hydrated from the server's own
   // environment by the tag parser; never read one here.
   if (/@env\./.test(trimmed)) return fault("reads the environment");
   const isList = trimmed.startsWith("[");
   if (!isList && !trimmed.startsWith('"')) {
      return fault(`is not quoted ("${trimmed}")`);
   }
   if (!isList && list === "required") return fault("is not a list");
   let parsed: ReturnType<typeof parseAnnotation>;
   try {
      parsed = parseAnnotation(`# v=${trimmed}`);
   } catch (error) {
      return fault(
         `does not parse (${error instanceof Error ? error.message : String(error)})`,
      );
   }
   if (parsed.log.length > 0) {
      return fault(`does not parse (${parsed.log[0]?.message})`);
   }
   const items = isList
      ? parsed.tag.array("v")?.map((item) => item.text())
      : [parsed.tag.text("v")];
   if (!items || items.some((item) => typeof item !== "string")) {
      return fault("holds something other than quoted text");
   }
   const values = Array.from(
      new Set((items as string[]).map((v) => v.trim())),
   ).sort(byCodepoint);
   if (values.length === 0) return fault("names nothing");
   if (values.some((v) => v === "")) return fault("names an empty value");
   return { values };
}

/** Codepoint order, so two servers on identical bytes agree. */
function byCodepoint(a: string, b: string): number {
   return a < b ? -1 : a > b ? 1 : 0;
}
