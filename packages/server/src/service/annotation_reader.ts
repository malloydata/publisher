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

/**
 * A prefix glued to its payload on a route Publisher reads: `#(carries)["x"]`,
 * `#"Orders`. Malloy calls these malformed and declares nothing, but
 * Publisher's readers have always read them, and a `#(carries)` that silently
 * stopped counting would fail its leak check open. So they keep their route,
 * and the rest of the line is the payload; reporting them is a finding's job.
 */
const GLUED_PREFIX = /^\s*##?\((\w+)\)(?=\S)([\s\S]*)$/;
const GLUED_DOC_STRING = /^\s*#"(?=[^\s"])([\s\S]*)$/;

/** A note's route and payload as Publisher reads it, or `undefined` when it is on no route. */
function readNote(
   text: string,
): { route: string; payload: string } | undefined {
   const route = routeOfNote(text);
   if (route !== undefined) return { route, payload: payloadOfNote(text) };
   const glued = GLUED_PREFIX.exec(text);
   if (glued && glued[1] !== undefined && glued[1] in PUBLISHER_ROUTES) {
      return { route: glued[1], payload: glued[2] ?? "" };
   }
   const docString = GLUED_DOC_STRING.exec(text);
   if (docString) return { route: '"', payload: docString[1] ?? "" };
   return undefined;
}

/**
 * The route a note is on as Publisher reads it (glued prefixes included), or
 * `undefined` when it is on no route.
 */
export function publisherRouteOf(text: string): string | undefined {
   return readNote(text)?.route;
}

/** Whether a note is on `route`, as Publisher reads it (glued prefixes included). */
export function isOnRoute(text: string, route: string): boolean {
   return readNote(text)?.route === route;
}

/** The payloads of every note in `texts` that is on `route`, in order. */
export function payloadsOnRoute(
   texts: ReadonlyArray<string>,
   route: string,
): string[] {
   const out: string[] = [];
   for (const text of texts) {
      const note = readNote(text);
      if (note?.route === route) out.push(note.payload);
   }
   return out;
}

/** A sorted set, in codepoint order, so two servers on identical bytes agree. */
export function tokenSet(tokens: Iterable<string>): string[] {
   return Array.from(new Set(tokens)).sort(byCodepoint);
}

/** Codepoint order, so two servers on identical bytes agree. */
export function byCodepoint(a: string, b: string): number {
   return a < b ? -1 : a > b ? 1 : 0;
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
 * their own message rather than a vaguer "unknown key". Narrower than the
 * `@` route's `keys`, which also hold the `#@ preaggregate` line's keys: a
 * persist source is checked against this set, a rollup against its own.
 * Anything else is a load-time warning, never a refusal, so a new directive
 * needs no Publisher change.
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
   /** The keys to accept, when narrower than the route's (a persist source's). */
   keys?: ReadonlySet<string>,
): { tag: Tag; findings: AnnotationFinding[] } {
   const { tag } = parseRouteTag(annotations, route);
   const spec = PUBLISHER_ROUTES[route];
   const accepted = keys ?? spec?.keys;
   const findings: AnnotationFinding[] = [];
   if (accepted) {
      for (const [key] of tag.entries()) {
         const retired = spec?.retiredKeys?.[key];
         if (retired !== undefined) {
            findings.push({
               route,
               tier: "malformed",
               code: `${route === "@" ? "persist" : route}-retired-key`,
               message: `\`${key}=\` is no longer read on \`#${route}\``,
               rewrite: retired,
            });
         } else if (!accepted.has(key)) {
            const near = nearestKey(key, accepted);
            findings.push({
               route,
               tier: "unservable",
               code: `${route === "@" ? "persist" : route}-unknown-key`,
               message: `\`${key}=\` is not a key \`#${route}\` reads, so it does nothing${near ? ` (did you mean \`${near}=\`?)` : ""}`,
            });
         }
      }
   }
   return { tag, findings };
}

/**
 * The findings on one persist source's `#@` route: a retired key
 * (malformed) and an unknown key (unservable). A line that does not parse is
 * reported from Malloy's build log ({@link persistUnparseableFinding}). Each
 * names the source in `subject`, and its model in `model` when known.
 */
export function persistAnnotationFindings(
   annotations: Annotations,
   subject: string,
   model?: string,
   { incremental = false }: { incremental?: boolean } = {},
): PackageAnnotationFinding[] {
   return readRouteTag(annotations, "@", RECOGNIZED_PERSIST_KEYS).findings.map(
      (finding) => {
         // On an incremental source a typo'd `merge_key=` has a quiet cost:
         // the delta is applied by range replace instead of by row identity.
         const consequence =
            incremental && finding.code === "persist-unknown-key"
               ? "; on this incremental source a misspelled merge_key= leaves its delta applied by range replace instead of by row identity"
               : "";
         return {
            ...finding,
            message: `#@ persist source "${subject}": ${finding.message}${consequence}`,
            subject,
            ...(model ? { model } : {}),
         };
      },
   );
}

/**
 * The accepted key within edit distance 2 of `key`, closest first, or
 * undefined: the likely intended spelling of an unknown key.
 */
function nearestKey(
   key: string,
   accepted: ReadonlySet<string>,
): string | undefined {
   let best: { key: string; distance: number } | undefined;
   for (const candidate of [...accepted].sort(byCodepoint)) {
      const distance = editDistance(key.toLowerCase(), candidate.toLowerCase());
      if (distance <= 2 && (!best || distance < best.distance)) {
         best = { key: candidate, distance };
      }
   }
   return best?.key;
}

/** Levenshtein distance between two short strings. */
export function editDistance(a: string, b: string): number {
   let previous = Array.from({ length: b.length + 1 }, (_, j) => j);
   for (let i = 1; i <= a.length; i++) {
      const current = [i];
      for (let j = 1; j <= b.length; j++) {
         current[j] = Math.min(
            (previous[j] ?? 0) + 1,
            (current[j - 1] ?? 0) + 1,
            (previous[j - 1] ?? 0) + (a[i - 1] === b[j - 1] ? 0 : 1),
         );
      }
      previous = current;
   }
   return previous[b.length] ?? 0;
}

/** A finding as a package load carries it: which model it is in, when known. */
export type PackageAnnotationFinding = AnnotationFinding & { model?: string };

/**
 * A `#@` tag Malloy's build walk could not parse. Malloy drops the source from
 * the build entirely, so nothing else records that it was annotated: this is
 * the only trace, and it is malformed (refused at publish).
 */
export function persistUnparseableFinding(
   entry: { message: string; at?: DocumentLocation },
   model: string,
   /**
    * The planned source the broken line is on, when Malloy still planned it
    * from its other `#@` lines; absent when the source was dropped.
    */
   planned?: { subject: string; line: string },
): PackageAnnotationFinding {
   const where = entry.at ? ` (line ${entry.at.range.start.line + 1})` : "";
   const message = planned
      ? `#@ persist source "${planned.subject}": the #@ line${where} ` +
        `\`${planned.line.trim()}\` does not parse (${entry.message}), so its ` +
        `directives are not read: the source is built from its other #@ ` +
        `lines without them. Fix the line's syntax.`
      : `a #@ annotation${where} does not parse (${entry.message}), so the ` +
        `source it is on is not materialized and is served live. Fix the ` +
        `annotation's syntax.`;
   return {
      route: "@",
      tier: "malformed",
      code: "persist-unparseable",
      message,
      model,
      ...(planned ? { subject: planned.subject } : {}),
      ...(entry.at ? { at: entry.at } : {}),
   };
}

/** A package warning's severity for a finding's tier: malformed is an error. */
export function findingSeverity(
   finding: Pick<AnnotationFinding, "tier">,
): "error" | "warn" {
   return finding.tier === "malformed" ? "error" : "warn";
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
   const values = tokenSet((items as string[]).map((v) => v.trim()));
   if (values.length === 0) return fault("names nothing");
   if (values.some((v) => v === "")) return fault("names an empty value");
   return { values };
}

const REGEXP_SPECIAL = /[.*+?^${}()|[\]\\]/g;

/**
 * Whether `text` reads as an author reaching for `route`, written as one of
 * `names` (case-insensitive), while Malloy routes it somewhere else, so no
 * reader of `route` ever sees it. Keyed on Malloy's route, so it cannot claim
 * another app's namespace:
 *
 *  - `""` (a MOTLY tag): only when the payload is a bracketed name, as in
 *    `# (doc) …`; an ordinary render tag never matches.
 *  - `undefined` (a malformed prefix): when the text was reaching for one of
 *    the names, as in `#( doc )`, `#(doc)X`, `#doc`.
 *  - any other route: when it is one of the names spelled another way
 *    (`#(Doc)`), never a different word.
 *
 * `names` must not be empty: an empty alternation would match every
 * malformed note. Shared by the gate refusal and the near-miss scan.
 */
export function isRouteAttempt(
   text: string,
   route: string,
   names: readonly string[],
): boolean {
   if (names.length === 0) {
      throw new Error(
         `No near-miss spellings for route \`${route}\`: an empty alternation ` +
            "matches every malformed note.",
      );
   }
   const trimmed = text.trimStart();
   const noted = routeOfNote(trimmed);
   if (noted === route) return false;
   const words = names
      .map((name) => name.replace(REGEXP_SPECIAL, "\\$&"))
      .join("|");
   if (noted === undefined) {
      return new RegExp(
         `^##?\\|?[ \\t]*(?:[([{<][ \\t]*)?(?:${words})`,
         "iu",
      ).test(trimmed);
   }
   if (noted === "") {
      return new RegExp(
         `^[ \\t]*[([{<][ \\t]*(?:${words})[ \\t]*[)\\]}>]`,
         "iu",
      ).test(payloadOfNote(trimmed));
   }
   const lowered = noted.toLowerCase();
   return names.some((name) => name.toLowerCase() === lowered);
}
