// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * The normaliser the materialization goldens are written through. It removes
 * only what differs between two runs of the same code, and nothing a refactor
 * could change:
 *
 *  - the temporary directory a case runs in, as a path or a `file:` URL;
 *  - content addresses hashed with a connection digest, which a DuckDB
 *    connection computes from its database path (a temporary file): each is
 *    replaced by the same address computed with a fixed digest, so an
 *    address still moves when the SQL or the folded layout does;
 *  - ISO timestamps (`dataAsOf`, build times) and wall-clock durations.
 *
 * Objects are written with sorted keys; arrays keep their order, which is
 * part of the behaviour (build order, considered order).
 */

import * as fs from "fs";
import { pathToFileURL } from "url";

/** Keys whose values are wall-clock measurements, dropped. */
const VOLATILE_KEYS = new Set([
   "durationMs",
   "executionTimeMs",
   "compileTimeMs",
   "ageSeconds",
   "builtAt",
   "createdAt",
   "updatedAt",
   "startedAt",
   "completedAt",
]);

/**
 * A served copy's storage alias, `__publisher_storage_<token>_<n>`: the token
 * is random per process and `n` counts the destinations it has aliased, so
 * both depend on what else ran first. See {@link Normalizer.addAlias}.
 */
const STORAGE_ALIAS = /__publisher_storage_[0-9a-f]{24}_\d+/g;

/**
 * A Windows path under a replaced root (`<pkg>\\m.malloy`, or JSON-escaped):
 * written with `/`, so a golden reads the same on every platform.
 */
const UNDER_LABEL = /(<(?:pkg|root|hammer)>)((?:\\{1,2}[\w.\- ]+)+)/g;

const ISO_TIMESTAMP =
   /\b\d{4}-\d{2}-\d{2}[T ]\d{2}:\d{2}:\d{2}(?:\.\d+)?(?:Z|[+-]\d{2}:?\d{2})\b/g;

export class Normalizer {
   private readonly replacements: [string, string][] = [];

   /** Replace `root` (a directory) wherever it appears, as `label`. */
   addRoot(root: string, label = "<root>"): this {
      const forms = new Set<string>([root]);
      try {
         forms.add(fs.realpathSync(root));
      } catch {
         // Not on disk (an in-memory corpus): the path as given.
      }
      for (const form of [...forms]) {
         forms.add(pathToFileURL(form).href);
         forms.add(form.replace(/\\/g, "/"));
         // JSON-escaped, for text that embeds a serialized path.
         forms.add(form.replace(/\\/g, "\\\\"));
      }
      for (const form of forms) this.replacements.push([form, label]);
      this.sortReplacements();
      return this;
   }

   /** Replace `alias`, a served copy's name for `destination`, with a stable label. */
   addAlias(alias: string, destination: string): this {
      return this.addReplacement(
         alias,
         `__served_copy_alias_of_${destination}`,
      );
   }

   /** Replace the string `from` with `to` wherever it appears. */
   addReplacement(from: string, to: string): this {
      if (from && from !== to) this.replacements.push([from, to]);
      this.sortReplacements();
      return this;
   }

   /** Longest first, so a root's URL form is replaced before its path. */
   private sortReplacements(): void {
      this.replacements.sort((x, y) => y[0].length - x[0].length);
   }

   text(value: string): string {
      let out = value.replace(/\r\n/g, "\n");
      for (const [from, to] of this.replacements) {
         if (out.includes(from)) out = out.split(from).join(to);
      }
      return out
         .replace(
            UNDER_LABEL,
            (_m, label: string, rest: string) =>
               `${label}${rest.replace(/\\{1,2}/g, "/")}`,
         )
         .replace(STORAGE_ALIAS, "__served_copy_alias_of_unknown")
         .replace(ISO_TIMESTAMP, "<timestamp>");
   }

   /** `value` as plain data: strings normalised, volatile keys dropped. */
   value(value: unknown, seen = new WeakSet<object>()): unknown {
      if (typeof value === "string") return this.text(value);
      if (
         value === null ||
         typeof value === "number" ||
         typeof value === "boolean"
      ) {
         return value;
      }
      if (typeof value === "bigint") return value.toString();
      if (value === undefined || typeof value === "function") return undefined;
      if (typeof value !== "object") return String(value);
      if (value instanceof Date) return "<timestamp>";
      if (seen.has(value)) return "<cycle>";
      seen.add(value);
      try {
         if (Array.isArray(value)) {
            return value.map((v) => this.value(v, seen) ?? null);
         }
         if (value instanceof Map) {
            return this.value(Object.fromEntries(value), seen);
         }
         if (value instanceof Set) return this.value([...value], seen);
         if (value instanceof URL) return this.text(value.href);
         const out: Record<string, unknown> = {};
         for (const key of Object.keys(value).sort()) {
            if (VOLATILE_KEYS.has(key)) continue;
            const v = this.value((value as Record<string, unknown>)[key], seen);
            if (v !== undefined) out[this.text(key)] = v;
         }
         return out;
      } finally {
         seen.delete(value);
      }
   }

   /** `value` normalised and written as indented JSON. */
   json(value: unknown): string {
      return JSON.stringify(this.value(value), null, 2);
   }
}
