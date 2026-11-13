// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * The source map's keying rule, enforced as a ratchet: a declaration is
 * identified by its `sourceID`, a stored table by `(sourceEntityId,
 * destination)`, and a source's name is for display and logs only, never a map
 * key or a gate (docs/materialization-source-map.md).
 *
 * This scans materialization's source for a source's (or binding's, plan's,
 * rollup's) `.name` / `.sourceName` used as a key: `.get/.has/.set/.delete/
 * .includes(x.name)` or an index `obj[x.name]` (not an array literal). The uses that exist are listed below with
 * the reason each is allowed; any new one fails. Phase 3 shrinks the list: when
 * a use is migrated to `sourceID`, delete its entry (a stale entry fails too, so
 * the list cannot drift).
 */

import { describe, expect, it } from "bun:test";
import * as fs from "fs";
import * as path from "path";

const ROOT = __dirname;

/**
 * `.sourceName` is a source's name on whatever carries it (a binding, a manifest
 * entry, a lift), so any variable counts; `.name` is too common to scan on any
 * variable, so it counts only on one whose name says it is a source, binding,
 * plan or rollup.
 */
const NAME_KEY = new RegExp(
   [
      String.raw`\.(?:set|get|has|delete|includes)\(\s*[A-Za-z_][A-Za-z0-9_]*\.sourceName\b`,
      String.raw`(?:[\w\])]|\?\.)\[\s*[A-Za-z_][A-Za-z0-9_]*\.sourceName\s*\]`,
      String.raw`\.(?:set|get|has|delete|includes)\(\s*[A-Za-z_]*(?:[Ss]ource|[Bb]inding|[Pp]lan|[Rr]ollup|persist)[A-Za-z_]*\.name\b`,
      String.raw`(?:[\w\])]|\?\.)\[\s*[A-Za-z_]*(?:[Ss]ource|[Bb]inding|[Pp]lan|[Rr]ollup)[A-Za-z_]*\.name\s*\]`,
   ].join("|"),
);

/**
 * file → the trimmed lines allowed to key by name, and why. A name is a fine key
 * where it IS the lookup's subject: a model's own namespace (`contents[name]`),
 * or a caller's request that names sources (a scoped run's `sourceNames`).
 */
const ALLOWED: Record<string, Record<string, string>> = {
   "plan/build_plan.ts": {
      "const plan = planByName.get(source.name);":
         "generated #(materialize) build sources by name within one generated companion, where each name is unique; migrate with the rollup plans",
      "if (include && !include.has(source.name)) continue;":
         "a scoped run's sourceNames filter is a caller request by name",
   },
   "run/storage_build_strategy.ts": {
      "}).filter((d) => onPath.has(d.sourceName));":
         "names on one downstream's path within one model's namespace (the lift is per model)",
   },
   "run/run_instructions.ts": {
      "if (include && !include.has(persistSource.name)) continue;":
         "a scoped run's sourceNames filter is a caller request by name",
   },
   "serve/materialization_serve_transform.ts": {
      "const named = byName.get(source.name) ?? [];":
         "planSourceIds resolves a manifest entry's name to plan sourceIDs (name → id index)",
      "byName.set(source.name, named);":
         "planSourceIds resolves a manifest entry's name to plan sourceIDs (name → id index)",
      "const sourceID = contents?.[binding.sourceName]?.sourceID;":
         "the model's own namespace, resolving a name to its sourceID",
      "else if (!group.includes(source.name)) group.push(source.name);":
         "aliases grouped per address for rebinding by name in Malloy text",
      "...(aliasesBySourceName[entry.sourceName] ?? []).filter(":
         "aliases grouped per address for rebinding by name in Malloy text",
      "const url = ctx.contents[lift.sourceName]?.location?.url;":
         "the model's own namespace",
      "dependsOn.set(b.sourceName, set);":
         "orders one serve shape's bindings by the Malloy names they declare",
      "if (!remaining.has(b.sourceName)) continue;":
         "orders one serve shape's bindings by the Malloy names they declare",
      "const ready = [...dependsOn.get(b.sourceName)!].every((d) =>":
         "orders one serve shape's bindings by the Malloy names they declare",
      "remaining.delete(b.sourceName);":
         "orders one serve shape's bindings by the Malloy names they declare",
      "if (remaining.has(b.sourceName)) {":
         "orders one serve shape's bindings by the Malloy names they declare",
   },
   "serve/serve_verdict.ts": {
      "const storageBound = facts.boundStorageNames.has(source.name);":
         "phase 3: storage bindings are emitted by name; migrate verdict facts to sourceID with the catalog",
      "!facts.servedStorageNames.has(source.name)":
         "phase 3: storage bindings are emitted by name; migrate verdict facts to sourceID with the catalog",
      "!facts.freshStorageNames.has(source.name)":
         "phase 3: storage bindings are emitted by name; migrate verdict facts to sourceID with the catalog",
   },
   "serve/storage_bindings.ts": {
      "} else if (!this.shadowedSourcesLogged.has(binding.sourceName)) {":
         "log dedupe only",
      "const def = contents?.[binding.sourceName] as":
         "the model's own namespace",
      "const def = contents?.[binding.sourceName];":
         "the model's own namespace",
   },
};

function sources(dir: string): string[] {
   const out: string[] = [];
   for (const entry of fs.readdirSync(dir, { withFileTypes: true })) {
      const full = path.join(dir, entry.name);
      if (entry.isDirectory()) out.push(...sources(full));
      else if (entry.name.endsWith(".ts") && !entry.name.endsWith(".spec.ts")) {
         out.push(full);
      }
   }
   return out;
}

describe("the keying rule: a source's name is never a map key", () => {
   const found = new Map<string, Set<string>>();
   for (const file of sources(ROOT)) {
      const rel = path.relative(ROOT, file).split(path.sep).join("/");
      for (const line of fs.readFileSync(file, "utf8").split("\n")) {
         if (!NAME_KEY.test(line)) continue;
         const trimmed = line.trim();
         if (trimmed.startsWith("*") || trimmed.startsWith("//")) continue;
         const set = found.get(rel) ?? new Set<string>();
         set.add(trimmed);
         found.set(rel, set);
      }
   }

   it("recognizes a name used as a key, and not a name in an array literal", () => {
      // The storage bind filter's lookup before the source map.
      expect(
         NAME_KEY.test("if (eligible.has(binding.sourceName)) return true;"),
      ).toBe(true);
      expect(NAME_KEY.test("refused[source.name] = errMessage(err);")).toBe(
         true,
      );
      expect(NAME_KEY.test("namesByAddress.set(id, [source.name]);")).toBe(
         false,
      );
      // `.sourceName` on any carrier, not only one named for a source.
      expect(NAME_KEY.test("if (remaining.has(b.sourceName)) continue;")).toBe(
         true,
      );
      expect(NAME_KEY.test("const x = byName[entry.sourceName];")).toBe(true);
      expect(NAME_KEY.test("...(e.sourceName ? [e.sourceName] : []),")).toBe(
         false,
      );
   });

   it("adds no new name-keyed lookup", () => {
      const added: string[] = [];
      for (const [file, lines] of found) {
         for (const line of lines) {
            if (!ALLOWED[file]?.[line]) added.push(`${file}: ${line}`);
         }
      }
      expect(added).toEqual([]);
   });

   it("lists no use that no longer exists", () => {
      const stale: string[] = [];
      for (const [file, lines] of Object.entries(ALLOWED)) {
         for (const line of Object.keys(lines)) {
            if (!found.get(file)?.has(line)) stale.push(`${file}: ${line}`);
         }
      }
      expect(stale).toEqual([]);
   });
});
