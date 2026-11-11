// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { Manifest } from "@malloydata/malloy";
import { describe, expect, it } from "bun:test";
import {
   manifestExcludingStorage,
   manifestForCompiler,
   redactConnectionSecrets,
   stagingSuffix,
   strictMissSourceId,
   tallySources,
   upstreamReuseFromManifest,
} from "./materialization_run_support";

/**
 * The pure helpers behind a materialization run (materialization_run_support.ts),
 * tested directly: secret redaction, staging names, manifest filtering and
 * upstream-reuse reporting, tallies, and the strict-miss reader.
 */

describe("redactConnectionSecrets", () => {
   it("strips credential values but keeps the message legible", () => {
      const source = {
         name: "orders_pg",
         type: "postgres",
         postgresConnection: {
            host: "db.internal",
            userName: "svc",
            password: "sup3r-secret-pw",
         },
      };
      const msg =
         "IO Error: could not connect password=sup3r-secret-pw to db.internal";
      const out = redactConnectionSecrets(msg, source);
      expect(out).not.toContain("sup3r-secret-pw");
      expect(out).toContain("could not connect");
      expect(out).toContain("db.internal"); // host isn't a secret — stays
   });

   it("passes a credential-free error (e.g. schema not found) through verbatim", () => {
      const source = {
         name: "lake",
         type: "ducklake",
         ducklakeConnection: {
            catalog: { postgresConnection: { password: "catpw123" } },
         },
      };
      const msg =
         "Catalog Error: Schema 'analytics' not found in DuckLake 'lake'";
      expect(redactConnectionSecrets(msg, source)).toBe(msg);
   });

   it("redacts a secret in its SQL-escaped form (single quotes doubled)", () => {
      // A DuckDB error echoes the offending statement, in which a secret with a
      // single quote appears escaped (`'` -> `''`), so the raw value won't match.
      const source = {
         name: "pg",
         type: "postgres",
         postgresConnection: { password: "pa'ss'word" },
      };
      const msg =
         "Parser Error: syntax error near 'password=pa''ss''word host=h'";
      const out = redactConnectionSecrets(msg, source);
      expect(out).not.toContain("pa''ss''word");
      expect(out).toContain("***");
   });

   it("still redacts when the connection graph contains a cycle", () => {
      // The callers pass a LIVE connection, whose driver/pool internals can hold
      // a back-reference. An unguarded walk recurses until the stack is
      // exhausted, and the resulting RangeError replaces the build error being
      // redacted -- so every failure on such a connection reports only
      // "Maximum call stack size exceeded".
      const source: Record<string, unknown> = {
         name: "sf",
         type: "snowflake",
         snowflakeConnection: { password: "sup3rsecret" },
      };
      source.pool = { owner: source };

      const out = redactConnectionSecrets(
         "Object 'x' already exists (password=sup3rsecret)",
         source,
      );
      expect(out).not.toContain("sup3rsecret");
      expect(out).toContain("***");
      expect(out).toContain("already exists");
   });
});

describe("stagingSuffix", () => {
   it("derives a short, stable suffix from the sourceEntityId", () => {
      expect(stagingSuffix("abcdef1234567890")).toBe("_abcdef123456");
   });
});

describe("manifestExcludingStorage (chained-storage inline)", () => {
   it("drops storage entries, keeps colocated entries with the manifest's quoting, preserves strict", () => {
      // The source manifest is already dialect-quoted by the seed loop (#904);
      // manifestExcludingStorage reuses that quoting for the kept entries.
      const manifest = new Manifest();
      manifest.strict = true;
      manifest.update("wh_up", { tableName: '"wh_up_v1"' });
      manifest.update("lake_up", { tableName: '"lake"."lake_up__mabc"' });
      const built: Record<string, unknown> = {
         wh_up: {
            sourceEntityId: "wh_up",
            physicalTableName: "wh_up_v1",
            connectionName: "duckdb",
            // no storageDestinationName -> colocated, keep it
         },
         lake_up: {
            sourceEntityId: "lake_up",
            physicalTableName: "lake_up__mabc",
            connectionName: "wh",
            storageDestinationName: "lake", // storage -> exclude it
         },
      };
      // eslint-disable-next-line @typescript-eslint/no-explicit-any
      const reduced = manifestExcludingStorage(manifest, built as any) as any;
      expect(Object.keys(reduced.entries)).toEqual(["wh_up"]);
      // Kept with the manifest's dialect-quoted name, not the raw physical name.
      expect(reduced.entries.wh_up.tableName).toBe('"wh_up_v1"');
      expect(reduced.entries.lake_up).toBeUndefined();
      expect(reduced.strict).toBe(true);
   });
});

describe("tallySources", () => {
   it("counts a failed source as failed, never as built", () => {
      // The counts are what an operator sees; the reason only exists inside the
      // manifest. A run that lost a source and still reported every source built
      // would leave the failure invisible outside the manifest JSON.
      const tally = tallySources(
         {
            ok: {
               sourceEntityId: "ok",
               sourceName: "healthy",
               physicalTableName: "ok_v1",
            },
         },
         {
            bad: {
               sourceEntityId: "bad",
               sourceName: "broken",
               physicalTableName: "bad_v1",
               reason: "Permission denied while writing to dataset analytics",
            },
         },
         {},
      );

      expect(tally.sourcesFailed).toBe(1);
      expect(tally.sourcesBuilt, "a failed source is not a built one").toBe(1);
   });

   it("does not count a carried source as built", () => {
      // A reused table was not built by this run. Counting it as built would
      // report work the run never did.
      const carried = {
         reused: {
            sourceEntityId: "reused",
            sourceName: "prior",
            physicalTableName: "prior_v1",
         },
      };
      const tally = tallySources(
         {
            ...carried,
            fresh: {
               sourceEntityId: "fresh",
               sourceName: "new",
               physicalTableName: "new_v1",
            },
         },
         {},
         carried,
      );

      expect(tally.sourcesBuilt).toBe(1);
      expect(tally.sourcesReused).toBe(1);
      expect(tally.sourcesFailed).toBe(0);
   });
});

describe("upstreamReuseFromManifest", () => {
   const addressBySourceId = {
      "daily@m": "addr-daily",
      "sites@m": "addr-sites",
   };
   const daily = { name: "daily", sourceID: "daily@m" };
   const sites = { name: "sites", sourceID: "sites@m" };
   const builtEntries = {
      "addr-daily": {
         sourceEntityId: "addr-daily",
         sourceName: "daily",
         physicalTableName: "daily__g1",
         storageDestinationName: "lake",
      },
   };

   it("reused when every upstream's address is in the manifest the SQL was rendered with", () => {
      const out = upstreamReuseFromManifest({
         reached: [daily, sites],
         addressBySourceId,
         sqlInlinesStored: false,
         compilerMissing: [],
         substituted: { "addr-daily": {}, "addr-sites": {} },
         full: { "addr-daily": {}, "addr-sites": {} },
         builtEntries: {},
         tier: "colocated",
      });
      expect(out).toEqual({
         fields: { upstreamReuse: "reused" },
         inManifestOnly: [],
         missing: [],
      });
   });

   it("an entry handed over by reference counts, whatever it is called: the lookup is by address, never by name", () => {
      // A thin reference carries no sourceName. By address it is present.
      const out = upstreamReuseFromManifest({
         reached: [daily],
         addressBySourceId,
         sqlInlinesStored: false,
         compilerMissing: [],
         substituted: { "addr-daily": { tableName: "daily__g1" } },
         full: { "addr-daily": { tableName: "daily__g1" } },
         builtEntries: {
            "addr-daily": {
               sourceEntityId: "addr-daily",
               physicalTableName: "daily__g1",
            },
         },
         tier: "colocated",
      });
      expect(out.fields).toEqual({ upstreamReuse: "reused" });
   });

   it("a colocated build recomputes a storage-tier upstream and says which destination holds it", () => {
      const out = upstreamReuseFromManifest({
         reached: [daily],
         addressBySourceId,
         sqlInlinesStored: true,
         compilerMissing: [],
         substituted: {},
         full: { "addr-daily": {} },
         builtEntries,
         tier: "colocated",
      });
      expect(out.inManifestOnly).toEqual(["daily"]);
      expect(out.fields.upstreamReuse).toBe("recomputed");
      expect(out.fields.upstreamRecomputeReason).toMatch(
         /'daily' is materialized in storage destination 'lake'/,
      );
   });

   it("a storage build treats a storage-tier upstream as a parent to stack on, not a reason", () => {
      const out = upstreamReuseFromManifest({
         reached: [daily],
         addressBySourceId,
         sqlInlinesStored: true,
         compilerMissing: [],
         substituted: {},
         full: { "addr-daily": {} },
         builtEntries,
         tier: "storage",
      });
      expect(out.inManifestOnly).toEqual(["daily"]);
      expect(out.missing).toEqual([]);
      expect(out.fields).toEqual({ upstreamReuse: "reused" });
   });

   it("an upstream the compiler misses is missing, and the reason says what would have supplied it", () => {
      const out = upstreamReuseFromManifest({
         reached: [daily],
         addressBySourceId,
         sqlInlinesStored: false,
         compilerMissing: [daily],
         substituted: {},
         full: {},
         builtEntries: {},
         tier: "storage",
      });
      expect(out.missing).toEqual(["daily"]);
      expect(out.fields.upstreamReuse).toBe("recomputed");
      expect(out.fields.upstreamRecomputeReason).toMatch(
         /neither built in this run nor supplied by reference/,
      );
   });

   it("a source with no stored upstream reports nothing", () => {
      expect(
         upstreamReuseFromManifest({
            reached: [],
            addressBySourceId,
            sqlInlinesStored: false,
            compilerMissing: [],
            substituted: {},
            full: {},
            builtEntries: {},
            tier: "colocated",
         }).fields,
      ).toEqual({});
   });

   it("the compiler's reach is the floor: a stored table inlined past the walk is recomputed, with a reason", () => {
      // Every upstream the walk named was read from its table, but the build
      // SQL differs from the SQL rendered with every stored entry available:
      // a join declared on a stored stop reached a storage-tier table.
      const out = upstreamReuseFromManifest({
         reached: [daily],
         addressBySourceId,
         substituted: { "addr-daily": {} },
         full: { "addr-daily": {} },
         builtEntries: {},
         tier: "storage",
         sqlInlinesStored: true,
         compilerMissing: [],
      });
      expect(out.inManifestOnly).toEqual([]);
      expect(out.missing).toEqual([]);
      expect(out.fields.upstreamReuse).toBe("recomputed");
      expect(out.fields.upstreamRecomputeReason).toMatch(
         /^the build SQL inlines a stored table reached through a refinement declared on a stored upstream/,
      );
   });

   it("the floor reports even when the walk named nothing", () => {
      const out = upstreamReuseFromManifest({
         reached: [],
         addressBySourceId,
         substituted: {},
         full: {},
         builtEntries: {},
         tier: "storage",
         sqlInlinesStored: true,
         compilerMissing: [],
      });
      expect(out.fields.upstreamReuse).toBe("recomputed");
   });

   it("an upstream with no content address is reached but unread: not missing, not a reason, not reported", () => {
      // Its SQL could not be rendered, so a build that read it would have
      // failed on its own render; the compiler's strict render is the oracle
      // for what the SQL reads, and it says nothing about this one.
      const out = upstreamReuseFromManifest({
         reached: [{ name: "given", sourceID: "given@m" }],
         addressBySourceId: { ...addressBySourceId, "given@m": undefined },
         substituted: {},
         full: {},
         builtEntries: {},
         tier: "storage",
         sqlInlinesStored: false,
         compilerMissing: [],
      });
      expect(out).toEqual({ fields: {}, inManifestOnly: [], missing: [] });
   });

   it("two models' same-named sources resolve by id, not by name", () => {
      const out = upstreamReuseFromManifest({
         reached: [
            { name: "daily", sourceID: "daily@a" },
            { name: "daily", sourceID: "daily@b" },
         ],
         addressBySourceId: { "daily@a": "addr-a", "daily@b": "addr-b" },
         substituted: { "addr-a": {} },
         full: { "addr-a": {} },
         builtEntries: {},
         tier: "storage",
         sqlInlinesStored: false,
         compilerMissing: [{ name: "daily", sourceID: "daily@b" }],
      });
      expect(out.missing).toEqual(["daily"]);
      expect(out.fields.upstreamReuse).toBe("recomputed");
   });

   it("a walk stop in no manifest that the compiler did not miss is a join the SQL never reads: not missing, not a reason", () => {
      const out = upstreamReuseFromManifest({
         reached: [daily, sites],
         addressBySourceId,
         substituted: { "addr-daily": {} },
         full: { "addr-daily": {} },
         builtEntries: {},
         tier: "storage",
         sqlInlinesStored: false,
         compilerMissing: [],
      });
      expect(out.missing).toEqual([]);
      expect(out.fields).toEqual({ upstreamReuse: "reused" });
   });

   it("a storage-tier stop the SQL never read is neither a parent nor a reason", () => {
      // In the full manifest, excluded from the warehouse manifest — and the
      // compare says nothing stored was inlined: a declared join the query
      // does not use. The build is the passthrough it always was.
      const out = upstreamReuseFromManifest({
         reached: [daily],
         addressBySourceId,
         substituted: {},
         full: { "addr-daily": {} },
         builtEntries,
         tier: "storage",
         sqlInlinesStored: false,
         compilerMissing: [],
      });
      expect(out.inManifestOnly).toEqual([]);
      expect(out.fields).toEqual({ upstreamReuse: "reused" });
   });

   it("a dependency only the compiler reaches is missing even though the walk never named it", () => {
      const out = upstreamReuseFromManifest({
         reached: [daily],
         addressBySourceId,
         substituted: { "addr-daily": {} },
         full: { "addr-daily": {} },
         builtEntries: {},
         tier: "storage",
         sqlInlinesStored: false,
         compilerMissing: [{ name: "counts", sourceID: "counts@m" }],
      });
      expect(out.missing).toEqual(["counts"]);
      expect(out.fields.upstreamReuse).toBe("recomputed");
      expect(out.fields.upstreamRecomputeReason).toMatch(
         /'counts' is not in this build's manifest/,
      );
   });

   it("a stop sharing the root's address is the table being built, not an upstream", () => {
      const out = upstreamReuseFromManifest({
         reached: [daily],
         addressBySourceId,
         substituted: {},
         full: {},
         builtEntries: {},
         tier: "storage",
         sqlInlinesStored: false,
         compilerMissing: [],
         rootAddress: "addr-daily",
      });
      expect(out).toEqual({ fields: {}, inManifestOnly: [], missing: [] });
   });
});

describe("manifestForCompiler", () => {
   const facts = {
      addressBySourceId: {
         "p@m": "addr-p-partitioned",
         "u@m": "addr-u",
         "x@m": undefined,
      },
      compilerKeyBySourceId: {
         "p@m": "key-p",
         "u@m": "addr-u",
         "x@m": "key-x",
      },
   };
   it("adds a partitioned entry under the compiler's key and leaves everything else alone", () => {
      const full = {
         entries: {
            "addr-p-partitioned": { tableName: "p__g1" },
            "addr-u": { tableName: "u__g1" },
         },
         strict: true,
      } as never;
      const out = manifestForCompiler(full, facts) as unknown as {
         entries: Record<string, unknown>;
         strict: boolean;
      };
      expect(out.entries["key-p"]).toEqual({ tableName: "p__g1" });
      expect(out.entries["addr-p-partitioned"]).toEqual({ tableName: "p__g1" });
      expect(Object.keys(out.entries).sort()).toEqual([
         "addr-p-partitioned",
         "addr-u",
         "key-p",
      ]);
      expect(out.strict).toBe(true);
   });
   it("returns the same manifest when nothing needs aliasing", () => {
      const full = { entries: { "addr-u": {} } } as never;
      expect(manifestForCompiler(full, facts)).toBe(full);
   });
});

describe("strictMissSourceId", () => {
   it("names the source a compiler strict miss reports, and nothing else", () => {
      const miss = Object.assign(
         new Error(
            "Persisted source 'counts@file:///m.malloy' not found in manifest (buildId: abc); strict manifest mode forbids fallback to live compilation.",
         ),
         { code: "runtime-manifest-strict-miss" },
      );
      expect(strictMissSourceId(miss)).toEqual({
         sourceID: "counts@file:///m.malloy",
      });
      // The code is the contract: a reworded message is still a miss, of an
      // unnamed source.
      expect(
         strictMissSourceId(
            Object.assign(new Error("manifest has no entry"), {
               code: "runtime-manifest-strict-miss",
            }),
         ),
      ).toEqual({ sourceID: undefined });
      expect(
         strictMissSourceId(
            new Error("Persisted source 'x' not found in manifest"),
         ),
      ).toBeUndefined();
      expect(
         strictMissSourceId(
            Object.assign(new Error("boom"), { code: "other" }),
         ),
      ).toBeUndefined();
      expect(strictMissSourceId(undefined)).toBeUndefined();
   });
});
