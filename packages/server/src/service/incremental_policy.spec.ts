// The incremental-refresh publish gate. Two layers, deliberately:
//
//  - Rule tests over constructed declarations, which can name a SUPPORTED
//    dialect (postgres) and so exercise every rule in isolation.
//  - Real-package tests (Environment + Package.create over temp dirs, the
//    pattern of persistence_policy.spec.ts), which prove the compile → resolve →
//    gate wiring, the load-tolerant/publish-strict split, and that the advisories
//    reach the package's warnings array.
//
// The real-package layer runs on DuckDB, which the dialect allowlist EXCLUDES —
// so every incremental fixture there also trips the dialect rule. That is itself
// worth asserting, and the other assertions use `toContain` accordingly.
import { afterEach, beforeEach, describe, expect, it } from "bun:test";
import * as fs from "fs/promises";
import * as os from "os";
import * as path from "path";

import { Environment } from "./environment";
import type { IncrementalDeclaration } from "./incremental_declaration";
import {
   incrementalPolicyAdvisories,
   incrementalPolicyRejections,
   type IncrementalPolicySource,
} from "./incremental_policy";
import type { Package } from "./package";

/** A declaration with nothing declared; each test overrides what it needs. */
function declaration(
   overrides: Partial<IncrementalDeclaration> = {},
): IncrementalDeclaration {
   return {
      incremental: false,
      declaredWatermark: false,
      declaredMergeKey: false,
      watermarkOrderable: false,
      mergeKeys: [],
      watermarkInMergeKeys: false,
      malformed: [],
      unknownKeys: [],
      calculateFields: [],
      outputColumns: ["order_date", "region", "order_id", "revenue"],
      ...overrides,
   };
}

/** A gate input on a SUPPORTED dialect, so the dialect rule stays quiet. */
function source(
   overrides: Partial<IncrementalPolicySource> = {},
): IncrementalPolicySource {
   return {
      sourceName: "daily_revenue",
      modelPath: "revenue.malloy",
      dialect: "postgres",
      declaration: declaration(),
      ...overrides,
   };
}

/** A resolved, orderable date watermark on `order_date`. */
const DATE_WATERMARK = {
   name: "order_date",
   kind: "dimension" as const,
   malloyType: "date",
};

function rejections(overrides: Partial<IncrementalPolicySource>): string[] {
   return incrementalPolicyRejections([source(overrides)]);
}

describe("incrementalPolicyRejections", () => {
   it("is silent for a source that declares nothing", () => {
      expect(rejections({})).toEqual([]);
   });

   it("is silent for a coherent keyless incremental declaration", () => {
      expect(
         rejections({
            declaration: declaration({
               refresh: "incremental",
               incremental: true,
               declaredWatermark: true,
               watermark: DATE_WATERMARK,
               watermarkOrderable: true,
            }),
         }),
      ).toEqual([]);
   });

   it("is silent for a coherent merge declaration", () => {
      expect(
         rejections({
            declaration: declaration({
               refresh: "incremental",
               incremental: true,
               declaredWatermark: true,
               watermark: DATE_WATERMARK,
               watermarkOrderable: true,
               declaredMergeKey: true,
               mergeKeys: [
                  { name: "order_id", kind: "dimension", malloyType: "string" },
               ],
            }),
         }),
      ).toEqual([]);
   });

   // ── The coherence chain, one broken link at a time ────────────────────

   it("rule 1: watermark= alone, with the three fixes named", () => {
      const [message, ...rest] = rejections({
         declaration: declaration({
            declaredWatermark: true,
            watermark: DATE_WATERMARK,
         }),
      });
      expect(rest).toEqual([]);
      expect(message).toContain('"daily_revenue"');
      expect(message).toContain("watermark=");
      expect(message).toContain('refresh="incremental"');
      // add the mode / remove the key / strip an inherited key
      expect(message).toContain("-watermark");
   });

   it("rule 1: names the mode the source actually declares", () => {
      const [message] = rejections({
         declaration: declaration({
            refresh: "full",
            declaredWatermark: true,
            watermark: DATE_WATERMARK,
         }),
      });
      expect(message).toContain('refresh="full"');
   });

   it("rule 3: merge_key= alone states the WHOLE chain, and rule 1 fires with it", () => {
      const messages = rejections({
         declaration: declaration({
            declaredMergeKey: true,
            mergeKeys: [
               { name: "order_id", kind: "dimension", malloyType: "string" },
            ],
         }),
      });
      // Not incremental AND no watermark: both links are broken, so both fire.
      expect(messages).toHaveLength(2);
      const chain = messages.find((m) => m.includes("chain"))!;
      expect(chain).toContain("merge_key= needs watermark=");
      expect(chain).toContain('watermark= needs refresh="incremental"');
      // …and never the generic rule-2 wording, which would misdescribe it.
      expect(chain).not.toContain("no watermark=");
   });

   it("rule 3: merge_key= with the mode but no watermark= still states the chain", () => {
      const messages = rejections({
         declaration: declaration({
            refresh: "incremental",
            incremental: true,
            declaredMergeKey: true,
            mergeKeys: [
               { name: "order_id", kind: "dimension", malloyType: "string" },
            ],
         }),
      });
      expect(messages.some((m) => m.includes("chain"))).toBe(true);
      // Rule 2 also fires here: the mode really is missing its watermark.
      expect(messages.some((m) => m.includes("no watermark="))).toBe(true);
      // Rule 1 does NOT: the source IS incremental.
      expect(
         messages.some((m) => m.includes("are ignored by a full rebuild")),
      ).toBe(false);
   });

   it("rule 2: the mode with no watermark at all", () => {
      const messages = rejections({
         declaration: declaration({
            refresh: "incremental",
            incremental: true,
         }),
      });
      expect(messages).toHaveLength(1);
      expect(messages[0]).toContain("no watermark=");
      expect(messages[0]).toContain('watermark="order_date"');
   });

   // ── Rule 4: the mode value ────────────────────────────────────────────

   it("rule 4: rejects a capitalized refresh= value as case-sensitive", () => {
      const messages = rejections({
         declaration: declaration({
            refresh: "Incremental",
            invalidRefresh: "Incremental",
         }),
      });
      expect(messages).toHaveLength(1);
      expect(messages[0]).toContain('refresh="Incremental"');
      expect(messages[0]).toContain("case-sensitive");
   });

   it("rule 4: rejects a boolean-ish refresh= value", () => {
      const [message] = rejections({
         declaration: declaration({ refresh: "true", invalidRefresh: "true" }),
      });
      expect(message).toContain('refresh="true"');
      expect(message).toContain('refresh="full"');
   });

   // ── Rule 5: malformed values ──────────────────────────────────────────

   it("rule 5: the tag-array form explains why the key looked undeclared", () => {
      const messages = rejections({
         declaration: declaration({
            refresh: "incremental",
            incremental: true,
            declaredWatermark: true,
            malformed: [{ key: "watermark", problem: "array" }],
         }),
      });
      const array = messages.find((m) => m.includes("tag ARRAY"))!;
      expect(array).toContain('watermark="order_date"');
      expect(array).toContain("scalar fields");
      // Rule 2 must NOT fire: the key IS declared, just unusably.
      expect(messages.some((m) => m.includes("no watermark="))).toBe(false);
   });

   it("rule 5: an empty value points at the negation for an inherited key", () => {
      const [message] = rejections({
         declaration: declaration({
            declaredWatermark: true,
            malformed: [{ key: "watermark", problem: "empty" }],
         }),
      });
      expect(message).toContain('watermark=""');
      expect(message).toContain("-watermark");
   });

   it("rule 5: an empty merge_key entry", () => {
      const messages = rejections({
         declaration: declaration({
            refresh: "incremental",
            incremental: true,
            declaredWatermark: true,
            watermark: DATE_WATERMARK,
            watermarkOrderable: true,
            declaredMergeKey: true,
            malformed: [{ key: "merge_key", problem: "empty-entry" }],
            mergeKeys: [
               { name: "order_id", kind: "dimension", malloyType: "string" },
               { name: "region", kind: "dimension", malloyType: "string" },
            ],
         }),
      });
      expect(messages).toHaveLength(1);
      expect(messages[0]).toContain('merge_key="order_id,,region"');
      expect(messages[0]).toContain("narrow row identity");
   });

   it("rule 5: a duplicated merge_key name", () => {
      const [message] = rejections({
         declaration: declaration({
            refresh: "incremental",
            incremental: true,
            declaredWatermark: true,
            watermark: DATE_WATERMARK,
            watermarkOrderable: true,
            declaredMergeKey: true,
            malformed: [
               { key: "merge_key", problem: "duplicate", detail: "order_id" },
            ],
            mergeKeys: [
               { name: "order_id", kind: "dimension", malloyType: "string" },
            ],
         }),
      });
      expect(message).toContain('"order_id" repeated');
   });

   // ── Rules 6 and 7: dialect ────────────────────────────────────────────

   const COHERENT = declaration({
      refresh: "incremental",
      incremental: true,
      declaredWatermark: true,
      watermark: DATE_WATERMARK,
      watermarkOrderable: true,
   });

   it("rule 6: rejects an unsupported dialect and lists the supported ones", () => {
      const messages = rejections({ dialect: "duckdb", declaration: COHERENT });
      expect(messages).toHaveLength(1);
      expect(messages[0]).toContain('dialect "duckdb"');
      expect(messages[0]).toContain("postgres, standardsql (BigQuery)");
      expect(messages[0]).toContain('refresh="full"');
   });

   it("rule 6: an unknown dialect is refused, not assumed capable", () => {
      const [message] = rejections({
         dialect: undefined,
         declaration: COHERENT,
      });
      expect(message).toContain("unknown");
   });

   it("rule 6 subsumes rule 7 today: an unsupported dialect reports once", () => {
      // Rule 7 stays a distinct rule so the two allowlists can widen
      // independently, but a dialect that fails rule 6 must not produce two
      // messages about the same thing.
      const messages = rejections({
         dialect: "duckdb",
         declaration: declaration({
            ...COHERENT,
            declaredMergeKey: true,
            mergeKeys: [
               { name: "order_id", kind: "dimension", malloyType: "string" },
            ],
         }),
      });
      expect(messages).toHaveLength(1);
      expect(messages[0]).toContain("transactional multi-statement DML");
   });

   it("both dialects on the allowlist accept a coherent declaration", () => {
      // Keyed by Malloy's own `dialectName`, so BigQuery is "standardsql". A
      // test that asserted "bigquery" here would pass while every real BigQuery
      // source failed the gate.
      for (const dialect of ["postgres", "standardsql"]) {
         expect(rejections({ dialect, declaration: COHERENT })).toEqual([]);
      }
   });

   it("rule 13: refuses incremental into a storage= destination", () => {
      const [message] = rejections({
         declaration: COHERENT,
         storageDestination: "lake",
      });
      expect(message).toContain('storage="lake"');
      expect(message).toContain("source warehouse");
   });

   // ── Rules 8 to 11: what the names resolved to ─────────────────────────

   it("rule 8: a dangling name lists the available columns", () => {
      const messages = rejections({
         declaration: declaration({
            refresh: "incremental",
            incremental: true,
            declaredWatermark: true,
            watermark: { name: "ordr_date", kind: "unresolved" },
         }),
      });
      expect(messages).toHaveLength(1);
      expect(messages[0]).toContain('"ordr_date"');
      expect(messages[0]).toContain('"order_date", "region"');
      expect(messages[0]).toContain("derived dimension");
   });

   it("rule 8: reports a dangling merge key alongside a good watermark", () => {
      const [message] = rejections({
         declaration: declaration({
            ...COHERENT,
            declaredMergeKey: true,
            mergeKeys: [{ name: "orderid", kind: "unresolved" }],
         }),
      });
      expect(message).toContain('"orderid"');
   });

   it("rule 11: an aggregate-produced column is not an identity or an order", () => {
      const messages = rejections({
         declaration: declaration({
            refresh: "incremental",
            incremental: true,
            declaredWatermark: true,
            watermark: {
               name: "revenue",
               kind: "aggregate",
               malloyType: "number",
            },
            watermarkOrderable: true,
         }),
      });
      expect(messages).toHaveLength(1);
      expect(messages[0]).toContain('"revenue"');
      expect(messages[0]).toContain("aggregate:");
      expect(messages[0]).toContain("group_by:");
   });

   it("rule 10: a non-orderable watermark type", () => {
      const messages = rejections({
         declaration: declaration({
            refresh: "incremental",
            incremental: true,
            declaredWatermark: true,
            watermark: {
               name: "flag",
               kind: "dimension",
               malloyType: "boolean",
            },
            watermarkOrderable: false,
         }),
      });
      expect(messages).toHaveLength(1);
      expect(messages[0]).toContain("type boolean");
      expect(messages[0]).toContain("no ordering");
   });

   it("rule 9: a merge_key= that repeats the watermark", () => {
      const messages = rejections({
         declaration: declaration({
            ...COHERENT,
            declaredMergeKey: true,
            mergeKeys: [
               { name: "order_id", kind: "dimension", malloyType: "string" },
               DATE_WATERMARK,
            ],
            watermarkInMergeKeys: true,
         }),
      });
      expect(messages).toHaveLength(1);
      expect(messages[0]).toContain('"order_date"');
      expect(messages[0]).toContain("inserted alongside the old");
   });

   // ── Rule 12: window fields ────────────────────────────────────────────

   it("rule 12: any calculate: field is refused, with the eventual criterion", () => {
      const messages = rejections({
         declaration: declaration({ ...COHERENT, calculateFields: ["prev"] }),
      });
      expect(messages).toHaveLength(1);
      expect(messages[0]).toContain('"prev"');
      expect(messages[0]).toContain("calculate:");
      // The criterion that will narrow this rule later, so the message does not
      // read as a permanent prohibition.
      expect(messages[0]).toContain("BACKWARD");
      // …and the explicit carve-out, so nobody reads it as "no non-additive
      // aggregates".
      expect(messages[0]).toContain("count_distinct");
   });

   it("rule 12: does not fire on a source with no window fields", () => {
      expect(rejections({ declaration: declaration({ ...COHERENT }) })).toEqual(
         [],
      );
   });

   // ── The trial compile ─────────────────────────────────────────────────

   it("reports a delta query that does not compile, quoting the compiler", () => {
      const messages = rejections({
         declaration: COHERENT,
         trialCompileError: "Cannot compare a date to a timestamp",
      });
      expect(messages).toHaveLength(1);
      expect(messages[0]).toContain("Cannot compare a date to a timestamp");
      expect(messages[0]).toContain("query stage over the source");
   });

   // ── Multiple sources ──────────────────────────────────────────────────

   it("reports every source's problems, in source order", () => {
      const messages = incrementalPolicyRejections([
         source({
            sourceName: "a",
            declaration: declaration({
               refresh: "incremental",
               incremental: true,
            }),
         }),
         source({ sourceName: "b", declaration: declaration() }),
         source({
            sourceName: "c",
            declaration: declaration({
               refresh: "nope",
               invalidRefresh: "nope",
            }),
         }),
      ]);
      expect(messages).toHaveLength(2);
      expect(messages[0]).toContain('"a"');
      expect(messages[1]).toContain('"c"');
   });
});

describe("incrementalPolicyAdvisories", () => {
   it("warns about an unrecognized key, naming the merge_key= failure mode", () => {
      const warnings = incrementalPolicyAdvisories([
         source({
            declaration: declaration({ unknownKeys: ["mergekey"] }),
         }),
      ]);
      expect(warnings).toHaveLength(1);
      expect(warnings[0].target).toBe("daily_revenue");
      expect(warnings[0].model).toBe("revenue.malloy");
      expect(warnings[0].message).toContain('"mergekey"');
      expect(warnings[0].message).toContain("merge_key=");
   });

   it("warns about a keyless delta with both consequences", () => {
      const warnings = incrementalPolicyAdvisories([
         source({
            declaration: declaration({
               refresh: "incremental",
               incremental: true,
               declaredWatermark: true,
               watermark: DATE_WATERMARK,
               watermarkOrderable: true,
            }),
         }),
      ]);
      expect(warnings).toHaveLength(1);
      // (a) a restated row with an advanced watermark appears twice…
      expect(warnings[0].message).toContain("appears twice");
      // (b) …and an unpartitioned target may re-read the whole table.
      expect(warnings[0].message).toContain("partitioned");
      expect(warnings[0].message).toContain('"order_date"');
   });

   it("does not warn when merge_key= is declared", () => {
      const warnings = incrementalPolicyAdvisories([
         source({
            declaration: declaration({
               refresh: "incremental",
               incremental: true,
               declaredWatermark: true,
               watermark: DATE_WATERMARK,
               watermarkOrderable: true,
               declaredMergeKey: true,
               mergeKeys: [
                  { name: "order_id", kind: "dimension", malloyType: "string" },
               ],
            }),
         }),
      ]);
      expect(warnings).toEqual([]);
   });

   it("does not warn for a non-incremental source", () => {
      expect(incrementalPolicyAdvisories([source({})])).toEqual([]);
   });
});

// ─────────────────────────────────────────────────────────────────────────
// The real thing: compile a package and read the gate off it.
// ─────────────────────────────────────────────────────────────────────────

describe("incremental policy gate over a real package", () => {
   let rootDir: string;
   let envPath: string;

   async function loadPackage(model: string): Promise<Package> {
      const dir = path.join(envPath, "pkg");
      await fs.mkdir(dir, { recursive: true });
      await fs.writeFile(
         path.join(dir, "publisher.json"),
         JSON.stringify({ name: "pkg", description: "fixture" }),
      );
      await fs.writeFile(path.join(dir, "model.malloy"), model);
      const env = await Environment.create("testEnv", envPath, []);
      await env.addPackage("pkg");
      return env.getPackage("pkg", false);
   }

   beforeEach(async () => {
      rootDir = await fs.mkdtemp(path.join(os.tmpdir(), "publisher-incr-"));
      envPath = path.join(rootDir, "env");
      await fs.mkdir(envPath, { recursive: true });
   });

   afterEach(async () => {
      await fs.rm(rootDir, { recursive: true, force: true }).catch(() => {});
   });

   const RAW = `##! experimental.persistence
source: raw is duckdb.sql("""
  SELECT 1 AS amount, DATE '2024-01-01' AS order_date, 'US' AS region,
         'o-1' AS order_id
""")
`;
   const ROLLUP = `source: daily is raw -> {
  group_by: order_date, region, order_id
  aggregate: revenue is amount.sum()
}`;

   it(
      "is inert for a plain persist source, and the package still loads",
      async () => {
         const pkg = await loadPackage(`${RAW}#@ persist name="t"\n${ROLLUP}`);
         expect(pkg.incrementalPolicyWarnings()).toEqual([]);
         expect(pkg.formatInvalidIncrementalPolicy()).toBe("");
      },
      { timeout: 30000 },
   );

   it(
      "rejects a declaration that only names the mode",
      async () => {
         const pkg = await loadPackage(
            `${RAW}#@ persist name="t" refresh="incremental"\n${ROLLUP}`,
         );
         const joined = pkg.formatInvalidIncrementalPolicy();
         expect(joined).toContain('"daily"');
         expect(joined).toContain("no watermark=");
      },
      { timeout: 30000 },
   );

   it(
      "LOADS a package it would refuse to publish (warn-only at load)",
      async () => {
         // The load-tolerant half of the split, and the reason it matters:
         // `refresh=` used to be inert, so a package already published with
         // refresh="incremental" and nothing else must keep serving.
         const pkg = await loadPackage(
            `${RAW}#@ persist name="t" refresh="incremental"\n${ROLLUP}`,
         );
         expect(pkg.getPackageMetadata().name).toBe("pkg");
         expect((await pkg.listModels()).length).toBeGreaterThan(0);
         expect(pkg.formatInvalidIncrementalPolicy()).not.toBe("");
      },
      { timeout: 30000 },
   );

   it(
      "resolves a watermark against the compiled output schema and refuses DuckDB",
      async () => {
         const pkg = await loadPackage(
            `${RAW}#@ persist name="t" refresh="incremental" watermark="order_date"\n${ROLLUP}`,
         );
         // The declaration itself is coherent; only the dialect stands in the way.
         const warnings = pkg.incrementalPolicyWarnings();
         expect(warnings).toHaveLength(1);
         expect(warnings[0]).toContain('dialect "duckdb"');
      },
      { timeout: 30000 },
   );

   it(
      "rejects a watermark that names no materialized column",
      async () => {
         const pkg = await loadPackage(
            `${RAW}#@ persist name="t" refresh="incremental" watermark="ordr_date"\n${ROLLUP}`,
         );
         const joined = pkg.formatInvalidIncrementalPolicy();
         expect(joined).toContain('"ordr_date"');
         expect(joined).toContain('"order_date"');
      },
      { timeout: 30000 },
   );

   it(
      "rejects a calculate: field while accepting count_distinct beside it",
      async () => {
         const gated = await loadPackage(
            `${RAW}#@ persist name="t" refresh="incremental" watermark="order_date"
source: daily is raw -> {
  group_by: order_date
  aggregate: revenue is amount.sum()
  calculate: prev is lag(revenue)
}`,
         );
         expect(gated.formatInvalidIncrementalPolicy()).toContain("calculate:");

         const allowed = await loadPackage(
            `${RAW}#@ persist name="t" refresh="incremental" watermark="order_date"
source: daily is raw -> {
  group_by: order_date
  aggregate: buyers is count(order_id)
}`,
         );
         // Only the dialect refusal — the non-additive aggregate is fine.
         expect(allowed.incrementalPolicyWarnings()).toHaveLength(1);
         expect(allowed.formatInvalidIncrementalPolicy()).not.toContain(
            "calculate:",
         );
      },
      { timeout: 60000 },
   );

   it(
      "surfaces the keyless and unknown-key advisories on the package warnings",
      async () => {
         const pkg = await loadPackage(
            `${RAW}#@ persist name="t" refresh="incremental" watermark="order_date" mergekey="order_id"\n${ROLLUP}`,
         );
         const messages = (pkg.getPackageMetadata().warnings ?? []).map(
            (w) => w.message ?? "",
         );
         expect(messages.some((m) => m.includes('"mergekey"'))).toBe(true);
         expect(messages.some((m) => m.includes("no merge_key="))).toBe(true);
      },
      { timeout: 30000 },
   );

   it(
      "does not advise a source that declares merge_key=",
      async () => {
         const pkg = await loadPackage(
            `${RAW}#@ persist name="t" refresh="incremental" watermark="order_date" merge_key="order_id"\n${ROLLUP}`,
         );
         const messages = (pkg.getPackageMetadata().warnings ?? []).map(
            (w) => w.message ?? "",
         );
         expect(messages.some((m) => m.includes("no merge_key="))).toBe(false);
      },
      { timeout: 30000 },
   );

   it(
      "gates an extending child on the declaration it INHERITED",
      async () => {
         // The child declares nothing at all, so every rule has to read the
         // effective merged tag to see it at all.
         const pkg = await loadPackage(
            `${RAW}#@ persist name="t" refresh="incremental"\n${ROLLUP}

source: child is daily extend { }`,
         );
         const joined = pkg.formatInvalidIncrementalPolicy();
         expect(joined).toContain('"daily"');
         expect(joined).toContain('"child"');
      },
      { timeout: 30000 },
   );

   it(
      "lets a child opt out of an inherited incremental declaration",
      async () => {
         const pkg = await loadPackage(
            `${RAW}#@ persist name="t" refresh="incremental" watermark="order_date"\n${ROLLUP}

#@ persist name="child_t" refresh="full" -watermark
source: child is daily extend { }`,
         );
         const warnings = pkg.incrementalPolicyWarnings();
         // Only the parent's dialect refusal remains; the child is out entirely.
         expect(warnings).toHaveLength(1);
         expect(warnings[0]).toContain('"daily"');
      },
      { timeout: 30000 },
   );
});
