// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

// The incremental-refresh gate. Two layers, deliberately:
//
//  - Rule tests over constructed declarations, which can name a SUPPORTED
//    dialect (postgres) and so exercise every rule in isolation.
//  - Real-package tests (Environment + Package.create over temp dirs, the
//    pattern of persistence_policy.spec.ts), which prove the compile → resolve →
//    gate wiring, that a rejection FAILS THE LOAD as a 400, and that the
//    advisories reach the package's warnings array without failing anything.
//
// The real-package layer runs on DuckDB, which the dialect allowlist EXCLUDES —
// so every incremental fixture there also trips the dialect rule, and therefore
// cannot load at all. That is itself worth asserting, and it is why the findings
// that need a LOADED incremental source (the keyless-delta and shared-address
// advisories) are only reachable from the unit layer.
import { afterEach, beforeEach, describe, expect, it } from "bun:test";
import * as fs from "fs/promises";
import * as os from "os";
import * as path from "path";

import { BadRequestError, internalErrorToHttpError } from "../errors";
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
      expect(messages[0]).toContain(
         "postgres, snowflake, standardsql (BigQuery)",
      );
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
      expect(messages[0]).toContain("transactional DML");
   });

   it("every dialect on the allowlist accepts a coherent declaration", () => {
      // Keyed by Malloy's own `dialectName`, so BigQuery is "standardsql". A
      // test that asserted "bigquery" here would pass while every real BigQuery
      // source failed the gate.
      for (const dialect of ["postgres", "snowflake", "standardsql"]) {
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
      expect(warnings[0].subject).toBe("daily_revenue");
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

   it("keeps every advisory OUT of the rejections, which is what fails a load", () => {
      // The regression risk in making rejections fatal: an advisory that leaked
      // into the rejection list would stop a legal package from serving. Both
      // advisories are on declarations the gate must find coherent.
      const keylessDelta = {
         declaration: declaration({
            refresh: "incremental",
            incremental: true,
            declaredWatermark: true,
            watermark: DATE_WATERMARK,
            watermarkOrderable: true,
         }),
      };
      const unknownKey = {
         declaration: declaration({ unknownKeys: ["mergekey"] }),
      };
      for (const overrides of [keylessDelta, unknownKey]) {
         expect(
            incrementalPolicyAdvisories([source(overrides)]).length,
         ).toBeGreaterThan(0);
         expect(rejections(overrides)).toEqual([]);
      }
   });

   describe("two sources sharing one content address", () => {
      const INCREMENTAL = {
         refresh: "incremental",
         incremental: true,
         declaredWatermark: true,
         watermark: DATE_WATERMARK,
         watermarkOrderable: true,
         declaredMergeKey: true,
         strategy: "merge" as const,
         mergeKeys: [
            {
               name: "order_id",
               kind: "dimension" as const,
               malloyType: "string",
            },
         ],
      };

      /** Two sources at the same address; `b` overrides the second's fields. */
      function pair(
         b: Partial<IncrementalPolicySource> = {},
      ): IncrementalPolicySource[] {
         return [
            source({
               sourceName: "daily_a",
               sourceEntityId: "addr-1",
               declaration: declaration(INCREMENTAL),
            }),
            source({
               sourceName: "daily_b",
               sourceEntityId: "addr-1",
               declaration: declaration(INCREMENTAL),
               ...b,
            }),
         ];
      }

      /** Only the shared-address advisory, which is all these tests assert on. */
      function collisionMessages(sources: IncrementalPolicySource[]) {
         return incrementalPolicyAdvisories(sources)
            .filter((w) => (w.message ?? "").includes("content address"))
            .map((w) => ({ subject: w.subject, message: w.message ?? "" }));
      }

      it("warns BOTH sources when their declarations disagree", () => {
         // The flip-flop: one ledger row cannot describe both lineages, so each
         // build finds the other's recorded and rebuilds, forever. Reported on
         // both because there is no basis for calling either the original.
         const warnings = collisionMessages(
            pair({
               declaration: declaration({
                  ...INCREMENTAL,
                  declaredMergeKey: false,
                  strategy: "range_replace",
                  mergeKeys: [],
               }),
            }),
         );
         expect(warnings).toHaveLength(2);
         expect(warnings[0].subject).toBe("daily_a");
         expect(warnings[0].message).toContain('"daily_b"');
         expect(warnings[1].subject).toBe("daily_b");
         expect(warnings[1].message).toContain('"daily_a"');
         for (const w of warnings) {
            expect(w.message).toContain("DISAGREE");
            expect(w.message).toContain("rebuilds in full");
         }
      });

      it("warns more mildly when the declarations agree", () => {
         // Still worth saying: one table is built and the other name= never
         // exists, which the model text does not show.
         const warnings = collisionMessages(pair());
         expect(warnings).toHaveLength(2);
         for (const w of warnings) {
            expect(w.message).toContain("Only one is materialized");
            expect(w.message).not.toContain("DISAGREE");
         }
      });

      it("ignores a difference the ledger does not record", () => {
         // outputColumns is not part of the recorded lineage, so a difference
         // there conflicts with nothing and must not read as a disagreement.
         const warnings = collisionMessages(
            pair({
               declaration: declaration({
                  ...INCREMENTAL,
                  outputColumns: ["order_date", "revenue"],
               }),
            }),
         );
         expect(warnings).toHaveLength(2);
         for (const w of warnings) {
            expect(w.message).not.toContain("DISAGREE");
         }
      });

      it("stays quiet for distinct addresses, and for a missing one", () => {
         expect(
            collisionMessages(pair({ sourceEntityId: "addr-2" })),
         ).toHaveLength(0);
         // An unreported address cannot be compared; the check goes silent rather
         // than treating "absent" as a shared value.
         expect(
            collisionMessages([
               source({
                  sourceName: "a",
                  declaration: declaration(INCREMENTAL),
               }),
               source({
                  sourceName: "b",
                  declaration: declaration(INCREMENTAL),
               }),
            ]),
         ).toHaveLength(0);
      });

      it("stays quiet when NEITHER source is incremental", () => {
         // The collapse is pre-existing, pinned behavior for ordinary sources.
         // This gate speaks only where it makes an incremental source silently
         // stop advancing.
         expect(
            collisionMessages([
               source({ sourceName: "a", sourceEntityId: "addr-1" }),
               source({ sourceName: "b", sourceEntityId: "addr-1" }),
            ]),
         ).toHaveLength(0);
      });
   });
});

// ─────────────────────────────────────────────────────────────────────────
// The real thing: compile a package and read the gate off it.
// ─────────────────────────────────────────────────────────────────────────

describe("incremental policy gate over a real package", () => {
   let rootDir: string;
   let envPath: string;

   /** Write the package tree, and the environment that will load it. */
   async function stage(model: string): Promise<Environment> {
      const dir = path.join(envPath, "pkg");
      await fs.mkdir(dir, { recursive: true });
      await fs.writeFile(
         path.join(dir, "publisher.json"),
         JSON.stringify({ name: "pkg", description: "fixture" }),
      );
      await fs.writeFile(path.join(dir, "model.malloy"), model);
      return Environment.create("testEnv", envPath, []);
   }

   async function loadPackage(model: string): Promise<Package> {
      const env = await stage(model);
      await env.addPackage("pkg");
      return env.getPackage("pkg", false);
   }

   /**
    * The gate's messages, read off the load they now FAIL. A rejected package
    * never becomes a `Package`, so this is how a real-package test sees them —
    * one message per line, in source order.
    */
   async function loadRejection(model: string): Promise<string[]> {
      try {
         await loadPackage(model);
      } catch (error) {
         expect(error).toBeInstanceOf(BadRequestError);
         return (error as Error).message.split("\n");
      }
      throw new Error("expected the load to fail, but the package loaded");
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
      "FAILS the load of a declaration that only names the mode, as a 400",
      async () => {
         // The severity, end to end: the status the author's client resolves and
         // the text it shows. A 400 and not the 424 a control plane defaults to,
         // because this is an authoring error, not a dependency failure.
         const model = `${RAW}#@ persist name="t" refresh="incremental"\n${ROLLUP}`;
         let error: unknown;
         try {
            await loadPackage(model);
         } catch (e) {
            error = e;
         }
         expect(error).toBeInstanceOf(BadRequestError);
         const http = internalErrorToHttpError(error as Error);
         expect(http.status).toBe(400);
         expect(http.json.message).toContain('"daily"');
         expect(http.json.message).toContain("no watermark=");
      },
      { timeout: 30000 },
   );

   it(
      "leaves a rejected package unserved, not serving with a warning",
      async () => {
         // What this replaced: the same package used to load, serve, and rebuild
         // in full forever with the rejection sitting in a log nobody reads.
         // `refresh=` was inert metadata before these rules, so a bare
         // refresh="incremental" is the one rejection a package published earlier
         // can carry — accepted, since it is an authoring error either way.
         const env = await stage(
            `${RAW}#@ persist name="t" refresh="incremental"\n${ROLLUP}`,
         );
         await expect(env.addPackage("pkg")).rejects.toBeInstanceOf(
            BadRequestError,
         );
         // And a read does not quietly succeed on a second attempt: the lazy
         // reload runs the same gate.
         await expect(env.getPackage("pkg", false)).rejects.toBeInstanceOf(
            BadRequestError,
         );
      },
      { timeout: 30000 },
   );

   it(
      "resolves a watermark against the compiled output schema and refuses DuckDB",
      async () => {
         const messages = await loadRejection(
            `${RAW}#@ persist name="t" refresh="incremental" watermark="order_date"\n${ROLLUP}`,
         );
         // The declaration itself is coherent; only the dialect stands in the
         // way — which on this dialect means the package cannot be served at all.
         expect(messages).toHaveLength(1);
         expect(messages[0]).toContain('dialect "duckdb"');
      },
      { timeout: 30000 },
   );

   it(
      "rejects a watermark that names no materialized column",
      async () => {
         const messages = await loadRejection(
            `${RAW}#@ persist name="t" refresh="incremental" watermark="ordr_date"\n${ROLLUP}`,
         );
         const joined = messages.join("\n");
         expect(joined).toContain('"ordr_date"');
         expect(joined).toContain('"order_date"');
      },
      { timeout: 30000 },
   );

   it(
      "rejects a calculate: field while accepting count_distinct beside it",
      async () => {
         const gated = await loadRejection(
            `${RAW}#@ persist name="t" refresh="incremental" watermark="order_date"
source: daily is raw -> {
  group_by: order_date
  aggregate: revenue is amount.sum()
  calculate: prev is lag(revenue)
}`,
         );
         expect(gated.join("\n")).toContain("calculate:");

         const allowed = await loadRejection(
            `${RAW}#@ persist name="t" refresh="incremental" watermark="order_date"
source: daily is raw -> {
  group_by: order_date
  aggregate: buyers is count(order_id)
}`,
         );
         // Only the dialect refusal — the non-additive aggregate is fine.
         expect(allowed).toHaveLength(1);
         expect(allowed[0]).not.toContain("calculate:");
      },
      { timeout: 60000 },
   );

   it(
      "surfaces the unknown-key advisory on the warnings WITHOUT failing the load",
      async () => {
         // The other half of the severity split, over a real package: an
         // unrecognized key is legal, so the package loads and the finding rides
         // `warnings`. (Its sibling, the keyless-delta advisory, needs an
         // incremental source — which on DuckDB the dialect rule rejects, so it
         // is only reachable from the unit tests above.)
         const pkg = await loadPackage(
            `${RAW}#@ persist name="t" mergekey="order_id"\n${ROLLUP}`,
         );
         const messages = (pkg.getPackageMetadata().warnings ?? []).map(
            (w) => w.message ?? "",
         );
         expect(messages.some((m) => m.includes('"mergekey"'))).toBe(true);
         expect(pkg.formatInvalidIncrementalPolicy()).toBe("");
      },
      { timeout: 30000 },
   );

   it(
      "really does compile two differently named sources to ONE address",
      async () => {
         // What makes the shared-address advisory worth having: nothing here says
         // "duplicate" — two names, two name= tables — and yet the compiler emits
         // byte-identical SQL, so one content address, one table, one boundary.
         // The advisory's own wording is covered by the unit tests above, which
         // can name a dialect that accepts an incremental declaration; here the
         // sources stay plain so the package loads and the plan can be read.
         const pkg = await loadPackage(
            `${RAW}` +
               `#@ persist name="t_a"\n` +
               `source: daily_a is raw -> {
  group_by: order_date, region, order_id
  aggregate: revenue is amount.sum()
}\n\n` +
               `#@ persist name="t_b"\n` +
               `source: daily_b is raw -> {
  group_by: order_date, region, order_id
  aggregate: revenue is amount.sum()
}\n`,
         );
         const sources = Object.values(pkg.getBuildPlan()?.sources ?? {});
         expect(sources.map((s) => s.name).sort()).toEqual([
            "daily_a",
            "daily_b",
         ]);
         expect(new Set(sources.map((s) => s.sourceEntityId)).size).toBe(1);
      },
      { timeout: 30000 },
   );

   it(
      "gates an extending child on the declaration it INHERITED, and reports BOTH",
      async () => {
         // Two things at once. The child declares nothing at all, so every rule
         // has to read the effective merged tag to see it — and both rejections
         // travel in the one thrown message, so a model with two broken
         // declarations takes one republish to fix rather than two.
         const messages = await loadRejection(
            `${RAW}#@ persist name="t" refresh="incremental"\n${ROLLUP}

source: child is daily extend { }`,
         );
         // Two sources, two rules each (no watermark=, and the dialect): all
         // four rejections travel, none is dropped for being the fifth thing
         // wrong with the package.
         expect(messages).toHaveLength(4);
         for (const name of ['"daily"', '"child"']) {
            expect(messages.filter((m) => m.includes(name))).toHaveLength(2);
         }
      },
      { timeout: 30000 },
   );

   it(
      "lets a child opt out of an inherited incremental declaration",
      async () => {
         const messages = await loadRejection(
            `${RAW}#@ persist name="t" refresh="incremental" watermark="order_date"\n${ROLLUP}

#@ persist name="child_t" refresh="full" -watermark
source: child is daily extend { }`,
         );
         // Only the parent's dialect refusal remains; the child is out entirely.
         expect(messages).toHaveLength(1);
         expect(messages[0]).toContain('"daily"');
      },
      { timeout: 30000 },
   );
});
