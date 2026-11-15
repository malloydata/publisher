// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import {
   materializePlansIn,
   sourcesIn,
   targetsIn,
} from "../../../test_helpers/materialization_test_fixtures";
import { describe, expect, it } from "bun:test";
import { planPackages } from "../../../test_helpers/materialization_fixture";
import { accessLiftEdits, applyEdits } from "../text/declaration_text";
import { addressSQL, buildSQLOf, deriveBuildPlanOutcome } from "./build_plan";
import { compilePackageBuildPlan } from "./build_plan_compile";
import { type MaterializePlan } from "../domain/table_plans";
import {
   FixedConnectionMap,
   InMemoryURLReader,
   Runtime,
} from "@malloydata/malloy";
import { buildCopyText } from "./materialize_build_copy";

/**
 * A `#(materialize)` source's build copy (`materialize_build_copy.ts`): the
 * access-modifier lift (P3) that lets the build select a source's hidden
 * fields, what the plan stores of them, and the copy differing from the
 * author's source in access modifiers only. One section per concern.
 */

describe("hidden fields stored through the build copy (P3)", () => {
   /**
    * P3 on the `#(materialize)` build side (materialization v2, phase 3): a
    * source's private and internal dimensions are stored under their own names,
    * built from a copy of the source's declaring file with its own access
    * modifiers lifted. A field private on a source it extends, and an
    * `except:`ed field, are never selected.
    */

   describe("accessLiftEdits", () => {
      const lift = (text: string) =>
         applyEdits(text, accessLiftEdits(text) ?? []) as string;

      it("deletes a statement's private or internal label, and leaves public alone", () => {
         expect(
            lift(
               "s is t extend {\n  private dimension: a is 1\n  internal measure: m is count()\n  public dimension: b is 2\n}",
            ),
         ).toBe(
            "s is t extend {\n  dimension: a is 1\n  measure: m is count()\n  public dimension: b is 2\n}",
         );
      });

      it("makes an include's labels public, and gives a block with no wildcard one", () => {
         expect(lift("s is t include { private: a; internal: b }")).toBe(
            "s is t include { public: a; public: b \n  public: *\n}",
         );
         expect(lift("s is t include { public: a; private: * }")).toBe(
            "s is t include { public: a; public: * }",
         );
      });

      it("leaves except:, strings, comments, annotations and field names alone", () => {
         const text = [
            "s is t include { public: *; except: k } extend {",
            "  # private: x",
            "  -- private dimension: y",
            "  dimension: z is 'private: q'",
            "  dimension: private_x is internal_y",
            "}",
         ].join("\n");
         expect(lift(text)).toBe(text);
      });

      it("is undefined for text that does not lex", () => {
         expect(accessLiftEdits("s is t extend { dimension: a is '")).toBe(
            undefined,
         );
      });

      it("refuses overlapping edits", () => {
         expect(
            applyEdits("abc", [
               { start: 0, end: 2, insert: "x" },
               { start: 1, end: 3, insert: "y" },
            ]),
         ).toBeUndefined();
      });
   });

   const HEAD = `##! experimental { access_modifiers }
source: raw is duckdb.sql("""
  select * from (values
    (1, 'acme', 'east', 10.5, timestamp '2026-01-01 10:00:00'),
    (2, 'globex', 'west', 20.0, timestamp '2026-01-02 10:00:00')
  ) as t(id, org_id, region, amount, ts)
""")
`;

   const packages = planPackages({}, { keepIncrementalUnbuildable: true });
   const settings = packages.settings;

   async function planOf(text: string | Record<string, string>) {
      const pkg = await packages.create(text);
      const c = await compilePackageBuildPlan(pkg);
      const plans = Object.values(
         materializePlansIn(c) ?? {},
      ) as MaterializePlan[];
      return { pkg, c, plans };
   }

   const stored = (p: MaterializePlan) =>
      p.columns.map((c) => (c.access ? `${c.column}:${c.access}` : c.column));

   describe("planning a source's hidden fields (P3)", () => {
      it(
         "stores its own private and internal fields, and the internal ones it inherits",
         async () => {
            const { pkg, c, plans } = await planOf(`${HEAD}
source: base is raw extend {
  internal dimension: region_code is upper(region)
  private dimension: base_secret is amount * 3
}

#(materialize) storage="lake"
source: orders is base extend {
  private dimension: secret is amount * 2
  internal dimension: big is amount > 15
}
`);
            const [p] = plans;
            expect(stored(p)).toEqual([
               "id",
               "org_id",
               "region",
               "amount",
               "ts",
               "region_code:internal",
               "secret:private",
               "big:internal",
            ]);
            // Never the field private on the source it extends.
            expect(stored(p).some((c) => c.startsWith("base_secret"))).toBe(
               false,
            );
            const source = sourcesIn(c)[p.sourceID];
            const sql = buildSQLOf(
               source,
               { entries: {} },
               targetsIn(c)?.[p.sourceID],
            );
            const duckdb = await pkg.getMalloyConnection!("duckdb");
            const rows = (
               await duckdb.runSQL(
                  `SELECT org_id, region_code, secret, big FROM (${sql}) t ORDER BY org_id`,
               )
            ).rows;
            expect(
               rows.map((r) => [
                  r.org_id,
                  r.region_code,
                  Number(r.secret),
                  r.big,
               ]),
            ).toEqual([
               ["acme", "EAST", 21, false],
               ["globex", "WEST", 40, true],
            ]);
            expect(
               addressSQL(source, targetsIn(c)?.[p.sourceID]),
            ).not.toContain("base_secret");
         },
         { timeout: 30000 },
      );

      it(
         "lifts an internal field a source in another file declares",
         async () => {
            const { plans } = await planOf({
               "base.malloy": `${HEAD}
source: base is raw extend { internal dimension: region_code is upper(region) }
`,
               "m.malloy": `import 'base.malloy'
#(materialize) storage="lake"
source: orders is base extend { dimension: east is region_code = 'EAST' }
`,
            });
            expect(stored(plans[0])).toContain("region_code:internal");
         },
         { timeout: 30000 },
      );

      it(
         "never stores an except:ed field, and stores a hidden one with a timeframe",
         async () => {
            const { plans } = await planOf(`${HEAD}
#(materialize) storage="lake"
source: orders is raw include { public: *; except: region } extend {
  private dimension: order_day is ts.day
  internal dimension: order_month is ts.month
}
`);
            // `order_month` depends on the query's time zone and its definition
            // is the source's own, so it is left out and computed at read (P9,
            // #1684). The probe cannot select the private `order_day`, so it is
            // stored as built, in the default zone, and is not zone-free: a
            // `timezone:` query over the table is answered live.
            expect(stored(plans[0])).toEqual([
               "id",
               "org_id",
               "amount",
               "ts",
               "order_day:private",
            ]);
            expect(plans[0].leftOut).toEqual([
               { dimension: "order_month", reads: ["time_zone"] },
            ]);
            expect(
               plans[0].columns.find((c) => c.dimension === "order_day")
                  ?.zoneFree,
            ).toBeUndefined();
         },
         { timeout: 30000 },
      );

      it(
         "leaves a given term over a private field out of the build, and stores the field",
         async () => {
            const { c, plans } =
               await planOf(`##! experimental { access_modifiers givens }
given: ORG :: string is 'acme'
source: raw is duckdb.sql("select 1 as id, 'acme' as org_id")

#(materialize) storage="lake"
source: orders is raw include { private: org_id; public: * } extend {
  where: org_id = $ORG
}
`);
            const [p] = plans;
            expect(stored(p)).toEqual(["id", "org_id:private"]);
            expect(p.readTimeTerms?.map((t) => t.code)).toEqual([
               "org_id = $ORG",
            ]);
            // One build copy: the term taken out and the modifier lifted.
            const sql = addressSQL(
               sourcesIn(c)[p.sourceID],
               targetsIn(c)?.[p.sourceID],
            );
            expect(sql).toContain('"org_id"');
            expect(sql).toContain("WHERE true");
         },
         { timeout: 30000 },
      );
   });

   describe("a source whose build reads a #@ persist source", () => {
      it(
         "is built from the author's file, storing its public fields only",
         async () => {
            const { c, plans } =
               await planOf(`##! experimental { access_modifiers persistence }
source: raw is duckdb.sql("select 1 as id, 'acme' as org_id")

#@ persist name="raw_t"
source: raw_p is raw -> { select: * }

#(materialize) storage="lake"
source: orders is raw_p -> { select: id, org_id } extend {
  private dimension: secret is id * 2
}
`);
            // Its copy would declare raw_p a second time, so no copy is made.
            expect(stored(plans[0])).toEqual(["id", "org_id"]);
            const nodes = c.builds.flatMap((b) => b.order);
            expect(
               nodes.every((id) => !id.includes(".materialize-build.")),
            ).toBe(true);
         },
         { timeout: 30000 },
      );
   });

   describe("a gated source's hidden columns land in the destination only with the switch on (question 14)", () => {
      const GATED = `##! experimental { access_modifiers givens }
given: GROUPS :: string[]
source: raw is duckdb.sql("select 1 as id, 'acme' as org_id, 2.5 as amount")

#(access_filter) org_id in $GROUPS
#(materialize) storage="lake"
source: orders is raw include { internal: org_id; private: id; public: * }
`;

      for (const on of [true, false]) {
         it(
            `gated serving ${on ? "on: planned with its hidden columns" : "off: refused, nothing built"}`,
            async () => {
               settings({
                  PERSIST_STORAGE_MODE: "on",
                  PUBLISHER_MATERIALIZE_GATED_SOURCES: String(on),
               });
               const { c, plans } = await planOf(GATED);
               expect(stored(plans[0])).toEqual([
                  "id:private",
                  "org_id:internal",
                  "amount",
               ]);
               const outcome = deriveBuildPlanOutcome(c, null);
               const [sourceID] = Object.keys(materializePlansIn(c) ?? {});
               if (on) {
                  expect(outcome.sourceEligibility.eligible).toEqual([
                     sourceID,
                  ]);
               } else {
                  expect(outcome.sourceEligibility.eligible).toEqual([]);
                  expect(outcome.plan?.refusedSources?.[sourceID]?.reason).toBe(
                     "authorize",
                  );
               }
            },
            { timeout: 30000 },
         );
      }
   });

   describe("an inlined join that is not public", () => {
      it(
         "plans a private or internal join_one, each joined column carrying the join's modifier, and builds it through the lifted copy",
         async () => {
            settings({ PERSIST_STORAGE_MODE: "on" });
            const { c, plans } =
               await planOf(`##! experimental { access_modifiers }
source: orders0 is duckdb.sql("select 1 as id, 'east' as region, 2.0 as amount")
source: regions is duckdb.sql("select 'east' as code, 'East' as label") extend {
  primary_key: code
}

#(materialize) storage="lake" inline=["r"]
source: by_region is orders0 extend {
  join_one: r is regions with region
}

#(materialize) storage="lake" inline=["r"]
source: hidden_join is orders0 extend {
  internal join_one: r is regions with region
  dimension: region_label is r.label
}

#(materialize) storage="lake" inline=["r"]
source: private_join is orders0 extend {
  private join_one: r is regions with region
}
`);
            const byName = new Map(plans.map((p) => [p.sourceName, p]));
            expect([...byName.keys()].sort()).toEqual([
               "by_region",
               "hidden_join",
               "private_join",
            ]);
            expect(stored(byName.get("by_region") as MaterializePlan)).toEqual([
               "id",
               "region",
               "amount",
               "r__code",
               "r__label",
            ]);
            expect(
               stored(byName.get("hidden_join") as MaterializePlan),
            ).toEqual([
               "id",
               "region",
               "amount",
               "region_label",
               "r__code:internal",
               "r__label:internal",
            ]);
            expect(
               stored(byName.get("private_join") as MaterializePlan),
            ).toEqual([
               "id",
               "region",
               "amount",
               "r__code:private",
               "r__label:private",
            ]);
            for (const name of ["hidden_join", "private_join"]) {
               const p = byName.get(name) as MaterializePlan;
               const sql = addressSQL(
                  sourcesIn(c)[p.sourceID],
                  targetsIn(c)?.[p.sourceID],
               );
               expect(sql).toContain('"r__label"');
            }
            expect(
               (c.annotationFindings ?? []).filter(
                  (f) => f.code === "materialize-not-built",
               ),
            ).toEqual([]);
         },
         { timeout: 30000 },
      );

      it(
         "plans a private or internal join_many's pair table, each joined column carrying the join's modifier, and builds it through the lifted copy",
         async () => {
            settings({ PERSIST_STORAGE_MODE: "on" });
            const { c, plans } =
               await planOf(`##! experimental { access_modifiers }
source: orders0 is duckdb.sql("select 1 as id, 'east' as region, 2.0 as amount") extend {
  primary_key: id
}
source: items is duckdb.sql("select 1 as item_id, 1 as order_id, 'x' as sku") extend {
  primary_key: item_id
}

#(materialize) storage="lake" inline=["i"]
source: hidden_many is orders0 extend {
  internal join_many: i is items on id = i.order_id
  measure: skus is count(i.sku)
}

#(materialize) storage="lake" inline=["i"]
source: private_many is orders0 extend {
  private join_many: i is items on id = i.order_id
  rename: amt is amount
}
`);
            const byName = new Map(plans.map((p) => [p.sourceName, p]));
            expect([...byName.keys()].sort()).toEqual([
               "hidden_many",
               "private_many",
            ]);
            expect(
               stored(byName.get("hidden_many") as MaterializePlan),
            ).toEqual([
               "id",
               "region",
               "amount",
               "i__item_id:internal",
               "i__order_id:internal",
               "i__sku:internal",
            ]);
            expect(
               stored(byName.get("private_many") as MaterializePlan),
            ).toEqual([
               "id",
               "region",
               "amt",
               "i__item_id:private",
               "i__order_id:private",
               "i__sku:private",
            ]);
            for (const name of ["hidden_many", "private_many"]) {
               const p = byName.get(name) as MaterializePlan;
               expect(p.pair?.join).toBe("i");
               const sql = addressSQL(
                  sourcesIn(c)[p.sourceID],
                  targetsIn(c)?.[p.sourceID],
               );
               expect(sql).toContain('"i__sku"');
            }
            expect(
               (c.annotationFindings ?? []).filter(
                  (f) => f.code === "materialize-not-built",
               ),
            ).toEqual([]);
         },
         { timeout: 30000 },
      );

      it(
         "refuses a path through a join its joined source hides, alone, so the model's other sources still build",
         async () => {
            settings({ PERSIST_STORAGE_MODE: "on" });
            const { c, plans } =
               await planOf(`##! experimental { access_modifiers }
source: orders0 is duckdb.sql("select 1 as id, 2.0 as amount") extend {
  primary_key: id
}
source: opts is duckdb.sql("select 7 as opt_id, 1 as item_id, 'k' as kind") extend {
  primary_key: opt_id
}
source: items is duckdb.sql("select 1 as item_id, 1 as order_id, 'x' as sku") extend {
  primary_key: item_id
  internal join_many: opts on opts.item_id = item_id
}

#(materialize) storage="lake" inline=["i.opts"]
source: hop is orders0 extend {
  internal join_many: i is items on id = i.order_id
}

#(materialize) storage="lake" inline=["o"]
source: one_hop is orders0 extend {
  private join_many: o is opts on id = o.item_id
}
`);
            expect(plans.map((p) => p.sourceName).sort()).toEqual(["one_hop"]);
            const finding = (c.annotationFindings ?? []).find(
               (f) => f.subject === "hop",
            );
            expect(finding?.code).toBe("materialize-not-built");
            expect(finding?.message).toContain(
               "whose `opts` is not public on the source `i` joins",
            );
         },
         { timeout: 30000 },
      );
   });
});

describe("the build copy lifts access modifiers only", () => {
   /**
    * The P3 build copy's lift, across every access-modifier syntax Malloy
    * accepts on a source declaration: the copy differs from the author's source
    * in access modifiers only (each field's name, type and expression are the
    * same), and what the plan stores is exactly the hidden fields the source can
    * read, each marked with the author's modifier.
    */

   const HEAD = `##! experimental { access_modifiers }
source: raw is duckdb.sql("""
  select * from (values
    (1, 'acme', 'east', 10.5, timestamp '2026-01-01 10:00:00'),
    (2, 'globex', 'west', 20.0, timestamp '2026-01-02 10:00:00')
  ) as t(id, org_id, region, amount, ts)
""")
`;

   const packages = planPackages({}, { keepIncrementalUnbuildable: true });

   type Field = Record<string, unknown>;

   /** A field with everything but access, and where it was written, kept. */
   function shape(field: Field): string {
      return JSON.stringify(field, (key, value) =>
         key === "accessModifier" ||
         key === "location" ||
         key === "at" ||
         key === "annotation" ||
         key === "annotations" ||
         key === "sourceID" ||
         key === "url"
            ? undefined
            : value,
      );
   }

   async function fieldsOf(
      files: Record<string, string>,
      entry: string,
      source: string,
      connection: unknown,
   ): Promise<Field[]> {
      const runtime = new Runtime({
         urlReader: new InMemoryURLReader(
            new Map(
               Object.entries(files).map(([name, text]) => [
                  `file:///p/${name}`,
                  text,
               ]),
            ),
         ),
         connections: new FixedConnectionMap(
            new Map([["duckdb", connection as never]]),
            "duckdb",
         ),
      });
      const model = await runtime
         .loadModel(new URL(`file:///p/${entry}`))
         .getModel();
      const def = model._modelDef.contents[source] as unknown as {
         fields: Field[];
      };
      return def.fields;
   }

   const nameOf = (f: Field) => String(f.as ?? f.name);
   const accessOf = (f: Field) => String(f.accessModifier ?? "public");

   /**
    * Plan `files`, lift the source's declaration as the build does, and compare
    * the copy with the author's source.
    */
   async function lift(files: Record<string, string>, entry = "m.malloy") {
      const pkg = await packages.create(files);
      const c = await compilePackageBuildPlan(pkg);
      const plans = Object.values(
         materializePlansIn(c) ?? {},
      ) as MaterializePlan[];
      expect(plans.length).toBe(1);
      const [plan] = plans;
      const copy = buildCopyText(files[entry], plan, undefined, {
         liftAccess: true,
      });
      if (!("copy" in copy)) throw new Error(copy.reason);
      const connection = await pkg.getMalloyConnection!("duckdb");
      const author = await fieldsOf(files, entry, plan.sourceName, connection);
      const lifted = await fieldsOf(
         { ...files, [entry]: copy.copy },
         entry,
         plan.sourceName,
         connection,
      );
      return { plan, author, lifted, copy: copy.copy };
   }

   const stored = (p: MaterializePlan) =>
      p.columns.map((c) => (c.access ? `${c.column}:${c.access}` : c.column));

   /** The copy changes access modifiers and nothing else. */
   function expectOnlyAccessLifted(author: Field[], lifted: Field[]) {
      expect(lifted.map(nameOf).sort()).toEqual(author.map(nameOf).sort());
      const byName = new Map(lifted.map((f) => [nameOf(f), f]));
      for (const f of author) {
         expect(shape(byName.get(nameOf(f)) as Field)).toBe(shape(f));
      }
   }

   const cases: {
      name: string;
      files: Record<string, string>;
      stored: string[];
      /** Fields still hidden in the copy (never stored). */
      stillHidden?: string[];
   }[] = [
      {
         name: "statement labels on every kind of definition",
         files: {
            "m.malloy": `${HEAD}
#(materialize) storage="lake"
source: orders is raw extend {
  private dimension: a is amount * 2, b is amount * 3
  internal
    dimension: c is upper(region)
  private measure: m is count()
  internal view: v is { group_by: region }
  private join_one: j is raw on id = j.id
  internal rename: org is org_id
}
`,
         },
         stored: [
            "id",
            "region",
            "amount",
            "ts",
            "org:internal",
            "a:private",
            "b:private",
            "c:internal",
         ],
      },
      {
         name: "include lists, wildcards and except:, chained",
         files: {
            "m.malloy": `${HEAD}
#(materialize) storage="lake"
source: orders is raw include { internal: org_id, region; except: ts; public: * } include { private: id; public: * }
`,
         },
         // A later block's `public: *` makes the earlier `internal` public again
         // (Malloy lets `public: *` expand internal, never private).
         stored: ["id:private", "org_id", "region", "amount"],
      },
      {
         name: "a block whose wildcard hides everything",
         files: {
            "m.malloy": `${HEAD}
#(materialize) storage="lake"
source: orders is raw include { internal: *; public: amount }
`,
         },
         stored: [
            "id:internal",
            "org_id:internal",
            "region:internal",
            "amount",
            "ts:internal",
         ],
      },
      {
         name: "a hidden field of a query-shaped source",
         files: {
            "m.malloy": `${HEAD}
#(materialize) storage="lake"
source: orders is raw -> { select: id, org_id, amount } include { private: org_id; public: * } extend {
  measure: n is count()
}
`,
         },
         stored: ["id", "amount", "org_id:private"],
      },
      {
         name: "internal fields inherited from another file, and a private one never",
         files: {
            "base.malloy": `${HEAD}
source: base is raw include { private: id; internal: org_id; public: * } extend {
  internal dimension: region_code is upper(region)
  private dimension: base_secret is amount * 3
}
`,
            "m.malloy": `import 'base.malloy'

#(materialize) storage="lake"
source: orders is base extend { dimension: east is region_code = 'EAST' }
`,
         },
         stored: [
            "org_id:internal",
            "region",
            "amount",
            "ts",
            "region_code:internal",
            "east",
         ],
         stillHidden: ["id", "base_secret"],
      },
   ];

   describe("the P3 build copy lifts access modifiers, and nothing else", () => {
      for (const testCase of cases) {
         it(
            testCase.name,
            async () => {
               const { plan, author, lifted } = await lift(testCase.files);
               expectOnlyAccessLifted(author, lifted);
               expect(stored(plan).sort()).toEqual([...testCase.stored].sort());
               // Each stored column carries the author's modifier.
               const authorAccess = new Map(author.map((f) => [nameOf(f), f]));
               for (const c of plan.columns) {
                  const was = accessOf(authorAccess.get(c.column) as Field);
                  expect(c.access ?? "public").toBe(was);
               }
               // A field the source cannot read stays hidden in the copy too.
               const copyAccess = new Map(
                  lifted.map((f) => [nameOf(f), accessOf(f)]),
               );
               for (const name of testCase.stillHidden ?? []) {
                  expect(copyAccess.get(name)).toBe("private");
                  expect(
                     stored(plan).some((s) => s.split(":")[0] === name),
                  ).toBe(false);
               }
            },
            { timeout: 30000 },
         );
      }
   });
});
