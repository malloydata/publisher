// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import {
   FixedConnectionMap,
   InMemoryURLReader,
   Runtime,
} from "@malloydata/malloy";
import { afterEach, beforeEach, describe, expect, it } from "bun:test";
import { tempBuildPlanPackages } from "../../../test_helpers/incremental_test_harness";
import { installSettings } from "../../../test_helpers/settings_env";
import { compilePackageBuildPlan } from "./build_plan_compile";
import { buildCopyText, type MaterializePlan } from "./build_file_generator";

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

const packages = tempBuildPlanPackages();
let restore: () => void;
beforeEach(() => {
   restore = installSettings({
      env: { PUBLISHER_MATERIALIZE: "true" },
   }).restore;
});
afterEach(async () => {
   restore?.();
   await packages.cleanup();
});

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
   const plans = Object.values(c.materializePlans ?? {}) as MaterializePlan[];
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
               expect(stored(plan).some((s) => s.split(":")[0] === name)).toBe(
                  false,
               );
            }
         },
         { timeout: 30000 },
      );
   }
});
