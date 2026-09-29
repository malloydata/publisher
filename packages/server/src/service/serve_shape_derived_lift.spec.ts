// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * Which non-persisted sources the serve shape carries, and what each one is
 * reduced to.
 *
 * Compile-backed, against a real `Runtime`, because the whole selection turns on
 * what the COMPILER does to a source that extends another: it copies every one
 * of the base's fields onto the extending source and prepends the base's
 * `where:` clauses to its filter list. A hand-built `contents` fixture would
 * encode our belief about that rather than the compiler's behaviour, and the
 * failure it would hide — re-emitting an inherited field, which is
 * `Cannot redefine` and fails the ENTIRE shape — is the expensive one.
 */
import { DuckDBConnection } from "@malloydata/db-duckdb";
import {
   FixedConnectionMap,
   InMemoryURLReader,
   Runtime,
   type ModelDef,
} from "@malloydata/malloy";
import { afterAll, beforeAll, describe, expect, it } from "bun:test";
import {
   liftDerivedSources,
   type DerivedSourceDef,
   type DerivedSourceLift,
} from "./materialization_serve_transform";

const ROOT = "file:///lift/";

describe("derived-source lift onto the serve shape", () => {
   let duckdb: DuckDBConnection;
   let connections: FixedConnectionMap;

   beforeAll(() => {
      duckdb = new DuckDBConnection("duckdb", ":memory:");
      connections = new FixedConnectionMap(
         new Map([["duckdb", duckdb]]),
         "duckdb",
      );
   });

   afterAll(async () => {
      await duckdb.close();
   });

   /** Compile `model` and lift against the named set of materialized sources. */
   async function liftsFor(
      model: string,
      materialized: string[],
      /** Every source with a binding, fresh or not. Defaults to `materialized`. */
      bound?: string[],
   ): Promise<DerivedSourceLift[]> {
      const runtime = new Runtime({
         urlReader: new InMemoryURLReader(
            new Map([[`${ROOT}m.malloy`, model]]),
         ),
         connections,
      });
      /* eslint-disable @typescript-eslint/no-explicit-any */
      const def = (
         (await runtime.loadModel(new URL(`${ROOT}m.malloy`)).getModel()) as any
      )._modelDef as ModelDef;
      /* eslint-enable @typescript-eslint/no-explicit-any */
      const contents = def.contents as unknown as Record<
         string,
         DerivedSourceDef & { sourceID?: unknown }
      >;
      const sourceNameById = new Map<string, string>();
      for (const [name, d] of Object.entries(contents)) {
         if (typeof d?.sourceID === "string")
            sourceNameById.set(d.sourceID, name);
      }
      return liftDerivedSources({
         contents,
         sourceNameById,
         shapeSourceNames: new Set(materialized),
         boundSourceNames: new Set(bound ?? materialized),
         // The scenarios below assert selection, which never depends on the
         // declaration text; returning it verbatim keeps a join liftable.
         liftText: () => "j is other on 1 = 1",
      });
   }

   const RICH_BASE = `##! experimental.givens

given: USER_ID :: number is 7

source: raw is duckdb.sql('SELECT 1 as id, 1 as user_id, 2 as amount')

source: base is raw extend {
  where: id = 1
  dimension: doubled is amount * 2
  measure: total is amount.sum()
  view: by_id is { group_by: id; aggregate: total }
}

source: entry is base extend {
  where: user_id = $USER_ID
}
`;

   it("carries a derived source over its materialized base", async () => {
      const lifts = await liftsFor(RICH_BASE, ["base"]);
      expect(lifts.map((l) => l.sourceName)).toEqual(["entry"]);
      expect(lifts[0].base).toBe("base");
   });

   it("re-emits only what the derived source adds, never the base's own fields", async () => {
      const [lift] = await liftsFor(RICH_BASE, ["base"]);
      // The compiler copies `doubled`, `total` and `by_id` onto `entry`. Emitting
      // any of them on a base that already declares them is `Cannot redefine`,
      // which fails the whole shape — so the lift must carry none of them.
      expect(lift.refinements.map((r) => r.kind).sort()).toEqual(["filter"]);
      const filters = lift.refinements.filter((r) => r.kind === "filter");
      expect(filters).toHaveLength(1);
      expect(filters[0].code).toContain("user_id");
      // The base's own `where: id = 1` is inherited and is the base's to apply.
      expect(filters.some((f) => f.code.includes("id = 1"))).toBe(false);
   });

   it("leaves a derived source off when its base is not materialized", async () => {
      // `base` withheld — not persisted, or its binding dropped by the freshness
      // gate. Either way there is nothing on the shape to extend.
      expect(await liftsFor(RICH_BASE, [])).toEqual([]);
   });

   const JOINS_UNMATERIALIZED = `source: raw is duckdb.sql('SELECT 1 as id, 2 as amount')
source: raw_other is duckdb.sql('SELECT 1 as id, 3 as qty')

source: base is raw extend { where: id = 1 }
source: other is raw_other extend { where: id = 1 }

source: entry is base extend {
  join_many: j is other on j.id = id
  where: j.qty > 0
}
`;

   it("leaves a derived source off ENTIRELY when a join target is not materialized", async () => {
      // Not "lifted without the join": a join_many fans rows out, so dropping one
      // silently changes what an aggregate over this source answers. The source's
      // own `where:` also reads the alias, so emitting it without the join would
      // fail the whole shape rather than just this source.
      expect(await liftsFor(JOINS_UNMATERIALIZED, ["base"])).toEqual([]);
   });

   it("carries the same source once its join target is materialized too", async () => {
      const lifts = await liftsFor(JOINS_UNMATERIALIZED, ["base", "other"]);
      expect(lifts.map((l) => l.sourceName)).toEqual(["entry"]);
      expect(
         lifts[0].refinements.filter((r) => r.kind === "join"),
      ).toHaveLength(1);
   });

   const CHAIN = `source: raw is duckdb.sql('SELECT 1 as id, 2 as amount')

source: base is raw extend { where: id = 1 }
source: mid is base extend { where: amount > 0 }
source: leaf is mid extend { where: amount < 100 }
`;

   it("carries a chain whole, each after the source it extends", async () => {
      const lifts = await liftsFor(CHAIN, ["base"]);
      // Emission order is the shape's declaration order, so a base must precede
      // anything extending it or the shape does not resolve.
      expect(lifts.map((l) => l.sourceName)).toEqual(["mid", "leaf"]);
      expect(lifts.map((l) => l.base)).toEqual(["base", "mid"]);
   });

   it("stops a chain at the first link it cannot carry", async () => {
      // `mid` is refused (its base is not on the shape), so `leaf` has no base
      // either — the fixpoint must not promote it over a link that was dropped.
      expect(await liftsFor(CHAIN, [])).toEqual([]);
   });

   it("leaves off a source whose own binding was withheld, rather than serving its base's", async () => {
      // `entry` has a binding of its own — it is a build target — but freshness
      // withheld it, so it is absent from the shape's FRESH set while `base` is
      // present. Lifting it here would answer a query naming `entry` from
      // `base`'s artifact and report `servedFrom: storage`, which is exactly what
      // its `freshnessFallback` of `live` or `fail` exists to prevent.
      //
      // The candidate test is therefore against every binding, not the fresh
      // ones: a source with NO binding is still a candidate.
      expect(await liftsFor(RICH_BASE, ["base"], ["base", "entry"])).toEqual(
         [],
      );
      // Control: with no binding of its own, the same source is carried.
      expect(
         (await liftsFor(RICH_BASE, ["base"], ["base"])).map(
            (l) => l.sourceName,
         ),
      ).toEqual(["entry"]);
   });

   describe("a query-derived source over a materialized one", () => {
      // The private-fact / public-wrapper idiom: `#@ persist` on the fact, and
      // queries name a wrapper whose query reads it. Malloy gives the wrapper no
      // `extends` and no `persistent`, so it has no binding of its own.
      const WRAPPERS = `##! experimental.persistence
source: raw is duckdb.sql('SELECT 1 as id, 2 as amount')
source: other is duckdb.sql('SELECT 1 as id')

source: _fact is raw -> { select: * } extend { measure: total is amount.sum() }

source: wrapper is _fact -> { select: * } extend { dimension: d is id + 1 }
source: by_id is _fact -> { group_by: id; aggregate: total }
source: wrapper_of_wrapper is wrapper -> { select: * }
source: reaches_other is _fact -> {
  extend: { join_one: o is other on o.id = id }
  group_by: o.id
}
source: joins_other is _fact -> { select: * } extend {
  join_one: o is other on o.id = id
}

#@ -persist
source: opted_out is _fact -> { select: * }
`;

      it("carries each wrapper whose every reference is on the shape, verbatim", async () => {
         const lifts = await liftsFor(WRAPPERS, ["_fact"]);
         expect(lifts.map((l) => l.sourceName)).toEqual([
            "wrapper",
            "by_id",
            "wrapper_of_wrapper",
         ]);
         for (const lift of lifts) {
            // The declaration itself, not refinements over the base: a query's
            // output inherits nothing to subtract.
            expect(lift.refinements).toEqual([]);
            expect(lift.text).toBeDefined();
         }
         expect(
            lifts.find((l) => l.sourceName === "wrapper_of_wrapper")?.base,
         ).toBe("wrapper");
      });

      it("leaves off a wrapper that reaches a source not on the shape", async () => {
         // `other` is not materialized: a stage join or a declared join to it could
         // not compile against the shape, so neither wrapper is carried.
         const names = (await liftsFor(WRAPPERS, ["_fact"])).map(
            (l) => l.sourceName,
         );
         expect(names).not.toContain("reaches_other");
         expect(names).not.toContain("joins_other");

         const withOther = (await liftsFor(WRAPPERS, ["_fact", "other"])).map(
            (l) => l.sourceName,
         );
         expect(withOther).toContain("reaches_other");
         expect(withOther).toContain("joins_other");
      });

      it("carries nothing over a base that is not on the shape", async () => {
         expect(await liftsFor(WRAPPERS, [])).toEqual([]);
         // Bound but withheld (stale past its window): still not on the shape.
         expect(await liftsFor(WRAPPERS, [], ["_fact"])).toEqual([]);
      });

      it("never carries a wrapper that has a binding of its own, or opts out", async () => {
         const names = (
            await liftsFor(WRAPPERS, ["_fact"], ["_fact", "wrapper"])
         ).map((l) => l.sourceName);
         expect(names).not.toContain("wrapper");
         // Its dependant cannot be carried either: `wrapper` is not on the shape.
         expect(names).not.toContain("wrapper_of_wrapper");
         expect(names).not.toContain("opted_out");
      });
   });
});
