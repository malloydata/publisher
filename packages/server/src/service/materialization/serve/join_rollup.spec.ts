// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { describe, expect, it } from "bun:test";
import type { SourceJoin } from "../text/source_map";
import type { RollupVariantPlan } from "../plan/rollup_plan";
import type { QueryLevelFacts } from "../../governance/ir/prepared_query";
import {
   checkJoinCoverage,
   joinedKeyProblem,
   joinRollupLegs,
   parentKeyProblem,
   unmatchedRowProblem,
} from "./join_rollup";

const CHILD = "segments@file:///p/m.malloy";

function join(over: Partial<SourceJoin> = {}): SourceJoin {
   return {
      name: "segments",
      relationship: "many",
      connection: "duckdb",
      sourceID: CHILD,
      primaryKey: "seg_id",
      keyColumn: "flight_id",
      baseKey: "id",
      condition: { joins: new Set(["segments"]), nested: false },
      fields: [],
      ...over,
   };
}

const leaf = (aggregate: "sum" | "count" | "min" | "max", expr: unknown) => ({
   column: "x__partial",
   leaf: { aggregate, expr, reads: [], joinPath: [] },
});
const method = (col: string, fn: string) => ({
   kind: "method",
   path: [col],
   fn,
   args: [],
});

function plan(over: Partial<RollupVariantPlan> = {}): RollupVariantPlan {
   return {
      variant: true,
      baseSourceName: "segments",
      baseDeclaredName: "segments",
      baseSourceID: CHILD,
      rollupSourceName: "segments__rollup__flight_id__00000000",
      grainDimensions: ["flight_id"],
      measures: [],
      rollupMeasures: [
         {
            name: "seg_miles",
            partials: [leaf("sum", method("miles", "sum"))],
            merged: { kind: "number", text: "0" },
            fanOutDimensions: [],
         },
         {
            name: "seg_count",
            partials: [
               leaf("count", { kind: "call", name: "count", args: [] }),
            ],
            merged: { kind: "number", text: "0" },
            fanOutDimensions: [],
         },
         {
            name: "padded",
            partials: [
               leaf("sum", {
                  kind: "call",
                  name: "sum",
                  args: [
                     {
                        kind: "binary",
                        op: "+",
                        left: { kind: "path", path: ["miles"] },
                        right: { kind: "number", text: "1" },
                     },
                  ],
               }),
            ],
            merged: { kind: "number", text: "0" },
            fanOutDimensions: [],
         },
         {
            name: "p_miles",
            partials: [
               leaf("sum", {
                  kind: "filtered",
                  expr: method("miles", "sum"),
                  where: [],
               }),
            ],
            merged: { kind: "number", text: "0" },
            fanOutDimensions: [],
         },
      ],
      fanOutDimensions: [],
      baseGated: false,
      ...over,
   } as RollupVariantPlan;
}

describe("joinRollupLegs", () => {
   const legsOf = (j: SourceJoin, p = plan(), filters = new Set<string>()) =>
      joinRollupLegs([j], new Map([[CHILD, [p]]]), filters, "id");

   it("offers a join keyed on one field each side, join_one at the key alone", () => {
      const [only] = legsOf(join());
      expect(only?.refusal).toBeUndefined();
      expect(only?.leg.relationship).toBe("one");
      const [finer] = legsOf(
         join(),
         plan({ grainDimensions: ["flight_id", "kind"] }),
      );
      expect(finer?.leg.relationship).toBe("many");
   });

   it("offers nothing for a join defined in place, a join_cross, or a source without rollups", () => {
      expect(legsOf(join({ sourceID: undefined }))).toEqual([]);
      expect(legsOf(join({ relationship: "cross" }))).toEqual([]);
      expect(joinRollupLegs([join()], new Map(), new Set(), "id")).toEqual([]);
   });

   it("refuses, with reasons, what the rollup's rows cannot stand in for", () => {
      const code = (j: SourceJoin, p?: RollupVariantPlan, f?: Set<string>) =>
         legsOf(j, p, f)[0]?.refusal?.code;
      expect(code(join(), plan({ baseGated: true }))).toBe("rollup_contract");
      expect(code(join({ keyColumn: undefined }))).toBe(
         "rollup_count_semantics",
      );
      expect(code(join({ baseKey: undefined }))).toBe("rollup_count_semantics");
      expect(
         code(
            join({
               condition: { joins: new Set(["segments", "x"]), nested: false },
            }),
         ),
      ).toBe("rollup_count_semantics");
      expect(code(join(), plan({ grainDimensions: ["kind"] }))).toBe(
         "rollup_grain_too_coarse",
      );
      expect(code(join(), plan(), new Set(["segments"]))).toBe(
         "rollup_count_semantics",
      );
      // A join key reached through the joined source's own join_many.
      expect(code(join(), plan({ fanOutDimensions: ["flight_id"] }))).toBe(
         "rollup_grain_too_coarse",
      );
   });

   it("leaves a fan-out dimension or a join-level measure to the per-query check (row 27)", () => {
      const [fanned] = legsOf(
         join(),
         plan({
            grainDimensions: ["flight_id", "line_kind"],
            fanOutDimensions: ["line_kind"],
         }),
      );
      expect(fanned?.refusal).toBeUndefined();
      const p = plan({ grainDimensions: ["flight_id", "kind"] });
      p.rollupMeasures[0]!.fanOutDimensions = ["flight_id"];
      expect(legsOf(join(), p)[0]?.refusal).toBeUndefined();
   });

   it("refuses a join read as a join_many keyed on a field the query source does not keep unique", () => {
      const fine = plan({ grainDimensions: ["flight_id", "kind"] });
      const keyed = (j: SourceJoin, p: RollupVariantPlan, pk?: string) =>
         joinRollupLegs([j], new Map([[CHILD, [p]]]), new Set(), pk)[0]
            ?.refusal;
      // A declared join_many keyed on the query source's primary key.
      expect(keyed(join(), plan(), "id")).toBeUndefined();
      expect(keyed(join(), fine, "id")).toBeUndefined();
      // A declared join_many from a source without one, or keyed elsewhere.
      expect(keyed(join(), plan(), undefined)?.code).toBe(
         "rollup_count_semantics",
      );
      expect(keyed(join(), plan(), "seg_id")?.callerDetail).toContain(
         "not the query source's primary key",
      );
      // A declared join_one at its key: many parents to one key is a join_one.
      const one = join({ relationship: "one" });
      expect(keyed(one, plan(), undefined)).toBeUndefined();
      // A declared join_one re-pointed as a join_many at a finer grain.
      expect(keyed(one, fine, undefined)?.code).toBe("rollup_count_semantics");
      expect(keyed(one, fine, "id")).toBeUndefined();
   });

   it("refuses a join another join's condition reads", () => {
      const other = join({
         name: "legs",
         sourceID: "legs@file:///p/m.malloy",
         condition: { joins: new Set(["legs", "segments"]), nested: false },
      });
      const legs = joinRollupLegs(
         [join(), other],
         new Map([[CHILD, [plan()]]]),
         new Set(),
         "id",
      );
      expect(legs[0]?.refusal?.code).toBe("rollup_count_semantics");
   });
});

describe("parentKeyProblem", () => {
   const legOf = (relationship: "one" | "many", declared: "one" | "many") => ({
      join: join({ relationship: declared }) as never,
      plan: plan(),
      relationship,
   });

   it("serves a join read as a join_many only over a key known unique and never NULL", () => {
      for (const leg of [
         legOf("one", "many"),
         legOf("many", "many"),
         legOf("many", "one"),
      ]) {
         expect(parentKeyProblem(leg, true)).toBeUndefined();
         expect(parentKeyProblem(leg, false)).toContain(
            "more than once or NULL",
         );
         expect(parentKeyProblem(leg, undefined)).toContain("was not checked");
      }
   });

   it("asks nothing of a join_one read at its key", () => {
      expect(parentKeyProblem(legOf("one", "one"), undefined)).toBeUndefined();
   });
});

describe("unmatchedRowProblem", () => {
   const isColumn = (p: readonly string[]) =>
      p.length === 1 && p[0] === "miles";
   const many = joinRollupLegs(
      [join()],
      new Map([[CHILD, [plan()]]]),
      new Set(),
      "id",
   )[0]!.leg;
   const one = joinRollupLegs(
      [join({ relationship: "one" })],
      new Map([[CHILD, [plan()]]]),
      new Set(),
      "id",
   )[0]!.leg;

   const noJoin = () => false;

   it("serves the joined source's own counts and sums, whatever their operand (malloydata/malloy#3208)", () => {
      for (const leg of [many, one]) {
         for (const m of ["seg_miles", "seg_count", "padded", "p_miles"]) {
            expect(
               unmatchedRowProblem(leg, new Set([m]), isColumn, noJoin),
            ).toBeUndefined();
         }
      }
   });

   const withPartials = (
      partials: ReturnType<typeof leaf>[],
   ): Parameters<typeof unmatchedRowProblem>[0] => {
      const p = plan();
      p.rollupMeasures[0]!.partials =
         partials as RollupVariantPlan["rollupMeasures"][number]["partials"];
      return joinRollupLegs(
         [join()],
         new Map([[CHILD, [p]]]),
         new Set(),
         "id",
      )[0]!.leg;
   };
   const read = new Set(["seg_miles"]);

   it("declines a min or max of a value that is not NULL on an unmatched parent", () => {
      const constant = withPartials([
         leaf("max", {
            kind: "call",
            name: "max",
            args: [{ kind: "number", text: "1" }],
         }),
      ]);
      expect(unmatchedRowProblem(constant, read, isColumn, noJoin)).toContain(
         "max of a value that is not NULL",
      );
      const column = withPartials([leaf("min", method("miles", "min"))]);
      expect(
         unmatchedRowProblem(column, read, isColumn, noJoin),
      ).toBeUndefined();
      // Only what the query reads is held to it.
      expect(
         unmatchedRowProblem(
            constant,
            new Set(["seg_count"]),
            isColumn,
            noJoin,
         ),
      ).toBeUndefined();
   });

   it("reads a column of the joined source's own keyed join as NULL on an unmatched parent", () => {
      const leg = withPartials([
         leaf("max", {
            kind: "method",
            path: ["kinds", "weight"],
            fn: "max",
            args: [],
         }),
      ]);
      const seen: (readonly string[])[] = [];
      const yes = (path: readonly string[]) => (seen.push(path), true);
      expect(unmatchedRowProblem(leg, read, yes, noJoin)).toBeUndefined();
      expect(seen).toEqual([["kinds", "weight"]]);
      expect(unmatchedRowProblem(leg, read, () => false, noJoin)).toContain(
         "not NULL",
      );
   });

   it("declines a sum or count at a join of the joined source that can match an unmatched parent's row", () => {
      const at = (aggregate: "sum" | "count") => {
         const l = leaf(aggregate, {
            kind: "method",
            path: aggregate === "sum" ? ["kinds", "weight"] : ["kinds"],
            fn: aggregate,
            args: [],
         });
         l.leaf.joinPath = ["kinds"] as never;
         return withPartials([l]);
      };
      for (const aggregate of ["sum", "count"] as const) {
         expect(
            unmatchedRowProblem(at(aggregate), read, isColumn, noJoin),
         ).toContain("is not keyed on a column of `segments`");
         const asked: string[] = [];
         expect(
            unmatchedRowProblem(at(aggregate), read, isColumn, (j) => {
               asked.push(j);
               return true;
            }),
         ).toBeUndefined();
         expect(asked).toEqual(["kinds"]);
      }
   });
});

describe("checkJoinCoverage (row 27)", () => {
   type Field = QueryLevelFacts["fields"][number];
   const level = (
      fields: Partial<Field>[],
      over: Partial<QueryLevelFacts> = {},
   ): QueryLevelFacts => ({
      kind: "reduce",
      fields: fields.map((f) => ({
         name: f.name ?? "f",
         computesAggregate: false,
         ungrouped: false,
         isNest: false,
         ...f,
      })),
      fixedByWhere: new Set(),
      fixedPathsByWhere: [],
      havings: [],
      ...over,
   });
   const fanned = plan({
      grainDimensions: ["flight_id", "line_kind"],
      fanOutDimensions: ["line_kind"],
   });
   const legOf = (p: RollupVariantPlan) =>
      joinRollupLegs([join()], new Map([[CHILD, [p]]]), new Set(), "id")[0]!
         .leg;
   const none = () => "none" as const;
   const miles = { name: "seg_miles", ref: ["segments", "seg_miles"] };
   const kind = { name: "line_kind", ref: ["segments", "line_kind"] };

   it("serves a fan-out dimension grouped or fixed, and declines it dropped (A6)", () => {
      const leg = legOf(fanned);
      expect(
         checkJoinCoverage(leg, level([kind, miles]), none),
      ).toBeUndefined();
      expect(
         checkJoinCoverage(
            leg,
            level([miles], { fixedPathsByWhere: [["segments", "line_kind"]] }),
            none,
         ),
      ).toBeUndefined();
      expect(checkJoinCoverage(leg, level([miles]), none)?.code).toBe(
         "rollup_grain_too_coarse",
      );
      // Another join's field of the same name does not fix it.
      expect(
         checkJoinCoverage(
            leg,
            level([miles], { fixedPathsByWhere: [["crew", "line_kind"]] }),
            none,
         )?.code,
      ).toBe("rollup_grain_too_coarse");
   });

   it("reads a level that aggregates only the parent's own rows as aggregating nothing through the join", () => {
      const leg = legOf(fanned);
      const count = { name: "flight_count", ref: ["flight_count"] };
      const nest = level([kind, miles]);
      expect(
         checkJoinCoverage(
            leg,
            level([count, { name: "k", isNest: true, nest }]),
            none,
         ),
      ).toBeUndefined();
      // A parent measure that reads the join is held to every requirement.
      expect(
         checkJoinCoverage(leg, level([count]), () => "aggregate")?.code,
      ).toBe("rollup_grain_too_coarse");
      // So is an expression the query writes over a joined measure.
      expect(
         checkJoinCoverage(
            leg,
            level([
               {
                  name: "m",
                  expressionType: "aggregate",
                  reads: [["segments", "seg_miles"]],
               },
            ]),
            none,
         )?.code,
      ).toBe("rollup_grain_too_coarse");
      // And a field whose reads were not recorded.
      expect(
         checkJoinCoverage(
            leg,
            level([{ name: "m", expressionType: "aggregate" }]),
            none,
         )?.code,
      ).toBe("rollup_grain_too_coarse");
   });

   it("carries a grouped or fixed dimension into nests, and checks each nest", () => {
      const leg = legOf(fanned);
      expect(
         checkJoinCoverage(
            leg,
            level([kind, { name: "n", isNest: true, nest: level([miles]) }]),
            none,
         ),
      ).toBeUndefined();
      expect(
         checkJoinCoverage(
            leg,
            level([{ name: "n", isNest: true, nest: level([miles]) }]),
            none,
         )?.code,
      ).toBe("rollup_grain_too_coarse");
      expect(
         checkJoinCoverage(leg, level([{ name: "n", isNest: true }]), none)
            ?.code,
      ).toBe("rollup_count_semantics");
   });

   it("declines an ungrouped aggregate and holds a having: to every requirement", () => {
      const leg = legOf(fanned);
      expect(
         checkJoinCoverage(
            leg,
            level([kind, { name: "t", ungrouped: true }]),
            none,
         )?.code,
      ).toBe("rollup_grain_too_coarse");
      expect(
         checkJoinCoverage(
            leg,
            level([], { havings: [{ computesAggregate: false }] }),
            none,
         )?.code,
      ).toBe("rollup_grain_too_coarse");
   });

   it("serves a join-level sum with its other grain dimensions fixed, the parent's key fixing the join's (A2)", () => {
      const p = plan({ grainDimensions: ["flight_id", "kind"] });
      p.rollupMeasures[0]!.fanOutDimensions = ["flight_id"];
      const leg = legOf(p);
      expect(checkJoinCoverage(leg, level([miles]), none)?.code).toBe(
         "rollup_grain_too_coarse",
      );
      expect(
         checkJoinCoverage(
            leg,
            level([
               { name: "flight_id", ref: ["segments", "flight_id"] },
               miles,
            ]),
            none,
         ),
      ).toBeUndefined();
      expect(
         checkJoinCoverage(
            leg,
            level([{ name: "id", ref: ["id"] }, miles]),
            none,
         ),
      ).toBeUndefined();
      expect(
         checkJoinCoverage(
            leg,
            level([miles], { fixedPathsByWhere: [["id"]] }),
            none,
         ),
      ).toBeUndefined();
      // A measure with no fan-out of its own, beside it, asks nothing.
      expect(
         checkJoinCoverage(
            leg,
            level([{ name: "seg_count", ref: ["segments", "seg_count"] }]),
            none,
         ),
      ).toBeUndefined();
   });

   it("asks nothing of a rollup with no fan-out", () => {
      expect(
         checkJoinCoverage(legOf(plan()), level([miles]), () => "aggregate"),
      ).toBeUndefined();
   });
});

describe("joinedKeyProblem", () => {
   const legOf = (p: RollupVariantPlan = plan()) =>
      joinRollupLegs([join()], new Map([[CHILD, [p]]]), new Set(), "id")[0]!
         .leg;

   it("declines a count or sum of the joined source's own rows unless its key was checked unique and never NULL", () => {
      for (const m of ["seg_miles", "seg_count"]) {
         const read = new Set([m]);
         expect(joinedKeyProblem(legOf(), read, "seg_id", false)).toContain(
            "more than once or NULL",
         );
         expect(joinedKeyProblem(legOf(), read, "seg_id", undefined)).toContain(
            "was not checked unique and never NULL",
         );
         expect(
            joinedKeyProblem(legOf(), read, "seg_id", true),
         ).toBeUndefined();
      }
   });

   it("asks nothing of a join keyed on the joined source's primary key, or of one with none", () => {
      const read = new Set(["seg_miles"]);
      expect(
         joinedKeyProblem(legOf(), read, "flight_id", undefined),
      ).toBeUndefined();
      expect(
         joinedKeyProblem(legOf(), read, undefined, undefined),
      ).toBeUndefined();
   });

   it("asks nothing of a min, a max, or a partial taken at a join of the joined source", () => {
      const p = plan();
      p.rollupMeasures[0]!.partials = [
         leaf("max", method("miles", "max")),
         leaf("min", method("miles", "min")),
         {
            column: "q__partial",
            leaf: {
               aggregate: "sum",
               expr: method("qty", "sum"),
               reads: [],
               joinPath: ["lines"],
            },
         },
      ] as RollupVariantPlan["rollupMeasures"][number]["partials"];
      expect(
         joinedKeyProblem(legOf(p), new Set(["seg_miles"]), "seg_id", false),
      ).toBeUndefined();
   });
});
