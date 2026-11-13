// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * The decoders in `prepared_query.ts` replaced ad hoc ones whose semantics
 * differ on purpose; each difference is pinned here so a "simplification"
 * that merges two of them fails by name.
 */
import { describe, expect, it } from "bun:test";
import type { ModelDef } from "@malloydata/malloy";
import {
   aggregateWrittenThroughJoin,
   joinGrainAggregatePaths,
   namedViewsRead,
   preparedQueryView,
   probeFilterList,
   queryFieldUsagePaths,
   resolveStructRef,
   resolveStructRefSource,
   sqlTargetRef,
   structRefName,
} from "./prepared_query";

const source = { type: "table", name: "s", fields: [] };
const notASource = { type: "query", name: "q" };
const modelDef = {
   contents: { s: source, q: notASource },
} as unknown as ModelDef;

describe("preparedQueryView", () => {
   it("reads _query and _modelDef without falling back", () => {
      const view = preparedQueryView({
         _query: {
            structRef: "s",
            compositeResolvedSourceDef: source,
            location: { url: "internal://x" },
            pipeline: [],
         },
      });
      expect(view.structRef).toBe("s");
      expect(view.compositeResolvedSourceDef).toBe(source);
      expect(view.location).toEqual({ url: "internal://x" });
      expect(view.pipeline).toEqual([]);
      expect(view.modelDef).toBeUndefined();
   });

   it("tolerates a prepared query with no _query", () => {
      const view = preparedQueryView({});
      expect(view.query).toBeUndefined();
      expect(view.structRef).toBeUndefined();
      expect(preparedQueryView(undefined).query).toBeUndefined();
   });
});

describe("structRef decoders", () => {
   it("resolveStructRef takes any contents entry for a name", () => {
      expect(resolveStructRef("s", modelDef)).toBe(source as never);
      expect(resolveStructRef("q", modelDef)).toBe(notASource as never);
      expect(resolveStructRef("missing", modelDef)).toBeUndefined();
      expect(resolveStructRef(source, undefined)).toBe(source as never);
      expect(resolveStructRef(7, modelDef)).toBeUndefined();
   });

   it("resolveStructRefSource requires a source for a name, not for an object", () => {
      expect(resolveStructRefSource("s", modelDef)).toBe(source as never);
      expect(resolveStructRefSource("q", modelDef)).toBeUndefined();
      expect(resolveStructRefSource(notASource, modelDef)).toBe(
         notASource as never,
      );
   });

   it("sqlTargetRef prefers the resolved composite member, never a union", () => {
      const member = { type: "table", name: "m" };
      expect(
         sqlTargetRef(
            preparedQueryView({
               _query: { structRef: "s", compositeResolvedSourceDef: member },
            }),
         ),
      ).toBe(member);
      expect(
         sqlTargetRef(preparedQueryView({ _query: { structRef: "s" } })),
      ).toBe("s");
   });

   it("structRefName is `as || name`: an empty alias falls through", () => {
      expect(structRefName("s")).toBe("s");
      expect(structRefName({ as: "a", name: "n" })).toBe("a");
      expect(structRefName({ as: "", name: "n" })).toBe("n");
      expect(structRefName(undefined)).toBeUndefined();
   });

   it("probeFilterList reads an inline ref only", () => {
      const filterList = [{ code: "x" }];
      expect(
         probeFilterList(
            preparedQueryView({ _query: { structRef: { filterList } } }),
         ),
      ).toBe(filterList);
      expect(
         probeFilterList(preparedQueryView({ _query: { structRef: "s" } })),
      ).toBeUndefined();
   });
});

describe("joinGrainAggregatePaths", () => {
   const view = (pipeline: unknown) =>
      preparedQueryView({ _query: { structRef: "s", pipeline } });
   it("reads every uniqueKeyRequirement path, nested segments included", () => {
      const out = joinGrainAggregatePaths(
         view([
            {
               expandedFieldUsage: [
                  { path: ["carriers", "x"] },
                  {
                     path: ["carriers"],
                     uniqueKeyRequirement: { isCount: false },
                  },
                  { path: [], uniqueKeyRequirement: { isCount: true } },
               ],
               queryFields: [
                  {
                     pipeline: [
                        {
                           expandedFieldUsage: [
                              { path: ["f", "c"], uniqueKeyRequirement: {} },
                           ],
                        },
                     ],
                  },
               ],
               // Not the query's reads.
               outputStruct: {
                  expandedFieldUsage: [
                     { path: ["o"], uniqueKeyRequirement: {} },
                  ],
               },
            },
         ]),
      );
      expect(out).toEqual({
         paths: [["carriers"], [], ["f", "c"]],
         unreadable: false,
      });
   });
   it("fails closed on a requirement it cannot read", () => {
      expect(
         joinGrainAggregatePaths(
            view([
               {
                  expandedFieldUsage: [{ path: "x", uniqueKeyRequirement: {} }],
               },
            ]),
         ).unreadable,
      ).toBe(true);
   });
});

describe("namedViewsRead", () => {
   const stage = (...queryFields: unknown[]) => [
      { type: "reduce", queryFields },
   ];
   const struct = {
      fields: [
         { type: "string", name: "c" },
         {
            type: "turtle",
            name: "leaf",
            pipeline: stage({ type: "fieldref", path: ["c"] }),
         },
         {
            type: "turtle",
            name: "outer",
            pipeline: stage({ type: "turtle", name: "leaf", pipeline: [] }),
         },
         {
            type: "turtle",
            name: "loop",
            pipeline: stage({ type: "fieldref", path: ["loop"] }),
         },
      ],
   };
   const view = (...queryFields: unknown[]) =>
      preparedQueryView({ _query: { pipeline: stage(...queryFields) } });

   it("reads the called view, then each nested one, each after what it nests", () => {
      expect(namedViewsRead(view(), struct, "outer")).toEqual([
         "leaf",
         "outer",
      ]);
   });

   it("reads a nest by name, by reference or expanded, and not a field or an unknown name", () => {
      expect(
         namedViewsRead(
            view(
               { type: "fieldref", path: ["c"] },
               { type: "fieldref", path: ["leaf"] },
               {
                  type: "turtle",
                  name: "mine",
                  pipeline: stage({ type: "fieldref", path: ["outer"] }),
               },
            ),
            struct,
         ),
      ).toEqual(["leaf", "outer"]);
   });

   it("reads a view that nests itself once", () => {
      expect(namedViewsRead(view(), struct, "loop")).toEqual(["loop"]);
   });
});

describe("join-to-rollup readers (A7)", () => {
   const view = (stage: unknown) =>
      preparedQueryView({ _query: { pipeline: [stage] } });
   const field = (...path: string[]) => ({ node: "field", path });
   const parent = {
      fields: [
         {
            name: "tot",
            expressionType: "aggregate",
            e: field("segments", "seg_miles"),
         },
         {
            name: "bad",
            expressionType: "aggregate",
            e: {
               node: "aggregate",
               function: "count",
               e: {},
               structPath: ["segments"],
            },
         },
         {
            name: "worse",
            expressionType: "aggregate",
            e: {
               node: "*",
               kids: { left: field("bad"), right: { node: "numberLiteral" } },
            },
         },
         {
            name: "fc",
            expressionType: "aggregate",
            e: { node: "aggregate", function: "count", e: {} },
         },
      ],
   };

   it("queryFieldUsagePaths reads every expanded path, nests included, and fails closed", () => {
      const v = view({
         expandedFieldUsage: [
            { path: ["segments", "seg_miles"] },
            { path: ["id"] },
         ],
         queryFields: [
            {
               type: "turtle",
               pipeline: [{ expandedFieldUsage: [{ path: ["carrier"] }] }],
            },
         ],
      });
      expect(queryFieldUsagePaths(v)).toEqual([
         ["segments", "seg_miles"],
         ["id"],
         ["carrier"],
      ]);
      expect(
         queryFieldUsagePaths(view({ expandedFieldUsage: [{ path: [1] }] })),
      ).toBeUndefined();
   });

   it("aggregateWrittenThroughJoin finds an aggregate the query writes over the join", () => {
      const at = view({
         queryFields: [
            {
               name: "c",
               e: {
                  node: "aggregate",
                  function: "count",
                  e: {},
                  structPath: ["segments"],
               },
            },
         ],
         expandedFieldUsage: [],
      });
      expect(aggregateWrittenThroughJoin(at, parent, "segments")).toBe("c");
      const over = view({
         queryFields: [
            {
               name: "d",
               e: {
                  node: "aggregate",
                  function: "distinct",
                  e: field("segments", "flight_id"),
               },
            },
         ],
         expandedFieldUsage: [],
      });
      expect(aggregateWrittenThroughJoin(over, parent, "segments")).toBe("d");
      // A count over the source's own rows is not through the join.
      const own = view({
         queryFields: [
            { name: "n", e: { node: "aggregate", function: "count", e: {} } },
         ],
         expandedFieldUsage: [],
      });
      expect(
         aggregateWrittenThroughJoin(own, parent, "segments"),
      ).toBeUndefined();
   });

   it("aggregateWrittenThroughJoin follows the source's measures the query reads", () => {
      const reads = (name: string) =>
         view({
            queryFields: [{ type: "fieldref", path: [name] }],
            expandedFieldUsage: [{ path: [name] }],
         });
      expect(
         aggregateWrittenThroughJoin(reads("tot"), parent, "segments"),
      ).toBeUndefined();
      expect(
         aggregateWrittenThroughJoin(reads("fc"), parent, "segments"),
      ).toBeUndefined();
      expect(
         aggregateWrittenThroughJoin(reads("bad"), parent, "segments"),
      ).toBe("bad");
      expect(
         aggregateWrittenThroughJoin(reads("worse"), parent, "segments"),
      ).toBe("worse");
      // A joined measure read by name is the joined source's own.
      const named = view({
         queryFields: [{ type: "fieldref", path: ["segments", "seg_miles"] }],
         expandedFieldUsage: [
            { path: ["segments", "seg_miles"] },
            { path: ["segments"], uniqueKeyRequirement: { isCount: false } },
         ],
      });
      expect(
         aggregateWrittenThroughJoin(named, parent, "segments"),
      ).toBeUndefined();
   });
});
