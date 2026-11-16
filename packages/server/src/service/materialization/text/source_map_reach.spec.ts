// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

// Which stored tables a source reaches, and whether it reaches the warehouse.

import { describe, expect, it } from "bun:test";

import { reachPersisted } from "./source_map";

import { authorModelLiftContext } from "./author_model";

// A compiled model's `contents`, reduced to what the walk and the lift read.
// sourceIDs are the names with an `@` suffix so a wrong lookup is visible in a
// failure. Which names are persist sources is the PLAN's to say (the
// `isPersisted` argument), never a definition's: `#@ persist` is inherited, a
// plain extension or rename of a persisted source is a persist source sharing
// the base's table, and the compiler copies the base's note onto an extension,
// so no definition can tell an inheritor from the source that wrote the
// annotation — and the design does not ask it to.
const modelId = (name: string) => `${name}@m`;
const tableDef = (name: string) => ({ sourceID: modelId(name), type: "table" });
const persistedDef = (name: string, base: string) => ({
   sourceID: modelId(name),
   type: "query_source",
   persistent: true,
   query: { structRef: modelId(base) },
});
const queryDef = (name: string, base: string, joins: string[] = []) => ({
   sourceID: modelId(name),
   type: "query_source",
   query: {
      structRef: modelId(base),
      pipeline: joins.map((j) => ({ join: "one", sourceID: modelId(j) })),
   },
});
const joinField = (j: string) => ({
   name: j,
   join: "one",
   sourceID: modelId(j),
});
/**
 * An `extend`. Of a persisted base it inherits `persistent: true` and the
 * base's table; `joins` are the ones it adds, `inherited` the base's it carries
 * again on its own field list, as the compiler lays them out.
 */
const extendDef = (
   name: string,
   base: string,
   opts: { joins?: string[]; inherited?: string[]; persistent?: boolean } = {},
) => ({
   sourceID: modelId(name),
   type: "query_source",
   extends: modelId(base),
   ...(opts.persistent ? { persistent: true } : {}),
   fields: [...(opts.inherited ?? []), ...(opts.joins ?? [])].map(joinField),
});
function modelCtx(contents: Record<string, unknown>) {
   const sourceNameById = new Map<string, string>();
   for (const [name, def] of Object.entries(contents)) {
      sourceNameById.set((def as { sourceID: string }).sourceID, name);
   }
   return {
      contents: contents as Parameters<typeof reachPersisted>[0]["contents"],
      sourceNameById,
   };
}
// The walk's predicate sees a definition's `sourceID`; these fixtures use
// `modelId(name)`, so a name list is the set of ids the plan would hold.
const persistedIn =
   (...names: string[]) =>
   (_name: string, sourceID: string) =>
      names.map(modelId).includes(sourceID);
/** The walk's report of a stop: the model's name for it and its id. */
const stop = (name: string) => ({ name, sourceID: modelId(name) });

describe("reachPersisted through an import", () => {
   it("stops at a hidden-dependency persist source the registry carries", () => {
      const ctx = authorModelLiftContext(
         {
            contents: {
               weekly: {
                  sourceID: "weekly@file:///p/orders.malloy",
                  type: "query_source",
                  persistent: true,
                  query: { structRef: "daily@file:///p/orders.malloy" },
               },
            },
            sourceRegistry: {
               "daily@file:///p/orders.malloy": {
                  entry: {
                     type: "query_source",
                     as: "daily",
                     persistent: true,
                     sourceID: "daily@file:///p/orders.malloy",
                     query: { structRef: "t@file:///p/orders.malloy" },
                  },
               },
               "t@file:///p/orders.malloy": {
                  entry: {
                     type: "sql_select",
                     as: "t",
                     sourceID: "t@file:///p/orders.malloy",
                  },
               },
            },
         },
         () => undefined,
      );
      const out = reachPersisted(
         ctx,
         "weekly",
         (_n, id) => id.startsWith("daily@") || id.startsWith("weekly@"),
      );
      expect(out.persisted).toEqual([
         { name: "daily", sourceID: "daily@file:///p/orders.malloy" },
      ]);
      expect(out.raw).toBe(false);
   });

   it("an id with no definition that the plan knows as a persist source is a stop, and a raw leaf never carries the model URL", () => {
      const c = modelCtx({
         monthly: {
            sourceID: modelId("monthly"),
            type: "query_source",
            persistent: true,
            query: { structRef: "weekly@file:///srv/models/orders.malloy" },
         },
      });
      const stop = reachPersisted(c, "monthly", (_n, id) =>
         id.startsWith("weekly@"),
      );
      expect(stop).toMatchObject({
         persisted: [
            {
               name: "weekly",
               sourceID: "weekly@file:///srv/models/orders.malloy",
            },
         ],
         raw: false,
      });
      const raw = reachPersisted(c, "monthly", () => false);
      expect(raw.raw).toBe(true);
      expect(raw.rawLeaves).toEqual(["weekly"]);
      expect(JSON.stringify(raw)).not.toContain("file:///");
   });
});

describe("reachPersisted", () => {
   it("stops at the first persist source on each path, through any number of intermediates", () => {
      const c = modelCtx({
         orders: tableDef("orders"),
         daily: persistedDef("daily", "orders"),
         daily_wide: queryDef("daily_wide", "daily"),
         daily_wider: extendDef("daily_wider", "daily_wide"),
         monthly: persistedDef("monthly", "daily_wider"),
      });
      expect(
         reachPersisted(c, "monthly", persistedIn("daily", "monthly")),
      ).toEqual({
         persisted: [stop("daily")],
         raw: false,
         rawLeaves: [],
         rawVia: [],
         visited: ["daily_wider", "daily_wide", "daily"],
      });
   });

   it("stops at an extension of a persisted source under its own name, leaving the table to the caller", () => {
      // `daily_regional is daily extend { … }` is a persist source in the plan
      // (it inherits the annotation) and shares `daily`'s table. The walk
      // records the name it reached; which table that is, and whether it is
      // present, is answered from the plan's address groups.
      const c = modelCtx({
         orders: tableDef("orders"),
         daily: persistedDef("daily", "orders"),
         daily_regional: extendDef("daily_regional", "daily", {
            persistent: true,
         }),
         monthly: persistedDef("monthly", "daily_regional"),
      });
      expect(
         reachPersisted(
            c,
            "monthly",
            persistedIn("daily", "daily_regional", "monthly"),
         ),
      ).toMatchObject({ persisted: [stop("daily_regional")], raw: false });
   });

   it("follows the joins an extension adds, which its table does not hold, and reports raw by them", () => {
      const c = modelCtx({
         orders: tableDef("orders"),
         regions: tableDef("regions"),
         daily: persistedDef("daily", "orders"),
         daily_regional: extendDef("daily_regional", "daily", {
            joins: ["regions"],
            persistent: true,
         }),
         monthly: persistedDef("monthly", "daily_regional"),
      });
      expect(
         reachPersisted(
            c,
            "monthly",
            persistedIn("daily", "daily_regional", "monthly"),
         ),
      ).toEqual({
         persisted: [stop("daily_regional")],
         raw: true,
         rawLeaves: ["regions"],
         rawVia: ["daily_regional"],
         visited: ["daily_regional", "regions"],
      });
   });

   it("does not follow a join the extension inherits, which its base's table already answers for", () => {
      // The base's join appears on the extension's field list too; it is the
      // base's to account for, and the base is a stored table.
      const c = modelCtx({
         orders: tableDef("orders"),
         regions: tableDef("regions"),
         daily: {
            ...persistedDef("daily", "orders"),
            fields: [joinField("regions")],
         },
         ext: extendDef("ext", "daily", {
            inherited: ["regions"],
            persistent: true,
         }),
         monthly: persistedDef("monthly", "ext"),
      });
      expect(
         reachPersisted(c, "monthly", persistedIn("daily", "ext", "monthly")),
      ).toMatchObject({ persisted: [stop("ext")], raw: false });
   });

   it("collects every persist source an intermediate joins", () => {
      const c = modelCtx({
         orders: tableDef("orders"),
         daily: persistedDef("daily", "orders"),
         sites: persistedDef("sites", "orders"),
         joined: extendDef("joined", "daily", { joins: ["sites"] }),
         monthly: persistedDef("monthly", "joined"),
      });
      expect(
         reachPersisted(c, "monthly", persistedIn("daily", "sites", "monthly")),
      ).toMatchObject({
         persisted: [stop("daily"), stop("sites")],
         raw: false,
      });
   });

   it("walks into a reference carried as the embedded definition", () => {
      // `monthly is (daily extend { … }) -> { … }`: the inline extension has no
      // name of its own, but its definition is embedded whole and reads `daily`.
      const inline = extendDef("daily_inline", "daily", { persistent: true });
      const c = modelCtx({
         orders: tableDef("orders"),
         daily: persistedDef("daily", "orders"),
         monthly: {
            ...persistedDef("monthly", "daily"),
            query: { structRef: { ...inline, sourceID: "anon" } },
         },
      });
      expect(
         reachPersisted(c, "monthly", persistedIn("daily", "monthly")),
      ).toMatchObject({ persisted: [stop("daily")], raw: false });
   });

   it("does not stop at the root, which is persisted by definition", () => {
      const c = modelCtx({
         orders: tableDef("orders"),
         daily: persistedDef("daily", "orders"),
      });
      expect(reachPersisted(c, "daily", persistedIn("daily"))).toMatchObject({
         persisted: [],
         raw: true,
         rawLeaves: ["orders"],
      });
   });

   it("treats a reference with no identity at all as raw", () => {
      const c = modelCtx({
         daily: persistedDef("daily", "orders"),
         monthly: {
            ...persistedDef("monthly", "daily"),
            query: { structRef: 42 },
         },
      });
      expect(
         reachPersisted(c, "monthly", persistedIn("daily", "monthly")),
      ).toMatchObject({ persisted: [], raw: true });
   });

   it("follows the sources a query pipeline joins, not only its base", () => {
      const c = modelCtx({
         orders: tableDef("orders"),
         regions: tableDef("regions"),
         daily: persistedDef("daily", "orders"),
         wide: queryDef("wide", "daily", ["regions"]),
         monthly: persistedDef("monthly", "wide"),
      });
      expect(
         reachPersisted(c, "monthly", persistedIn("daily", "monthly")),
      ).toMatchObject({
         persisted: [stop("daily")],
         raw: true,
         rawLeaves: ["regions"],
         rawVia: ["wide"],
      });
   });

   it("treats a pipeline nested too deeply to walk as reaching raw", () => {
      let nested: unknown = { join: "one", sourceID: modelId("daily") };
      for (let i = 0; i < 250; i++) nested = { inner: nested };
      const c = modelCtx({
         orders: tableDef("orders"),
         daily: persistedDef("daily", "orders"),
         wide: {
            ...queryDef("wide", "daily"),
            query: { structRef: modelId("daily"), pipeline: [nested] },
         },
         monthly: persistedDef("monthly", "wide"),
      });
      expect(
         reachPersisted(c, "monthly", persistedIn("daily", "monthly")),
      ).toMatchObject({
         raw: true,
         rawLeaves: ["(unnamed)"],
         rawVia: ["wide"],
      });
   });
});
