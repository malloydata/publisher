// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { describe, expect, it } from "bun:test";
import type { EnvironmentStore } from "../../service/environment_store";
import { oneLineSummary, resolveLevel } from "./get_context_levels";
import { registerGetContextTool } from "./get_context_tool";

type Pkg = { name: string; description?: string };
type EnvSpec = {
   packages: Pkg[];
   failed?: Record<string, string>;
   stale?: Record<string, string>;
};

/** One package with a source, two fields, and a join, enough to list. */
const servedPackage = {
   listSkills: () => [],
   listModels: async () => [{ path: "shop.malloy" }],
   getModel: () => ({
      getSourceInfos: () => [
         {
            name: "orders",
            annotations: ["#(doc) One row per order."],
            schema: {
               fields: [
                  {
                     kind: "dimension",
                     name: "status",
                     annotations: ["#(doc) Order status."],
                  },
                  { kind: "measure", name: "order_count", annotations: [] },
               ],
            },
         },
      ],
      getQueries: () => [],
   }),
};

function storeOf(envs: Record<string, EnvSpec>): EnvironmentStore {
   return {
      listEnvironments: async () =>
         Object.keys(envs).map((name) => ({ name })) as never,
      getEnvironment: async (name: string) => {
         const spec = envs[name];
         return {
            listPackages: async () => spec.packages,
            getFailedPackages: () => new Map(Object.entries(spec.failed ?? {})),
            getStaleCompileErrors: () =>
               new Map(
                  Object.entries(spec.stale ?? {}).map(([k, message]) => [
                     k,
                     { message, failedAt: "2026-11-04T00:00:00Z" },
                  ]),
               ),
            getPackage: async () => servedPackage,
         } as never;
      },
   } as unknown as EnvironmentStore;
}

type Handler = (p: Record<string, unknown>) => Promise<{
   isError?: boolean;
   content: Array<{ resource?: { text: string } }>;
}>;

function handlerFor(store: EnvironmentStore): Handler {
   let handler: Handler | undefined;
   registerGetContextTool(
      {
         tool: (name: string, _d: string, _s: unknown, h: Handler) => {
            if (name === "get_context") handler = h;
         },
      } as never,
      store,
   );
   return handler!;
}

async function call(store: EnvironmentStore, params: Record<string, unknown>) {
   const out = await handlerFor(store)(params);
   return {
      isError: out.isError === true,
      payload: JSON.parse(out.content[0].resource!.text),
   };
}

const TWO_ENVS = storeOf({
   prod: {
      packages: [
         { name: "sales", description: "Orders and revenue.\nMore detail." },
         { name: "finance" },
      ],
      failed: { broken: "Compile failed: unexpected token" },
   },
   dev: { packages: [{ name: "scratch" }] },
   empty: { packages: [] },
});

describe("get_context with no package named", () => {
   it("lists the environments that hold packages, with how many each serves", async () => {
      const { isError, payload } = await call(TWO_ENVS, {});
      expect(isError).toBe(false);
      expect(payload).toMatchObject({
         sources: [],
         listing: "environments",
         environments: [
            { environment: "prod", packages: 2 },
            { environment: "dev", packages: 1 },
         ],
         total_available: 2,
         returned: 2,
      });
      // A package that failed to load is not counted, and is named instead.
      expect(payload.warnings).toEqual([
         "A package failed to load and is not listed: prod/broken. list_packages and get_status say why.",
      ]);
   });

   it("lists one environment's packages, by name, with each description's first line", async () => {
      const { payload } = await call(TWO_ENVS, { environment: "prod" });
      expect(payload).toMatchObject({
         sources: [],
         listing: "packages",
         packages: [
            { environment: "prod", package: "finance" },
            {
               environment: "prod",
               package: "sales",
               one_line_summary: "Orders and revenue.",
            },
         ],
      });
      expect(payload.packages[0].one_line_summary).toBeUndefined();
   });

   it("lists the packages directly when only one environment holds any", async () => {
      const store = storeOf({
         prod: { packages: [{ name: "a" }, { name: "b" }] },
         empty: { packages: [] },
      });
      const { payload } = await call(store, {});
      expect(payload.listing).toBe("packages");
      expect(
         payload.packages.map((p: { package: string }) => p.package),
      ).toEqual(["a", "b"]);
   });

   it("enters the only package served, and lists its sources with package_info", async () => {
      const store = storeOf({
         prod: { packages: [{ name: "sales", description: "Orders." }] },
      });
      const { isError, payload } = await call(store, {});
      expect(isError).toBe(false);
      expect(payload.listing).toBeUndefined();
      expect(
         payload.sources.map(
            (s: { source_info: { resource_id: { source: string } } }) =>
               s.source_info.resource_id.source,
         ),
      ).toEqual(["orders"]);
      expect(payload.package_info).toEqual({
         environment: "prod",
         package: "sales",
         description: "Orders.",
      });
      expect(payload.warnings[0]).toBe(
         "This server serves one package, so this call entered prod/sales, and lists its sources. Name it in scopes from now on.",
      );
   });

   it("enters an environment's lone package", async () => {
      const outcome = await resolveLevel({ environment: "dev" }, TWO_ENVS);
      expect(outcome.kind).toBe("scoped");
      if (outcome.kind !== "scoped") return;
      expect(outcome.params.scopes).toEqual([
         { environment: "dev", package: "scratch" },
      ]);
      expect(outcome.params.search_targets).toEqual([
         { target_type: "source" },
      ]);
   });

   it("searches the only package when a call sends targets and no scope", async () => {
      const store = storeOf({ prod: { packages: [{ name: "sales" }] } });
      const targets = [{ target_type: "measure", search_text: "revenue" }];
      const outcome = await resolveLevel(
         { search_targets: targets as never },
         store,
      );
      expect(outcome.kind).toBe("scoped");
      if (outcome.kind !== "scoped") return;
      expect(outcome.params.search_targets).toEqual(targets as never);
      expect(outcome.packageInfo).toBeUndefined();
      expect(outcome.warnings[0]).toContain(
         "entered prod/sales and searched it",
      );
   });

   it("refuses a search across several packages, naming them", async () => {
      const targets = [{ target_type: "measure", search_text: "revenue" }];
      const one = await call(TWO_ENVS, {
         environment: "prod",
         search_targets: targets,
      });
      expect(one.isError).toBe(true);
      expect(one.payload.error).toContain(
         'A search needs one package: Publisher indexes each package on its own. Fix: add scopes: [{"environment": "prod", "package": "<one of: sales, finance>"}].',
      );
      const all = await call(TWO_ENVS, { search_targets: targets });
      expect(all.isError).toBe(true);
      expect(all.payload.error).toContain(
         "one of prod/sales, prod/finance, dev/scratch.",
      );
   });

   it("refuses an environment that is not served, naming those that are", async () => {
      const { isError, payload } = await call(TWO_ENVS, {
         environment: "staging",
      });
      expect(isError).toBe(true);
      expect(payload.error).toContain(
         'Invalid environment: "staging" is not served here. Fix: use one of prod, dev, empty, or omit environment to list them.',
      );
   });

   it("refuses an offset on a listing of environments or packages", async () => {
      expect((await call(TWO_ENVS, { offset: 5 })).payload.error).toContain(
         "Invalid offset: a listing of environments is not paged",
      );
   });

   it("keeps offset when it enters the lone package, so its pages can be read", async () => {
      // One package with more sources than a page.
      const store = storeOf({ prod: { packages: [{ name: "sales" }] } });
      const page = async (offset?: number) =>
         (
            await call(store, {
               search_targets: [{ target_type: "source" }],
               limit: 1,
               ...(offset !== undefined ? { offset } : {}),
            })
         ).payload;
      const first = await page();
      expect(first.returned).toBe(1);
      expect(first.warnings[0]).toBe(
         "This server serves one package, so this call entered prod/sales, and lists what the targets name in it. Name it in scopes from now on.",
      );
      // The response's next_offset, sent back, reads past the first page.
      const second = await page(1);
      expect(second.returned).toBe(0);
      expect(second.next_offset).toBeUndefined();
   });

   it("names failed packages and unreadable environments when it enters the lone package", async () => {
      const store = storeOf({
         prod: {
            packages: [{ name: "sales" }],
            failed: { broken: "Compile failed" },
         },
      });
      const outcome = await resolveLevel({}, store);
      expect(outcome.kind === "scoped" && outcome.warnings).toEqual([
         "This server serves one package, so this call entered prod/sales, and lists its sources. Name it in scopes from now on.",
         "A package failed to load and is not listed: prod/broken. list_packages and get_status say why.",
      ]);
   });

   it("does not repeat the stale note for the package it entered", async () => {
      const store = storeOf({
         prod: { packages: [{ name: "sales" }], stale: { sales: "boom" } },
         dev: { packages: [], stale: {} },
      });
      const outcome = await resolveLevel({}, store);
      expect(outcome.kind === "scoped" && outcome.warnings).toEqual([
         "This server serves one package, so this call entered prod/sales, and lists its sources. Name it in scopes from now on.",
      ]);
   });

   it("names other environments' failures when one environment holds every package", async () => {
      const store = storeOf({
         prod: { packages: [{ name: "a" }, { name: "b" }] },
         other: { packages: [], failed: { gone: "Compile failed" } },
      });
      const { payload } = await call(store, {});
      expect(payload.listing).toBe("packages");
      expect(payload.warnings).toEqual([
         "A package failed to load and is not listed: other/gone. list_packages and get_status say why.",
      ]);
   });

   it("refuses a search naming an environment with no package that loaded, rather than listing it", async () => {
      const { isError, payload } = await call(TWO_ENVS, {
         environment: "empty",
         search_targets: [{ target_type: "measure", search_text: "revenue" }],
      });
      expect(isError).toBe(true);
      expect(payload.error).toContain(
         'A search needs one package, and environment "empty" has none to search: it serves no package that loaded.',
      );
   });
});

describe("get_context naming a package with no targets", () => {
   it("lists the package's sources, with package_info", async () => {
      const { payload } = await call(TWO_ENVS, {
         scopes: [{ environment: "prod", package: "sales" }],
      });
      expect(payload.package_info).toEqual({
         environment: "prod",
         package: "sales",
         description: "Orders and revenue.\nMore detail.",
      });
      expect(payload.sources).toHaveLength(1);
   });

   it("lists a source's fields, without package_info", async () => {
      const { payload } = await call(TWO_ENVS, {
         scopes: [{ environment: "prod", package: "sales", source: "orders" }],
      });
      expect(payload.package_info).toBeUndefined();
      expect(
         payload.sources[0].entities.map((e: { name: string }) => e.name),
      ).toEqual(["status", "order_count"]);
   });

   it("cuts a description too long to send beside the sources to its first line", async () => {
      const long = `Line one.\n${"x".repeat(20_000)}`;
      const store = storeOf({
         prod: {
            packages: [{ name: "sales", description: long }, { name: "b" }],
         },
      });
      const outcome = await resolveLevel(
         { scopes: [{ environment: "prod", package: "sales" }] },
         store,
      );
      expect(outcome.kind === "scoped" && outcome.packageInfo).toEqual({
         environment: "prod",
         package: "sales",
         one_line_summary: "Line one.",
      });
      expect(outcome.kind === "scoped" && outcome.warnings[0]).toContain(
         "too long to send beside its sources",
      );
   });

   it("refuses an environment that contradicts the scope", async () => {
      const { isError, payload } = await call(TWO_ENVS, {
         environment: "dev",
         scopes: [{ environment: "prod", package: "sales" }],
      });
      expect(isError).toBe(true);
      expect(payload.error).toContain(
         'Invalid environment: "dev" contradicts scopes[0].environment "prod".',
      );
   });
});

describe("oneLineSummary", () => {
   it("takes the first non-empty line, and cuts a long one at a word", () => {
      expect(oneLineSummary("\n  First line.  \nSecond.")).toBe("First line.");
      expect(oneLineSummary(undefined)).toBeUndefined();
      const long = `${"word ".repeat(30)}end`;
      const cut = oneLineSummary(long)!;
      expect(cut.endsWith("word…")).toBe(true);
      expect(cut.length).toBeLessThanOrEqual(121);
   });
});
