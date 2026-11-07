// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * Every route that runs a query writes exactly one access record for it, from
 * the first step of the request to the last: success, a refusal before the
 * model, a refusal inside it, a client abort and a timeout. Read off the
 * logger line `emitAccessRecord` writes, since `withAccessRecord` is its only
 * caller.
 */
import { afterEach, beforeEach, describe, expect, it, spyOn } from "bun:test";
import sinon from "sinon";
import request from "supertest";
import type { Connection } from "@malloydata/malloy";
import { ConnectionController } from "../controller/connection.controller";
import { LegacyReportController } from "../controller/legacy_report.controller";
import { QueryController } from "../controller/query.controller";
import { AccessDeniedError, PayloadTooLargeError } from "../errors";
import { logger } from "../logger";
import { registerExecuteQueryTool } from "../mcp/tools/execute_query_tool";
import { defaultIndexValueFetcher } from "../retrieval/index/values_index";
import { queryDistinctValues } from "../retrieval/search/values";
import { tryAcquireQuerySlot } from "../query_concurrency";
import { withAuthorizeModel } from "../test_helpers/authorize_fixture";
import { buildRouteApp } from "../test_helpers/route_app";
import { withSettings } from "../test_helpers/settings_env";
import { accessIdentityOf, type AccessRecord } from "./access_record";
import type { EnvironmentStore } from "./environment_store";

const MODEL = `##! experimental.givens
#(secure)
given: ORGS :: string[]
#(secure)
given: ROLES :: string[]

#(access_filter) org_id in $ORGS
source: accounts_src is duckdb.table('accounts') extend {
   #(classification) "phi.quasi_identifier"
   dimension: area is region
   measure: n is count()
}

#(authorize) 'clinician' in $ROLES
source: chart is duckdb.table('child') extend {
   measure: n is count()
}

source: open_src is duckdb.table('child') extend {
   dimension: label is name
   measure: n is count()
}
`;

const GATED = "run: accounts_src -> { group_by: area; aggregate: n }";

let info: ReturnType<typeof spyOn>;
beforeEach(() => {
   info = spyOn(logger, "info").mockImplementation(() => logger);
});
afterEach(() => {
   info.mockRestore();
   sinon.restore();
});

function records(): AccessRecord[] {
   return (info.mock.calls as unknown[][])
      .filter(([message]) => message === "Access record")
      .map(([, record]) => record as AccessRecord);
}

function storeOf(model: unknown, version?: string): EnvironmentStore {
   const pkg = {
      getModel: (path: string) => (path === "m.malloy" ? model : undefined),
      getDeclaredQueryMetadata: () => null,
      getVersionId: () => version,
   };
   const environment = {
      assertCanAdmitQuery: () => {},
      getPackage: async () => pkg,
      getApiConnection: () => {
         throw new Error("no such connection");
      },
   };
   return {
      getEnvironment: async () => environment,
      mcpIncludeHiddenFilesAndSources: false,
   } as unknown as EnvironmentStore;
}

/** A model whose query runs until it is aborted, by the client or the timeout. */
const HUNG = {
   carriesReportTag: () => false,
   getQueryResults: (_audit: unknown, ...args: unknown[]): Promise<never> =>
      new Promise((_, reject) =>
         (args[6] as AbortSignal).addEventListener("abort", () =>
            reject(new Error("aborted")),
         ),
      ),
};

function rest(
   store: EnvironmentStore,
   opts: {
      modelPath?: string;
      query?: string;
      givens?: Record<string, never>;
      signal?: AbortSignal;
   } = {},
) {
   return new QueryController(store).getQuery({
      environmentName: "env",
      packageName: "pkg",
      modelPath: opts.modelPath ?? "m.malloy",
      sourceName: "",
      queryName: "",
      query: opts.query ?? "run: open_src -> { aggregate: n }",
      compactJson: true,
      filterParams: {},
      givens: opts.givens,
      bypassAuthorize: false,
      includeHiddenFilesAndSources: false,
      clientSignal: opts.signal,
      identity: { caller: "alice@example.com" },
   });
}

describe("REST query writes exactly one record", () => {
   it("on success, with the caller and what the model learned", async () => {
      await withAuthorizeModel(MODEL, async ({ model }) => {
         await rest(storeOf(model), {
            query: GATED,
            givens: { ORGS: ["org1"] } as never,
         });
         const [record, ...others] = records();
         expect(others).toEqual([]);
         expect(record).toMatchObject({
            outcome: "ok",
            status: 200,
            caller: "alice@example.com",
            source: "accounts_src",
            decision: "filtered",
            fieldsReadComplete: true,
            servedFrom: null,
         });
         expect(record.fieldsRead?.map((f) => f.path)).toEqual(["area", "n"]);
      });
   });

   it("on a refusal before the model is reached", async () => {
      await withAuthorizeModel(MODEL, async ({ model }) => {
         await expect(
            rest(storeOf(model), { modelPath: "nope.malloy" }),
         ).rejects.toThrow();
         expect(records()).toMatchObject([
            { outcome: "refused_surface", status: 404 },
         ]);
      });
   });

   it("on a refusal inside the model", async () => {
      await withAuthorizeModel(MODEL, async ({ model }) => {
         await expect(
            rest(storeOf(model), { query: "run: chart -> { aggregate: n }" }),
         ).rejects.toThrow();
         expect(records()).toMatchObject([
            { outcome: "denied_lock", status: 403 },
         ]);
      });
   });

   it("as cancelled when the client aborted", async () => {
      const disconnected = new AbortController();
      const answered = rest(storeOf(HUNG), { signal: disconnected.signal });
      setTimeout(
         () => disconnected.abort(new Error("client disconnected")),
         20,
      );
      await expect(answered).rejects.toThrow();
      expect(records()).toMatchObject([{ outcome: "cancelled", status: 499 }]);
   });

   it("on a timeout", async () => {
      await withSettings(
         { env: { PUBLISHER_QUERY_TIMEOUT_MS: "20" } },
         async () => {
            await expect(rest(storeOf(HUNG))).rejects.toThrow();
         },
      );
      expect(records()).toMatchObject([{ outcome: "error", status: 504 }]);
   });

   it("on the legacy model query route, carrying the caller and trace the request named", async () => {
      await withAuthorizeModel(MODEL, async ({ model }) => {
         const app = buildRouteApp({
            queryController: new QueryController(storeOf(model)),
         });
         await request(app)
            .post("/api/v0/projects/env/packages/pkg/models/m.malloy/query")
            .send({ query: "run: open_src -> { aggregate: n }" })
            .set({
               "x-publisher-caller": "alice@example.com",
               traceparent:
                  "00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01",
            })
            .expect(200);
         expect(records()).toMatchObject([
            {
               outcome: "ok",
               caller: "alice@example.com",
               traceId: "0af7651916cd43dd8448eb211c80319c",
            },
         ]);
      });
   });
});

/** The MCP execute_query handler over a store, as the SDK would call it. */
function mcpHandler(store: EnvironmentStore) {
   let handler!: (
      params: Record<string, unknown>,
      extra?: unknown,
   ) => Promise<{ isError?: boolean }>;
   registerExecuteQueryTool(
      {
         registerTool: (_n: string, _config: unknown, h: typeof handler) => {
            handler = h;
         },
      } as never,
      store,
   );
   return handler;
}

const MCP_ARGS = {
   environmentName: "env",
   packageName: "pkg",
   modelPath: "m.malloy",
};
const EXTRA = {
   requestInfo: {
      headers: {
         "x-publisher-caller": "agent@example.com",
         traceparent: "00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01",
      },
   },
};

describe("MCP execute_query writes exactly one record", () => {
   it("on success, carrying the caller, the trace and the fields read", async () => {
      await withAuthorizeModel(MODEL, async ({ model }) => {
         const result = await mcpHandler(storeOf(model))(
            { ...MCP_ARGS, query: GATED, givens: { ORGS: ["org1"] } },
            EXTRA,
         );
         expect(result.isError).toBeFalsy();
         const [record, ...others] = records();
         expect(others).toEqual([]);
         expect(record).toMatchObject({
            outcome: "ok",
            caller: "agent@example.com",
            traceId: "0af7651916cd43dd8448eb211c80319c",
            source: "accounts_src",
            decision: "filtered",
            fieldsReadComplete: true,
         });
         expect(record.fieldsRead?.map((f) => f.path)).toEqual(["area", "n"]);
      });
   });

   it("naming the package version that answered", async () => {
      await withAuthorizeModel(MODEL, async ({ model }) => {
         await mcpHandler(storeOf(model, "v7"))(
            { ...MCP_ARGS, query: GATED, givens: { ORGS: ["org1"] } },
            EXTRA,
         );
         expect(records()).toMatchObject([{ outcome: "ok", version: "v7" }]);
      });
   });

   it("on an argument refusal, still throwing the protocol error", async () => {
      await withAuthorizeModel(MODEL, async ({ model }) => {
         await expect(
            mcpHandler(storeOf(model))({ ...MCP_ARGS }, EXTRA),
         ).rejects.toThrow();
         await expect(
            mcpHandler(storeOf(model))(
               { ...MCP_ARGS, query: GATED, queryName: "q" },
               EXTRA,
            ),
         ).rejects.toThrow();
         expect(records()).toMatchObject([
            { outcome: "bad_request", caller: "agent@example.com" },
            { outcome: "bad_request", caller: "agent@example.com" },
         ]);
      });
   });

   it("on a model-access error, still answering with the tool error", async () => {
      await withAuthorizeModel(MODEL, async ({ model }) => {
         const result = await mcpHandler(storeOf(model))(
            { ...MCP_ARGS, modelPath: "nope.malloy", query: GATED },
            EXTRA,
         );
         expect(result.isError).toBe(true);
         expect(records()).toMatchObject([{ outcome: "refused_surface" }]);
      });
   });

   it("on a refusal at the concurrency cap", async () => {
      await withSettings(
         { env: { PUBLISHER_MAX_CONCURRENT_QUERIES: "1" } },
         async () => {
            const held = tryAcquireQuerySlot("test");
            try {
               await withAuthorizeModel(MODEL, async ({ model }) => {
                  const result = await mcpHandler(storeOf(model))(
                     { ...MCP_ARGS, query: GATED },
                     EXTRA,
                  );
                  expect(result.isError).toBe(true);
                  expect(records()).toMatchObject([
                     { outcome: "error", status: 503 },
                  ]);
               });
            } finally {
               held.release();
            }
         },
      );
   });
});

describe("value search writes exactly one record per lookup", () => {
   const lookup = (store: EnvironmentStore, source: string) =>
      queryDistinctValues({
         environmentStore: store,
         environmentName: "env",
         packageName: "pkg",
         modelPath: "m.malloy",
         source,
         dimension: "label",
         limit: 5,
         timeoutMs: 5000,
         counted: false,
         routeLabel: "mcp:getContext:values",
      });

   it("when it reads", async () => {
      await withAuthorizeModel(MODEL, async ({ model }) => {
         await lookup(storeOf(model), "open_src");
         expect(records()).toMatchObject([
            { outcome: "ok", requestedSource: "open_src", adHoc: true },
         ]);
      });
   });

   it("carrying the caller and trace the request named", async () => {
      await withAuthorizeModel(MODEL, async ({ model }) => {
         await queryDistinctValues({
            environmentStore: storeOf(model),
            environmentName: "env",
            packageName: "pkg",
            modelPath: "m.malloy",
            source: "open_src",
            dimension: "label",
            limit: 5,
            timeoutMs: 5000,
            counted: false,
            routeLabel: "mcp:getContext:values",
            identity: accessIdentityOf(EXTRA.requestInfo.headers),
         });
         expect(records()).toMatchObject([
            {
               caller: "agent@example.com",
               traceId: "0af7651916cd43dd8448eb211c80319c",
            },
         ]);
      });
   });

   it("when the lock refuses it", async () => {
      await withAuthorizeModel(MODEL, async ({ model }) => {
         await expect(lookup(storeOf(model), "chart")).rejects.toThrow();
         expect(records()).toMatchObject([{ outcome: "denied_lock" }]);
      });
   });

   it("naming the package version that answered", async () => {
      await withAuthorizeModel(MODEL, async ({ model }) => {
         await lookup(storeOf(model, "v7"), "open_src");
         expect(records()).toMatchObject([{ version: "v7" }]);
      });
   });

   it("not at all when the index build reads, which is the system's and not a caller's", async () => {
      await withAuthorizeModel(MODEL, async ({ model }) => {
         const read = await defaultIndexValueFetcher({
            environmentStore: storeOf(model),
            environmentName: "env",
            packageName: "pkg",
            modelPath: "m.malloy",
            source: "open_src",
            dimension: "label",
            limit: 5,
            timeoutMs: 5000,
         });
         expect(read.values.length).toBeGreaterThan(0);
         expect(records()).toEqual([]);
      });
   });
});

describe("legacy notebook cell writes exactly one record", () => {
   const cell = (model: unknown, path = "m.malloy") =>
      new LegacyReportController(storeOf(model)).executeNotebookCell(
         "env",
         "pkg",
         path,
         0,
         undefined,
         undefined,
         undefined,
         undefined,
         {},
      );
   const notebook = (run: () => Promise<unknown>) => ({
      isNotebook: () => true,
      executeNotebookCell: run,
   });

   it("on success", async () => {
      await cell(notebook(async () => ({ type: "markdown" })));
      expect(records()).toMatchObject([{ outcome: "ok", status: 200 }]);
   });

   it("on a refusal before the model", async () => {
      await expect(
         cell(
            notebook(async () => ({})),
            "nope",
         ),
      ).rejects.toThrow();
      expect(records()).toMatchObject([{ outcome: "refused_surface" }]);
   });

   it("on a refusal inside the model", async () => {
      await expect(
         cell(
            notebook(async () => {
               throw new AccessDeniedError("denied");
            }),
         ),
      ).rejects.toThrow();
      expect(records()).toMatchObject([{ outcome: "denied_lock" }]);
   });
});

describe("a request refused before its controller runs still writes one record", () => {
   const MODEL_QUERY =
      "/api/v0/environments/env/packages/pkg/models/m.malloy/query";
   const LEGACY_MODEL_QUERY =
      "/api/v0/projects/env/packages/pkg/models/m.malloy/query";
   const cell = (prefix: string, index: string, query = "") =>
      `/api/v0/${prefix}/env/packages/pkg/notebooks/n.malloynb/cells/${index}${query}`;
   const CALLER = { "x-publisher-caller": "alice@example.com" };
   const appOver = (store: EnvironmentStore) =>
      buildRouteApp({
         environmentStore: store,
         queryController: new QueryController(store),
      });
   const atCap = (run: () => Promise<void>) =>
      withSettings(
         { env: { PUBLISHER_MAX_CONCURRENT_QUERIES: "1" } },
         async () => {
            const held = tryAcquireQuerySlot("test");
            try {
               await run();
            } finally {
               held.release();
            }
         },
      );

   it("on a bad includeHiddenFilesAndSources", async () => {
      await request(appOver(storeOf({})))
         .post(`${MODEL_QUERY}?includeHiddenFilesAndSources=maybe`)
         .set(CALLER)
         .send({ query: "run: open_src -> { aggregate: n }" })
         .expect(400);
      expect(records()).toMatchObject([
         {
            outcome: "bad_request",
            status: 400,
            modelPath: "m.malloy",
            adHoc: true,
            caller: "alice@example.com",
         },
      ]);
   });

   it.each([
      ["the model query route", MODEL_QUERY],
      ["the legacy model query route", LEGACY_MODEL_QUERY],
   ])("at the concurrency cap on %s", async (_name, path) => {
      await atCap(async () => {
         await request(appOver(storeOf({})))
            .post(path)
            .set(CALLER)
            .send({ queryName: "by_area" })
            .expect(503);
      });
      expect(records()).toMatchObject([
         {
            outcome: "error",
            status: 503,
            modelPath: "m.malloy",
            queryName: "by_area",
            adHoc: false,
            caller: "alice@example.com",
         },
      ]);
   });

   it.each([
      ["a cell index that is not a number", "abc", ""],
      ["filter_params that are not JSON", "0", "?filter_params=%7B"],
      ["a bad bypass_filters", "0", "?bypass_filters=maybe"],
      ["givens that are not JSON", "0", "?givens=%7B"],
   ])("on the notebook cell route with %s", async (_name, index, query) => {
      await request(appOver(storeOf({})))
         .get(cell("environments", index, query))
         .set(CALLER)
         .expect(400);
      expect(records()).toMatchObject([
         {
            outcome: "bad_request",
            status: 400,
            modelPath: "n.malloynb",
            caller: "alice@example.com",
         },
      ]);
   });

   it.each([
      ["a cell index that is not a number", "abc", ""],
      ["filter_params that are not JSON", "0", "?filter_params=%7B"],
      ["a bad bypass_filters", "0", "?bypass_filters=maybe"],
   ])(
      "on the legacy notebook cell route with %s",
      async (_name, index, query) => {
         await request(appOver(storeOf({})))
            .get(cell("projects", index, query))
            .set(CALLER)
            .expect(400);
         expect(records()).toMatchObject([
            { outcome: "bad_request", status: 400, modelPath: "n.malloynb" },
         ]);
      },
   );

   it.each([["environments"], ["projects"]])(
      "at the concurrency cap on the %s notebook cell route",
      async (prefix) => {
         await atCap(async () => {
            await request(appOver(storeOf({})))
               .get(cell(prefix, "0"))
               .set(CALLER)
               .expect(503);
         });
         expect(records()).toMatchObject([
            { outcome: "error", status: 503, modelPath: "n.malloynb" },
         ]);
      },
   );

   it("once, not twice, when the request reaches its controller", async () => {
      await withAuthorizeModel(MODEL, async ({ model }) => {
         await request(appOver(storeOf(model)))
            .post(`${MODEL_QUERY}?includeHiddenFilesAndSources=false`)
            .send({ query: "run: open_src -> { aggregate: n }" })
            .expect(200);
         expect(records()).toHaveLength(1);
      });
   });
});

describe("raw SQL writes exactly one record, as ungated", () => {
   const controllerOver = (runSQL: sinon.SinonStub, extra: object = {}) => {
      const environment = {
         assertCanAdmitQuery: () => {},
         getApiConnection: () => ({ name: "warehouse", type: "postgres" }),
      };
      const controller = new ConnectionController({
         getEnvironment: async () => environment,
      } as unknown as EnvironmentStore);
      sinon
         .stub(
            controller as unknown as {
               getMalloyConnection: () => Promise<Connection>;
            },
            "getMalloyConnection",
         )
         .resolves({ runSQL, ...extra } as unknown as Connection);
      return controller;
   };

   it("when it serves", async () => {
      const controller = controllerOver(
         sinon.stub().resolves({ rows: [{ a: 1 }], totalRows: 1 }),
      );
      await controller.getConnectionQueryData(
         "env",
         "warehouse",
         "SELECT a FROM t",
         "",
         undefined,
         undefined,
         undefined,
         {},
      );
      expect(records()).toMatchObject([
         {
            outcome: "ok",
            decision: "ungated",
            connection: "warehouse",
            adHoc: true,
         },
      ]);
   });

   it("on the temporary-table route too, as the same raw SQL", async () => {
      const controller = controllerOver(sinon.stub(), {
         manifestTemporaryTable: async () => "tmp_1",
      });
      await controller.getConnectionTemporaryTable(
         "env",
         "warehouse",
         "SELECT a FROM t",
         undefined,
         undefined,
         { caller: "alice@example.com" },
      );
      expect(records()).toMatchObject([
         {
            outcome: "ok",
            decision: "ungated",
            connection: "warehouse",
            adHoc: true,
            caller: "alice@example.com",
         },
      ]);
   });

   it("when it is refused", async () => {
      await withSettings(
         { env: { PUBLISHER_MAX_QUERY_ROWS: "1" } },
         async () => {
            const controller = controllerOver(
               sinon
                  .stub()
                  .resolves({ rows: [{ a: 1 }, { a: 2 }], totalRows: 2 }),
            );
            await expect(
               controller.getConnectionQueryData(
                  "env",
                  "warehouse",
                  "SELECT a FROM t",
                  "",
                  undefined,
                  undefined,
                  undefined,
                  {},
               ),
            ).rejects.toBeInstanceOf(PayloadTooLargeError);
            expect(records()).toMatchObject([
               { outcome: "error", decision: "ungated" },
            ]);
         },
      );
   });

   it("redacts a credential the driver quotes back in its error", async () => {
      const controller = controllerOver(
         sinon
            .stub()
            .rejects(
               new Error(
                  "connect failed for postgres://svc:hunter2@db:5432/x password=hunter2",
               ),
            ),
      );
      await expect(
         controller.getConnectionQueryData(
            "env",
            "warehouse",
            "SELECT 1",
            "",
            undefined,
            undefined,
            undefined,
            {},
         ),
      ).rejects.toThrow();
      const [record] = records();
      expect(record.error).toBeDefined();
      expect(record.error).not.toContain("hunter2");
   });

   it("on the legacy routes, carrying the caller and trace the request named", async () => {
      const controller = controllerOver(
         sinon.stub().resolves({ rows: [{ a: 1 }], totalRows: 1 }),
         { manifestTemporaryTable: async () => "tmp_1" },
      );
      const app = buildRouteApp({ connectionController: controller });
      const base = "/api/v0/projects/env";
      const named = {
         "x-publisher-caller": "alice@example.com",
         traceparent: "00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01",
      };
      const sql = { sqlStatement: "SELECT a FROM t", options: "" };
      await request(app)
         .get(`${base}/connections/warehouse/queryData`)
         .query(sql)
         .set(named)
         .expect(200);
      await request(app)
         .post(`${base}/packages/pkg/connections/warehouse/sqlQuery`)
         .send(sql)
         .set(named)
         .expect(200);
      await request(app)
         .post(`${base}/connections/warehouse/sqlTemporaryTable`)
         .send(sql)
         .set(named)
         .expect(200);
      expect(records()).toHaveLength(3);
      for (const record of records()) {
         expect(record).toMatchObject({
            decision: "ungated",
            caller: "alice@example.com",
            traceId: "0af7651916cd43dd8448eb211c80319c",
         });
      }
   });
});
