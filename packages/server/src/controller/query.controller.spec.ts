// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import type * as Malloy from "@malloydata/malloy-interfaces";
import { afterAll, beforeAll, describe, expect, it } from "bun:test";
import sinon from "sinon";
import { BadRequestError, ModelNotFoundError } from "../errors";
import type { EnvironmentStore } from "../service/environment_store";
import {
   loadTempPackage,
   type LoadedTempPackage,
} from "../test_helpers/temp_package";
import { QueryController } from "./query.controller";

/**
 * Real Malloy results, so the render-tag validator reads what a model really
 * returns: a field with an unknown tag, a field with a Publisher-owned one, and
 * a renderer child tag on the query's own (root) field.
 */
let tagged: Malloy.Result;
let rootColspan: Malloy.Result;
let plain: Malloy.Result;
let temp: LoadedTempPackage;

beforeAll(async () => {
   temp = await loadTempPackage({
      "m.malloy": "source: s is duckdb.sql('select 1 as a')\n",
   });
   const model = temp.pkg.getModel("m.malloy")!;
   const run = async (query: string) =>
      (await model.getQueryResults(undefined, undefined, query)).result;
   tagged = await run(
      "run: s -> {\n # bogus_tag\n group_by: a\n # drill\n aggregate: n is count() }",
   );
   rootColspan = await run("# colspan=2\nrun: s -> { group_by: a }");
   plain = await run("run: s -> { group_by: a }");
});

afterAll(async () => {
   await temp.cleanup();
});

function controllerWith({
   result = plain,
   model = true,
   carriesReportTag = false,
}: {
   result?: Malloy.Result;
   model?: boolean;
   carriesReportTag?: boolean;
} = {}) {
   const getQueryResults = sinon.stub().resolves({
      result,
      serializedResult: "SERIALIZED",
      rowLimit: 10,
      rowLimitSource: "server_default",
      queryCorrelationId: "corr-1",
      servedFrom: "storage",
      executionTimeMs: 12,
      queryCostBytes: 345,
   });
   const fakeModel = {
      getQueryResults,
      carriesReportTag: () => carriesReportTag,
   };
   const pkg = {
      getModel: () => (model ? fakeModel : undefined),
      getDeclaredQueryMetadata: () => null,
      // A package with no published versions.
      getVersionId: () => undefined,
   };
   const getEnvironment = sinon.stub().resolves({
      assertCanAdmitQuery: () => {},
      getPackage: async () => pkg,
      getApiConnection: () => {
         throw new Error("no such connection");
      },
   });
   const store = { getEnvironment } as unknown as EnvironmentStore;
   return {
      controller: new QueryController(store),
      getQueryResults,
      getEnvironment,
   };
}

/** getQuery with only the arguments a case varies; the rest left default. */
function run(
   controller: QueryController,
   opts: {
      compactJson?: boolean;
      metadata?: Parameters<QueryController["getQuery"]>[10];
      bypassAuthorize?: boolean;
      includeHidden?: boolean;
      signal?: AbortSignal;
   } = {},
) {
   return controller.getQuery(
      "env",
      "pkg",
      "m.malloy",
      "s",
      "",
      "run: s -> { group_by: a }",
      opts.compactJson,
      undefined,
      undefined,
      undefined,
      opts.metadata,
      opts.bypassAuthorize,
      opts.includeHidden,
      opts.signal,
   );
}

describe("QueryController.getQuery", () => {
   it.each([
      ["a non-object queryMetadata", { queryMetadata: "team=x" }],
      ["a non-string metadata value", { queryMetadata: { team: 1 } }],
      ["an unknown queryClass", { queryClass: "bogus" }],
   ])(
      "refuses %s with a 400, before touching the environment",
      async (_, metadata) => {
         const { controller, getEnvironment } = controllerWith();
         await expect(run(controller, { metadata })).rejects.toBeInstanceOf(
            BadRequestError,
         );
         expect(getEnvironment.called).toBe(false);
      },
   );

   it("answers a missing model with a plain 404", async () => {
      const { controller } = controllerWith({ model: false });
      const error = await run(controller).catch((e) => e);
      expect(error).toBeInstanceOf(ModelNotFoundError);
      expect(error.message).toBe('No queryable model "m.malloy".');
   });

   it("maps the model's answer into the response", async () => {
      const { controller } = controllerWith();
      expect(await run(controller)).toEqual({
         result: "SERIALIZED",
         resource:
            "/api/v0/environments/env/packages/pkg/models/m.malloy/query",
         renderLogs: undefined,
         queryRowLimit: 10,
         queryRowLimitSource: "server_default",
         queryCorrelationId: "corr-1",
         servedFrom: "storage",
         executionTimeMs: 12,
         queryCostBytes: 345,
      });
   });

   it("passes the result shape, flags and request metadata through to the model", async () => {
      const { controller, getQueryResults } = controllerWith();
      await run(controller, {
         compactJson: true,
         bypassAuthorize: true,
         includeHidden: true,
         metadata: { queryMetadata: { team: "x" }, queryClass: "interactive" },
      });
      await run(controller);
      const [compact, full] = getQueryResults.getCalls().map((c) => c.args);
      expect(compact.slice(8)).toEqual(["compact", true, true]);
      expect(full.slice(8)).toEqual(["full", undefined, undefined]);
      expect(compact[7]).toMatchObject({
         request: { team: "x" },
         queryClass: "interactive",
         environment: "env",
         correlationId: expect.any(String),
      });
   });

   it("cancels the model's query when the client goes away", async () => {
      const { controller, getQueryResults } = controllerWith();
      const client = new AbortController();
      let seen: AbortSignal | undefined;
      getQueryResults.callsFake(async (...args: unknown[]) => {
         seen = args[6] as AbortSignal;
         client.abort("client gone");
         return { result: plain, serializedResult: "S" };
      });
      await run(controller, { signal: client.signal });
      expect(seen?.aborted).toBe(true);
      expect(seen?.reason).toBe("client gone");
   });

   it.each([
      [
         "keeps an unknown tag, drops a Publisher-owned one",
         "tagged",
         false,
         ["bogus_tag"],
      ],
      [
         "keeps a root child tag outside a report",
         "rootColspan",
         false,
         ["colspan"],
      ],
      ["drops a root child tag in a report", "rootColspan", true, []],
   ] as const)("render logs: %s", async (_, which, carriesReportTag, kept) => {
      const result = which === "tagged" ? tagged : rootColspan;
      const { controller } = controllerWith({ result, carriesReportTag });
      const { renderLogs } = await run(controller);
      const tags = (renderLogs ?? []).map(
         (log) => /Unknown render tag '([^']+)'/.exec(log.message ?? "")?.[1],
      );
      expect(tags).toEqual([...kept]);
      // No logs at all is absent, not an empty array.
      if (kept.length === 0) expect(renderLogs).toBeUndefined();
   });
});
