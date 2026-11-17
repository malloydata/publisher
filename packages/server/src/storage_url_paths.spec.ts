// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * P6-21 (F-27): every path driver text reaches a caller by names no storage
 * URL: compile diagnostics (REST and MCP `compile_model`, one controller), a
 * REST MalloyError body, a run's recorded failure reasons and load errors,
 * and `/status`'s `loadErrors`.
 */

import { MalloyError } from "@malloydata/malloy";
import { describe, expect, it } from "bun:test";
import { CompileController } from "./controller/compile.controller";
import { internalErrorToHttpError } from "./errors";
import { EnvironmentStore } from "./service/environment_store";
import { notServed } from "./service/materialization/run/manifest_committer";
import { redactConnectionSecrets } from "./service/materialization/run/materialization_run_support";
import { fakeOf } from "./test_helpers/store_fakes";

const URL_TEXT =
   "HTTP GET error on 'https://storage.googleapis.com/bkt/org_1/db_2/t.parquet' (HTTP 500)";
const LEAKS = /googleapis|bkt|org_1|db_2/;

describe("P6-21: no storage URL reaches a caller", () => {
   it("in compile diagnostics", async () => {
      const controller = new CompileController(
         fakeOf({
            getEnvironment: async () => ({
               compileSource: async () => ({
                  problems: [
                     { severity: "error", message: URL_TEXT, code: "x" },
                  ],
               }),
            }),
         }) as never,
      );
      const result = await controller.compile(
         "env",
         "pkg",
         "m.malloy",
         "run: x -> { select: * }",
         false,
         undefined,
         "append",
      );
      expect(result.problems[0].message).toContain("<storage path>");
      expect(result.problems[0].message).not.toMatch(LEAKS);
   });

   it("in a REST MalloyError body", () => {
      const answer = internalErrorToHttpError(new MalloyError(URL_TEXT, []), {
         log: false,
      });
      expect(answer.status).toBe(400);
      expect(answer.json.message).toContain("<storage path>");
      expect(answer.json.message).not.toMatch(LEAKS);
   });

   it("in a source's recorded failure reason", () => {
      expect(redactConnectionSecrets(URL_TEXT)).not.toMatch(LEAKS);
   });

   it("in a run's load error and record error", () => {
      const served = notServed("reload", URL_TEXT) as {
         error: string;
         metadata: { autoLoadError: string };
      };
      expect(served.error).not.toMatch(LEAKS);
      expect(served.metadata.autoLoadError).not.toMatch(LEAKS);
   });

   it("in /status loadErrors", () => {
      const store = Object.create(EnvironmentStore.prototype) as Record<
         string,
         unknown
      >;
      store.failedEnvironments = new Map([["env", URL_TEXT]]);
      store.environments = new Map();
      const status: {
         environments: unknown[];
         loadErrors?: { message?: string }[];
      } = { environments: [] };
      (
         store as unknown as { addStatusProblems(s: unknown): void }
      ).addStatusProblems.call(store, status);
      expect(status.loadErrors?.[0].message).toContain("<storage path>");
      expect(status.loadErrors?.[0].message).not.toMatch(LEAKS);
   });
});
