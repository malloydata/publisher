// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/// <reference types="bun-types" />

/**
 * `POST …/models/{path}/compile` at `scope: "package"` over real HTTP: a clean
 * package dry-runs clean, an edit that breaks ANOTHER file is reported against
 * that file, and an unknown scope is a 400 naming the valid set. The scope
 * semantics themselves are unit-tested in `src/service/compile_scopes.spec.ts`;
 * this pins the route's status/body contract on the real stack.
 */

import { afterAll, beforeAll, describe, expect, it } from "bun:test";
import { writeTempPackage } from "../../../src/test_helpers/temp_package";
import { startPackageEnv, type PackageEnv } from "../../harness/reports";

const BASE = `source: base_source is duckdb.sql("select 1 as id") extend {
   measure: n is count()
}
`;
const TRACKS = `import "base.malloy"
source: tracks is base_source extend { dimension: id2 is id * 2 }
`;

interface CompileBody {
   status?: string;
   message?: string;
   problems?: { severity: string; model?: string }[];
}

describe("POST …/compile at package scope (E2E)", () => {
   let env: PackageEnv;
   let removePackage: () => void;
   const compile = async (body: unknown) => {
      const res = await env.post(env.api("/models/base.malloy/compile"), body);
      return { status: res.status, body: (await res.json()) as CompileBody };
   };

   beforeAll(async () => {
      const { dir, cleanup } = writeTempPackage(
         { "base.malloy": BASE, "tracks.malloy": TRACKS },
         { name: "scopes" },
      );
      removePackage = cleanup;
      env = await startPackageEnv("compile-scopes-http-env", { scopes: dir });
   }, 300_000);

   afterAll(async () => {
      await env?.stop();
      removePackage?.();
   });

   it("dry-runs a clean package as saved, with no source", async () => {
      expect(await compile({ scope: "package" })).toEqual({
         status: 200,
         body: { status: "success", problems: [] },
      });
   });

   it("reports an edit that breaks another file against that file", async () => {
      const { status, body } = await compile({
         scope: "package",
         source: BASE.replace("base_source", "renamed"),
      });
      expect(status).toBe(200);
      expect(body.status).toBe("error");
      const errorModels = (body.problems ?? [])
         .filter((p) => p.severity === "error")
         .map((p) => p.model);
      expect(errorModels).toContain("tracks.malloy");
   });

   it("refuses an unknown scope with 400, naming the valid set", async () => {
      const { status, body } = await compile({ scope: "everything" });
      expect(status).toBe(400);
      expect(body.message).toContain('"append", "file", "package"');
   });
});
