// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/// <reference types="bun-types" />

/**
 * A `files` overlay on `POST …/compile` and `POST …/query` over real HTTP: the
 * overlaid files are read instead of, or beside, the package's own for that one
 * request, are never written or seen by a later request, are bounded, and gate
 * as package files.
 */

import { MAX_OVERLAY_BYTES, MAX_OVERLAY_FILES } from "../../../src/constants";
import { afterAll, beforeAll, describe, expect, it } from "bun:test";
import * as fs from "fs";
import { writeTempPackage } from "../../../src/test_helpers/temp_package";
import { startPackageEnv, type PackageEnv } from "../../harness/reports";

const BASE = `source: base_source is duckdb.sql("select 1 as id, 'a' as name") extend {
   measure: n is count()
}
`;
const TRACKS = `import "base.malloy"
source: tracks is base_source extend { dimension: id2 is id * 2 }
`;
const gated = (role: string) => `##! experimental.givens

#(secure)
given:
  ROLE :: string

#(authorize) '${role}' = $ROLE
source: secret is duckdb.sql("select 7 as v") extend {
   measure: total is sum(v)
}
`;

interface Problem {
   severity: string;
   message: string;
   model?: string;
   at?: { range?: { start?: { line?: number } } };
}
interface CompileBody {
   status?: string;
   message?: string;
   problems?: Problem[];
}

const errorsOf = (body: CompileBody) =>
   (body.problems ?? []).filter((p) => p.severity === "error");

describe("a request's files overlay (E2E)", () => {
   let env: PackageEnv;
   let removePackage: () => void;
   const compile = async (modelPath: string, body: unknown) => {
      const res = await env.post(env.api(`/models/${modelPath}/compile`), body);
      return { status: res.status, body: (await res.json()) as CompileBody };
   };
   const query = async (modelPath: string, body: unknown) => {
      const res = await env.query(modelPath, body);
      return {
         status: res.status,
         body: (await res.json()) as { result?: string; message?: string },
      };
   };
   const rowsOf = (result: string | undefined) =>
      (JSON.parse(result ?? "{}") as { data?: { array_value?: unknown[] } })
         .data?.array_value ?? [];

   beforeAll(async () => {
      const { dir, cleanup } = writeTempPackage(
         {
            "base.malloy": BASE,
            "tracks.malloy": TRACKS,
            "gated.malloy": gated("admin"),
         },
         { name: "overlay" },
      );
      removePackage = cleanup;
      env = await startPackageEnv("request-overlay-env", { overlay: dir });
   }, 300_000);

   afterAll(async () => {
      await env?.stop();
      removePackage?.();
   });

   describe("compile", () => {
      it("reads an overlaid file instead of the package's: an import of it sees the overlay", async () => {
         const { status, body } = await compile("tracks.malloy", {
            scope: "file",
            source: TRACKS,
            files: [
               {
                  path: "base.malloy",
                  content: BASE.replace("base_source", "renamed"),
               },
            ],
         });
         expect(status).toBe(200);
         expect(body.status).toBe("error");
         expect(errorsOf(body).map((p) => p.model)).toContain("tracks.malloy");
      });

      it("compiles the model's own text from the overlay at file scope, and says where each error is", async () => {
         const { status, body } = await compile("tracks.malloy", {
            scope: "file",
            files: [
               {
                  path: "tracks.malloy",
                  content: `import "base.malloy"\nsource: tracks is base_source extend {\n   dimension: bad is nope\n}\n`,
               },
            ],
         });
         expect(status).toBe(200);
         expect(body.status).toBe("error");
         const [error] = errorsOf(body);
         expect(error.model).toBe("tracks.malloy");
         expect(error.at?.range?.start?.line).toBe(2);
      });

      it("adds an overlaid file the package does not have, so an import of it resolves", async () => {
         const { status, body } = await compile("tracks.malloy", {
            scope: "file",
            source: `import "extra.malloy"\nsource: more is extra extend { dimension: w is v + 1 }\n`,
            files: [
               {
                  path: "extra.malloy",
                  content: `source: extra is duckdb.sql("select 1 as v")\n`,
               },
            ],
         });
         expect(status).toBe(200);
         expect(body).toEqual({ status: "success", problems: [] });
      });

      it("compiles an overlaid file new to the package at package scope, and names it", async () => {
         const { status, body } = await compile("base.malloy", {
            scope: "package",
            files: [
               {
                  path: "added.malloy",
                  content: `import "base.malloy"\nsource: added is base_source extend { dimension: x is nope }\n`,
               },
            ],
         });
         expect(status).toBe(200);
         expect(errorsOf(body).map((p) => p.model)).toContain("added.malloy");
      });

      it("refuses files at append scope", async () => {
         const { status, body } = await compile("base.malloy", {
            source: "run: base_source -> { aggregate: n }",
            files: [{ path: "base.malloy", content: BASE }],
         });
         expect(status).toBe(400);
         expect(body.message).toContain('scope "append"');
      });

      it("refuses a model given both as source and in files", async () => {
         const { status } = await compile("tracks.malloy", {
            scope: "file",
            source: TRACKS,
            files: [{ path: "tracks.malloy", content: TRACKS }],
         });
         expect(status).toBe(400);
      });
   });

   describe("query", () => {
      it("runs a query against the overlaid model", async () => {
         const overlaid = TRACKS.replace(
            "dimension: id2 is id * 2",
            "dimension: id2 is id * 2\n   dimension: id3 is id * 3",
         );
         const { status, body } = await query("tracks.malloy", {
            query: "run: tracks -> { group_by: id3 }",
            files: [{ path: "tracks.malloy", content: overlaid }],
         });
         expect(status).toBe(200);
         expect(rowsOf(body.result)).toHaveLength(1);
      });

      it("answers a model the overlay breaks with the compile's 400", async () => {
         const { status } = await query("tracks.malloy", {
            query: "run: tracks -> { aggregate: n }",
            files: [
               {
                  path: "base.malloy",
                  content: BASE.replace("base_source", "renamed"),
               },
            ],
         });
         expect(status).toBeGreaterThanOrEqual(400);
         expect(status).toBeLessThan(500);
      });
   });

   describe("never outlives its request", () => {
      it("leaves the package's own files, models and answers as they were", async () => {
         await compile("tracks.malloy", {
            scope: "file",
            source: TRACKS,
            files: [
               { path: "base.malloy", content: "source: oops is nope" },
               {
                  path: "leak.malloy",
                  content: "source: leak is duckdb.sql('select 1 as l')",
               },
            ],
         });
         await query("tracks.malloy", {
            query: "run: tracks -> { aggregate: n }",
            files: [{ path: "base.malloy", content: "source: oops is nope" }],
         });

         const plain = await query("tracks.malloy", {
            query: "run: tracks -> { aggregate: n }",
         });
         expect(plain.status).toBe(200);
         const without = await query("tracks.malloy", {
            query: "run: tracks -> { group_by: id3 }",
         });
         expect(without.status).toBe(400);
         expect(await compile("base.malloy", { scope: "package" })).toEqual({
            status: 200,
            body: { status: "success", problems: [] },
         });
         expect(fs.existsSync(env.served("leak.malloy"))).toBe(false);
         expect(fs.readFileSync(env.served("base.malloy"), "utf8")).toBe(BASE);
      });
   });

   describe("bounds and entries", () => {
      it("refuses more files than the server takes with 413", async () => {
         const files = Array.from(
            { length: MAX_OVERLAY_FILES + 1 },
            (_, i) => ({
               path: `f${i}.malloy`,
               content: "",
            }),
         );
         const { status, body } = await compile("base.malloy", {
            scope: "package",
            files,
         });
         expect(status).toBe(413);
         expect(body.message).toContain(`at most ${MAX_OVERLAY_FILES}`);
      });

      it("refuses more bytes than the server takes with 413", async () => {
         const { status, body } = await query("tracks.malloy", {
            query: "run: tracks -> { aggregate: n }",
            files: [
               {
                  path: "base.malloy",
                  content: BASE + " ".repeat(MAX_OVERLAY_BYTES),
               },
            ],
         });
         expect(status).toBe(413);
         expect(body.message).toContain("bytes of text");
      });

      it.each([
         ["a parent segment", "../escape.malloy"],
         ["an absolute path", "/etc/escape.malloy"],
         ["a backslash", "dir\\escape.malloy"],
         ["a dotfile segment", ".hidden/x.malloy"],
         ["a file that is not a model", "data.csv"],
      ])("refuses %s with 400", async (_why, badPath) => {
         const { status } = await compile("base.malloy", {
            scope: "package",
            files: [{ path: badPath, content: "" }],
         });
         expect(status).toBe(400);
      });

      it("refuses a path given twice, and an entry that is not { path, content }", async () => {
         for (const files of [
            [
               { path: "x.malloy", content: "" },
               { path: "x.malloy", content: "" },
            ],
            [{ path: "x.malloy" }],
            "x.malloy",
         ]) {
            const { status } = await compile("base.malloy", {
               scope: "package",
               files,
            });
            expect(status).toBe(400);
         }
      });
   });

   describe("overlay files are package files", () => {
      const run = "run: secret -> { aggregate: total }";

      it("keeps the package's gate on a query without an overlay", async () => {
         const { status } = await query("gated.malloy", {
            query: run,
            givens: { ROLE: "viewer" },
         });
         expect(status).toBe(403);
      });

      it("gates an overlaid model by its own annotations, as written", async () => {
         const reopened = await query("gated.malloy", {
            query: run,
            givens: { ROLE: "viewer" },
            files: [{ path: "gated.malloy", content: gated("viewer") }],
         });
         expect(reopened.status).toBe(200);
         const stillLocked = await query("gated.malloy", {
            query: run,
            givens: { ROLE: "viewer" },
            files: [{ path: "gated.malloy", content: gated("admin") }],
         });
         expect(stillLocked.status).toBe(403);
      });

      it("still refuses a gate in caller query text beside an overlay", async () => {
         const { status } = await query("gated.malloy", {
            query: `#(authorize) true\nrun: secret -> { aggregate: total }`,
            givens: { ROLE: "admin" },
            files: [{ path: "gated.malloy", content: gated("admin") }],
         });
         expect(status).toBe(400);
      });
   });
});
