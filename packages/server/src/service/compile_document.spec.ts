// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { type GivenValue } from "@malloydata/malloy";
import { afterEach, beforeEach, describe, expect, it } from "bun:test";
import fs from "fs/promises";
import os from "os";
import path from "path";
import { CompileRefusedError } from "../errors";
import type { CompiledDocument } from "./compile_document";
import type { DashboardQueryTileSpec } from "./dashboard";
import { Environment } from "./environment";

const manifestOf = (result: { document?: CompiledDocument }) => {
   if (!result.document?.manifest) throw new Error("no manifest");
   return result.document.manifest;
};
const tilesOf = (result: { document?: CompiledDocument }) =>
   (manifestOf(result).tiles ?? []) as DashboardQueryTileSpec[];

// Compile of a source that carries a model-level `## artifact` tag answers with
// the document it describes. Driven through a real installed package.

const MODEL = `##! experimental.givens
## base note, which a document must never inherit
## artifact { kind=dashboard title="Base" }

given:
  ROLE :: string
  REGION :: filter<string> is f''

#(secure)
given: TENANT :: string is 'w'

#(authorize) 'analyst' = $ROLE
source: gated is duckdb.sql("SELECT 1 as x, 'w' as region") extend {
  measure: c is count()
  # label="Gated total"
  view: v is { aggregate: c }
}

source: open_src is duckdb.sql("SELECT 1 as x, 'w' as region") extend {
  measure: c is count()
  view: v is { aggregate: c where: region ~ $REGION }
  view: sv is { aggregate: c where: region = $TENANT }
}

run: open_src -> { aggregate: c }
`;

const DOC_TILES = `## artifact { kind=dashboard tiles=["open_src -> v", "gated -> v"] }
`;

describe("compile of a document (compileSource)", () => {
   let rootDir: string;
   let env: Environment;

   async function install(
      files: Record<string, string>,
      manifest: Record<string, unknown> = {},
   ) {
      await env.installPackage("pkg", async (stagingPath) => {
         await fs.mkdir(stagingPath, { recursive: true });
         await fs.writeFile(
            path.join(stagingPath, "publisher.json"),
            JSON.stringify({ name: "pkg", description: "doc", ...manifest }),
         );
         for (const [name, text] of Object.entries(files)) {
            await fs.writeFile(path.join(stagingPath, name), text);
         }
      });
   }

   beforeEach(async () => {
      rootDir = await fs.mkdtemp(path.join(os.tmpdir(), "publisher-doc-"));
      const envPath = path.join(rootDir, "env");
      await fs.mkdir(envPath, { recursive: true });
      env = await Environment.create("testEnv", envPath, []);
   });

   afterEach(async () => {
      await fs.rm(rootDir, { recursive: true, force: true }).catch(() => {});
   });

   const compile = (source: string, givens?: Record<string, GivenValue>) =>
      env.compileSource("pkg", "model.malloy", source, false, givens);

   describe("fragment isolation", () => {
      beforeEach(async () => {
         await install({ "model.malloy": MODEL });
      });

      it("reads only the submitted text: the base's run: and ## notes never join the document", async () => {
         const { problems, document } = await compile(
            `## artifact { kind=notebook }
##(markdown) Hello
run: open_src -> { aggregate: c }
`,
         );
         expect(problems.filter((p) => p.severity === "error")).toEqual([]);
         expect(document?.kind).toBe("notebook");
         // The base declares a dashboard tag of its own; none of it reaches the document.
         expect(document?.manifest).toBeUndefined();
         expect(document?.cells.map((c) => [c.kind, c.restricted])).toEqual([
            ["markdown", undefined],
            ["query", undefined],
         ]);
      });

      it("defaults the kind to notebook and takes a dashboard from the tag", async () => {
         const notebook = await compile(
            `## artifact { tiles=["open_src -> v"] }\n`,
         );
         expect(notebook.document?.kind).toBe("notebook");
         const dashboard = await compile(
            DOC_TILES.replace("gated -> v", "open_src -> v"),
         );
         expect(dashboard.document?.kind).toBe("dashboard");
         expect(tilesOf(dashboard)).toHaveLength(2);
      });

      it("reads the control row off the compiled tiles and does not run anything", async () => {
         const result = await compile(
            `## artifact { kind=dashboard tiles=["open_src -> v"] }\n`,
         );
         expect(tilesOf(result)[0].givenNames).toEqual(["REGION"]);
         expect(manifestOf(result).givens?.map((g) => g.name)).toEqual([
            "REGION",
         ]);
      });

      it("marks a #(secure) given so a host can withhold its control", async () => {
         const result = await compile(
            `## artifact { kind=dashboard tiles=["open_src -> sv"] }\n`,
         );
         const row = manifestOf(result).givens ?? [];
         expect(row.map((g) => [g.name, g.secure])).toEqual([["TENANT", true]]);
      });

      it("answers a source that is not a document as it always did", async () => {
         const result = await compile("run: open_src -> { aggregate: c }");
         expect(result.document).toBeUndefined();
         expect(result.problems).toEqual([]);
      });
   });

   describe("per-cell and per-tile authorize", () => {
      beforeEach(async () => {
         await install({ "model.malloy": MODEL });
      });

      it("a member sees every tile unrestricted", async () => {
         const tiles = tilesOf(await compile(DOC_TILES, { ROLE: "analyst" }));
         expect(tiles.map((t) => t.restricted)).toEqual([undefined, undefined]);
         expect(tiles[1].label).toBe("Gated total");
      });

      it("a non-member gets the gated tile bare and the rest compiled", async () => {
         const result = await compile(DOC_TILES, { ROLE: "nobody" });
         const { problems } = result;
         const tiles = tilesOf(result);
         expect(tiles[0].restricted).toBeUndefined();
         expect(tiles[0].givenNames).toEqual(["REGION"]);
         expect(tiles[1]).toEqual({
            kind: "query",
            query: "gated -> v",
            restricted: true,
         });
         expect(problems).toEqual([]);
      });

      it("a non-member's gated cell is restricted and its diagnostics do not exist", async () => {
         const text = `## artifact { kind=notebook }
run: open_src -> { aggregate: c }
run: gated -> { select: no_such_column_in_gated }
`;
         const nonMember = await compile(text, { ROLE: "nobody" });
         expect(nonMember.problems).toEqual([]);
         expect(nonMember.document?.cells.map((c) => c.restricted)).toEqual([
            undefined,
            true,
         ]);
         const member = await compile(text, { ROLE: "analyst" });
         expect(member.problems.some((p) => p.severity === "error")).toBe(true);
      });

      it("restriction follows a derived name: a source defined from a gated one taints its readers", async () => {
         const text = `## artifact { kind=notebook }
source: derived is gated extend { measure: n is count() }
run: derived -> { aggregate: n }
run: open_src -> { aggregate: c }
`;
         const { document, problems } = await compile(text, { ROLE: "nobody" });
         expect(problems).toEqual([]);
         expect(document?.cells.map((c) => c.restricted)).toEqual([
            true,
            true,
            undefined,
         ]);
      });

      it("text that does not parse is refused by the construct gate before any cell is read", async () => {
         await expect(
            compile(
               `## artifact { kind=notebook }\nrun: open_src -> { aggregate: c }\nthis is not malloy\n`,
            ),
         ).rejects.toBeInstanceOf(CompileRefusedError);
      });
   });

   describe("query boundary", () => {
      beforeEach(async () => {
         await install(
            {
               "index.malloy": `source: helper is duckdb.sql("select 1 as id") extend {
  measure: c is count()
  view: hv is { aggregate: c }
}
source: customers is duckdb.sql("select 1 as id") extend {
  measure: c is count()
  view: v is { aggregate: c }
}
export { customers }
`,
            },
            { explores: ["index.malloy"] },
         );
      });

      it("refuses a document whose tile targets a source off the surface, so a compile that passes also runs", async () => {
         const { document, problems } = await env.compileSource(
            "pkg",
            "index.malloy",
            `## artifact { tiles=["helper -> hv"] }\n`,
         );
         expect(document).toBeUndefined();
         expect(problems.map((p) => p.code)).toEqual(["query-not-queryable"]);
      });

      it("admits a document whose tiles read the surface", async () => {
         const result = await env.compileSource(
            "pkg",
            "index.malloy",
            `## artifact { tiles=["customers -> v"] }\n`,
         );
         expect(result.problems).toEqual([]);
         expect(tilesOf(result)).toHaveLength(1);
      });
   });
});
