// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/// <reference types="bun-types" />

/**
 * The report write endpoint, `PUT …/models/{path}`, over real HTTP: the route,
 * its status codes and JSON bodies, and that a written file is served
 * afterwards — the contract external callers and the skills depend on. What
 * the controller refuses, and why, is `dashboard.controller.write.spec`'s;
 * what the service does on disk is `environment_write_model_file.spec`'s.
 *
 * Runs against a copy of the dashboards fixture, since these tests write to it.
 */

import { afterAll, beforeAll, describe, expect, it } from "bun:test";
import fsSync from "fs";
import fs from "fs/promises";
import path from "path";
import {
   conversionRefused,
   convertLegacyNotebook,
} from "../../../../sdk/src/components/ReportBuilder/legacyNotebook";
import { newNotebookSource } from "../../../../sdk/src/components/DocumentCreate/newNotebook";
import {
   fixture,
   hashOf,
   startPackageEnv,
   type PackageEnv,
} from "../../harness/reports";

const PKG = "dashboards-test";

/** A dashboard that compiles against the fixture's `orders` model. */
const dashboardSource = (title: string) => `##! experimental.givens
##" Written by the write-path integration test.
## artifact { layout=grid title="${title}" tiles=["written"] } dashboard {columns=12}
import { orders } from '../orders.malloy'

query: written is orders -> {
   aggregate:
      # label="Orders"
      # colspan=12
      order_count
}
`;

describe("PUT model source: dashboards", () => {
   let env: PackageEnv;
   /** The hash of the text currently on disk, as the last write reported it. */
   let currentHash: string;
   const put = (modelPath: string, body: unknown) => env.put(modelPath, body);
   const reportNames = async (): Promise<string[]> =>
      (await env.getJson<{ name?: string }[]>(env.api("/reports"))).map(
         (r) => r.name ?? "",
      );

   /** A dashboard's title as the package currently serves it. */
   const titleOf = async (name: string): Promise<string> =>
      (
         await env.getJson<{ title?: string }>(
            env.api(`/reports/dashboards/${name}.malloy`),
         )
      ).title ?? "";

   /** The text the package holds for a file, or undefined when it has none. */
   const sourceOf = async (modelPath: string): Promise<string | undefined> => {
      const res = await fetch(env.api(`/models/${modelPath}`));
      if (!res.ok) return undefined;
      return ((await res.json()) as { sourceText?: string }).sourceText;
   };

   beforeAll(async () => {
      env = await startPackageEnv(
         "dashboard-write-env",
         { [PKG]: fixture(PKG) },
         { copy: true },
      );
   }, 300_000);

   afterAll(() => env?.stop());

   it("creates a new dashboard with 201, and serves it immediately", async () => {
      const source = dashboardSource("Created");
      const res = await put("dashboards/created.malloy", { source });
      expect(res.status).toBe(201);

      const body = (await res.json()) as {
         resource: string;
         path: string;
         contentHash: string;
         created: boolean;
      };
      expect(body.created).toBe(true);
      expect(body.path).toBe("dashboards/created.malloy");
      expect(body.contentHash).toBe(hashOf(source));
      currentHash = body.contentHash;
      expect(body.resource).toBe(
         `/api/v0/environments/dashboard-write-env/packages/${PKG}/models/dashboards/created.malloy`,
      );

      // The point of reloading in place: the dashboard is discoverable and
      // running without anyone restarting or reloading the package.
      expect(await reportNames()).toContain("created");
   });

   it("refuses to create over an existing file with 409, and changes nothing", async () => {
      const res = await put("dashboards/created.malloy", {
         source: dashboardSource("Should not land"),
      });
      expect(res.status).toBe(409);
      expect((await res.text()).toLowerCase()).toContain("already exists");
      expect(await titleOf("created")).toBe("Created");
   });

   it("replaces with 200 when the hash matches what the caller opened", async () => {
      // The hash the create handed back IS what the caller opened: a client
      // saves, keeps the hash, and saves again without re-reading.
      const next = dashboardSource("Replaced");
      const res = await put("dashboards/created.malloy", {
         source: next,
         expectedHash: currentHash,
      });
      expect(res.status).toBe(200);
      const body = (await res.json()) as {
         created: boolean;
         contentHash: string;
      };
      expect(body.created).toBe(false);
      expect(body.contentHash).toBe(hashOf(next));
      currentHash = body.contentHash;

      expect(await titleOf("created")).toBe("Replaced");
   });

   it("refuses a stale hash with 409, without merging", async () => {
      const res = await put("dashboards/created.malloy", {
         source: dashboardSource("From a stale read"),
         expectedHash: hashOf("text that was never on disk"),
      });
      expect(res.status).toBe(409);
      expect((await res.text()).toLowerCase()).toContain("changed");
      // Still the text the previous test wrote.
      expect(await titleOf("created")).toBe("Replaced");
   });

   describe("a dashboard with no tiles yet", () => {
      const EMPTY = `##! experimental.givens
## artifact { title="Empty start" layout=grid tiles=[] }
`;

      it("writes the empty layout, a tile into it, and the empty text back with the returned hash", async () => {
         const created = await put("dashboards/empty-start.malloy", {
            source: EMPTY,
         });
         expect(created.status).toBe(201);
         const first = (await created.json()) as { contentHash: string };
         expect(first.contentHash).toBe(hashOf(EMPTY));

         const withTile = `##! experimental.givens
import { orders } from '../orders.malloy'
## artifact { title="Empty start" layout=grid tiles=["orders -> totals"] }
`;
         const filled = await put("dashboards/empty-start.malloy", {
            source: withTile,
            expectedHash: first.contentHash,
         });
         expect(filled.status).toBe(200);
         const second = (await filled.json()) as { contentHash: string };

         const undone = await put("dashboards/empty-start.malloy", {
            source: EMPTY,
            expectedHash: second.contentHash,
         });
         expect(undone.status).toBe(200);
         expect(await sourceOf("dashboards/empty-start.malloy")).toBe(EMPTY);
      });

      // A tagged report with no tiles is not served yet, but it is still a
      // report: a query run against it must drop the renderer's complaints
      // about tile layout tags the same way the model's own check does.
      it("runs a query in it with no spurious render warning for a tile tag", async () => {
         const written = await put("dashboards/tileless-query.malloy", {
            source: `##! experimental.givens
import { orders } from '../orders.malloy'
## artifact { title="Tileless" layout=grid tiles=[] }

# colspan=6
query: wide is orders -> { aggregate: order_count }
`,
         });
         expect(written.status).toBe(201);
         const res = await fetch(
            env.api("/models/dashboards/tileless-query.malloy/query"),
            {
               method: "POST",
               headers: { "Content-Type": "application/json" },
               body: JSON.stringify({ query: "run: wide" }),
            },
         );
         expect(res.status).toBe(200);
         const body = (await res.json()) as {
            renderLogs?: { message?: string }[];
         };
         expect(body.renderLogs).toBeUndefined();
      });
   });

   it("refuses source that does not compile with 400, naming where", async () => {
      const res = await put("dashboards/broken.malloy", {
         source: `##! experimental.givens
## artifact { layout=grid title="Broken" tiles=["broken"] } dashboard {columns=12}
import { orders } from '../orders.malloy'

query: broken is orders -> { aggregate: no_such_measure }
`,
      });
      expect(res.status).toBe(400);
      const text = await res.text();
      expect(text).toContain("does not compile");
      // A caller fixing this needs a coordinate, not just a complaint.
      expect(text).toMatch(/line \d+:\d+/);

      // Nothing was written, so the package has no such dashboard to serve.
      expect(await reportNames()).not.toContain("broken");
   });

   it("creates the notebook the SDK writes for a new one, create-only", async () => {
      // The template writes its import relative to where the report goes.
      const source = newNotebookSource(
         {
            title: "Created by the SDK",
            modelPath: "orders.malloy",
            source: "orders",
            view: "totals",
         },
         "notebooks/sdk-created.malloy",
      );
      const res = await put("notebooks/sdk-created.malloy", { source });
      expect(res.status).toBe(201);
      expect(await reportNames()).toContain("sdk-created");
   });

   for (const [name, source] of [
      ["crlf", "## artifact { layout=linear }\r\n\r\n##(markdown) Prose.\r\n"],
      [
         "no_final_newline",
         "## artifact { layout=linear }\n\n##(markdown) Prose.",
      ],
      [
         "license_above_flags",
         "// Copyright (c) Example\n// SPDX-License-Identifier: MIT\n##! experimental.givens\n## artifact { layout=linear }\n\n##(markdown) Prose.\n",
      ],
      [
         "late_import",
         "##! experimental.givens\n## artifact { layout=linear }\nimport '../orders.malloy'\n\nrun: orders -> { aggregate: order_count }\n\nimport '../givens.malloy'\n\n##(markdown) After a late import.\n",
      ],
      ["bare_tag", "## artifact\n\n##(markdown) Prose.\n"],
   ] as const) {
      it(`writes a legitimate ${name} notebook and serves it`, async () => {
         const res = await put(`notebooks/${name}.malloy`, { source });
         expect(res.status).toBe(201);
         expect(await reportNames()).toContain(name);
      });
   }

   const GATE_PROSE =
      "Rows are limited by #(access_filter), and a source is locked by #(authorize).";

   it("writes a notebook whose markdown prose names #(authorize) and #(access_filter)", async () => {
      const res = await put("notebooks/gate_prose.malloy", {
         source: `## artifact { layout=linear }\nimport '../orders.malloy'\n\n##|(markdown)\n${GATE_PROSE}\n|##\n\nrun: orders -> { aggregate: order_count }\n`,
      });
      expect(res.status).toBe(201);
   });

   it("writes a notebook whose one-line markdown cell names #(access_filter)", async () => {
      const res = await put("notebooks/gate_line.malloy", {
         source: `## artifact { layout=linear }\nimport '../orders.malloy'\n\n##(markdown) Rows are limited by #(access_filter).\n\nrun: orders -> { aggregate: order_count }\n`,
      });
      expect(res.status).toBe(201);
   });

   it("refuses a notebook that declares a real gate, with 400, and writes nothing", async () => {
      const res = await put("notebooks/gate_real.malloy", {
         source: `## artifact { layout=linear }\nimport '../orders.malloy'\n\n##|(markdown)\n${GATE_PROSE}\n|##\n\n#(authorize) true\nsource: mine is orders extend {}\n\nrun: mine -> { aggregate: order_count }\n`,
      });
      expect(res.status).toBe(400);
      expect(await res.text()).toContain("not permitted in caller-submitted");
      expect(await sourceOf("notebooks/gate_real.malloy")).toBeUndefined();
   });

   const query = (body: unknown) => env.query("orders.malloy", body);

   it("runs a query whose markdown block prose names #(authorize)", async () => {
      const res = await query({
         query: `#|(markdown)\n${GATE_PROSE}\n|#\nrun: orders -> { aggregate: order_count }`,
         compactJson: true,
      });
      expect(res.status).toBe(200);
   });

   it("refuses a query that declares a real gate after its prose, with 400", async () => {
      const res = await query({
         query: `#|(markdown)\n${GATE_PROSE}\n|#\n#(authorize) true\nsource: mine is orders extend {}\nrun: mine -> { aggregate: order_count }`,
         compactJson: true,
      });
      expect(res.status).toBe(400);
      expect(await res.text()).toContain("not permitted in caller-submitted");
   });

   it("refuses a tagged notebook whose statement above the tag fails the file-scope lint, with 400", async () => {
      // A statement above the tag: the notebook lint reports it as an error.
      const res = await put("notebooks/misplaced.malloy", {
         source: `##! experimental.givens
import { orders } from '../orders.malloy'

## artifact { layout=linear }

##(markdown) Prose.
`,
      });
      expect(res.status).toBe(400);
      expect(await res.text()).toContain("does not compile");
   });

   it("refuses a body with no source, with 400", async () => {
      const res = await put("dashboards/nobody.malloy", { notSource: "x" });
      expect(res.status).toBe(400);
      expect(await res.text()).toContain("source");
   });

   it("404s for a package that does not exist", async () => {
      const res = await env.put(
         "dashboards/x.malloy",
         { source: dashboardSource("Nope") },
         "no-such-package",
      );
      expect(res.status).toBe(404);
   });
});

/** The builder's conversion of each legacy cell notebook is written, served and run by a real environment, and restored byte for byte. */
describe("PUT model source: notebooks written by the notebook builder", () => {
   const PLAIN = "notebooks-malloyyo";
   const SURFACE = "notebooks-malloyyo-surface";
   let env: PackageEnv;
   const pkgUrl = (pkg: string, sub: string) => env.api(sub, pkg);

   beforeAll(async () => {
      env = await startPackageEnv(
         "notebook-write-env",
         { [PLAIN]: fixture(PLAIN), [SURFACE]: fixture(SURFACE) },
         { copy: true },
      );
   }, 300_000);

   afterAll(() => env?.stop());

   /** Every cell-format notebook in the fixture; `layout` already lists its tiles and `refused` has a statement no cell can hold. */
   const CELL_NOTEBOOKS = fsSync
      .readdirSync(path.join(fixture(PLAIN), "notebooks"))
      .filter(
         (f) =>
            f.endsWith(".malloy") &&
            !["layout.malloy", "refused.malloy"].includes(f),
      );

   const modelsUrl = (modelPath: string) =>
      pkgUrl(PLAIN, `/models/${modelPath}`);

   const putPlain = (modelPath: string, body: unknown) =>
      env.put(modelPath, body, PLAIN);

   const sourceOf = async (modelPath: string): Promise<string> =>
      (
         (await (await fetch(modelsUrl(modelPath))).json()) as {
            sourceText: string;
         }
      ).sourceText;

   it("covers the fixture's cell notebooks", () => {
      expect(CELL_NOTEBOOKS.length).toBeGreaterThan(5);
   });

   for (const file of CELL_NOTEBOOKS) {
      it(`converts ${file}, serves its tiles and runs each, then restores the original byte for byte`, async () => {
         const modelPath = `notebooks/${file}`;
         const original = await sourceOf(modelPath);
         const converted = await convertLegacyNotebook(original);
         if (conversionRefused(converted)) throw new Error(converted.refused);

         const saved = await putPlain(modelPath, {
            source: converted.text,
            expectedHash: hashOf(original),
         });
         expect(saved.status).toBe(200);

         const report = (await (
            await fetch(pkgUrl(PLAIN, `/reports/${modelPath}`))
         ).json()) as { tiles?: Array<{ kind: string; query?: string }> };
         const tiles = report.tiles ?? [];
         expect(tiles).toHaveLength(converted.tiles);
         for (const tile of tiles.filter((t) => t.kind === "query")) {
            const ran = await env.query(
               modelPath,
               { query: `run: ${tile.query}`, compactJson: true },
               PLAIN,
            );
            expect(ran.status).toBe(200);
         }

         const restored = await putPlain(modelPath, {
            source: original,
            expectedHash: hashOf(converted.text),
         });
         expect(restored.status).toBe(200);
         expect(await sourceOf(modelPath)).toBe(original);
      });
   }

   it("serves a dashboard's text tiles over HTTP, with their markdown", async () => {
      const res = await fetch(
         pkgUrl(PLAIN, "/reports/dashboards/text_tiles.malloy"),
      );
      expect(res.status).toBe(200);
      const manifest = (await res.json()) as {
         tiles?: Array<{
            kind: string;
            name?: string;
            markdown?: string;
            query?: string;
         }>;
      };
      expect(manifest.tiles).toMatchObject([
         { kind: "text", name: "intro", markdown: "## How to read this page" },
         { kind: "query", query: "orders -> kpis" },
      ]);
   });

   it("hands the editor a curated package's notebook text when it asks for the hidden files", async () => {
      const file = "notebooks/cells.malloy";
      const onDisk = await fs.readFile(
         path.join(env.location(SURFACE), file),
         "utf8",
      );
      const withheld = await fetch(pkgUrl(SURFACE, `/models/${file}`));
      expect(withheld.status).toBe(200);
      expect(
         ((await withheld.json()) as { sourceText?: string }).sourceText,
      ).toBeUndefined();
      const res = await fetch(
         pkgUrl(SURFACE, `/models/${file}?includeHiddenFilesAndSources=true`),
      );
      expect(res.status).toBe(200);
      expect(((await res.json()) as { sourceText?: string }).sourceText).toBe(
         onDisk,
      );
   });
});
