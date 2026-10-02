// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/// <reference types="bun-types" />

/**
 * `PUT …/models/dashboards/{slug}.malloy` over real HTTP.
 *
 * The write path had unit coverage at the controller (against a stubbed
 * environment) and at the service (against a real directory), and a browser
 * test that drives the builder. Nothing exercised the endpoint itself: the
 * route, the status codes, the JSON bodies, and the fact that a written file
 * is actually being served afterwards. Those are the contract external callers
 * and the skills depend on, and all of them were assertions nobody had made.
 *
 * Runs against a copy of the dashboards fixture, because these tests write to
 * the package and the fixture in the repository is not the place for that.
 */

import { afterAll, beforeAll, describe, expect, it } from "bun:test";
import { createHash } from "crypto";
import fsSync from "fs";
import fs from "fs/promises";
import os from "os";
import path from "path";
import { fileURLToPath } from "url";
import {
   conversionRefused,
   convertLegacyNotebook,
} from "../../../../sdk/src/components/DashboardBuilder/legacyNotebook";
import { newNotebookSource } from "../../../../sdk/src/components/DocumentCreate/newNotebook";
import { RestE2EEnv, startRestE2E } from "../../harness/rest_e2e";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const FIXTURE = path.resolve(__dirname, "../../fixtures/dashboards-test");
const ENV_NAME = "dashboard-write-env";
const PKG = "dashboards-test";

const hashOf = (text: string) =>
   createHash("sha256").update(text, "utf8").digest("hex");

/** An untagged file under notebooks/: a shared include, not a notebook. */
const SHARED_INCLUDE = "##(markdown) A shared include other models import.\n";
/** An include whose only tag is commented out: the text looks tagged, the compiled model has no note. */
const COMMENTED_INCLUDE =
   "/*\n## artifact { kind=notebook }\n*/\n##(markdown) A shared include.\n";

/** A dashboard that compiles against the fixture's `orders` model. */
const dashboardSource = (title: string) => `##! experimental.givens
import { orders } from '../orders.malloy'

#" Written by the write-path integration test.
# artifact { title="${title}" } dashboard {columns=12}
query: written is orders -> {
   aggregate:
      # label="Orders"
      # colspan=12
      order_count
}
`;

describe("PUT model source: dashboards", () => {
   let env: (RestE2EEnv & { stop(): Promise<void> }) | null = null;
   let baseUrl: string;
   let location: string;
   /** The hash of the text currently on disk, as the last write reported it. */
   let currentHash: string;

   const modelsUrl = (modelPath: string) =>
      `${baseUrl}/api/v0/environments/${ENV_NAME}/packages/${PKG}/models/${modelPath}`;

   /** A dashboard's title as the package currently serves it. */
   const titleOf = async (name: string): Promise<string> => {
      const res = await fetch(
         `${baseUrl}/api/v0/environments/${ENV_NAME}/packages/${PKG}/dashboards/${name}`,
      );
      expect(res.status).toBe(200);
      return ((await res.json()) as { title?: string }).title ?? "";
   };

   /** The text the package holds for a file (its own copy, not the registered location), or undefined when it has none. */
   const sourceOf = async (modelPath: string): Promise<string | undefined> => {
      const res = await fetch(modelsUrl(modelPath));
      if (!res.ok) return undefined;
      return ((await res.json()) as { sourceText?: string }).sourceText;
   };

   const put = (modelPath: string, body: unknown) =>
      fetch(modelsUrl(modelPath), {
         method: "PUT",
         headers: { "Content-Type": "application/json" },
         body: JSON.stringify(body),
      });

   beforeAll(async () => {
      env = await startRestE2E();
      baseUrl = env.baseUrl;
      location = await fs.mkdtemp(
         path.join(os.tmpdir(), "publisher-write-e2e-"),
      );
      await fs.cp(FIXTURE, location, { recursive: true });
      await fs.mkdir(path.join(location, "notebooks"), { recursive: true });
      await fs.writeFile(
         path.join(location, "notebooks/shared.malloy"),
         SHARED_INCLUDE,
      );
      await fs.writeFile(
         path.join(location, "notebooks/commented.malloy"),
         COMMENTED_INCLUDE,
      );

      const created = await fetch(`${baseUrl}/api/v0/environments`, {
         method: "POST",
         headers: { "Content-Type": "application/json" },
         body: JSON.stringify({
            name: ENV_NAME,
            packages: [{ name: PKG, location }],
            connections: [],
         }),
      });
      if (!created.ok) {
         throw new Error(
            `Failed to create test environment (${created.status}): ${await created.text()}`,
         );
      }
      const deadline = Date.now() + 30_000;
      while (Date.now() < deadline) {
         try {
            const res = await fetch(
               `${baseUrl}/api/v0/environments/${ENV_NAME}/packages/${PKG}`,
            );
            if (res.ok) break;
         } catch {
            // not ready yet
         }
         await new Promise((r) => setTimeout(r, 500));
      }
   });

   afterAll(async () => {
      if (baseUrl) {
         await fetch(`${baseUrl}/api/v0/environments/${ENV_NAME}`, {
            method: "DELETE",
         }).catch(() => undefined);
      }
      await env?.stop();
      env = null;
      if (location) await fs.rm(location, { recursive: true, force: true });
   });

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
         `/api/v0/environments/${ENV_NAME}/packages/${PKG}/models/dashboards/created.malloy`,
      );

      // The point of reloading in place: the dashboard is discoverable and
      // running without anyone restarting or reloading the package.
      const listed = await fetch(
         `${baseUrl}/api/v0/environments/${ENV_NAME}/packages/${PKG}/dashboards`,
      );
      expect(listed.status).toBe(200);
      const names = ((await listed.json()) as Array<{ name?: string }>).map(
         (d) => d.name,
      );
      expect(names).toContain("created");
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

   it("refuses source that does not compile with 400, naming where", async () => {
      const res = await put("dashboards/broken.malloy", {
         source: `##! experimental.givens
import { orders } from '../orders.malloy'

# artifact { title="Broken" } dashboard {columns=12}
query: broken is orders -> { aggregate: no_such_measure }
`,
      });
      expect(res.status).toBe(400);
      const text = await res.text();
      expect(text).toContain("does not compile");
      // A caller fixing this needs a coordinate, not just a complaint.
      expect(text).toMatch(/line \d+:\d+/);

      // Nothing was written, so the package has no such dashboard to serve.
      const listed = await fetch(
         `${baseUrl}/api/v0/environments/${ENV_NAME}/packages/${PKG}/dashboards`,
      );
      const names = ((await listed.json()) as Array<{ name?: string }>).map(
         (d) => d.name,
      );
      expect(names).not.toContain("broken");
   });

   it("writes only dashboard files, refusing anything else with 400", async () => {
      for (const badPath of [
         "orders.malloy",
         "dashboards/nested/deep.malloy",
         "dashboards/notebook.malloynb",
      ]) {
         const res = await put(badPath, { source: dashboardSource("Nope") });
         expect(res.status).toBe(400);
      }
   });

   it("writes a tagged notebook under notebooks/ and serves it afterwards", async () => {
      const source = `##! experimental.givens
## artifact { kind=notebook title="Written notebook" }

##(markdown) Written by the write-path integration test.
`;
      const res = await put("notebooks/written.malloy", { source });
      expect(res.status).toBe(201);
      const body = (await res.json()) as { path: string; contentHash: string };
      expect(body.path).toBe("notebooks/written.malloy");
      expect(body.contentHash).toBe(hashOf(source));

      const served = await fetch(modelsUrl("notebooks/written.malloy"));
      expect(served.status).toBe(200);
      expect(
         ((await served.json()) as { sourceText?: string }).sourceText,
      ).toBe(source);
   });

   it("creates the notebook the SDK writes for a new one, create-only", async () => {
      const source = newNotebookSource({
         title: "Created by the SDK",
         modelPath: "orders.malloy",
         source: "orders",
         view: "totals",
      });
      const res = await put("notebooks/sdk-created.malloy", { source });
      expect(res.status).toBe(201);
      const again = await put("notebooks/sdk-created.malloy", { source });
      expect(again.status).toBe(409);
      const served = await fetch(modelsUrl("notebooks/sdk-created.malloy"));
      expect(served.status).toBe(200);
   });

   it("refuses an untagged notebooks/ file, a shared include, with 400", async () => {
      const res = await put("notebooks/untagged.malloy", {
         source: "##(markdown) Just prose, no artifact tag.\n",
      });
      expect(res.status).toBe(400);
      expect(await res.text()).toContain("## artifact");
   });

   it("refuses a tagged write over an existing untagged notebooks/ file with 400, leaving it intact", async () => {
      const res = await put("notebooks/shared.malloy", {
         source: "## artifact { kind=notebook }\n",
         expectedHash: hashOf(SHARED_INCLUDE),
      });
      expect(res.status).toBe(400);
      expect(await res.text()).toContain("shared include");
      const served = await fetch(modelsUrl("notebooks/shared.malloy"));
      expect(
         ((await served.json()) as { sourceText?: string }).sourceText,
      ).toBe(SHARED_INCLUDE);
   });

   it("refuses a tagged write over an include whose only tag is inside a block comment, leaving it intact", async () => {
      const res = await put("notebooks/commented.malloy", {
         source: "## artifact { kind=notebook }\n",
         expectedHash: hashOf(COMMENTED_INCLUDE),
      });
      expect(res.status).toBe(400);
      expect(await res.text()).toContain("shared include");
      expect(await sourceOf("notebooks/commented.malloy")).toBe(
         COMMENTED_INCLUDE,
      );
   });

   it("rolls back a new notebook whose only tag is inside a block comment, so no unserved file lands", async () => {
      const res = await put("notebooks/hidden_tag.malloy", {
         source: "/*\n## artifact { kind=notebook }\n*/\n##(markdown) Prose.\n",
      });
      expect(res.status).toBe(500);
      expect(await sourceOf("notebooks/hidden_tag.malloy")).toBeUndefined();
   });

   it("rolls back a new notebook whose only tag is inside a string, so no unserved file lands", async () => {
      const res = await put("notebooks/string_tag.malloy", {
         source:
            'import \'../orders.malloy\'\nrun: orders -> {\n  select: note is """\n## artifact { kind=notebook }\n"""\n}\n',
      });
      expect(res.status).toBe(500);
      expect(await sourceOf("notebooks/string_tag.malloy")).toBeUndefined();
   });

   /** A tile-layout document of the given kind, which compiles against the fixture's `orders`. */
   const layoutSource = (kind: string, title: string) =>
      `##! experimental.givens\n## artifact { kind=${kind} title="${title}" tiles=["orders -> totals"] }\nimport { orders } from '../orders.malloy'\n`;

   const dashboardPaths = async (): Promise<string[]> => {
      const listed = await fetch(
         `${baseUrl}/api/v0/environments/${ENV_NAME}/packages/${PKG}/dashboards`,
      );
      return ((await listed.json()) as Array<{ path: string }>).map(
         (d) => d.path,
      );
   };

   describe("a slug another file already holds", () => {
      const held = layoutSource("dashboard", "Holder");
      let before: string[];

      beforeAll(async () => {
         const res = await put("dashboards/dup.malloy", { source: held });
         expect(res.status).toBe(201);
         before = await dashboardPaths();
      });

      it("refuses a notebooks/ file that declares kind=dashboard, with 409 and nothing written", async () => {
         const res = await put("notebooks/dup.malloy", {
            source: layoutSource("dashboard", "Shadowed"),
         });
         expect(res.status).toBe(409);
         expect(await res.text()).toContain("already holds the dashboard name");
         expect(await sourceOf("notebooks/dup.malloy")).toBeUndefined();
         expect(await dashboardPaths()).toEqual(before);
      });

      it("refuses flipping a served notebook of that slug to kind=dashboard, leaving the notebook as it was", async () => {
         const notebook = layoutSource("notebook", "Same slug");
         expect(
            (await put("notebooks/dup.malloy", { source: notebook })).status,
         ).toBe(201);
         const res = await put("notebooks/dup.malloy", {
            source: layoutSource("dashboard", "Flipped"),
            expectedHash: hashOf(notebook),
         });
         expect(res.status).toBe(409);
         expect(await sourceOf("notebooks/dup.malloy")).toBe(notebook);
         expect(await dashboardPaths()).toEqual(before);
      });

      it("still re-saves the holder, and flips a notebook of an unheld slug to a dashboard and back", async () => {
         const resaved = layoutSource("dashboard", "Holder again");
         const resave = await put("dashboards/dup.malloy", {
            source: resaved,
            expectedHash: hashOf(held),
         });
         expect(resave.status).toBe(200);

         const notebook = layoutSource("notebook", "Flip me");
         expect(
            (await put("notebooks/flip.malloy", { source: notebook })).status,
         ).toBe(201);
         const asDashboard = layoutSource("dashboard", "Flip me");
         const flipped = await put("notebooks/flip.malloy", {
            source: asDashboard,
            expectedHash: hashOf(notebook),
         });
         expect(flipped.status).toBe(200);
         expect(await dashboardPaths()).toContain("notebooks/flip.malloy");
         const back = await put("notebooks/flip.malloy", {
            source: notebook,
            expectedHash: hashOf(asDashboard),
         });
         expect(back.status).toBe(200);
         expect(await dashboardPaths()).not.toContain("notebooks/flip.malloy");
      });

      it("serves a notebook written into dashboards/ as a notebook, not a dashboard", async () => {
         const res = await put("dashboards/as_notebook.malloy", {
            source: layoutSource("notebook", "Misfiled"),
         });
         expect(res.status).toBe(201);
         expect(await dashboardPaths()).not.toContain(
            "dashboards/as_notebook.malloy",
         );
         const listed = await fetch(
            `${baseUrl}/api/v0/environments/${ENV_NAME}/packages/${PKG}/notebooks`,
         );
         expect(JSON.stringify(await listed.json())).toContain(
            "dashboards/as_notebook.malloy",
         );
      });
   });

   for (const [name, source] of [
      ["crlf", "## artifact { kind=notebook }\r\n\r\n##(markdown) Prose.\r\n"],
      [
         "no_final_newline",
         "## artifact { kind=notebook }\n\n##(markdown) Prose.",
      ],
      [
         "license_above_flags",
         "// Copyright (c) Example\n// SPDX-License-Identifier: MIT\n##! experimental.givens\n## artifact { kind=notebook }\n\n##(markdown) Prose.\n",
      ],
      [
         "late_import",
         "##! experimental.givens\n## artifact { kind=notebook }\nimport '../orders.malloy'\n\nrun: orders -> { aggregate: order_count }\n\nimport '../givens.malloy'\n\n##(markdown) After a late import.\n",
      ],
      ["bare_tag", "## artifact\n\n##(markdown) Prose.\n"],
   ] as const) {
      it(`writes a legitimate ${name} notebook and serves it`, async () => {
         const res = await put(`notebooks/${name}.malloy`, { source });
         expect(res.status).toBe(201);
         const listed = await fetch(
            `${baseUrl}/api/v0/environments/${ENV_NAME}/packages/${PKG}/notebooks`,
         );
         expect(JSON.stringify(await listed.json())).toContain(
            `notebooks/${name}.malloy`,
         );
      });
   }

   const GATE_PROSE =
      "Rows are limited by #(access_filter), and a source is locked by #(authorize).";

   it("writes a notebook whose markdown prose names #(authorize) and #(access_filter)", async () => {
      const res = await put("notebooks/gate_prose.malloy", {
         source: `## artifact { kind=notebook }\nimport '../orders.malloy'\n\n##|(markdown)\n${GATE_PROSE}\n|##\n\nrun: orders -> { aggregate: order_count }\n`,
      });
      expect(res.status).toBe(201);
   });

   it("writes a notebook whose one-line markdown cell names #(access_filter)", async () => {
      const res = await put("notebooks/gate_line.malloy", {
         source: `## artifact { kind=notebook }\nimport '../orders.malloy'\n\n##(markdown) Rows are limited by #(access_filter).\n\nrun: orders -> { aggregate: order_count }\n`,
      });
      expect(res.status).toBe(201);
   });

   it("refuses a notebook that declares a real gate, with 400, and writes nothing", async () => {
      const res = await put("notebooks/gate_real.malloy", {
         source: `## artifact { kind=notebook }\nimport '../orders.malloy'\n\n##|(markdown)\n${GATE_PROSE}\n|##\n\n#(authorize) true\nsource: mine is orders extend {}\n\nrun: mine -> { aggregate: order_count }\n`,
      });
      expect(res.status).toBe(400);
      expect(await res.text()).toContain("not permitted in caller-submitted");
      expect(await sourceOf("notebooks/gate_real.malloy")).toBeUndefined();
   });

   const query = (body: unknown) =>
      fetch(`${modelsUrl("orders.malloy")}/query`, {
         method: "POST",
         headers: { "Content-Type": "application/json" },
         body: JSON.stringify(body),
      });

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

## artifact { kind=notebook }

##(markdown) Prose.
`,
      });
      expect(res.status).toBe(400);
      expect(await res.text()).toContain("does not compile");
   });

   it("refuses a nested notebooks/ path with 400", async () => {
      const res = await put("notebooks/nested/x.malloy", {
         source: "## artifact { kind=notebook }\n",
      });
      expect(res.status).toBe(400);
   });

   it("refuses a body with no source, with 400", async () => {
      const res = await put("dashboards/nobody.malloy", { notSource: "x" });
      expect(res.status).toBe(400);
      expect(await res.text()).toContain("source");
   });

   it("404s for a package that does not exist", async () => {
      const res = await fetch(
         `${baseUrl}/api/v0/environments/${ENV_NAME}/packages/no-such-package/models/dashboards/x.malloy`,
         {
            method: "PUT",
            headers: { "Content-Type": "application/json" },
            body: JSON.stringify({ source: dashboardSource("Nope") }),
         },
      );
      expect(res.status).toBe(404);
   });
});

/** The builder's conversion of each legacy cell notebook is written, served and run by a real environment, and restored byte for byte. */
describe("PUT model source: notebooks written by the notebook builder", () => {
   const NB_ENV = "notebook-write-env";
   const PLAIN = "notebooks-malloyyo";
   const SURFACE = "notebooks-malloyyo-surface";
   let env: (RestE2EEnv & { stop(): Promise<void> }) | null = null;
   let baseUrl: string;
   let root: string;

   const pkgUrl = (pkg: string, sub: string) =>
      `${baseUrl}/api/v0/environments/${NB_ENV}/packages/${pkg}${sub}`;

   beforeAll(async () => {
      env = await startRestE2E();
      baseUrl = env.baseUrl;
      root = await fs.mkdtemp(
         path.join(os.tmpdir(), "publisher-nb-write-e2e-"),
      );
      for (const pkg of [PLAIN, SURFACE])
         await fs.cp(
            path.resolve(__dirname, `../../fixtures/${pkg}`),
            path.join(root, pkg),
            { recursive: true },
         );
      const created = await fetch(`${baseUrl}/api/v0/environments`, {
         method: "POST",
         headers: { "Content-Type": "application/json" },
         body: JSON.stringify({
            name: NB_ENV,
            packages: [PLAIN, SURFACE].map((name) => ({
               name,
               location: path.join(root, name),
            })),
            connections: [],
         }),
      });
      if (!created.ok) {
         throw new Error(
            `Failed to create test environment (${created.status}): ${await created.text()}`,
         );
      }
      for (const pkg of [PLAIN, SURFACE]) {
         const loaded = await fetch(pkgUrl(pkg, ""));
         if (!loaded.ok) {
            throw new Error(`${pkg} did not load (${loaded.status})`);
         }
      }
   });

   afterAll(async () => {
      if (baseUrl) {
         await fetch(`${baseUrl}/api/v0/environments/${NB_ENV}`, {
            method: "DELETE",
         }).catch(() => undefined);
      }
      await env?.stop();
      env = null;
      if (root) await fs.rm(root, { recursive: true, force: true });
   });

   /** Every cell-format notebook in the fixture; `layout` already lists its tiles and `refused` has a statement no cell can hold. */
   const CELL_NOTEBOOKS = fsSync
      .readdirSync(path.resolve(__dirname, `../../fixtures/${PLAIN}/notebooks`))
      .filter(
         (f) =>
            f.endsWith(".malloy") &&
            !["layout.malloy", "refused.malloy"].includes(f),
      );

   const modelsUrl = (modelPath: string) =>
      pkgUrl(PLAIN, `/models/${modelPath}`);

   const putPlain = (modelPath: string, body: unknown) =>
      fetch(modelsUrl(modelPath), {
         method: "PUT",
         headers: { "Content-Type": "application/json" },
         body: JSON.stringify(body),
      });

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

         const notebook = (await (
            await fetch(pkgUrl(PLAIN, `/notebooks/${modelPath}`))
         ).json()) as {
            dashboard?: { tiles?: Array<{ kind: string; query?: string }> };
         };
         const tiles = notebook.dashboard?.tiles ?? [];
         expect(tiles).toHaveLength(converted.tiles);
         for (const tile of tiles.filter((t) => t.kind === "query")) {
            const ran = await fetch(`${modelsUrl(modelPath)}/query`, {
               method: "POST",
               headers: { "Content-Type": "application/json" },
               body: JSON.stringify({
                  query: `run: ${tile.query}`,
                  compactJson: true,
               }),
            });
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
      const res = await fetch(pkgUrl(PLAIN, "/dashboards/text_tiles"));
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
      const onDisk = await fs.readFile(path.join(root, SURFACE, file), "utf8");
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
