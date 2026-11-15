// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/// <reference types="bun-types" />

/**
 * A package's model reads its own package's files and nothing else. An
 * `import` that leaves the package root (its version root, for a versioned
 * package), by `../`, by an absolute path or through a symlink, is refused
 * with one fixed message that names no path and reads the same whether or not
 * the target exists, so it is neither a read of another package's model nor a
 * probe of the server's disk. Checked over HTTP on every read path a model
 * compile takes: the load, a reload, `/compile` at each scope, and MCP's
 * `compile_model`.
 */

import { Client } from "@modelcontextprotocol/sdk/client/index.js";
import { StreamableHTTPClientTransport } from "@modelcontextprotocol/sdk/client/streamableHttp.js";
import { afterAll, afterEach, beforeAll, describe, expect, it } from "bun:test";
import fs from "fs";
import http from "http";
import os from "os";
import path from "path";
import { RestE2EEnv, startRestE2E } from "../../harness/rest_e2e";
import {
   parseEnvironmentName,
   parsePackageName,
} from "../../../src/service/identity";
import { installVariables } from "../../../src/test_helpers/settings_env";
import { writePackageFiles } from "../../../src/test_helpers/temp_package";
import { IMPORT_OUTSIDE_PACKAGE_MESSAGE } from "../../../src/utils";
import { pollFor, TERMINAL_STATUSES } from "../materialization/polling";

const ENV_NAME = "import-confinement-env";
const FLAG = "PUBLISHER_PACKAGE_VERSIONING";

let env: (RestE2EEnv & { stop(): Promise<void> }) | undefined;
let baseUrl = "";
let root = "";
let mcpServer: http.Server | undefined;
let mcpClient: Client | undefined;
let restoreSettings: (() => void) | undefined;

function versioning(value: "on" | "off"): void {
   restoreSettings?.();
   restoreSettings = installVariables({ [FLAG]: value });
}

const SECRET = `source: secret is duckdb.sql("SELECT 7 AS answer")\n`;
const BASE = `source: numbers is duckdb.sql("SELECT 1 AS answer")\n`;

/** A package folder under the suite's temp root, with `files`. */
function writePackage(
   folder: string,
   files: Record<string, string>,
   manifest: Record<string, unknown> = { name: folder },
): string {
   const dir = path.join(root, folder);
   fs.rmSync(dir, { recursive: true, force: true });
   writePackageFiles(dir, files, { manifest });
   return dir;
}

const api = (sub: string) =>
   `${baseUrl}/api/v0/environments/${ENV_NAME}/packages${sub}`;

/** Publish `location` as `name`: the status, and the body as text. */
async function publishing(
   name: string,
   location: string,
): Promise<{ status: number; text: string }> {
   const res = await fetch(api(""), {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: JSON.stringify({ name, location }),
   });
   return { status: res.status, text: await res.text() };
}

async function publish(name: string, location: string): Promise<number> {
   return (await publishing(name, location)).status;
}

/** A publish refused because a model's import left the package. */
function expectRefusedImport(refused: { status: number; text: string }) {
   expect(refused.status).toBe(424);
   expect(refused.text).toContain(IMPORT_OUTSIDE_PACKAGE_MESSAGE);
   expect(refused.text).not.toContain("SELECT 7");
}

/** The package's served directory, as the server holds it. */
async function packagePath(name: string): Promise<string> {
   const { bootMainServer } = await import("../../harness/main_server");
   const { environmentStore } = await bootMainServer();
   const environment = await environmentStore.getEnvironment(
      parseEnvironmentName(ENV_NAME),
   );
   return (
      await environment.getPackage(parsePackageName(name))
   ).getPackagePath();
}

async function answerOf(pkg: string, model: string, query: string) {
   const res = await fetch(api(`/${pkg}/models/${model}/query`), {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: JSON.stringify({ query, compactJson: true }),
   });
   const body = (await res.json()) as { result?: string };
   expect(res.status).toBe(200);
   return (JSON.parse(body.result!) as { answer: number }[])[0].answer;
}

async function compile(
   pkg: string,
   model: string,
   body: Record<string, unknown>,
): Promise<string> {
   const res = await fetch(api(`/${pkg}/models/${model}/compile`), {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: JSON.stringify(body),
   });
   return `${res.status} ${await res.text()}`;
}

/** `text` with every spelling of `paths` taken out, to compare two refusals. */
function withoutPaths(text: string, ...paths: string[]): string {
   let out = text;
   for (const p of paths) {
      for (const spelling of [p, encodeURI(p), JSON.stringify(p).slice(1, -1)])
         out = out.replaceAll(spelling, "<path>");
   }
   return out;
}

beforeAll(async () => {
   versioning("off");
   root = fs.realpathSync(
      fs.mkdtempSync(path.join(os.tmpdir(), "import-confinement-")),
   );
   env = await startRestE2E();
   baseUrl = env.baseUrl;
   const created = await fetch(`${baseUrl}/api/v0/environments`, {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: JSON.stringify({ name: ENV_NAME, packages: [], connections: [] }),
   });
   expect(created.status).toBeLessThan(300);

   const { bootMainServer } = await import("../../harness/main_server");
   const { mcp } = await bootMainServer();
   mcpServer = await new Promise<http.Server>((resolve) => {
      const srv = http
         .createServer(mcp)
         .listen(0, "127.0.0.1", () => resolve(srv));
   });
   const { port } = mcpServer.address() as { port: number };
   mcpClient = new Client({ name: "import-confinement", version: "1.0.0" });
   await mcpClient.connect(
      new StreamableHTTPClientTransport(
         new URL(`http://127.0.0.1:${port}/mcp`),
      ),
   );
}, 180_000);

afterEach(() => versioning("off"));

afterAll(async () => {
   await mcpClient?.close().catch(() => undefined);
   mcpServer?.closeAllConnections?.();
   await new Promise<void>((r) =>
      mcpServer ? mcpServer.close(() => r()) : r(),
   );
   await fetch(`${baseUrl}/api/v0/environments/${ENV_NAME}`, {
      method: "DELETE",
   }).catch(() => undefined);
   await env?.stop();
   fs.rmSync(root, { recursive: true, force: true });
   restoreSettings?.();
});

describe("versioning off: a sibling package", () => {
   beforeAll(async () => {
      expect(
         await publish(
            "sibling",
            writePackage("sibling", { "x.malloy": SECRET }),
         ),
      ).toBe(200);
      expect(
         await publish(
            "escaper",
            writePackage("escaper", {
               "ok.malloy": BASE,
               "models/a.malloy": BASE,
               "models/sub/b.malloy": `import "../a.malloy"\nsource: b is numbers\n`,
            }),
         ),
      ).toBe(200);
   }, 120_000);

   it("refuses a ../ into the sibling package at load", async () => {
      expectRefusedImport(
         await publishing(
            "escaper2",
            writePackage("escaper2", {
               "index.malloy": `import "../sibling/x.malloy"\nsource: s is secret\n`,
            }),
         ),
      );
   });

   it("still serves the sibling package itself", async () => {
      expect(
         await answerOf(
            "sibling",
            "x.malloy",
            "run: secret -> { select: answer }",
         ),
      ).toBe(7);
   });

   it("still serves a ../ between the package's own subdirectories", async () => {
      expect(
         await answerOf(
            "escaper",
            "models/sub/b.malloy",
            "run: b -> { select: answer }",
         ),
      ).toBe(1);
   });

   it("refuses the escape at /compile scope file and package, and on MCP compile_model", async () => {
      const escape = `import "../sibling/x.malloy"\nsource: s is secret\n`;
      for (const scope of ["file", "package"]) {
         const text = await compile("escaper", "ok.malloy", {
            source: escape,
            scope,
         });
         expect(text).toContain(IMPORT_OUTSIDE_PACKAGE_MESSAGE);
      }
      const result = (await mcpClient!.callTool({
         name: "compile_model",
         arguments: {
            environmentName: ENV_NAME,
            packageName: "escaper",
            modelPath: "ok.malloy",
            source: escape,
            scope: "file",
         },
      })) as { content: { text: string }[] };
      expect(JSON.stringify(result.content)).toContain(
         IMPORT_OUTSIDE_PACKAGE_MESSAGE,
      );
   });

   it("still compiles append text over a model whose import stays inside (the virtual file is served)", async () => {
      const text = await compile("escaper", "models/sub/b.malloy", {
         source: "query: q is b -> { select: answer }",
         scope: "append",
      });
      expect(text).toStartWith("200");
      expect(text).not.toContain(IMPORT_OUTSIDE_PACKAGE_MESSAGE);
   });

   it("refuses an absolute path identically whether or not the target exists", async () => {
      const there = path.join(root, "sibling", "x.malloy");
      // Spelled with as many characters, so the problem ranges agree too.
      const absent = path.join(root, "sibling", "y.malloy");
      const texts: string[] = [];
      for (const target of [there, absent]) {
         const text = await compile("escaper", "ok.malloy", {
            source: `import "${target}"\nsource: s is secret\n`,
            scope: "file",
         });
         expect(text).toContain(IMPORT_OUTSIDE_PACKAGE_MESSAGE);
         expect(text).not.toContain("ENOENT");
         texts.push(withoutPaths(text, there, absent));
      }
      expect(texts[0]).toBe(texts[1]);
   });

   it("refuses a symlink inside the served package that leads out of it", async () => {
      const served = await packagePath("escaper");
      const link = path.join(served, "link");
      fs.symlinkSync(path.join(root, "sibling"), link);
      try {
         for (const scope of ["file", "package"]) {
            const text = await compile("escaper", "ok.malloy", {
               source: `import "link/x.malloy"\nsource: s is secret\n`,
               scope,
            });
            expect(text).toContain(IMPORT_OUTSIDE_PACKAGE_MESSAGE);
            expect(text).not.toContain("SELECT 7");
         }
      } finally {
         fs.rmSync(link);
      }
   });

   it("refuses the escape on a reload, and keeps serving the model compiled before it", async () => {
      // A reload of a package published from a location reads it again.
      const added = path.join(root, "escaper", "late.malloy");
      fs.writeFileSync(
         added,
         `import "../sibling/x.malloy"\nsource: s is secret\n`,
      );
      try {
         const res = await fetch(api(`/escaper?reload=true`));
         expectRefusedImport({ status: res.status, text: await res.text() });
         expect(
            await answerOf(
               "escaper",
               "ok.malloy",
               "run: numbers -> { select: answer }",
            ),
         ).toBe(1);
      } finally {
         fs.rmSync(added);
      }
   });
});

describe("versioning on: beside the version root", () => {
   it("refuses a ../ out of the version root into the package's own directory", async () => {
      versioning("on");
      const v1 = writePackage(
         "versioned",
         { "ok.malloy": BASE },
         { name: "versioned", version: "1.0.0" },
      );
      expect(await publish("versioned", v1)).toBe(200);
      // publisher_data/<env>/versioned/q/x.malloy, beside the version root.
      const versionRoot = await packagePath("versioned");
      const beside = path.join(path.dirname(versionRoot), "q");
      fs.mkdirSync(beside, { recursive: true });
      fs.writeFileSync(path.join(beside, "x.malloy"), SECRET);

      const v2 = writePackage(
         "versioned",
         {
            "ok.malloy": BASE,
            "index.malloy": `import "../q/x.malloy"\nsource: s is secret\n`,
         },
         { name: "versioned", version: "1.0.1" },
      );
      expectRefusedImport(await publishing("versioned", v2));
      // The published version still serves.
      expect(
         await answerOf(
            "versioned",
            "ok.malloy",
            "run: numbers -> { select: answer }",
         ),
      ).toBe(1);
   }, 120_000);
});

describe("generated companions", () => {
   it("builds and serves a #(materialize) source imported across the package's directories", async () => {
      // The generated build source and the served copy are virtual files
      // compiled beside the author's: they must still read the author's
      // files, through a ../ that stays inside.
      expect(
         await publish(
            "built",
            writePackage("built", {
               "models/orders.malloy":
                  '#(materialize) name="confined_orders"\n' +
                  `source: orders is duckdb.sql("SELECT 1 AS id, 5 AS answer") extend { primary_key: id }\n`,
               "models/sub/rollup.malloy": `import "../orders.malloy"\nsource: rollup is orders\n`,
            }),
         ),
      ).toBe(200);
      const pkg = (await (await fetch(api("/built"))).json()) as {
         buildPlan?: { sources?: Record<string, { name?: string }> };
      };
      expect(
         Object.values(pkg.buildPlan?.sources ?? {}).map((s) => s.name),
      ).toEqual(["orders"]);

      const created = (await (
         await fetch(api("/built/materializations"), {
            method: "POST",
            headers: { "content-type": "application/json" },
            body: "{}",
         })
      ).json()) as { id: string };
      const run = await pollFor(
         async () =>
            (await (
               await fetch(api(`/built/materializations/${created.id}`))
            ).json()) as { status: string; error?: string | null },
         (m) => TERMINAL_STATUSES.includes(m.status),
         "the build did not settle",
         120_000,
      );
      expect(run.error ?? null).toBeNull();
      expect(run.status).toBe("MANIFEST_FILE_READY");

      // Served from a model one directory down, which reads it through ../.
      const res = await fetch(
         api("/built/models/models/sub/rollup.malloy/query"),
         {
            method: "POST",
            headers: { "content-type": "application/json" },
            body: JSON.stringify({
               query: "run: orders -> { select: answer }",
               compactJson: true,
            }),
         },
      );
      const body = (await res.json()) as {
         result: string;
         path?: { servedBy?: { kind?: string } };
      };
      expect(res.status).toBe(200);
      expect((JSON.parse(body.result) as { answer: number }[])[0].answer).toBe(
         5,
      );
      expect(body.path?.servedBy?.kind).toBe("materialized");
   }, 180_000);
});
