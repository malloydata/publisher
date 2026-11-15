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
import {
   linkTextKeptVerbatim,
   slashed,
} from "../../../src/test_helpers/link_text";
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
         // Both spellings masked, and the names (as long as each other) too:
         // a Windows answer spells the path its own way.
         texts.push(
            withoutPaths(text, there, absent).replace(/[xy]\.malloy/g, "<f>"),
         );
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

describe("a model or notebook file that is itself a link out of the package", () => {
   const MARKER = "TOPSECRET-NOTEBOOK-TEXT";
   let secrets = "";
   beforeAll(() => {
      secrets = path.join(root, "secrets");
      fs.mkdirSync(secrets, { recursive: true });
      fs.writeFileSync(
         path.join(secrets, "notes.malloynb"),
         `>>>markdown\n# ${MARKER}\n>>>malloy\n${SECRET}`,
      );
      fs.writeFileSync(path.join(secrets, "model.malloy"), SECRET);
   });

   /** A package of `ok.malloy` and `link`, a symlink to `target`. */
   function linkedPackage(folder: string, link: string, target: string) {
      const dir = writePackage(folder, { "ok.malloy": BASE });
      fs.symlinkSync(target, path.join(dir, link));
      return dir;
   }

   it("refuses a notebook that links out, leaking none of its text", async () => {
      const refused = await publishing(
         "nblink",
         linkedPackage(
            "nblink",
            "leak.malloynb",
            path.join(secrets, "notes.malloynb"),
         ),
      );
      expectRefusedImport(refused);
      expect(refused.text).not.toContain(MARKER);
      // Nothing was installed to read it back through.
      const reports = await fetch(api("/nblink/reports"));
      expect(await reports.text()).not.toContain(MARKER);
   });

   it("refuses a dangling notebook link out exactly as one whose target exists", async () => {
      // `nopes` is spelled with as many characters as `notes`.
      const texts: string[] = [];
      for (const [folder, target] of [
         ["nbone1", "notes.malloynb"],
         ["nbone2", "nopes.malloynb"],
      ]) {
         const refused = await publishing(
            folder,
            linkedPackage(folder, "leak.malloynb", path.join(secrets, target)),
         );
         expectRefusedImport(refused);
         texts.push(refused.text.replaceAll(folder, "<pkg>"));
      }
      expect(texts[0]).toBe(texts[1]);
   });

   it("refuses a .malloy model that links out with the import refusal, not a 500", async () => {
      const refused = await publishing(
         "mlink",
         linkedPackage(
            "mlink",
            "leak.malloy",
            path.join(secrets, "model.malloy"),
         ),
      );
      expectRefusedImport(refused);
      const dangling = await publishing(
         "mlinx",
         linkedPackage(
            "mlinx",
            "leak.malloy",
            path.join(secrets, "mod3l.malloy"),
         ),
      );
      expectRefusedImport(dangling);
      expect(dangling.text.replaceAll("mlinx", "<pkg>")).toBe(
         refused.text.replaceAll("mlink", "<pkg>"),
      );
      // A directory outside, linked under a model's name, is listed and
      // refused the same way: its existence does not decide whether it loads.
      fs.mkdirSync(path.join(secrets, "dir.malloy"), { recursive: true });
      const directory = await publishing(
         "mlinz",
         linkedPackage(
            "mlinz",
            "leak.malloy",
            path.join(secrets, "dir.malloy"),
         ),
      );
      expectRefusedImport(directory);
      expect(directory.text.replaceAll("mlinz", "<pkg>")).toBe(
         refused.text.replaceAll("mlink", "<pkg>"),
      );
   });

   it("loads a package with a directory link out under a name that is not a model, as one with a link to nothing", async () => {
      fs.mkdirSync(path.join(secrets, "adir"), { recursive: true });
      for (const [folder, target] of [
         ["dlink1", path.join(secrets, "adir")],
         ["dlink2", path.join(secrets, "nope")],
      ]) {
         expect(
            await publish(folder, linkedPackage(folder, "lib", target)),
         ).toBe(200);
      }
   });
});

describe("publisher.json that is a link out of the package", () => {
   const MARKER = "TOPSECRET-MANIFEST";
   let outside = "";
   beforeAll(() => {
      outside = path.join(root, "manifests");
      fs.mkdirSync(path.join(outside, "dir"), { recursive: true });
      fs.writeFileSync(
         path.join(outside, "array.json"),
         JSON.stringify([MARKER, MARKER]),
      );
      fs.writeFileSync(
         path.join(outside, "objct.json"),
         JSON.stringify({ name: "x", description: MARKER }),
      );
      fs.writeFileSync(
         path.join(outside, "dened.json"),
         JSON.stringify({ name: "x", description: MARKER }),
      );
      fs.chmodSync(path.join(outside, "dened.json"), 0);
   });
   afterAll(() => {
      fs.chmodSync(path.join(outside, "dened.json"), 0o644);
   });

   /** A package whose publisher.json is a link to `target`. */
   function linkedManifest(folder: string, target: string): string {
      const dir = writePackage(folder, { "ok.malloy": BASE });
      fs.rmSync(path.join(dir, "publisher.json"));
      fs.symlinkSync(target, path.join(dir, "publisher.json"));
      return dir;
   }

   it("refuses every one identically, naming none of what it leads to", async () => {
      // Package names and targets of one length, so the answers can match.
      const cases: [string, string][] = [
         ["pjarr", path.join(outside, "array.json")],
         ["pjobj", path.join(outside, "objct.json")],
         ["pjmis", path.join(outside, "absnt.json")],
         ["pjdir", path.join(outside, "dir")],
         ["pjden", path.join(outside, "dened.json")],
      ];
      const answers: string[] = [];
      for (const [folder, target] of cases) {
         const refused = await publishing(
            folder,
            linkedManifest(folder, target),
         );
         expect(refused.status).toBeGreaterThanOrEqual(400);
         expect(refused.text).not.toContain(MARKER);
         answers.push(
            `${refused.status} ${refused.text.replaceAll(folder, "<pkg>")}`,
         );
      }
      expect(new Set(answers).size).toBe(1);
      // The server's own path to the package is not in the answer.
      expect(answers[0]).not.toContain("publisher_data");
      const status = await (await fetch(`${baseUrl}/api/v0/status`)).text();
      expect(status).not.toContain(MARKER);
   });
});

describe("a copy-installed package's relative in-package links", () => {
   it("stay relative, so the copy loads and serves through them", async () => {
      const dir = writePackage("rellinks", {
         "shared/s.malloy": BASE,
         "shared/t.malloy": `source: more is duckdb.sql("SELECT 2 AS answer")\n`,
         "m.malloy":
            'import "models/s.malloy"\nimport "models/sh/t.malloy"\nsource: n2 is numbers\nsource: m2 is more\n',
      });
      fs.mkdirSync(path.join(dir, "models"));
      fs.symlinkSync(
         "../shared/s.malloy",
         path.join(dir, "models", "s.malloy"),
      );
      fs.symlinkSync("../shared", path.join(dir, "models", "sh"));
      expect(await publish("rellinks", dir)).toBe(200);
      const served = await packagePath("rellinks");
      expect(
         slashed(fs.readlinkSync(path.join(served, "models", "s.malloy"))),
      ).toBe("../shared/s.malloy");
      expect(
         await answerOf(
            "rellinks",
            "m.malloy",
            "run: n2 -> { select: answer }",
         ),
      ).toBe(1);
      expect(
         await answerOf(
            "rellinks",
            "m.malloy",
            "run: m2 -> { select: answer }",
         ),
      ).toBe(2);
   });
});

describe("a .. after a symlinked directory, resolved as the kernel resolves it", () => {
   // pkg/x/y/sub -> <root>/dd/a/b, so `x/y/sub/../../../<name>` leads to
   // <root>/dd/<name>, outside the package. Only `pres.json` and `pres.malloy`
   // exist there. Each package also holds a file of that name inside it,
   // which a resolution by spelling would land on.
   let dd = "";
   beforeAll(() => {
      dd = path.join(root, "dd");
      fs.mkdirSync(path.join(dd, "a", "b"), { recursive: true });
      fs.writeFileSync(path.join(dd, "pres.json"), '{"name": "x"}');
      fs.writeFileSync(path.join(dd, "pres.malloy"), SECRET);
   });

   function viaLink(
      folder: string,
      inside: string,
      link: string,
      name: string,
   ) {
      const dir = writePackage(folder, { "ok.malloy": BASE });
      fs.mkdirSync(path.join(dir, "x", "y"), { recursive: true });
      fs.symlinkSync(path.join(dd, "a", "b"), path.join(dir, "x", "y", "sub"));
      fs.writeFileSync(path.join(dir, name), inside);
      if (link === "publisher.json")
         fs.rmSync(path.join(dir, "publisher.json"));
      fs.symlinkSync(`x/y/sub/../../../${name}`, path.join(dir, link));
      return dir;
   }

   /** The two answers, present and absent, with the package names masked. */
   async function pair(
      make: (folder: string, name: string) => string,
      ext: string,
   ): Promise<string[]> {
      const answers: string[] = [];
      for (const [folder, name] of [
         [`dd${ext}p`, `pres.${ext}`],
         [`dd${ext}a`, `abst.${ext}`],
      ]) {
         const res = await publishing(folder, make(folder, name));
         answers.push(
            `${res.status} ${res.text.replaceAll(folder, "<pkg>").replaceAll(name, "<name>")}`,
         );
      }
      return answers;
   }

   it("answers the same for a manifest whether the outside file exists or not", async () => {
      const [present, absent] = await pair(
         (folder, name) =>
            viaLink(
               folder,
               JSON.stringify({ name: folder }),
               "publisher.json",
               name,
            ),
         "json",
      );
      expect(present).toBe(absent);
      expect(present).not.toStartWith("2");
   });

   it("answers the same for an import whether the outside file exists or not", async () => {
      const [present, absent] = await pair((folder, name) => {
         const dir = viaLink(folder, BASE, "l.malloy", name);
         fs.writeFileSync(
            path.join(dir, "m.malloy"),
            'import "l.malloy"\nsource: n2 is numbers\n',
         );
         return dir;
      }, "malloy");
      expect(present).toBe(absent);
      expect(present).toContain(IMPORT_OUTSIDE_PACKAGE_MESSAGE);
   });

   it("answers the same for a model file whether the outside file exists or not", async () => {
      const [present, absent] = await pair(
         (folder, name) => viaLink(folder, BASE, "x.malloy", name),
         "mdl",
      );
      expect(present).toBe(absent);
      expect(present).toContain(IMPORT_OUTSIDE_PACKAGE_MESSAGE);
   });
});

describe("a publish whose publisher.json is a link inside the package", () => {
   it("answers 2xx and records the location", async () => {
      const dir = writePackage("pjinside", { "ok.malloy": BASE });
      fs.mkdirSync(path.join(dir, "conf"));
      fs.renameSync(
         path.join(dir, "publisher.json"),
         path.join(dir, "conf", "manifest.json"),
      );
      fs.symlinkSync("conf/manifest.json", path.join(dir, "publisher.json"));
      const published = await publishing("pjinside", dir);
      expect(published.status).toBe(200);
      const pkg = (await (await fetch(api("/pjinside"))).json()) as {
         location?: string;
      };
      expect(pkg.location).toBe(dir);
   });
});

describe("the data probe of a package's files", () => {
   it("probes only regular files inside the package, never a link out", async () => {
      const outside = path.join(root, "outdata");
      fs.mkdirSync(path.join(outside, "dir.csv"), { recursive: true });
      fs.writeFileSync(path.join(outside, "secret.csv"), "SECRETCOLUMN\n1\n");
      fs.writeFileSync(
         path.join(outside, "dir.csv", "inner.csv"),
         "SECRETCOLUMN\n1\n",
      );
      const dir = writePackage("probe", {
         "ok.malloy": BASE,
         "own.csv": "owncolumn\n1\n",
      });
      fs.symlinkSync(
         path.join(outside, "secret.csv"),
         path.join(dir, "leak.csv"),
      );
      fs.symlinkSync(path.join(outside, "dir.csv"), path.join(dir, "dir.csv"));
      fs.symlinkSync(path.join(outside, "dir.csv"), path.join(dir, "data"));
      // A file of the package's own the probe cannot read: listed with its
      // error, which names no server path.
      fs.writeFileSync(path.join(dir, "bad.parquet"), "not a parquet file");
      expect(await publish("probe", dir)).toBe(200);
      const res = await fetch(api("/probe/databases"));
      const text = await res.text();
      expect(res.status).toBe(200);
      expect(text).toContain("own.csv");
      expect(text).not.toContain("SECRETCOLUMN");
      expect(text).not.toContain("leak.csv");
      expect(text).not.toContain("dir.csv");
      expect(text).toContain("bad.parquet");
      // Named by its package-relative path, with the connection's reason,
      // never the SQL the probe compiled.
      expect(text).toContain("Cannot read bad.parquet: ");
      expect(text).not.toContain("duckdb.table");
      expect(text).not.toContain("publisher_data");
   });
});

describe("a path that goes out of the package and comes back", () => {
   // `../orx/../<pkg>/…` passes through a sibling package that exists (orx)
   // or does not (ory), and returns into the package itself; the absolute
   // form spells the environment's own path. Either way the walk refuses at
   // the sibling, before looking at it, so the two answers match.
   let envPath = "";
   beforeAll(async () => {
      expect(
         await publish("orx", writePackage("orx", { "ok.malloy": BASE })),
      ).toBe(200);
      const { bootMainServer } = await import("../../harness/main_server");
      const { environmentStore } = await bootMainServer();
      envPath = (
         await environmentStore.getEnvironment(parseEnvironmentName(ENV_NAME))
      ).getEnvironmentPath();
   });

   /** The link text through `via` back to `<pkg>/<rest>`, in `form`. */
   const text = (form: string, via: string, pkg: string, rest: string) =>
      form === "rel"
         ? `../${via}/../${pkg}/${rest}`
         : `${envPath}/${via}/../${pkg}/${rest}`;

   /** Publish one package per sibling, masked so the answers can be compared. */
   async function pair(
      tag: string,
      make: (dir: string, folder: string, via: string) => void,
   ): Promise<string[]> {
      const answers: string[] = [];
      for (const via of ["orx", "ory"]) {
         const folder = `${tag}${via.slice(-1)}`;
         const dir = writePackage(folder, {
            "ok.malloy": BASE,
            "lib/real.malloy": BASE,
            "conf.json": JSON.stringify({ name: folder }),
            "real.csv": "owncolumn\n1\n",
         });
         make(dir, folder, via);
         const res = await publishing(folder, dir);
         let databases = "";
         if (res.status === 200) {
            databases = await (await fetch(api(`/${folder}/databases`))).text();
         }
         // A served package is compared by what its data probe lists (its
         // own body carries a revision id); a refused one by its refusal.
         answers.push(
            (res.status === 200
               ? `200 ${databases}`
               : `${res.status} ${res.text}`
            )
               .replaceAll(folder, "<pkg>")
               .replaceAll(via, "<via>"),
         );
      }
      return answers;
   }

   for (const form of ["rel", "abs"]) {
      it(`answers the same for an import, a model file, the manifest and the data probe (${form})`, async () => {
         const imp = await pair(`i${form}`, (dir, folder, via) => {
            fs.symlinkSync(
               text(form, via, folder, "lib/real.malloy"),
               path.join(dir, "probe.malloy"),
            );
            fs.writeFileSync(
               path.join(dir, "m.malloy"),
               'import "probe.malloy"\nsource: n2 is numbers\n',
            );
         });
         expect(imp[0]).toBe(imp[1]);
         // Where a link keeps its text the walk refuses at the sibling;
         // where it was normalized when made (Windows) it never spells a
         // step out, and leads to the package's own file. The pairs match
         // either way.
         const kept = linkTextKeptVerbatim();
         if (kept) expect(imp[0]).toContain(IMPORT_OUTSIDE_PACKAGE_MESSAGE);
         else expect(imp[0]).toStartWith("200");

         const model = await pair(`m${form}`, (dir, folder, via) => {
            fs.symlinkSync(
               text(form, via, folder, "lib/real.malloy"),
               path.join(dir, "x.malloy"),
            );
         });
         expect(model[0]).toBe(model[1]);
         if (kept) expect(model[0]).toContain(IMPORT_OUTSIDE_PACKAGE_MESSAGE);
         else expect(model[0]).toStartWith("200");

         const manifest = await pair(`j${form}`, (dir, folder, via) => {
            fs.rmSync(path.join(dir, "publisher.json"));
            fs.symlinkSync(
               text(form, via, folder, "conf.json"),
               path.join(dir, "publisher.json"),
            );
         });
         expect(manifest[0]).toBe(manifest[1]);
         if (kept) expect(manifest[0]).not.toStartWith("200");
         else expect(manifest[0]).toStartWith("200");

         const probe = await pair(`d${form}`, (dir, folder, via) => {
            fs.symlinkSync(
               text(form, via, folder, "real.csv"),
               path.join(dir, "d.csv"),
            );
         });
         expect(probe[0]).toBe(probe[1]);
         expect(probe[0]).toStartWith("200");
         expect(probe[0]).toContain("real.csv");
         if (kept) expect(probe[0]).not.toContain("d.csv");
         else expect(probe[0]).toContain("d.csv");
      });
   }
});

describe("a publish whose manifest update fails after the install committed", () => {
   it("answers 200, serves, and records the location", async () => {
      // A manifest naming another package, so the install rewrites it, and
      // read-only, so that rewrite fails after the swap.
      const dir = writePackage(
         "ropub",
         { "ok.malloy": BASE },
         { name: "other" },
      );
      fs.chmodSync(path.join(dir, "publisher.json"), 0o444);
      const published = await publishing("ropub", dir);
      expect(published.status).toBe(200);
      expect(
         await answerOf(
            "ropub",
            "ok.malloy",
            "run: numbers -> { select: answer }",
         ),
      ).toBe(1);
      const pkg = (await (await fetch(api("/ropub"))).json()) as {
         location?: string;
      };
      expect(pkg.location).toBe(dir);
   });
});

describe("the remaining lookups through an author's link answer the same whatever it leads to", () => {
   // <root>/rest/present exists, <root>/rest/absent does not.
   let rest = "";
   beforeAll(() => {
      rest = path.join(root, "rest");
      fs.mkdirSync(path.join(rest, "present"), { recursive: true });
      fs.writeFileSync(
         path.join(rest, "present", "page.html"),
         "<p>SECRETPAGE</p>",
      );
      fs.writeFileSync(path.join(rest, "present", "prompt.md"), "SECRETPROMPT");
   });

   it("a public/ link out answers 403 whether or not its target exists", async () => {
      const answers: string[] = [];
      for (const [folder, which] of [
         ["pubp", "present"],
         ["puba", "absent_"],
      ]) {
         const dir = writePackage(folder, {
            "ok.malloy": BASE,
            "public/index.html": "<p>own</p>",
         });
         fs.symlinkSync(
            path.join(rest, which, "page.html"),
            path.join(dir, "public", "leak.html"),
         );
         expect(await publish(folder, dir)).toBe(200);
         const res = await fetch(
            `${baseUrl}/environments/${ENV_NAME}/packages/${folder}/leak.html`,
         );
         const body = await res.text();
         expect(body).not.toContain("SECRETPAGE");
         answers.push(`${res.status} ${body.replaceAll(folder, "<pkg>")}`);
         const list = await (await fetch(api(`/${folder}/data-apps`))).text();
         expect(list).not.toContain("leak.html");
      }
      expect(answers[0]).toBe(answers[1]);
      expect(answers[0]).toStartWith("403");
   });

   it("a retrieval prompt that links out is refused the same whether or not its target exists", async () => {
      const answers: string[] = [];
      for (const [folder, which] of [
         ["prmp", "present"],
         ["prma", "absent_"],
      ]) {
         const dir = writePackage(
            folder,
            { "ok.malloy": BASE },
            {
               name: folder,
               retrieval: { prompts: { judge: "prompts/judge.md" } },
            },
         );
         fs.mkdirSync(path.join(dir, "prompts"));
         fs.symlinkSync(
            path.join(rest, which, "prompt.md"),
            path.join(dir, "prompts", "judge.md"),
         );
         const res = await publishing(folder, dir);
         expect(res.text).not.toContain("SECRETPROMPT");
         answers.push(`${res.status} ${res.text.replaceAll(folder, "<pkg>")}`);
      }
      expect(answers[0]).toBe(answers[1]);
      expect(answers[0]).not.toStartWith("200");
   });

   it("a directory link that goes out and back lists, hashes and loads the same", async () => {
      const outcomes: string[] = [];
      for (const [folder, which] of [
         ["tlsp", "present"],
         ["tlsa", "absent_"],
      ]) {
         const dir = writePackage(folder, {
            "ok.malloy": BASE,
            "sub/a.malloy": BASE,
         });
         // Through <root>/rest/<which>, then back up and down to the served
         // copy's own sub/: absolute, as the copy keeps it verbatim.
         const { bootMainServer } = await import("../../harness/main_server");
         const { environmentStore } = await bootMainServer();
         const servedSub = path.join(
            (
               await environmentStore.getEnvironment(
                  parseEnvironmentName(ENV_NAME),
               )
            ).getEnvironmentPath(),
            folder,
            "sub",
         );
         const through = path.join(rest, which);
         fs.symlinkSync(
            `${through}/${path.relative(through, servedSub)}`,
            path.join(dir, "d"),
         );
         const res = await publishing(folder, dir);
         expect(res.status).toBe(200);
         const models =
            res.status === 200
               ? await (await fetch(api(`/${folder}/models`))).text()
               : "";
         outcomes.push(`${res.status} ${models}`.replaceAll(folder, "<pkg>"));
      }
      expect(outcomes[0]).toBe(outcomes[1]);
   });
});

describe("a public/ that is itself a link", () => {
   // <root>/pubout/present holds secret.html; <root>/pubout/absent_ does not exist.
   let pubout = "";
   beforeAll(() => {
      pubout = path.join(root, "pubout");
      fs.mkdirSync(path.join(pubout, "present"), { recursive: true });
      fs.writeFileSync(
         path.join(pubout, "present", "secret.html"),
         "<p>SECRETPUBLIC</p>",
      );
   });

   const page = (folder: string, file: string) =>
      fetch(`${baseUrl}/environments/${ENV_NAME}/packages/${folder}/${file}`);

   /** What serving `file` and listing the data apps answer, masked. */
   async function answer(folder: string, file: string): Promise<string> {
      const res = await page(folder, file);
      const body = await res.text();
      const list = await (await fetch(api(`/${folder}/data-apps`))).text();
      expect(body).not.toContain("SECRETPUBLIC");
      expect(list).not.toContain("secret.html");
      return `${res.status} ${body} | ${list}`.replaceAll(folder, "<pkg>");
   }

   it("serves nothing and lists nothing for a public/ that links out, whether or not its target exists", async () => {
      const answers: string[] = [];
      for (const [folder, which] of [
         ["plkp", "present"],
         ["plka", "absent_"],
      ]) {
         const dir = writePackage(folder, { "ok.malloy": BASE });
         fs.symlinkSync(path.join(pubout, which), path.join(dir, "public"));
         expect(await publish(folder, dir)).toBe(200);
         answers.push(await answer(folder, "secret.html"));
      }
      expect(answers[0]).toBe(answers[1]);
      expect(answers[0]).toStartWith("403");
   });

   it("does the same for a public/ that goes out and comes back", async () => {
      const { bootMainServer } = await import("../../harness/main_server");
      const { environmentStore } = await bootMainServer();
      const envPath = (
         await environmentStore.getEnvironment(parseEnvironmentName(ENV_NAME))
      ).getEnvironmentPath();
      const answers: string[] = [];
      for (const [folder, which] of [
         ["pbkp", "present"],
         ["pbka", "absent_"],
      ]) {
         const dir = writePackage(folder, {
            "ok.malloy": BASE,
            "web/index.html": "<p>own</p>",
         });
         const through = path.join(pubout, which);
         fs.symlinkSync(
            `${through}/${path.relative(through, path.join(envPath, folder, "web"))}`,
            path.join(dir, "public"),
         );
         expect(await publish(folder, dir)).toBe(200);
         answers.push(await answer(folder, "index.html"));
      }
      expect(answers[0]).toBe(answers[1]);
      // Normalized when made (Windows), the link leads to the package's own
      // web/ and serves; kept (POSIX), it is refused. Alike either way.
      if (linkTextKeptVerbatim()) expect(answers[0]).toStartWith("403");
      else expect(answers[0]).toStartWith("200");
   });

   it("serves nothing for a public/ that links to the filesystem root", async () => {
      const dir = writePackage("plroot", { "ok.malloy": BASE });
      fs.symlinkSync("/", path.join(dir, "public"));
      expect(await publish("plroot", dir)).toBe(200);
      const res = await page("plroot", "etc/hosts");
      const body = await res.text();
      expect(res.status).toBe(403);
      expect(body).not.toContain("localhost");
   });

   it("still serves and lists a public/ that links inside the package", async () => {
      const dir = writePackage("plweb", {
         "ok.malloy": BASE,
         "web/index.html": "<title>Own app</title><p>own</p>",
      });
      fs.symlinkSync("web", path.join(dir, "public"));
      expect(await publish("plweb", dir)).toBe(200);
      const res = await page("plweb", "index.html");
      expect(res.status).toBe(200);
      expect(await res.text()).toContain("own");
      expect(await (await fetch(api("/plweb/data-apps"))).text()).toContain(
         "index.html",
      );
   });
});
