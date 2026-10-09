// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/// <reference types="bun-types" />

/**
 * Every package-scoped read route serves the version a request names, and
 * `latest` when it names none, with versioning on (PUBLISHER_PACKAGE_VERSIONING
 * =on). Two versions of one package are published, each answering with its
 * own number from its model, its dashboard title, its notebook text and its
 * data-app page, so what a route returns proves which version's files it read.
 */

import { afterAll, beforeAll, describe, expect, it } from "bun:test";
import fs from "fs";
import os from "os";
import path from "path";
import { RestE2EEnv, startRestE2E } from "../../harness/rest_e2e";

const ENV_NAME = "package-versions-routes-env";
const PKG = "versioned";
const PLAIN = "plain";

let env: (RestE2EEnv & { stop(): Promise<void> }) | undefined;
let baseUrl = "";
let root = "";
const savedFlag = process.env.PUBLISHER_PACKAGE_VERSIONING;

/** One package tree whose every surface names `answer`. */
function writePackage(
   dir: string,
   manifest: Record<string, unknown>,
   answer: number,
): string {
   fs.mkdirSync(path.join(dir, "dashboards"), { recursive: true });
   fs.mkdirSync(path.join(dir, "public"), { recursive: true });
   fs.writeFileSync(path.join(dir, "publisher.json"), JSON.stringify(manifest));
   fs.writeFileSync(
      path.join(dir, "model.malloy"),
      `source: numbers is duckdb.sql("SELECT ${answer} AS answer") extend {\n` +
         `  view: which_version is { select: answer }\n}\n`,
   );
   fs.writeFileSync(
      path.join(dir, "dashboards", "overview.malloy"),
      `import { numbers } from '../model.malloy'\n\n` +
         `# artifact { title="Overview v${answer}" } dashboard\n` +
         `query: overview is numbers -> which_version\n`,
   );
   fs.writeFileSync(
      path.join(dir, "notes.malloynb"),
      `>>>markdown\n# Notes v${answer}\n>>>malloy\nimport "model.malloy"\n` +
         `>>>malloy\nrun: numbers -> which_version\n`,
   );
   fs.writeFileSync(
      path.join(dir, "public", "index.html"),
      `<!doctype html><html><head><title>App v${answer}</title></head>` +
         `<body>page v${answer}</body></html>`,
   );
   return dir;
}

const api = (suffix: string) =>
   `${baseUrl}/api/v0/environments/${ENV_NAME}/packages/${suffix}`;

async function json(res: Response): Promise<Record<string, unknown>> {
   return (await res.json()) as Record<string, unknown>;
}

async function answerOf(versionId?: string): Promise<number> {
   const res = await fetch(api(`${PKG}/models/model.malloy/query`), {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: JSON.stringify({
         query: "run: numbers -> which_version",
         compactJson: true,
         ...(versionId !== undefined ? { versionId } : {}),
      }),
   });
   expect(res.status).toBe(200);
   const rows = JSON.parse(String((await json(res)).result)) as {
      answer: number;
   }[];
   return Number(rows[0].answer);
}

describe("package-scoped routes with versioning on", () => {
   beforeAll(async () => {
      process.env.PUBLISHER_PACKAGE_VERSIONING = "on";
      root = fs.realpathSync(
         fs.mkdtempSync(path.join(os.tmpdir(), "package-versions-routes-")),
      );
      env = await startRestE2E();
      baseUrl = env.baseUrl;

      const plainDir = writePackage(
         path.join(root, "plain"),
         { name: PLAIN },
         0,
      );
      const created = await fetch(`${baseUrl}/api/v0/environments`, {
         method: "POST",
         headers: { "content-type": "application/json" },
         body: JSON.stringify({
            name: ENV_NAME,
            packages: [{ name: PLAIN, location: plainDir }],
            connections: [],
         }),
      });
      expect(created.status).toBeLessThan(300);

      for (const [version, answer] of [
         ["1.0.0", 1],
         ["2.0.0", 2],
      ] as const) {
         const location = writePackage(
            path.join(root, `v${answer}`),
            { name: PKG, version, description: `release ${version}` },
            answer,
         );
         const res = await fetch(
            `${baseUrl}/api/v0/environments/${ENV_NAME}/packages`,
            {
               method: "POST",
               headers: { "content-type": "application/json" },
               body: JSON.stringify({ name: PKG, location }),
            },
         );
         expect(res.status).toBe(200);
         expect((await json(res)).versionId).toBe(version);
      }
   }, 180_000);

   afterAll(async () => {
      await fetch(`${baseUrl}/api/v0/environments/${ENV_NAME}`, {
         method: "DELETE",
      }).catch(() => undefined);
      await env?.stop();
      if (savedFlag === undefined)
         delete process.env.PUBLISHER_PACKAGE_VERSIONING;
      else process.env.PUBLISHER_PACKAGE_VERSIONING = savedFlag;
      fs.rmSync(root, { recursive: true, force: true });
   });

   it("GET package answers the named version, latest by default", async () => {
      expect((await json(await fetch(api(PKG)))).versionId).toBe("2.0.0");
      expect(
         (await json(await fetch(api(`${PKG}?versionId=1.0.0`)))).versionId,
      ).toBe("1.0.0");
      expect(
         (await json(await fetch(api(`${PKG}?versionId=`)))).versionId,
      ).toBe("2.0.0");
   });

   it("reload=true returns a published version unchanged", async () => {
      const res = await fetch(api(`${PKG}?versionId=1.0.0&reload=true`));
      expect(res.status).toBe(200);
      expect((await json(res)).versionId).toBe("1.0.0");
      expect(await answerOf("1.0.0")).toBe(1);
   });

   it("model query reads the body's versionId", async () => {
      expect(await answerOf()).toBe(2);
      expect(await answerOf("1.0.0")).toBe(1);
      expect(await answerOf("")).toBe(2);
   });

   it("models list and model get read the named version's files", async () => {
      expect((await fetch(api(`${PKG}/models?versionId=1.0.0`))).status).toBe(
         200,
      );
      const v1 = await json(
         await fetch(api(`${PKG}/models/model.malloy?versionId=1.0.0`)),
      );
      expect(String(v1.sourceText)).toContain("SELECT 1 AS answer");
      const latest = await json(await fetch(api(`${PKG}/models/model.malloy`)));
      expect(String(latest.sourceText)).toContain("SELECT 2 AS answer");
   });

   it("compile compiles against the named version", async () => {
      const compile = async (query: string) =>
         json(
            await fetch(api(`${PKG}/models/model.malloy/compile${query}`), {
               method: "POST",
               headers: { "content-type": "application/json" },
               body: JSON.stringify({
                  source: "run: numbers -> which_version",
                  includeSql: true,
               }),
            }),
         );
      expect(String((await compile("?versionId=1.0.0")).sql)).toContain(
         "SELECT 1 AS answer",
      );
      expect(String((await compile("")).sql)).toContain("SELECT 2 AS answer");
   });

   it("dashboards are the named version's", async () => {
      const list = (await (
         await fetch(api(`${PKG}/dashboards?versionId=1.0.0`))
      ).json()) as { title?: string }[];
      expect(list.map((d) => d.title)).toEqual(["Overview v1"]);
      const one = await json(
         await fetch(api(`${PKG}/dashboards/overview?versionId=1.0.0`)),
      );
      expect(one.title).toBe("Overview v1");
      expect(
         (await json(await fetch(api(`${PKG}/dashboards/overview`)))).title,
      ).toBe("Overview v2");
   });

   it("notebooks, and a notebook cell, are the named version's", async () => {
      expect(
         (await fetch(api(`${PKG}/notebooks?versionId=1.0.0`))).status,
      ).toBe(200);
      const nb = JSON.stringify(
         await json(
            await fetch(api(`${PKG}/notebooks/notes.malloynb?versionId=1.0.0`)),
         ),
      );
      expect(nb).toContain("Notes v1");
      const cell = await fetch(
         api(`${PKG}/notebooks/notes.malloynb/cells/2?versionId=1.0.0`),
      );
      expect(cell.status).toBe(200);
      expect(JSON.stringify(await cell.json())).toContain("SELECT 1 AS answer");
   });

   it("databases, data apps and events take the version", async () => {
      expect(
         (await fetch(api(`${PKG}/databases?versionId=1.0.0`))).status,
      ).toBe(200);
      const apps = (await (
         await fetch(api(`${PKG}/data-apps?versionId=1.0.0`))
      ).json()) as { title?: string }[];
      expect(apps.map((a) => a.title)).toEqual(["App v1"]);

      const abort = new AbortController();
      const events = await fetch(api(`${PKG}/events?versionId=1.0.0`), {
         signal: abort.signal,
      });
      expect(events.status).toBe(200);
      abort.abort();
      expect((await fetch(api(`${PKG}/events?versionId=9.9.9`))).status).toBe(
         404,
      );
   });

   it("static files serve the version the URL names", async () => {
      const page = (q: string) =>
         fetch(
            `${baseUrl}/environments/${ENV_NAME}/packages/${PKG}/index.html${q}`,
         );
      expect(await (await page("?versionId=1.0.0")).text()).toContain(
         "page v1",
      );
      expect(await (await page("")).text()).toContain("page v2");
      // A package with no versions ignores the parameter, so a proxy that
      // forwards a page's query string keeps working.
      const plain = await fetch(
         `${baseUrl}/environments/${ENV_NAME}/packages/${PLAIN}/index.html?versionId=1.0.0`,
      );
      expect(plain.status).toBe(200);
      expect(await plain.text()).toContain("page v0");
   });

   it("the package duckdb connection is the named version's", async () => {
      expect(
         (await fetch(api(`${PKG}/connections/duckdb/schemas?versionId=1.0.0`)))
            .status,
      ).toBe(200);
      expect(
         (await fetch(api(`${PKG}/connections/duckdb/schemas?versionId=9.9.9`)))
            .status,
      ).toBe(404);
   });

   it("refuses an in-place model write to a versioned package", async () => {
      const res = await fetch(api(`${PKG}/models/dashboards/new.malloy`), {
         method: "PUT",
         headers: { "content-type": "application/json" },
         body: JSON.stringify({
            source:
               "import { numbers } from '../model.malloy'\n# artifact dashboard\nquery: q is numbers -> which_version\n",
         }),
      });
      expect(res.status).toBe(409);
      expect((await json(res)).reason).toBe("PACKAGE_IS_VERSIONED");
   });

   it("answers an unknown version 404, a malformed one 400, on every route", async () => {
      const routes = [
         `${PKG}`,
         `${PKG}/models`,
         `${PKG}/models/model.malloy`,
         `${PKG}/dashboards`,
         `${PKG}/dashboards/overview`,
         `${PKG}/notebooks`,
         `${PKG}/notebooks/notes.malloynb`,
         `${PKG}/databases`,
         `${PKG}/data-apps`,
      ];
      for (const route of routes) {
         const sep = route.includes("?") ? "&" : "?";
         const missing = await fetch(api(`${route}${sep}versionId=9.9.9`));
         expect([route, missing.status]).toEqual([route, 404]);
         expect((await json(missing)).reason).toBe("VERSION_NOT_FOUND");
         const malformed = await fetch(api(`${route}${sep}versionId=latest`));
         expect([route, malformed.status]).toEqual([route, 400]);
         expect((await json(malformed)).reason).toBe("VERSION_ID_INVALID");
      }
   });

   it("answers a named version on a package with none 404", async () => {
      const res = await fetch(api(`${PLAIN}/models?versionId=1.0.0`));
      expect(res.status).toBe(404);
      expect((await json(res)).reason).toBe("VERSION_NOT_FOUND");
      expect((await fetch(api(`${PLAIN}/models`))).status).toBe(200);
   });
});
