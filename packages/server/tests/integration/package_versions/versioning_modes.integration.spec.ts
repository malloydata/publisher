// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/// <reference types="bun-types" />

/**
 * Package versioning across its modes, on 17 routes that read a `versionId`:
 * package, models, model, query, compile, dashboards, notebooks, one
 * notebook, data apps, databases, the package-scoped connection (schemas,
 * sqlQuery, sqlSource, sqlTemporaryTable), materializations, events and
 * static files. Each handler reads the version itself, so each is probed.
 * Not probed: one dashboard, one notebook cell, a connection's tables and
 * one table, and one materialization by id.
 *
 * The feature ships dormant, so the property that matters most is that the
 * modes do not leak into one another: with `packageVersioning` off a package
 * is the mutable slot it always was, a server that published versions keeps
 * serving them when the setting is turned off again, an unversioned package
 * never answers as if it were a version, and packages whose names carry a
 * version (`pkg___<version>`, unversioned) keep working whatever their
 * requests carry. Each route is probed the same way in each mode, so a route
 * that forgets the version shows up as one failing row.
 *
 * Each package tree carries markers of its own number `n`: the model answers
 * it, a source and a model file are named for it, a data file too, and its
 * page says it. So a route whose answer can say which tree served it is
 * checked on that answer, not only on its status: one that resolved the
 * version and then read `latest` fails. Routes whose answer is the same for
 * every tree (dashboards, notebooks, connection schemas, materializations,
 * events) are checked on status alone.
 */

import { afterAll, beforeAll, describe, expect, it } from "bun:test";
import fs from "fs/promises";
import os from "os";
import path from "path";
import { RestE2EEnv, startRestE2E } from "../../harness/rest_e2e";

const ENV_NAME = "versioning-modes-env";

interface Probe {
   status: number;
   reason?: string;
   /** What the tree that answered says it is, when the route can tell. */
   answer?: number | string | null;
}

/** The number a marker in `pattern` carries anywhere in `body`, or null. */
const marker =
   (pattern: RegExp) =>
   (body: unknown): number | null => {
      const m = pattern.exec(
         typeof body === "string" ? body : JSON.stringify(body),
      );
      return m ? Number(m[1]) : null;
   };

/** One route that reads a `versionId`, and how to call it. */
interface Route {
   name: string;
   call(base: string, versionId: string | undefined): Promise<Probe>;
}

const json = { "Content-Type": "application/json" };

const query = (versionId: string | undefined) =>
   versionId === undefined ? "" : `?versionId=${encodeURIComponent(versionId)}`;

async function probe(res: Response, read?: (body: unknown) => Probe["answer"]) {
   const text = await res.text();
   let body: unknown;
   try {
      body = JSON.parse(text);
   } catch {
      body = text;
   }
   const out: Probe = { status: res.status };
   if (res.status >= 400) {
      out.reason = (body as { reason?: string } | undefined)?.reason;
   } else if (read) {
      out.answer = read(body);
   }
   return out;
}

const get = (url: string, read?: (body: unknown) => Probe["answer"]) =>
   fetch(url).then((res) => probe(res, read));

/**
 * The routes, each probed in every mode. `base` is the package's API path.
 * The query route carries the version in its body, and create-materialization
 * too, the rest in the query string.
 */
const ROUTES: Route[] = [
   {
      name: "GET package",
      call: (base, v) =>
         get(`${base}${query(v)}`, (b) => {
            const pkg = b as { versionId?: string | null };
            return pkg.versionId ?? null;
         }),
   },
   {
      name: "GET models",
      call: (base, v) =>
         get(`${base}/models${query(v)}`, marker(/marker_(\d+)\.malloy/)),
   },
   {
      name: "GET model",
      call: (base, v) =>
         get(`${base}/models/report.malloy${query(v)}`, marker(/tag_(\d+)/)),
   },
   {
      name: "POST query",
      call: (base, v) =>
         fetch(`${base}/models/report.malloy/query`, {
            method: "POST",
            headers: json,
            body: JSON.stringify({
               query: "run: report -> { select: n }",
               compactJson: true,
               ...(v !== undefined ? { versionId: v } : {}),
            }),
         }).then((res) =>
            probe(
               res,
               (b) => JSON.parse((b as { result: string }).result)[0].n,
            ),
         ),
   },
   {
      name: "POST compile",
      call: (base, v) =>
         fetch(`${base}/models/report.malloy/compile${query(v)}`, {
            method: "POST",
            headers: json,
            body: JSON.stringify({
               source: "run: report -> { select: n }",
               includeSql: true,
            }),
         }).then((res) => probe(res, marker(/SELECT (\d+) as n/))),
   },
   {
      name: "GET dashboards",
      call: (base, v) => get(`${base}/dashboards${query(v)}`),
   },
   {
      name: "GET notebooks",
      call: (base, v) => get(`${base}/notebooks${query(v)}`),
   },
   {
      name: "GET data-apps",
      call: (base, v) =>
         get(`${base}/data-apps${query(v)}`, marker(/answers (\d+)/)),
   },
   {
      name: "GET databases",
      call: (base, v) =>
         get(`${base}/databases${query(v)}`, marker(/data_(\d+)\.csv/)),
   },
   {
      name: "GET connection schemas",
      call: (base, v) => get(`${base}/connections/duckdb/schemas${query(v)}`),
   },
   {
      // The package's own duckdb sandbox reads the tree's files, so a query of
      // its data file says which tree the connection was opened on.
      name: "POST connection sqlQuery",
      call: (base, v) =>
         fetch(`${base}/connections/duckdb/sqlQuery${query(v)}`, {
            method: "POST",
            headers: json,
            body: JSON.stringify({
               sqlStatement: "SELECT n FROM read_csv_auto('data_*.csv')",
            }),
         }).then((res) => probe(res, marker(/\\?"n\\?":\s*\\?"?(\d+)/))),
   },
   {
      // Status only: a statement's source and a temporary table answer the
      // same for every tree. What they prove is that the version is read.
      name: "POST connection sqlSource",
      call: (base, v) =>
         fetch(`${base}/connections/duckdb/sqlSource${query(v)}`, {
            method: "POST",
            headers: json,
            body: JSON.stringify({
               sqlStatement: "SELECT n FROM read_csv_auto('data_*.csv')",
            }),
         }).then((res) => probe(res)),
   },
   {
      name: "POST connection sqlTemporaryTable",
      call: (base, v) =>
         fetch(`${base}/connections/duckdb/sqlTemporaryTable${query(v)}`, {
            method: "POST",
            headers: json,
            body: JSON.stringify({
               sqlStatement: "SELECT n FROM read_csv_auto('data_*.csv')",
            }),
         }).then((res) => probe(res)),
   },
   {
      name: "GET notebook",
      call: (base, v) =>
         get(
            `${base}/notebooks/notes.malloynb${query(v)}`,
            marker(/notebook answers (\d+)/),
         ),
   },
   {
      name: "GET materializations",
      call: (base, v) => get(`${base}/materializations${query(v)}`),
   },
   {
      // A stream: only its status is read, then it is let go.
      name: "GET events",
      call: async (base, v) => {
         const abort = new AbortController();
         const res = await fetch(`${base}/events${query(v)}`, {
            signal: abort.signal,
         });
         const out: Probe = { status: res.status };
         if (res.status >= 400) {
            out.reason = ((await res.json()) as { reason?: string }).reason;
         }
         abort.abort();
         return out;
      },
   },
   {
      // A refusal here is the JSON error every route answers, so its reason
      // is read like theirs: a missing file's HTML 404 cannot pass for one.
      name: "GET static page",
      call: (base, v) =>
         fetch(`${base.replace("/api/v0", "")}/index.html${query(v)}`).then(
            (res) => probe(res, marker(/answers (\d+)/)),
         ),
   },
];

/** Routes whose answer says which tree served it. */
const ANSWERING = new Set([
   "GET package",
   "GET models",
   "GET model",
   "POST query",
   "POST compile",
   "GET data-apps",
   "GET databases",
   "POST connection sqlQuery",
   "GET notebook",
   "GET static page",
]);

const route = (name: string): Route => {
   const found = ROUTES.find((r) => r.name === name);
   if (!found) throw new Error(`no route ${name}`);
   return found;
};

describe("package versioning across its modes", () => {
   let env: (RestE2EEnv & { stop(): Promise<void> }) | null = null;
   let baseUrl: string;
   let scratch: string;
   const saved = {
      versioning: process.env.PUBLISHER_PACKAGE_VERSIONING,
      promotion: process.env.PUBLISHER_VERSION_PROMOTION,
   };

   const pkgApi = (name: string) =>
      `${baseUrl}/api/v0/environments/${ENV_NAME}/packages/${name}`;

   function setModes(versioning: "on" | "off", promotion?: "explicit") {
      process.env.PUBLISHER_PACKAGE_VERSIONING = versioning;
      if (promotion) process.env.PUBLISHER_VERSION_PROMOTION = promotion;
      else delete process.env.PUBLISHER_VERSION_PROMOTION;
   }

   /** A package directory whose model and page answer `n`. */
   async function packageDir(
      name: string,
      version: string | null,
      n: number,
   ): Promise<string> {
      const dir = await fs.mkdtemp(path.join(scratch, `${name}-`));
      await fs.writeFile(
         path.join(dir, "publisher.json"),
         JSON.stringify(version === null ? { name } : { name, version }),
      );
      await fs.writeFile(
         path.join(dir, "report.malloy"),
         `source: report is duckdb.sql("SELECT ${n} as n")\n` +
            `source: tag_${n} is report\n`,
      );
      await fs.writeFile(
         path.join(dir, `marker_${n}.malloy`),
         `source: marker is duckdb.sql("SELECT ${n} as m")\n`,
      );
      await fs.writeFile(path.join(dir, `data_${n}.csv`), `n\n${n}\n`);
      await fs.writeFile(
         path.join(dir, "notes.malloynb"),
         `>>>markdown\n# notebook answers ${n}\n`,
      );
      await fs.mkdir(path.join(dir, "public"));
      await fs.writeFile(
         path.join(dir, "public/index.html"),
         `<!doctype html><title>answers ${n}</title>`,
      );
      return dir;
   }

   async function publish(
      name: string,
      version: string | null,
      n: number,
   ): Promise<Response> {
      return fetch(`${baseUrl}/api/v0/environments/${ENV_NAME}/packages`, {
         method: "POST",
         headers: json,
         body: JSON.stringify({
            name,
            location: await packageDir(name, version, n),
         }),
      });
   }

   /** Every route's probe of one package with one versionId. */
   async function probeAll(
      name: string,
      versionId: string | undefined,
   ): Promise<Record<string, Probe>> {
      const out: Record<string, Probe> = {};
      for (const route of ROUTES) {
         out[route.name] = await route.call(pkgApi(name), versionId);
      }
      return out;
   }

   /** The same expectation for every route, so a failure names its route. */
   function expectEveryRoute(
      probes: Record<string, Probe>,
      expected: (route: string) => Partial<Probe>,
   ) {
      const actual = Object.fromEntries(
         Object.entries(probes).map(([route, p]) => [
            route,
            Object.fromEntries(
               Object.keys(expected(route)).map((k) => [
                  k,
                  p[k as keyof Probe],
               ]),
            ),
         ]),
      );
      const wanted = Object.fromEntries(
         Object.keys(probes).map((route) => [route, expected(route)]),
      );
      expect(actual).toEqual(wanted);
   }

   /** An unversioned package answers, and answers `n` where a route can say. */
   function servedUnversioned(n: number) {
      return (name: string): Partial<Probe> => {
         if (name === "GET package") return { status: 200, answer: null };
         return ANSWERING.has(name)
            ? { status: 200, answer: n }
            : { status: 200 };
      };
   }

   /** A versioned package answers from `version`, whose tree is `n`'s. */
   function servedVersion(version: string, n: number) {
      return (name: string): Partial<Probe> => {
         if (name === "GET package") return { status: 200, answer: version };
         return ANSWERING.has(name)
            ? { status: 200, answer: n }
            : { status: 200 };
      };
   }

   /** The same refusal, reason included, on every route. */
   function refused(status: number, reason: string) {
      return (): Partial<Probe> => ({ status, reason });
   }

   beforeAll(async () => {
      setModes("off");
      env = await startRestE2E();
      baseUrl = env.baseUrl;
      scratch = await fs.mkdtemp(path.join(os.tmpdir(), "versioning-modes-"));
      const created = await fetch(`${baseUrl}/api/v0/environments`, {
         method: "POST",
         headers: json,
         body: JSON.stringify({
            name: ENV_NAME,
            packages: [],
            connections: [],
         }),
      });
      expect(created.status).toBe(200);
   });

   afterAll(async () => {
      if (saved.versioning === undefined)
         delete process.env.PUBLISHER_PACKAGE_VERSIONING;
      else process.env.PUBLISHER_PACKAGE_VERSIONING = saved.versioning;
      if (saved.promotion === undefined)
         delete process.env.PUBLISHER_VERSION_PROMOTION;
      else process.env.PUBLISHER_VERSION_PROMOTION = saved.promotion;
      if (env) {
         await fetch(`${baseUrl}/api/v0/environments/${ENV_NAME}`, {
            method: "DELETE",
         });
         await env.stop();
      }
      await fs.rm(scratch, { recursive: true, force: true });
   });

   describe("packageVersioning off", () => {
      beforeAll(async () => {
         setModes("off");
         expect((await publish("plain", "1.0.0", 1)).status).toBe(200);
         // Credible's shape: the control plane loads each version as its own
         // unversioned package, named for it.
         expect((await publish("sales___1.0.3", "1.0.3", 3)).status).toBe(200);
      });

      it("serves an unversioned package on every route when no version is named", async () => {
         expectEveryRoute(
            await probeAll("plain", undefined),
            servedUnversioned(1),
         );
      });

      it("treats an empty versionId as none, on every route", async () => {
         expectEveryRoute(await probeAll("plain", ""), servedUnversioned(1));
      });

      it("refuses a named version on every route but static files, and never answers from the one tree", async () => {
         expectEveryRoute(await probeAll("plain", "1.0.0"), (name) =>
            name === "GET static page"
               ? { status: 200, answer: 1 }
               : { status: 404, reason: "VERSION_NOT_FOUND" },
         );
      });

      it("refuses a versionId that is not a semantic version with 400 on every route but static files", async () => {
         expectEveryRoute(await probeAll("plain", "v1"), (name) =>
            name === "GET static page"
               ? { status: 200, answer: 1 }
               : { status: 400, reason: "VERSION_ID_INVALID" },
         );
      });

      it("serves a pkg___<version> package on every route, a page's forwarded versionId included", async () => {
         expectEveryRoute(
            await probeAll("sales___1.0.3", undefined),
            servedUnversioned(3),
         );
         const page = await route("GET static page").call(
            pkgApi("sales___1.0.3"),
            "1.0.3",
         );
         expect(page).toEqual({ status: 200, answer: 3 });
      });

      it("keeps a publish over an unversioned package the in-place replace it always was", async () => {
         expect((await publish("plain", "1.0.0", 11)).status).toBe(200);
         expect(
            (await route("POST query").call(pkgApi("plain"), undefined)).answer,
         ).toBe(11);
         const versions = await fetch(`${pkgApi("plain")}/versions`);
         expect(await versions.json()).toEqual([]);
         // The deprecated PATCH still edits an unversioned package.
         const patch = await fetch(pkgApi("plain"), {
            method: "PATCH",
            headers: json,
            body: JSON.stringify({ name: "plain", description: "edited" }),
         });
         expect(patch.status).toBe(200);
         const after = (await (await fetch(pkgApi("plain"))).json()) as {
            description?: string;
         };
         expect(after.description).toBe("edited");
      });

      it("refuses the lifecycle routes for a package with no versions", async () => {
         for (const res of [
            await fetch(`${pkgApi("plain")}/latest`, {
               method: "PUT",
               headers: json,
               body: JSON.stringify({ versionId: "1.0.0" }),
            }),
            await fetch(`${pkgApi("plain")}/versions/1.0.0`, {
               method: "PATCH",
               headers: json,
               body: JSON.stringify({ archiveStatus: "archive" }),
            }),
            await fetch(`${pkgApi("plain")}/versions/1.0.0/manifest`, {
               method: "PUT",
               headers: json,
               body: JSON.stringify({ manifestLocation: null }),
            }),
         ]) {
            expect(res.status).toBe(404);
            expect(((await res.json()) as { reason?: string }).reason).toBe(
               "VERSION_NOT_FOUND",
            );
         }
      });
   });

   describe("packageVersioning on", () => {
      beforeAll(async () => {
         // Made with versioning off, by this block's own setup rather than an
         // earlier block's test, so the block runs alone (-t) too.
         setModes("off");
         expect((await publish("early", "1.0.0", 12)).status).toBe(200);
         expect((await publish("sales___2.0.1", "2.0.1", 4)).status).toBe(200);
         setModes("on");
         expect((await publish("ver", "1.0.0", 1)).status).toBe(200);
         expect((await publish("ver", "1.1.0", 2)).status).toBe(200);
      });

      it("serves latest on every route when no version is named", async () => {
         expectEveryRoute(
            await probeAll("ver", undefined),
            servedVersion("1.1.0", 2),
         );
      });

      it("serves the named version on every route", async () => {
         expectEveryRoute(
            await probeAll("ver", "1.0.0"),
            servedVersion("1.0.0", 1),
         );
      });

      it("serves two versions at the same time, each from its own tree", async () => {
         // Interleaved, so a lock or cache shared across versions would mix
         // their answers.
         const asks = ["1.0.0", "1.1.0", "1.0.0", "1.1.0", "1.0.0", "1.1.0"];
         const answers = await Promise.all(
            asks.map((v) => route("POST query").call(pkgApi("ver"), v)),
         );
         expect(answers.map((a) => a.answer)).toEqual([1, 2, 1, 2, 1, 2]);
      });

      it("treats an empty versionId as none on a versioned package too: latest answers", async () => {
         expectEveryRoute(await probeAll("ver", ""), servedVersion("1.1.0", 2));
      });

      it("refuses an unknown version on every route", async () => {
         expectEveryRoute(
            await probeAll("ver", "9.9.9"),
            refused(404, "VERSION_NOT_FOUND"),
         );
      });

      it("refuses an archived version on every route, and serves it again once unarchived", async () => {
         const archive = (archiveStatus: string) =>
            fetch(`${pkgApi("ver")}/versions/1.0.0`, {
               method: "PATCH",
               headers: json,
               body: JSON.stringify({ archiveStatus }),
            });
         expect((await archive("archive")).status).toBe(200);
         expectEveryRoute(
            await probeAll("ver", "1.0.0"),
            refused(410, "VERSION_ARCHIVED"),
         );
         expect((await archive("unarchive")).status).toBe(200);
         expectEveryRoute(
            await probeAll("ver", "1.0.0"),
            servedVersion("1.0.0", 1),
         );
      });

      it("answers the same through the deprecated /projects aliases, on every route they alias", async () => {
         const aliased = [
            "GET package",
            "GET models",
            "GET model",
            "POST query",
            "POST compile",
            "GET notebooks",
            "GET databases",
            "GET connection schemas",
            "GET materializations",
         ];
         const legacyApi = `${baseUrl}/api/v0/projects/${ENV_NAME}/packages/ver`;
         for (const versionId of [undefined, "1.0.0", "9.9.9"]) {
            const viaEnvironments: Record<string, Probe> = {};
            const viaProjects: Record<string, Probe> = {};
            for (const route of ROUTES.filter((r) =>
               aliased.includes(r.name),
            )) {
               viaEnvironments[route.name] = await route.call(
                  pkgApi("ver"),
                  versionId,
               );
               viaProjects[route.name] = await route.call(legacyApi, versionId);
            }
            expect({ versionId, probes: viaProjects }).toEqual({
               versionId,
               probes: viaEnvironments,
            });
         }
      });

      it("still serves an unversioned package made before versioning was on, as unversioned", async () => {
         expectEveryRoute(
            await probeAll("early", undefined),
            servedUnversioned(12),
         );
         expectEveryRoute(await probeAll("early", "1.0.0"), (name) =>
            name === "GET static page"
               ? { status: 200, answer: 12 }
               : { status: 404, reason: "VERSION_NOT_FOUND" },
         );
      });

      it("leaves an existing pkg___<version> package unversioned once versioning is on", async () => {
         expectEveryRoute(
            await probeAll("sales___2.0.1", undefined),
            servedUnversioned(4),
         );
      });
   });

   describe("turning versioning off again", () => {
      beforeAll(async () => {
         // Published with versioning on by this block's own setup, then the
         // setting goes off.
         setModes("on");
         expect((await publish("kept", "1.0.0", 1)).status).toBe(200);
         expect((await publish("kept", "1.1.0", 2)).status).toBe(200);
         setModes("off");
      });

      it("keeps serving every published version by name, and latest without one", async () => {
         expectEveryRoute(
            await probeAll("kept", "1.0.0"),
            servedVersion("1.0.0", 1),
         );
         expectEveryRoute(
            await probeAll("kept", undefined),
            servedVersion("1.1.0", 2),
         );
      });

      it("still refuses in-place changes to the versioned package: a publish over it, PATCH and a model write", async () => {
         const reasonOf = async (res: Response) =>
            ((await res.json()) as { reason?: string }).reason;
         const legacy = await publish("kept", "1.1.0", 99);
         expect(legacy.status).toBe(409);
         expect(await reasonOf(legacy)).toBe("PACKAGE_IS_VERSIONED");
         const patch = await fetch(pkgApi("kept"), {
            method: "PATCH",
            headers: json,
            body: JSON.stringify({ name: "kept", description: "edited" }),
         });
         expect(patch.status).toBe(409);
         expect(await reasonOf(patch)).toBe("PACKAGE_IS_VERSIONED");
         // A dashboard write: the one model write the route takes.
         const write = await fetch(
            `${pkgApi("kept")}/models/dashboards/probe.malloy`,
            {
               method: "PUT",
               headers: json,
               body: JSON.stringify({
                  source:
                     '## artifact { kind=dashboard tiles=[] }\nimport "../report.malloy"\n',
               }),
            },
         );
         expect(write.status).toBe(409);
         expect(await reasonOf(write)).toBe("PACKAGE_IS_VERSIONED");
         expect(
            (await route("POST query").call(pkgApi("kept"), undefined)).answer,
         ).toBe(2);
      });

      it("still moves latest and archives versions, which are not publishes", async () => {
         const latest = await fetch(`${pkgApi("kept")}/latest`, {
            method: "PUT",
            headers: json,
            body: JSON.stringify({ versionId: "1.0.0" }),
         });
         expect(latest.status).toBe(200);
         expect(
            (await route("POST query").call(pkgApi("kept"), undefined)).answer,
         ).toBe(1);
         const back = await fetch(`${pkgApi("kept")}/latest`, {
            method: "PUT",
            headers: json,
            body: JSON.stringify({ versionId: "1.1.0" }),
         });
         expect(back.status).toBe(200);
         const archiveStatus = (status: string) =>
            fetch(`${pkgApi("kept")}/versions/1.0.0`, {
               method: "PATCH",
               headers: json,
               body: JSON.stringify({ archiveStatus: status }),
            });
         expect((await archiveStatus("archive")).status).toBe(200);
         expect(
            (await route("POST query").call(pkgApi("kept"), "1.0.0")).status,
         ).toBe(410);
         expect((await archiveStatus("unarchive")).status).toBe(200);
         expect(
            (await route("POST query").call(pkgApi("kept"), "1.0.0")).answer,
         ).toBe(1);
      });
   });

   describe("packageVersioning on, versionPromotion explicit", () => {
      beforeAll(async () => {
         setModes("on", "explicit");
         expect((await publish("held", "1.0.0", 5)).status).toBe(200);
      });

      afterAll(() => setModes("on"));

      it("serves the published version by name on every route before anything is latest", async () => {
         expectEveryRoute(
            await probeAll("held", "1.0.0"),
            servedVersion("1.0.0", 5),
         );
      });

      it("refuses a request that names no version on every route while nothing is latest", async () => {
         expectEveryRoute(
            await probeAll("held", undefined),
            refused(404, "VERSION_NOT_FOUND"),
         );
      });

      it("serves latest on every route once it is set, and a later publish does not move it", async () => {
         const set = await fetch(`${pkgApi("held")}/latest`, {
            method: "PUT",
            headers: json,
            body: JSON.stringify({ versionId: "1.0.0" }),
         });
         expect(set.status).toBe(200);
         expectEveryRoute(
            await probeAll("held", undefined),
            servedVersion("1.0.0", 5),
         );
         expect((await publish("held", "2.0.0", 6)).status).toBe(200);
         expectEveryRoute(
            await probeAll("held", undefined),
            servedVersion("1.0.0", 5),
         );
         expectEveryRoute(
            await probeAll("held", "2.0.0"),
            servedVersion("2.0.0", 6),
         );
      });
   });
});
