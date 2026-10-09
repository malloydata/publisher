// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/// <reference types="bun-types" />

/**
 * Package versions on 17 routes that read a `versionId`: package, models,
 * model, query, compile, dashboards, notebooks, one notebook, data apps,
 * databases, the package-scoped connection (schemas, sqlQuery, sqlSource,
 * sqlTemporaryTable), materializations, events and static files. Each handler
 * reads the version itself, so each is probed, for each kind of package: one
 * with no versions (loaded with its environment), a versioned one under each
 * promotion mode, and one named for its version, as an orchestrator that
 * predates versions publishes it. A route that forgets the version shows up
 * as one failing row.
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
const PLAIN_ENV = "versioning-modes-plain-env";

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

describe("package versions across promotion modes and package kinds", () => {
   let env: (RestE2EEnv & { stop(): Promise<void> }) | null = null;
   let baseUrl: string;
   let scratch: string;
   const savedPromotion = process.env.PUBLISHER_VERSION_PROMOTION;

   const pkgApi = (name: string, environment = ENV_NAME) =>
      `${baseUrl}/api/v0/environments/${environment}/packages/${name}`;

   function setPromotion(promotion?: "explicit") {
      if (promotion) process.env.PUBLISHER_VERSION_PROMOTION = promotion;
      else delete process.env.PUBLISHER_VERSION_PROMOTION;
   }

   /** A package directory whose model, files and page answer `n`. */
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
      version: string,
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
      api: string,
      versionId: string | undefined,
   ): Promise<Record<string, Probe>> {
      const out: Record<string, Probe> = {};
      for (const route of ROUTES) {
         out[route.name] = await route.call(api, versionId);
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

   /** A package with no versions answers, and answers `n` where it can say. */
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
      setPromotion();
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
      // An environment whose package comes with it, as one configured in
      // publisher.config.json does: a package with no versions.
      const plain = await fetch(`${baseUrl}/api/v0/environments`, {
         method: "POST",
         headers: json,
         body: JSON.stringify({
            name: PLAIN_ENV,
            packages: [
               { name: "plain", location: await packageDir("plain", null, 1) },
            ],
            connections: [],
         }),
      });
      expect(plain.status).toBe(200);
   });

   afterAll(async () => {
      if (savedPromotion === undefined)
         delete process.env.PUBLISHER_VERSION_PROMOTION;
      else process.env.PUBLISHER_VERSION_PROMOTION = savedPromotion;
      if (env) {
         for (const name of [ENV_NAME, PLAIN_ENV]) {
            await fetch(`${baseUrl}/api/v0/environments/${name}`, {
               method: "DELETE",
            });
         }
         await env.stop();
      }
      await fs.rm(scratch, { recursive: true, force: true });
   });

   describe("a package with no versions", () => {
      const api = () => pkgApi("plain", PLAIN_ENV);

      it("is served on every route when no version is named, or an empty one", async () => {
         expectEveryRoute(
            await probeAll(api(), undefined),
            servedUnversioned(1),
         );
         expectEveryRoute(await probeAll(api(), ""), servedUnversioned(1));
      });

      it("refuses a named version on every route but static files, and never answers from its one tree", async () => {
         expectEveryRoute(await probeAll(api(), "1.0.0"), (name) =>
            name === "GET static page"
               ? { status: 200, answer: 1 }
               : { status: 404, reason: "VERSION_NOT_FOUND" },
         );
      });

      it("refuses a versionId that is not a semantic version with 400 on every route but static files", async () => {
         expectEveryRoute(await probeAll(api(), "v1"), (name) =>
            name === "GET static page"
               ? { status: 200, answer: 1 }
               : { status: 400, reason: "VERSION_ID_INVALID" },
         );
      });

      it("is still edited in place by PATCH, and has no versions to move or archive", async () => {
         const patch = await fetch(api(), {
            method: "PATCH",
            headers: json,
            body: JSON.stringify({ name: "plain", description: "edited" }),
         });
         expect(patch.status).toBe(200);
         const after = (await (await fetch(api())).json()) as {
            description?: string;
         };
         expect(after.description).toBe("edited");
         for (const res of [
            await fetch(`${api()}/latest`, {
               method: "PUT",
               headers: json,
               body: JSON.stringify({ version: "1.0.0" }),
            }),
            await fetch(`${api()}/versions/1.0.0`, {
               method: "PATCH",
               headers: json,
               body: JSON.stringify({ archiveStatus: "archive" }),
            }),
            await fetch(`${api()}/versions/1.0.0/manifest`, {
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

   describe("a versioned package, promoted on publish", () => {
      beforeAll(async () => {
         setPromotion();
         expect((await publish("ver", "1.0.0", 1)).status).toBe(200);
         expect((await publish("ver", "1.1.0", 2)).status).toBe(200);
      });

      it("serves latest on every route when no version is named, or an empty one", async () => {
         expectEveryRoute(
            await probeAll(pkgApi("ver"), undefined),
            servedVersion("1.1.0", 2),
         );
         expectEveryRoute(
            await probeAll(pkgApi("ver"), ""),
            servedVersion("1.1.0", 2),
         );
      });

      it("serves the named version on every route", async () => {
         expectEveryRoute(
            await probeAll(pkgApi("ver"), "1.0.0"),
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

      it("refuses an unknown version on every route but static files, which serve latest", async () => {
         expectEveryRoute(await probeAll(pkgApi("ver"), "9.9.9"), (name) =>
            name === "GET static page"
               ? { status: 200, answer: 2 }
               : { status: 404, reason: "VERSION_NOT_FOUND" },
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
            await probeAll(pkgApi("ver"), "1.0.0"),
            refused(410, "VERSION_ARCHIVED"),
         );
         expect((await archive("unarchive")).status).toBe(200);
         expectEveryRoute(
            await probeAll(pkgApi("ver"), "1.0.0"),
            servedVersion("1.0.0", 1),
         );
      });

      it("answers latest through the deprecated /projects aliases, which take no version", async () => {
         const aliased = [
            "GET package",
            "GET models",
            "GET model",
            "POST query",
            "GET notebooks",
            "GET databases",
            "GET connection schemas",
         ];
         const legacyApi = `${baseUrl}/api/v0/projects/${ENV_NAME}/packages/ver`;
         const viaEnvironments: Record<string, Probe> = {};
         const viaProjects: Record<string, Probe> = {};
         for (const r of ROUTES.filter((r) => aliased.includes(r.name))) {
            viaEnvironments[r.name] = await r.call(pkgApi("ver"), undefined);
            viaProjects[r.name] = await r.call(legacyApi, undefined);
         }
         expect(viaProjects).toEqual(viaEnvironments);
         // A version named there is refused, never answered from latest.
         const named = await route("GET models").call(legacyApi, "1.0.0");
         expect(named.status).toBe(501);
      });

      it("refuses an in-place change: a publish of different content under a version, and a dashboard write", async () => {
         const reasonOf = async (res: Response) =>
            ((await res.json()) as { reason?: string }).reason;
         const conflict = await publish("ver", "1.1.0", 99);
         expect([conflict.status, await reasonOf(conflict)]).toEqual([
            409,
            "VERSION_CONFLICT",
         ]);
         const write = await fetch(
            `${pkgApi("ver")}/models/dashboards/probe.malloy`,
            {
               method: "PUT",
               headers: json,
               body: JSON.stringify({
                  source:
                     '## artifact { kind=dashboard tiles=[] }\nimport "../report.malloy"\n',
               }),
            },
         );
         expect([write.status, await reasonOf(write)]).toEqual([
            409,
            "PACKAGE_IS_VERSIONED",
         ]);
         expect(
            (await route("POST query").call(pkgApi("ver"), undefined)).answer,
         ).toBe(2);
      });
   });

   describe("a package named for its version, as an orchestrator that predates versions publishes it", () => {
      beforeAll(async () => {
         setPromotion();
         expect((await publish("sales___1.0.3", "1.0.3", 3)).status).toBe(200);
      });

      it("is served on every route with no version, an empty one, or its own", async () => {
         for (const versionId of [undefined, "", "1.0.3"]) {
            expectEveryRoute(
               await probeAll(pkgApi("sales___1.0.3"), versionId),
               servedVersion("1.0.3", 3),
            );
         }
      });

      it("serves its page whatever version a forwarded query string names", async () => {
         for (const versionId of ["1.0.3", "2.0.0", "not-a-version"]) {
            expect(
               await route("GET static page").call(
                  pkgApi("sales___1.0.3"),
                  versionId,
               ),
            ).toEqual({ status: 200, answer: 3 });
         }
      });
   });

   describe("explicit promotion", () => {
      beforeAll(async () => {
         setPromotion("explicit");
         expect((await publish("held", "1.0.0", 5)).status).toBe(200);
      });

      afterAll(() => setPromotion());

      it("serves the published version by name on every route before anything is latest", async () => {
         expectEveryRoute(
            await probeAll(pkgApi("held"), "1.0.0"),
            servedVersion("1.0.0", 5),
         );
      });

      it("refuses a request that names no version on every route but static files while nothing is latest", async () => {
         expectEveryRoute(await probeAll(pkgApi("held"), undefined), (name) =>
            name === "GET materializations"
               ? // Listing a versioned package's runs with no latest yet lists them all.
                 { status: 200 }
               : { status: 404, reason: "VERSION_NOT_FOUND" },
         );
      });

      it("serves latest on every route once it is set, and a later publish does not move it", async () => {
         const set = await fetch(`${pkgApi("held")}/latest`, {
            method: "PUT",
            headers: json,
            body: JSON.stringify({ version: "1.0.0" }),
         });
         expect(set.status).toBe(200);
         expectEveryRoute(
            await probeAll(pkgApi("held"), undefined),
            servedVersion("1.0.0", 5),
         );
         expect((await publish("held", "2.0.0", 6)).status).toBe(200);
         expectEveryRoute(
            await probeAll(pkgApi("held"), undefined),
            servedVersion("1.0.0", 5),
         );
         expectEveryRoute(
            await probeAll(pkgApi("held"), "2.0.0"),
            servedVersion("2.0.0", 6),
         );
      });
   });
});
