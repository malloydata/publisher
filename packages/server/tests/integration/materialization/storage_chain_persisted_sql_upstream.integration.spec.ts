// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/// <reference types="bun-types" />

import { PostgresConnection } from "@malloydata/db-postgres";
import {
   afterAll,
   beforeAll,
   describe,
   expect,
   it,
   setDefaultTimeout,
} from "bun:test";
import fs from "fs";
import path from "path";
import {
   spawnServer,
   tempDir,
   type SpawnedServer,
} from "../../harness/spawn_server";
import { pollFor, TERMINAL_STATUSES } from "./polling";

/**
 * A chained `storage=` `#@ persist` query over a persisted SQL source
 * (gtm-engine 0.4.19 to 0.4.21: `ga4_events is warehouse.sql(…) include {…}
 * extend {…}`, `web_sessions is ga4_events -> { group_by: …; min(page_path)
 * … }`, `gads_paid_sessions_daily is web_sessions -> {…}`, each in its own
 * file, an index importing them).
 *
 * The plan's chained string reads counted only a query-shaped upstream, so
 * `web_sessions` recorded none, and its build, which stacks on the SQL
 * source's table, refused: "its string reads were not recorded". Now the
 * reads are recorded: under a byte-wise warehouse it builds, and a
 * single-source refresh (as the control plane sends one) builds over the
 * upstream's table; under a collation the destination cannot reproduce for
 * the string it orders, it is refused when planned, saying why.
 *
 * Opt-in: PUBLISHER_TEST_POSTGRES_URL, a server the test may create and drop
 * databases on.
 */

const url = process.env.PUBLISHER_TEST_POSTGRES_URL;
setDefaultTimeout(600_000);

const EVENTS = `##! experimental.access_modifiers
##! experimental.persistence
#@ persist name="gtm_ga4_events" storage="lake"
source: ga4_events is pg.sql("""
  SELECT * FROM (VALUES
    ('u1', 1, 'https://x.com/pricing?a=1', 'page_view', true, TIMESTAMP '2026-01-01 10:00:00'),
    ('u1', 1, 'https://x.com/', 'scroll', false, TIMESTAMP '2026-01-01 10:01:00'),
    ('u2', 2, 'https://x.com/docs', 'page_view', true, TIMESTAMP '2026-01-02 11:00:00')
  ) AS t(user_pseudo_id, ga_session_id, page_location, event_name, is_entrance, event_at)
""")
include {
  public:
    user_pseudo_id
    ga_session_id
    page_location
    event_name
    is_entrance
    event_at
}
extend {
  dimension:
    session_key is concat(user_pseudo_id, '-', ga_session_id::string)
    page_path is concat('/', replace(page_location, 'https://x.com/', '') ?? '')
    is_page_view is event_name = 'page_view'
}
`;
const SESSIONS = (sameFile: boolean) => `##! experimental.access_modifiers
##! experimental.persistence
${sameFile ? "" : "import 'events.malloy'\n"}
#@ persist name="gtm_web_sessions" storage="lake"
source: web_sessions is ga4_events -> {
  group_by: session_key, user_pseudo_id
  aggregate:
    events_in_session is count()
    page_views_in_session is count() { where: is_page_view }
    scroll_event_count is count() { where: event_name = 'scroll' }
    landing_page_raw is min(page_path) { where: is_entrance }
    started_at_raw is min(event_at)
}
extend {
  dimension: landing_page is landing_page_raw
}
`;
const GADS = `##! experimental.access_modifiers
##! experimental.persistence
import '../web/sessions.malloy'

#@ persist name="gads_paid_sessions_daily" storage="lake"
source: gads_paid_sessions_daily is web_sessions -> {
  group_by: user_pseudo_id
  aggregate: sessions is count(), first_landing is min(landing_page_raw)
}
`;
const INDEX = `##! experimental.access_modifiers
##! experimental.persistence
import 'web/events.malloy'
import 'web/sessions.malloy'
import 'google_ads/web.malloy'
export { web_sessions, gads_paid_sessions_daily }
`;

interface Run {
   status: string;
   error?: string | null;
   manifest?: {
      failures?: Record<string, { sourceName?: string; reason?: string }>;
      entries?: Record<string, Record<string, unknown>>;
   };
}

for (const locale of ["C", "en_US.utf8"]) {
   describe.skipIf(!url)(
      `a chained #@ persist query over a persisted SQL source (warehouse locale ${locale})`,
      () => {
         const token = Math.random().toString(36).slice(2, 8);
         const catalog = `sr_lake_${token}`;
         const warehouse = `sr_wh_${token}`;
         let admin: PostgresConnection;
         let server: SpawnedServer;
         const dirs: { cleanup(): void }[] = [];
         const api = () =>
            `${server.baseUrl}/api/v0/environments/e1/packages/pa`;
         async function run(body: unknown = {}): Promise<Run> {
            const created = (await (
               await fetch(`${api()}/materializations`, {
                  method: "POST",
                  headers: { "content-type": "application/json" },
                  body: JSON.stringify(body),
               })
            ).json()) as { id: string };
            return pollFor(
               async () =>
                  (await (
                     await fetch(`${api()}/materializations/${created.id}`)
                  ).json()) as Run,
               (m) => TERMINAL_STATUSES.includes(m.status),
               "the run did not settle",
               300_000,
            );
         }
         async function plan() {
            return (await (await fetch(`${api()}/build-plan`)).json()) as {
               sources?: Record<
                  string,
                  { name: string; sourceEntityId: string; sourceID: string }
               >;
               refusedSources?: Record<
                  string,
                  { name?: string; reason?: string; message?: string }
               >;
            };
         }

         beforeAll(async () => {
            admin = new PostgresConnection({
               name: "admin",
               connectionString: url!,
            });
            await admin.runSQL(`CREATE DATABASE ${catalog}`);
            await admin.runSQL(
               `CREATE DATABASE ${warehouse} LOCALE '${locale}' ENCODING 'UTF8' TEMPLATE template0`,
            );
            const parsed = new URL(url!);
            const pg = {
               host: parsed.hostname,
               port: Number(parsed.port || 5432),
               userName: decodeURIComponent(parsed.username),
               password: decodeURIComponent(parsed.password),
            };
            const lake = tempDir("sr-lake-");
            dirs.push(lake);
            const dir = tempDir("sr-pkg-");
            dirs.push(dir);
            fs.writeFileSync(
               path.join(dir.dir, "publisher.json"),
               JSON.stringify({
                  name: "pa",
                  version: "1.0.0",
                  description: "x",
               }),
            );
            fs.mkdirSync(path.join(dir.dir, "web"));
            fs.mkdirSync(path.join(dir.dir, "google_ads"));
            fs.writeFileSync(path.join(dir.dir, "web/events.malloy"), EVENTS);
            fs.writeFileSync(
               path.join(dir.dir, "web/sessions.malloy"),
               SESSIONS(false),
            );
            fs.writeFileSync(path.join(dir.dir, "google_ads/web.malloy"), GADS);
            fs.writeFileSync(path.join(dir.dir, "index.malloy"), INDEX);
            server = await spawnServer({
               config: {
                  frozenConfig: false,
                  environments: [
                     {
                        name: "e1",
                        packages: [{ name: "pa", location: dir.dir }],
                        connections: [
                           {
                              name: "pg",
                              type: "postgres",
                              postgresConnection: {
                                 ...pg,
                                 databaseName: warehouse,
                              },
                           },
                        ],
                        storageDestinations: [
                           {
                              name: "lake",
                              type: "ducklake",
                              ducklakeConnection: {
                                 catalog: {
                                    postgresConnection: {
                                       ...pg,
                                       databaseName: catalog,
                                    },
                                 },
                                 storage: { bucketUrl: lake.dir },
                              },
                           },
                        ],
                     },
                  ],
               },
               env: {
                  PUBLISHER_MATERIALIZE: "true",
                  PERSIST_STORAGE_MODE: "on",
               },
            });
            await server.waitUntilServing();
         });
         afterAll(async () => {
            await server?.stop();
            for (const db of [catalog, warehouse]) {
               await admin
                  ?.runSQL(`DROP DATABASE IF EXISTS ${db} WITH (FORCE)`)
                  .catch(() => undefined);
            }
            await admin?.close().catch(() => undefined);
            for (const d of dirs) d.cleanup();
         });

         if (locale === "C") {
            it("builds the chain, and a single-source refresh of each downstream over its upstream's table", async () => {
               const first = await run();
               expect([first.status, first.manifest?.failures ?? {}]).toEqual([
                  "MANIFEST_FILE_READY",
                  {},
               ]);
               const sources = Object.values((await plan())?.sources ?? {});
               const byName = Object.fromEntries(
                  sources.map((x) => [x.name, x]),
               );
               const entries = first.manifest?.entries ?? {};
               const tableOf = (n: string) =>
                  entries[byName[n].sourceEntityId]
                     ?.physicalTableName as string;
               for (const [n, up] of [
                  ["web_sessions", "ga4_events"],
                  ["gads_paid_sessions_daily", "web_sessions"],
               ] as const) {
                  const refresh = await run({
                     buildInstructions: {
                        sources: [
                           {
                              sourceEntityId: byName[n].sourceEntityId,
                              sourceID: byName[n].sourceID,
                              materializedTableId: `mt-${n}`,
                              physicalTableName: `${n}_refresh`,
                              realization: "COPY",
                              destination: "lake",
                           },
                        ],
                        referenceManifest: [
                           {
                              sourceEntityId: byName[up].sourceEntityId,
                              physicalTableName: tableOf(up),
                              destination: "lake",
                           },
                        ],
                     },
                  });
                  expect([n, refresh.status, refresh.error ?? null]).toEqual([
                     n,
                     "MANIFEST_FILE_READY",
                     null,
                  ]);
               }
            });
         } else {
            it("refuses the downstream that orders a string, saying why, never for unrecorded reads", async () => {
               const first = await run();
               const failed = Object.values(
                  first.manifest?.failures ?? {},
               ).find((f) => f.sourceName === "web_sessions");
               const refused = Object.values(
                  (await plan())?.refusedSources ?? {},
               ).find((r) => r.name === "web_sessions");
               const why = refused?.message ?? failed?.reason ?? "";
               expect(why).toContain("orders 'page_path'");
               expect(why).not.toContain("not recorded");
            });
         }
      },
   );
}
