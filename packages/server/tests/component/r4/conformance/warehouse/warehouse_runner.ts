// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * Runs one warehouse conformance case through the production path, end to
 * end: a real Environment whose `wh` connection is the warehouse and whose
 * `lake` destination is a DuckLake on a Postgres catalog, the package loaded
 * by the load pool, every query answered live, a real MaterializationService
 * run (colocated tables built in the warehouse with the native driver,
 * `storage=` tables read through DuckDB's warehouse passthrough into the
 * lake), the run's own bind, and every query answered again.
 *
 * Unlike the DuckDB fixture's runner (src/test_helpers/conformance/
 * package_runner.ts), which builds every table by hand on DuckDB, nothing
 * here is stood in for: what the warehouse returns, the build writes and the
 * served copy reads is what a deployment would see.
 */

import { PostgresConnection } from "@malloydata/db-postgres";
import type { GivenValue } from "@malloydata/malloy";
import * as fs from "fs";
import * as os from "os";
import * as path from "path";
import { unrecordedAudit } from "../../../../../src/service/access_record";
import { Environment } from "../../../../../src/service/environment";
import { parsePackageName } from "../../../../../src/service/identity";
import type { MaterializationService } from "../../../../../src/service/materialization/run/materialization_service";
import { QueryPrivileges } from "../../../../../src/service/model_query_request";
import type { Package } from "../../../../../src/service/package";
import type {
   Materialization,
   ResourceRepository,
} from "../../../../../src/storage/DatabaseInterface";
import { DuckDBConnection as StorageDuckDB } from "../../../../../src/storage/duckdb/DuckDBConnection";
import { createDuckDBRepository } from "../../../../../src/storage/duckdb/DuckDBRepository";
import { initializeSchema } from "../../../../../src/storage/duckdb/schema";
import {
   freshLoadPool,
   type Outcome,
   type Row,
   withSwitch,
} from "../../../../../src/test_helpers/conformance/package_runner";
import { serviceOverEnvironment } from "../../../../../src/test_helpers/materialization_service_harness";
import { writePackageFiles } from "../../../../../src/test_helpers/temp_package";
import type { WarehouseTarget } from "./targets";

/** The storage destination every `storage=` source in a shape names. */
export const LAKE = "lake";

/** Settled run statuses. */
const TERMINAL = ["MANIFEST_FILE_READY", "FAILED", "CANCELLED"];

/** How long one case's run may take before it is reported as never settling. */
const RUN_DEADLINE_MS = 15 * 60_000;

/** One case: a rendered package and the queries to answer, live then served. */
export interface WarehouseCase {
   /** Short and unique: names the environment and the lake's catalog schema. */
   tag: string;
   files: Record<string, string>;
   queries: {
      model: string;
      malloy: string;
      givens?: Record<string, unknown>;
   }[];
   /** Settings of the shape's own, over the shipped switch's. */
   env?: Record<string, string>;
}

/** The run's record, as the report shows it. */
export interface CaseBuild {
   status: string;
   error?: string;
   /** Each manifest entry's source and table, `storage=` ones with their destination. */
   tables: string[];
   /** The run's metadata, trimmed: refusals and the bind's outcome. */
   metadata?: unknown;
}

export interface CaseRun {
   /** Set when the package did not load or the run could not start. */
   loadError?: string;
   live: Outcome[];
   served: Outcome[];
   build?: CaseBuild;
   warnings: { code?: string; subject?: string; message?: string }[];
   verdicts: string[];
}

/** libpq key=value form of a postgres:// URL, as DuckLake's catalog config takes it. */
function postgresParts(url: string) {
   const u = new URL(url);
   return {
      host: u.hostname,
      port: Number(u.port || 5432),
      databaseName: u.pathname.replace(/^\//, ""),
      userName: decodeURIComponent(u.username),
      password: decodeURIComponent(u.password),
   };
}

/**
 * The harness for one warehouse: the run repository every case's service
 * records in, and the Postgres the lakes' catalogs live in.
 */
export class WarehouseHarness {
   private db?: StorageDuckDB;
   private repo?: ResourceRepository;
   private env?: Environment;
   private service?: MaterializationService;
   private catalog?: PostgresConnection;

   constructor(
      private readonly target: WarehouseTarget,
      /** This run's name, which every lake catalog schema starts with. */
      private readonly runName: string,
      /** A postgres:// URL for the DuckLake catalogs; without one there is no `storage=` tier. */
      private readonly catalogUrl: string | undefined,
   ) {}

   /** Whether `storage=` cases can run: a DuckLake needs its Postgres catalog. */
   get hasLake(): boolean {
      return !!this.catalogUrl;
   }

   async open(): Promise<void> {
      this.db = new StorageDuckDB(":memory:");
      await this.db.initialize();
      await initializeSchema(this.db);
      this.repo = createDuckDBRepository(this.db);
      this.service = serviceOverEnvironment(
         () => this.repo!,
         () => {
            if (!this.env) throw new Error("no case environment is open");
            return this.env;
         },
      );
      if (this.catalogUrl) {
         this.catalog = new PostgresConnection({
            name: "lake_catalog",
            connectionString: this.catalogUrl,
         });
      }
   }

   async close(): Promise<void> {
      await this.catalog?.close().catch(() => undefined);
      await this.db?.close().catch(() => undefined);
   }

   /** Run one case; never throws. */
   async run(c: WarehouseCase): Promise<CaseRun> {
      const out: CaseRun = { live: [], served: [], warnings: [], verdicts: [] };
      const restore = withSwitch("cutover", c.env);
      const root = fs.realpathSync(
         fs.mkdtempSync(path.join(os.tmpdir(), `whconf-${c.tag}-`)),
      );
      const envName = `whc_${c.tag}`;
      const catalogSchema = `${this.runName}_${c.tag}`.toLowerCase();
      let pkg: Package | undefined;
      // This case's own: a previous case whose hook timed out may still be
      // settling, and must not clear or reach this one's environment.
      let env: Environment | undefined;
      try {
         // A worker reads its settings at spawn: plan under this case's.
         await freshLoadPool();
         const envPath = path.join(root, "env");
         writePackageFiles(path.join(envPath, "pkg"), c.files, {
            manifest: { name: "pkg", description: "warehouse conformance" },
         });
         const destinations = [];
         if (this.catalog) {
            await this.catalog.runSQL(
               `CREATE SCHEMA IF NOT EXISTS "${catalogSchema}"`,
            );
            const dataPath = path.join(root, "lake");
            fs.mkdirSync(dataPath, { recursive: true });
            destinations.push({
               name: LAKE,
               type: "ducklake",
               ducklakeConnection: {
                  catalog: {
                     postgresConnection: postgresParts(this.catalogUrl!),
                     metadataSchema: catalogSchema,
                  },
                  storage: { bucketUrl: `${dataPath}/` },
               },
            });
         }
         env = await Environment.create(
            envName,
            envPath,
            [this.target.connection()],
            destinations as never,
         );
         // The run service reads the environment through this.
         this.env = env;
         await this.db!.run(
            "INSERT INTO environments (id, name, path, created_at, updated_at) VALUES (?, ?, ?, now(), now())",
            [`env-${c.tag}`, envName, envPath],
         );
         try {
            await env.addPackage("pkg");
            pkg = await env.getPackage(parsePackageName("pkg"), false);
         } catch (error) {
            out.loadError = message(error);
            return out;
         }
         for (const q of c.queries) out.live.push(await runQuery(pkg, q));
         out.build = await this.build(envName);
         out.warnings = (pkg.getPackageMetadata().warnings ?? []).map((w) => ({
            code: (w as { code?: string }).code,
            subject: (w as { subject?: string }).subject,
            message: (w as { message?: string }).message,
         }));
         for (const q of c.queries) out.served.push(await runQuery(pkg, q));
         out.verdicts = pkg
            .materialization()
            .serveVerdicts()
            .map(
               (v) =>
                  `${v.sourceName}:${v.verdict}${v.reason ? `:${v.reason}` : ""}`,
            );
      } catch (error) {
         out.loadError ??= message(error);
      } finally {
         await env?.closeAllConnections().catch(() => undefined);
         if (this.env === env) this.env = undefined;
         restore();
         await this.catalog
            ?.runSQL(`DROP SCHEMA IF EXISTS "${catalogSchema}" CASCADE`)
            .catch(() => undefined);
         fs.rmSync(root, { recursive: true, force: true });
      }
      return out;
   }

   /** Start a run of every planned source, and wait until it settles and its worker is gone. */
   private async build(envName: string): Promise<CaseBuild> {
      const created = await this.service!.createMaterialization(
         envName,
         "pkg",
         {
            forceRefresh: true,
         },
      );
      const deadline = Date.now() + RUN_DEADLINE_MS;
      let m: Materialization | null = null;
      for (;;) {
         m = await this.repo!.getMaterializationById(created.id);
         if (m && TERMINAL.includes(m.status)) break;
         if (Date.now() > deadline) {
            return { status: "NEVER_SETTLED", tables: [] };
         }
         await sleep(100);
      }
      const running = (
         this.service as unknown as {
            runningAbortControllers: Map<string, unknown>;
         }
      ).runningAbortControllers;
      while (running.has(created.id)) await sleep(20);
      m = (await this.repo!.getMaterializationById(created.id)) ?? m;
      const entries = Object.values(
         (m.manifest as { entries?: Record<string, Record<string, unknown>> })
            ?.entries ?? {},
      );
      return {
         status: m.status,
         ...(m.error ? { error: m.error } : {}),
         tables: entries.map(
            (e) =>
               `${String(e.sourceName)} -> ${String(e.physicalTableName)}${
                  e.storageDestinationName
                     ? ` @${String(e.storageDestinationName)}`
                     : ""
               }`,
         ),
         ...(m.metadata ? { metadata: m.metadata } : {}),
      };
   }
}

function sleep(ms: number): Promise<void> {
   return new Promise((resolve) => setTimeout(resolve, ms));
}

function message(error: unknown): string {
   return error instanceof Error ? error.message : String(error);
}

/** `value` with every `reference_id = "…"` (minted per compile) blanked. */
function withoutReferenceIds(value: unknown): unknown {
   if (value === undefined) return value;
   return JSON.parse(
      JSON.stringify(value).replace(/reference_id = \\"[^"\\]*\\"/g, ""),
   );
}

/** A leg's skip in a path report's `considered`, as package_runner reads it. */
function skipOf(
   kind: "view" | "materialized",
   considered: readonly unknown[] | undefined,
) {
   const skip = (considered ?? []).find(
      (c) =>
         (c as { kind?: unknown }).kind === kind &&
         (c as { outcome?: unknown }).outcome === "skipped",
   ) as { reason?: unknown; detail?: unknown } | undefined;
   if (!skip) return {};
   return {
      [kind === "view" ? "viewSkip" : "servedSkip"]: {
         ...(typeof skip.reason === "string" ? { reason: skip.reason } : {}),
         ...(typeof skip.detail === "string" ? { detail: skip.detail } : {}),
      },
   };
}

/** One query against the package, as the DuckDB fixture's runner answers it. */
async function runQuery(
   pkg: Package,
   q: { model: string; malloy: string; givens?: Record<string, unknown> },
): Promise<Outcome> {
   const model = pkg.getModel(q.model);
   if (!model) return { error: `${q.model} did not load` };
   try {
      const result = await model.runQuery(
         unrecordedAudit(),
         {
            query: q.malloy,
            ...(q.givens
               ? { givens: q.givens as Record<string, GivenValue> }
               : {}),
         } as never,
         QueryPrivileges.of({ includeHiddenFilesAndSources: true }),
      );
      return {
         answer: {
            rows: result.compactResult as unknown as Row[],
            fields: (result.result?.schema?.fields ?? []).map((f) => ({
               name: f.name,
               kind: (f as { kind?: string }).kind,
               type: withoutReferenceIds((f as { type?: unknown }).type),
               annotations: (
                  (f as { annotations?: { value: string }[] }).annotations ?? []
               ).map((a) =>
                  a.value.trim().replace(/reference_id = "[^"]*"/g, ""),
               ),
            })),
            servedFrom: result.servedFrom ?? null,
            servedBy: result.path?.servedBy?.kind ?? "live",
            ...(result.path?.servedBy?.via
               ? { via: result.path.servedBy.via }
               : {}),
            ...skipOf("view", result.path?.considered),
            ...skipOf("materialized", result.path?.considered),
         },
      };
   } catch (error) {
      return { error: message(error) };
   }
}
