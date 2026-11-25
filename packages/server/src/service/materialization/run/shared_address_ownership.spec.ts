// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import type { Connection as MalloyConnection } from "@malloydata/malloy";
import { afterEach, beforeEach, describe, expect, it } from "bun:test";
import * as fs from "fs";
import * as path from "path";
import type {
   Materialization,
   ResourceRepository,
} from "../../../storage/DatabaseInterface";
import { DuckDBConnection } from "../../../storage/duckdb/DuckDBConnection";
import {
   compiledWith,
   fakeSource,
} from "../../../test_helpers/materialization_test_fixtures";
import { installSettings } from "../../../test_helpers/settings_env";
import { writePackageFiles } from "../../../test_helpers/temp_package";
import { unrecordedAudit } from "../../access_record";
import { Environment } from "../../environment";
import { parsePackageName, VersionRef } from "../../identity";
import { storageBindingResolverFor } from "../../versions/materialization_scope";
import type { BuildInstruction, ManifestEntry } from "../domain/types";
import { InstructedBuild } from "./instructed_build";
import { type BuildEnvironment } from "./build_upstreams";
import { MaterializationService } from "./materialization_service";
import { ownedTableComment, TABLE_OWNED_ELSEWHERE } from "./table_ownership";
import {
   openTempStorage,
   disposeTempStorage,
} from "../../../test_helpers/temp_storage";
import { serviceOverEnvironment } from "../../../test_helpers/materialization_service_harness";

/**
 * Two persisted sources that compile to one build: `named`, at its author's
 * `name=`, and `twin`, with none. Each names its own table, so each has its
 * own address (foldDistinctTableNames) and an auto-run builds two tables. The
 * ownership check refuses a `name=` another package (or nobody provable)
 * holds: `named` is not built and answers live, the planted table is exactly
 * as it was (rows and comment), and `twin` never writes it, building and
 * serving its own table instead. (A host that instructs one table for the
 * shared build is the build loop's case, below.)
 *
 * The database tier and a `scope: package` version, on a real DuckDB
 * warehouse; the `storage=` tier is
 * tests/component/materialization_shared_address_ownership.component.spec.ts.
 */

const ENV = "e1";
const ENV_ID = "env-1";
const PKG = "pa";
/** The mark another package's build leaves on its table. */
const OTHER_MARK = ownedTableComment(`${ENV}/pb`);

/**
 * `named` and `twin` share one address (the same build over the same
 * connection; `extend` adds nothing to it); `solo` is a table of its own, so
 * the run commits and its manifest can be read.
 */
const MODEL = [
   "##! experimental.persistence",
   `source: base is warehouse.sql("SELECT 'live' AS who, 1 AS k")`,
   '#@ persist name="named_t"',
   "source: named is base -> { group_by: who }",
   "#@ persist",
   "source: twin is base -> { group_by: who }",
   "#@ persist",
   "source: solo is base -> { group_by: k }",
   "",
].join("\n");

const WAREHOUSE = {
   name: "warehouse",
   type: "duckdb",
   duckdbConnection: { setupSQL: "SELECT 1" },
} as const;

let root: string;
let envPath: string;
let db: DuckDBConnection;
let repo: ResourceRepository;
let env: Environment;
let service: MaterializationService;
let restoreSettings: () => void;

beforeEach(async () => {
   restoreSettings = installSettings({
      env: { PUBLISHER_ALLOW_DUCKDB_SETUP_SQL: "true" },
   }).restore;
   ({ root, envPath, db, repo } = await openTempStorage(
      "shared-address-ownership-",
      {
         id: ENV_ID,
         name: ENV,
      },
   ));
});

afterEach(async () => {
   await env?.closeAllConnections().catch(() => undefined);
   await disposeTempStorage({ db, root });
   restoreSettings();
});

function newService(): MaterializationService {
   return serviceOverEnvironment(
      () => repo,
      () => env,
   );
}

async function build(
   options: Parameters<MaterializationService["createMaterialization"]>[2] = {},
): Promise<Materialization> {
   const created = await service.createMaterialization(ENV, PKG, options);
   const deadline = Date.now() + 60_000;
   for (;;) {
      const m = await repo.getMaterializationById(created.id);
      if (
         m &&
         ["MANIFEST_FILE_READY", "FAILED", "CANCELLED"].includes(m.status)
      ) {
         const running = (
            service as unknown as {
               runningAbortControllers: Map<string, unknown>;
            }
         ).runningAbortControllers;
         while (running.has(created.id)) {
            await new Promise((resolve) => setTimeout(resolve, 10));
         }
         return (await repo.getMaterializationById(created.id)) ?? m;
      }
      if (Date.now() > deadline) throw new Error("run never settled");
      await new Promise((resolve) => setTimeout(resolve, 20));
   }
}

async function sql(text: string): Promise<Record<string, unknown>[]> {
   const connection = await env.getMalloyConnection("warehouse");
   return (await connection.runSQL(text)).rows as Record<string, unknown>[];
}

/** Where DuckDB's catalog says `table` is, as a mark names it. */
async function locationOf(table: string): Promise<string> {
   return String(
      (
         await sql(
            `SELECT database_name || '.' || schema_name || '.' || table_name AS loc FROM duckdb_tables() WHERE table_name = '${table}'`,
         )
      )[0]?.loc,
   );
}

/** The planted table, as a reader of the warehouse sees it. */
async function planted(): Promise<{ rows: unknown[]; comment: unknown }> {
   const rows = (await sql("SELECT who FROM named_t ORDER BY 1")).map(
      (r) => r.who,
   );
   const comment = (
      await sql(
         "SELECT comment FROM duckdb_tables() WHERE table_name = 'named_t'",
      )
   )[0]?.comment;
   return { rows, comment };
}

/** What `source` answers, and whether a stored table answered it. */
async function answer(
   source: string,
   versionId?: string,
): Promise<{ who: string; servedFrom: string | null }> {
   const pkg = await env.getPackage(parsePackageName(PKG), false, {
      versionId: VersionRef.parse(versionId),
   });
   const result = await pkg
      .getModel("model.malloy")!
      .runQuery(unrecordedAudit(), {
         query: `run: ${source} -> { select: who }`,
      });
   const typed = result as unknown as {
      compactResult: { who: string }[];
      servedFrom?: string;
   };
   return {
      who: typed.compactResult[0].who,
      servedFrom: typed.servedFrom ?? null,
   };
}

/** The run's failures and entries, by source name. */
function outcome(m: Materialization) {
   const manifest = m.manifest as {
      entries?: Record<
         string,
         { sourceName: string; physicalTableName?: string; error?: unknown }
      >;
      failures?: Record<string, { sourceName: string; reason: string }>;
   } | null;
   return {
      built: Object.values(manifest?.entries ?? {})
         .filter((e) => !e.error && e.physicalTableName)
         .map((e) => e.sourceName)
         .sort(),
      failures: Object.values(manifest?.failures ?? {}),
   };
}

/**
 * `named` refused and answering live, `solo` and `twin` built, `twin` served
 * from its own table, and the planted table exactly as it was.
 */
async function expectNamedRefused(
   m: Materialization,
   before: { rows: unknown[]; comment: unknown },
   holderText: string,
   versionId?: string,
): Promise<void> {
   expect([m.status, m.error]).toEqual(["MANIFEST_FILE_READY", null]);
   const { built, failures } = outcome(m);
   expect(built).toEqual(["solo", "twin"]);
   expect(m.metadata).toMatchObject({ sourcesBuilt: 2 });
   expect(failures).toHaveLength(1);
   expect(failures[0].sourceName).toBe("named");
   expect(failures[0].reason).toContain(`(${TABLE_OWNED_ELSEWHERE})`);
   expect(failures[0].reason).toContain(holderText);
   expect(await planted()).toEqual(before);
   expect(await answer("named", versionId)).toEqual({
      who: "live",
      servedFrom: null,
   });
   await expectServedFromOwnTable(m, "twin", versionId);
}

/**
 * `source` reads the table its run built for it, and only it: marking that
 * table's rows changes its answer.
 */
async function expectServedFromOwnTable(
   m: Materialization,
   source: string,
   versionId?: string,
): Promise<void> {
   const entries = Object.values(
      (
         m.manifest as {
            entries: Record<
               string,
               { sourceName: string; physicalTableName: string }
            >;
         }
      ).entries,
   ).filter((e) => e.sourceName === source);
   expect(entries).toHaveLength(1);
   const table = entries[0].physicalTableName;
   expect(table).not.toBe("named_t");
   await sql(`UPDATE "${table}" SET who = '${source}_stored'`);
   expect((await answer(source, versionId)).who).toBe(`${source}_stored`);
}

describe("the database tier: a refused name= refuses its own source, and a twin builds its own table", () => {
   beforeEach(async () => {
      writePackageFiles(
         path.join(envPath, PKG),
         { "model.malloy": MODEL },
         { manifest: { name: PKG, description: "fixture" } },
      );
      env = await Environment.create(ENV, envPath, [WAREHOUSE as never]);
      await env.addPackage(PKG);
      service = newService();
   });

   it("another package's table at the name: named does not build, and its rows and mark stay", async () => {
      await sql("CREATE TABLE named_t AS SELECT 'pb' AS who");
      await sql(`COMMENT ON TABLE named_t IS '${OTHER_MARK}'`);
      const before = await planted();
      expect(before).toEqual({ rows: ["pb"], comment: OTHER_MARK });

      await expectNamedRefused(
         await build(),
         before,
         `a table package '${ENV}/pb' built`,
      );
   });

   it("a table no run of the package wrote (a user's own, unmarked): no source builds over it", async () => {
      await sql("CREATE TABLE named_t AS SELECT 'mine' AS who");
      const before = await planted();
      expect(before).toEqual({ rows: ["mine"], comment: null });

      await expectNamedRefused(
         await build(),
         before,
         "a table with no Publisher mark that this package's records do not show it built",
      );
   });

   it("once the name is free named_t is built, marked as this package's, and serves named alone", async () => {
      const m = await build();
      expect([m.status, m.error]).toEqual(["MANIFEST_FILE_READY", null]);
      expect(outcome(m).failures).toEqual([]);
      expect(outcome(m).built).toEqual(["named", "solo", "twin"]);
      expect(await planted()).toEqual({
         rows: ["live"],
         comment: ownedTableComment(
            `${ENV}/${PKG}`,
            await locationOf("named_t"),
         ),
      });
      // The tables Publisher named carry the package's mark too.
      for (const entry of Object.values(
         (m.manifest as { entries: Record<string, ManifestEntry> }).entries,
      )) {
         const comment = (
            await sql(
               `SELECT comment FROM duckdb_tables() WHERE table_name = '${entry.physicalTableName}'`,
            )
         )[0]?.comment;
         expect([entry.sourceName, comment]).toEqual([
            entry.sourceName,
            ownedTableComment(
               `${ENV}/${PKG}`,
               await locationOf(entry.physicalTableName),
            ),
         ]);
      }
      await sql("UPDATE named_t SET who = 'stored'");
      expect((await answer("named")).who).toBe("stored");
      await expectServedFromOwnTable(m, "twin");
      expect((await answer("named")).who).toBe("stored");
   });
});

describe('a "scope: package" version: a refused name= refuses its own source, and a twin builds its own table', () => {
   beforeEach(async () => {
      env = await Environment.create(ENV, envPath, [WAREHOUSE as never]);
      env.bindVersions(repo, ENV_ID, {
         downloaderFor: (_packageName, location) => async (stagingPath) => {
            await fs.promises.cp(location, stagingPath, { recursive: true });
         },
         ensurePackageRecord: async (packageName, description) => {
            if (await repo.getPackageByName(ENV_ID, packageName)) return false;
            await repo.createPackage({
               environmentId: ENV_ID,
               name: packageName,
               description,
               manifestPath: "",
            });
            return true;
         },
         removePackageRecord: async (packageName) => {
            const row = await repo.getPackageByName(ENV_ID, packageName);
            if (row) await repo.deletePackage(row.id);
         },
      });
      env.setStorageBindingResolver(storageBindingResolverFor(repo, ENV_ID));
      const location = path.join(root, "sources", "1.0.0");
      writePackageFiles(
         location,
         { "model.malloy": MODEL },
         {
            manifest: {
               name: PKG,
               version: "1.0.0",
               materialization: { scope: "package" },
            },
         },
      );
      await env.getVersionService()!.publish(
         PKG,
         async (stagingPath) => {
            await fs.promises.cp(location, stagingPath, { recursive: true });
         },
         { sourceLocation: location, promotion: "on-publish" },
      );
      service = newService();
   });

   it("another package's table at the name: named does not build, and its rows and mark stay", async () => {
      await sql("CREATE TABLE named_t AS SELECT 'pb' AS who");
      await sql(`COMMENT ON TABLE named_t IS '${OTHER_MARK}'`);
      const before = await planted();

      await expectNamedRefused(
         await build(),
         before,
         `a table package '${ENV}/pb' built`,
         "1.0.0",
      );
   });
});

/**
 * The build loop on its own, with the run order and the warehouse's answer to
 * the ownership probe in the test's hands: the order a real auto-run never
 * produces (the source with no `name=` first) and the number of probes are
 * what tell the two halves of the rule apart.
 */
describe("the build loop: one ownership decision per shared table", () => {
   const ADDRESS = "bsharedownership";

   function harness(
      order: string[],
      holder: string | null,
      options: { hostInstructed?: boolean; twinOnly?: boolean } = {},
   ) {
      const probes: string[] = [];
      const statements: string[] = [];
      const connection = {
         name: "duckdb",
         dialectName: "duckdb",
         runSQL: async (text: string) => {
            if (/duckdb_tables|information_schema/i.test(text)) {
               probes.push(text);
               // Once a source has built, the table is there: the mark's
               // probe reads where the catalog says it is.
               if (built.length > 0) {
                  return {
                     rows: [
                        {
                           kind: "BASE TABLE",
                           comment: holder,
                           location_database: "memory",
                           location_schema: "main",
                           location_name: "named_t",
                        },
                     ],
                  };
               }
               return {
                  rows:
                     holder === null
                        ? []
                        : [{ kind: "BASE TABLE", comment: holder }],
               };
            }
            statements.push(text);
            return { rows: [] };
         },
      } as unknown as MalloyConnection;
      const named = fakeSource({
         name: "named",
         sourceEntityId: ADDRESS,
         annotationFields: { name: "named_t" },
      });
      const twin = fakeSource({ name: "twin", sourceEntityId: ADDRESS });
      const compiled = compiledWith(
         options.twinOnly ? { twin } : { named, twin },
         [order],
         new Map([["duckdb", connection]]),
      );
      const built: string[] = [];
      const retired: string[] = [];
      const build = new InstructedBuild({
         buildOneSource: (async ({
            persistSource: source,
            instruction,
         }: {
            persistSource: { name: string };
            instruction: BuildInstruction;
         }): Promise<ManifestEntry> => {
            built.push(source.name);
            return {
               sourceEntityId: ADDRESS,
               sourceName: source.name,
               physicalTableName: instruction.physicalTableName,
               materializedTableId: instruction.materializedTableId,
               connectionName: "duckdb",
               realization: "COPY",
            } as ManifestEntry;
         }) as never,
         reclaimStorageTablesFromFailedRun: async () => {},
         tableNotOwned: async (_env, _pkg, target) => {
            retired.push(target.physicalTableName);
         },
      });
      const environment = {
         getApiConnection: () => {
            throw new Error("unused");
         },
         getEnvironmentName: () => ENV,
         getEnvironmentPath: () => "/tmp/env",
      } as unknown as BuildEnvironment;
      const run = () =>
         build.run({
            compiled,
            environment,
            instructions: [
               {
                  sourceEntityId: ADDRESS,
                  materializedTableId: "local-shared",
                  physicalTableName: "named_t",
                  realization: "COPY",
               },
            ],
            seedEntries: {},
            signal: new AbortController().signal,
            owner: { environmentId: ENV_ID, packageName: PKG },
            ...(options.hostInstructed ? { hostInstructed: true } : {}),
         });
      return { run, built, probes, statements, retired };
   }

   for (const order of [
      ["named", "twin"],
      ["twin", "named"],
   ]) {
      it(`refuses the table for every source on it, probing it once, when ${order[0]} comes first`, async () => {
         const h = harness(order, OTHER_MARK);
         // Nothing else built, so the run fails, naming the refusal.
         const error = await h.run().then(
            () => undefined,
            (err: unknown) => err as Error,
         );
         // Whichever source probes first, the shared reason names the
         // source whose name= was refused.
         expect(error?.message).toContain(
            `'named' is not built (${TABLE_OWNED_ELSEWHERE}): its name= 'named_t' on connection 'duckdb' is held by a table package '${ENV}/pb' built`,
         );
         expect(error?.message).not.toContain("'twin' is not built");
         expect(h.built).toEqual([]);
         expect(h.statements).toEqual([]);
         expect(h.probes).toHaveLength(1);
         expect(h.retired).toEqual(["named_t"]);
      });
   }

   // Every database table a build writes is marked, not only an author's
   // name=, so a caller's drop can prove it is this package's once its run's
   // record is gone. The mark is written after the build, naming where the
   // catalog says the table is (one probe).
   for (const hostInstructed of [false, true]) {
      it(`marks a table no author named as this package's${hostInstructed ? ", on a host-instructed run" : ""}`, async () => {
         const h = harness(["twin"], null, { hostInstructed, twinOnly: true });
         const result = await h.run();
         expect(h.built).toEqual(["twin"]);
         expect(result.failures).toEqual({});
         expect(h.probes).toHaveLength(1);
         expect(h.statements).toEqual([
            `COMMENT ON TABLE "named_t" IS '${ownedTableComment(`${ENV}/${PKG}`, "memory.main.named_t")}'`,
         ]);
      });
   }

   it("a mark that cannot be written leaves the build as it was", async () => {
      const h = harness(["twin"], null, { twinOnly: true });
      const failing = h.statements;
      // The warehouse refuses the COMMENT: the table is still built and
      // committed, and the run's records say it is the package's.
      const original = failing.push.bind(failing);
      failing.push = (...items: string[]) => {
         original(...items);
         throw new Error("permission denied to comment");
      };
      const result = await h.run();
      expect(h.built).toEqual(["twin"]);
      expect(result.failures).toEqual({});
      expect(Object.values(result.entries)).toHaveLength(1);
   });

   it("marks the shared table as this package's when the source with no name= builds it", async () => {
      const h = harness(["twin", "named"], null);
      const result = await h.run();
      expect(h.built).toEqual(["twin"]);
      expect(result.failures).toEqual({});
      expect(h.statements).toEqual([
         `COMMENT ON TABLE "named_t" IS '${ownedTableComment(`${ENV}/${PKG}`, "memory.main.named_t")}'`,
      ]);
   });
});
