// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * Reclaimer.dropConnectionTable's guards, one by one, against a connection
 * whose answers to the ownership probe are the test's: what refuses, with
 * which reason, what is dropped and how it is quoted in each registered
 * dialect, the locks it takes and in what order, and what it counts. The
 * real drops (DuckDB, DuckLake, Postgres) are
 * tests/component/drop_materialized_table.component.spec.ts.
 */

import type { Connection as MalloyConnection } from "@malloydata/malloy";
import { afterEach, beforeEach, describe, expect, it } from "bun:test";
import {
   InvalidArgumentError,
   MaterializationConflictError,
   MaterializationNotFoundError,
   MaterializedTableDropRefusedError,
   TableNotFoundError,
} from "../../../errors";
import { resetMaterializationTelemetryForTesting } from "../../../materialization_metrics";
import type { Materialization } from "../../../storage/DatabaseInterface";
import {
   startMetricsHarness,
   type MetricsHarness,
} from "../../../test_helpers/metrics_harness";
import { quoteTablePath } from "../../quoting";
import type { DialectName } from "../domain/dialect";
import type { ManifestEntry } from "../domain/types";
import { backupSuffix, stagingSuffix } from "./materialization_run_support";
import { Reclaimer } from "./reclaimer";
import { ownedTableComment, tableIdentityAt } from "./table_ownership";

const ENV = "e1";
const ENV_ID = "env-1";
const PKG = "pa";
const CONN = "warehouse";
const TABLE = "analytics.orders_t";
/** This package's mark, on the table it names (Postgres keeps case). */
const OWN = ownedTableComment(`${ENV}/${PKG}`, TABLE);
const OTHER = ownedTableComment(`${ENV}/pb`, TABLE);

/** What the probe finds at the name: rows of {kind, comment}, or a throw. */
type Holder = { kind: string; comment: string | null }[] | "fails";

interface Harness {
   reclaimer: Reclaimer;
   statements: string[];
   locks: string[];
   drop(options?: {
      materializationId?: string;
      physicalTableName?: string;
   }): Promise<"dropped" | "absent">;
}

function entry(
   physicalTableName: string,
   extra: Partial<ManifestEntry> = {},
): ManifestEntry {
   return {
      sourceEntityId: "abcdef0123456789",
      sourceName: "orders",
      physicalTableName,
      connectionName: CONN,
      ...extra,
   } as ManifestEntry;
}

function run(
   id: string,
   status: string,
   entries: ManifestEntry[],
   extra: Partial<Materialization> = {},
): Materialization {
   return {
      id,
      environmentId: ENV_ID,
      packageName: PKG,
      version: null,
      status,
      manifest: {
         entries: Object.fromEntries(entries.map((e) => [e.sourceEntityId, e])),
      },
      metadata: { mode: "orchestrated" },
      error: null,
      createdAt: new Date(
         Date.parse("2026-11-01T00:00:00Z") +
            Number(id.replace(/\D/g, "") || 0) * 1000,
      ),
      completedAt: null,
      ...extra,
   } as unknown as Materialization;
}

function harness(
   holder: Holder,
   options: {
      dialect?: string;
      runs?: Materialization[];
      bound?: string[];
      versionBound?: string[];
      /** What the warehouse throws for the table's DROP. */
      dropError?: unknown;
   } = {},
): Harness {
   const statements: string[] = [];
   const locks: string[] = [];
   const dialect = options.dialect ?? "postgres";
   const connection = {
      name: CONN,
      dialectName: dialect,
      runSQL: async (sql: string) => {
         if (/^DROP /.test(sql)) {
            if (options.dropError) throw options.dropError;
            locks.push("drop");
            statements.push(sql);
            return { rows: [] };
         }
         if (/^(COMMENT|ALTER)/.test(sql)) {
            statements.push(sql);
            return { rows: [] };
         }
         if (holder === "fails")
            throw new Error("permission denied for schema");
         return { rows: holder };
      },
   } as unknown as MalloyConnection;
   const loaded = {
      getPackageName: () => PKG,
      boundTableNames: () => options.bound ?? [],
   };
   const version = {
      getPackageName: () => "pv",
      boundTableNames: () => options.versionBound ?? [],
   };
   const environment = {
      getEnvironmentName: () => ENV,
      getApiConnection: () => ({}),
      getLoadedPackages: () => [loaded],
      getVersionService: () => (options.versionBound ? {} : null),
      peekVersion: (pkg: string, v: string) =>
         pkg === "pv" && v === "1.0.0" ? version : undefined,
   };
   const runs = options.runs ?? [];
   const reclaimer = new Reclaimer({
      repository: () =>
         ({
            listMaterializations: async () => runs,
            listVersionsByEnvironment: async () => [
               { packageName: "pv", versionId: "1.0.0" },
            ],
            deleteIncrementalLedgerEntry: async () => {
               locks.push("forget");
            },
         }) as never,
      host: {} as never,
      lifecycle: {} as never,
      dropManifestEntries: async () => [],
      withRunLock: async (_e, _i, pkg, versionId, fn) => {
         locks.push(`run:${pkg}:${versionId ?? "-"}`);
         return fn();
      },
      withTableLock: async (_i, pkg, fn) => {
         locks.push(`table:${pkg}`);
         return fn();
      },
   });
   return {
      reclaimer,
      statements,
      locks,
      drop: (o = {}) =>
         reclaimer.dropConnectionTable({
            environment: environment as never,
            environmentName: ENV,
            environmentId: ENV_ID,
            packageName: PKG,
            connectionName: CONN,
            connection,
            physicalTableName: o.physicalTableName ?? TABLE,
            ...(o.materializationId
               ? { materializationId: o.materializationId }
               : {}),
         }),
   };
}

const table = (comment: string | null) => [{ kind: "BASE TABLE", comment }];

async function refusal(p: Promise<unknown>): Promise<Error> {
   return p.then(
      () => {
         throw new Error("expected a refusal");
      },
      (err: unknown) => err as Error,
   );
}

async function expectReason(
   p: Promise<unknown>,
   reason: string,
   text?: string,
): Promise<void> {
   const err = await refusal(p);
   expect(err).toBeInstanceOf(MaterializedTableDropRefusedError);
   expect((err as MaterializedTableDropRefusedError).reason as string).toBe(
      reason,
   );
   if (text) expect(err.message).toContain(text);
}

let metrics: MetricsHarness;
beforeEach(async () => {
   resetMaterializationTelemetryForTesting();
   metrics = await startMetricsHarness();
});
afterEach(async () => {
   await metrics.shutdown();
});

describe("ownership", () => {
   it("drops a table marked as this package's, under the package, table and name locks in that order", async () => {
      const h = harness(table(OWN));
      expect(await h.drop()).toBe("dropped");
      expect(h.statements).toEqual([
         `DROP TABLE IF EXISTS "analytics"."orders_t"`,
      ]);
      expect(h.locks).toEqual([
         `run:${PKG}:-`,
         `table:${PKG}`,
         "drop",
         "forget",
      ]);
      expect(
         await metrics.collectCounter(
            "publisher_materialization_drop_tables_total",
            { outcome: "success", tier: "database", occasion: "caller" },
         ),
      ).toBe(1);
   });

   it("refuses a table another package marked, without naming that package: TABLE_OWNED_ELSEWHERE", async () => {
      const h = harness(table(OTHER));
      const err = await refusal(h.drop());
      expect((err as MaterializedTableDropRefusedError).reason as string).toBe(
         "TABLE_OWNED_ELSEWHERE",
      );
      expect(err.message).not.toContain("pb");
      expect(h.statements).toEqual([]);
   });

   it("refuses a view Publisher did not make: TABLE_OWNED_ELSEWHERE", async () => {
      const h = harness([{ kind: "VIEW", comment: null }]);
      await expectReason(h.drop(), "TABLE_OWNED_ELSEWHERE");
      expect(h.statements).toEqual([]);
   });

   it("refuses an unmarked table no run is named for: TABLE_OWNERSHIP_UNVERIFIED", async () => {
      const h = harness(table(null));
      await expectReason(
         h.drop(),
         "TABLE_OWNERSHIP_UNVERIFIED",
         "Pass the materializationId",
      );
      expect(h.statements).toEqual([]);
   });

   it("drops an unmarked table the named terminal run records on this connection, with its leftovers", async () => {
      const recorded = entry(TABLE);
      const h = harness(table(null), {
         runs: [run("r1", "FAILED", [recorded])],
      });
      expect(await h.drop({ materializationId: "r1" })).toBe("dropped");
      const id = recorded.sourceEntityId;
      expect(h.statements).toEqual([
         `DROP TABLE IF EXISTS "analytics"."orders_t"`,
         `DROP TABLE IF EXISTS ${quoteTablePath(`${TABLE}${stagingSuffix(id)}`, "postgres")}`,
         `DROP TABLE IF EXISTS ${quoteTablePath(`${TABLE}${backupSuffix(id)}`, "postgres")}`,
      ]);
   });

   it("refuses an unmarked table the named run records on another connection", async () => {
      const h = harness(table(null), {
         runs: [
            run("r1", "FAILED", [entry(TABLE, { connectionName: "other" })]),
         ],
      });
      await expectReason(
         h.drop({ materializationId: "r1" }),
         "TABLE_OWNERSHIP_UNVERIFIED",
         "run r1 does not record it on this connection",
      );
   });

   it("refuses when the probe fails, without echoing the driver", async () => {
      const h = harness("fails");
      const err = await refusal(h.drop());
      expect((err as MaterializedTableDropRefusedError).reason).toBe(
         "TABLE_OWNERSHIP_UNVERIFIED",
      );
      expect(err.message).not.toContain("permission denied");
   });

   it("refuses on a dialect it cannot ask", async () => {
      const h = harness(table(OWN), { dialect: "nosuchdialect" });
      await expectReason(
         h.drop(),
         "TABLE_OWNERSHIP_UNVERIFIED",
         "dialect 'nosuchdialect'",
      );
   });

   it("a missing table a run records answers absent, dropping nothing", async () => {
      const h = harness([], { runs: [run("r1", "CANCELLED", [entry(TABLE)])] });
      expect(await h.drop({ materializationId: "r1" })).toBe("absent");
      expect(h.statements).toEqual([]);
   });

   it("a missing table nothing records answers TABLE_NOT_FOUND", async () => {
      const h = harness([]);
      expect(await refusal(h.drop())).toBeInstanceOf(TableNotFoundError);
   });
});

/**
 * A comment travels with its table: renamed (`ALTER TABLE … RENAME`), moved
 * (`SET SCHEMA`), or copied with its comments (`CREATE TABLE … LIKE …
 * INCLUDING COMMENTS`, a Snowflake CLONE, a BigQuery copy), the copy carries
 * the mark of the table it came from. The drop accepts a mark only on the
 * table it names, so each is refused unless a run record names it there.
 */
describe("the mark names its table", () => {
   const own = (table: string) => ownedTableComment(`${ENV}/${PKG}`, table);
   const cases: [string, string, string][] = [
      ["renamed", "analytics.orders_t", "analytics.keep_me"],
      ["moved to another schema", "analytics.orders_t", "archive.orders_t"],
      [
         "copied with its comment",
         "analytics.orders_t",
         "analytics.orders_copy",
      ],
      ["unqualified, then reached qualified", "orders_t", "public.orders_t"],
   ];
   for (const [how, built, probed] of cases) {
      it(`${how}: refused, TABLE_OWNERSHIP_UNVERIFIED`, async () => {
         const h = harness(table(own(built)));
         await expectReason(
            h.drop({ physicalTableName: probed }),
            "TABLE_OWNERSHIP_UNVERIFIED",
         );
         expect(h.statements).toEqual([]);
      });

      it(`${how}: dropped when the named run records the new name`, async () => {
         const h = harness(table(own(built)), {
            runs: [run("r1", "FAILED", [entry(probed)])],
         });
         expect(
            await h.drop({
               physicalTableName: probed,
               materializationId: "r1",
            }),
         ).toBe("dropped");
      });
   }

   it("a mark an earlier release wrote (no table=) is not proof", async () => {
      const h = harness(table(ownedTableComment(`${ENV}/${PKG}`)));
      await expectReason(h.drop(), "TABLE_OWNERSHIP_UNVERIFIED");
      expect(h.statements).toEqual([]);
   });

   it("DuckDB ignores case: the mark matches the name in any case", async () => {
      const h = harness(table(own("analytics.orders_t")), {
         dialect: "duckdb",
      });
      expect(await h.drop({ physicalTableName: "Analytics.ORDERS_T" })).toBe(
         "dropped",
      );
   });

   it("Postgres keeps quoted case: a mark for another case is another table", async () => {
      const h = harness(table(own("analytics.Orders_T")));
      await expectReason(h.drop(), "TABLE_OWNERSHIP_UNVERIFIED");
   });
});

describe("what the warehouse refuses", () => {
   const coded = (code: string, message: string) =>
      Object.assign(new Error(message), { code });
   const failures = () =>
      metrics.collectCounter("publisher_materialization_drop_tables_total", {
         outcome: "failure",
         tier: "database",
         occasion: "caller",
      });

   it("dependent objects (Postgres 2BP01): TABLE_IN_USE, counted as a failed drop", async () => {
      const h = harness(table(OWN), {
         dropError: coded(
            "2BP01",
            "cannot drop table orders_t because other objects depend on it",
         ),
      });
      await expectReason(h.drop(), "TABLE_IN_USE", "depend on it");
      expect(await failures()).toBe(1);
   });

   it("a missing privilege (42501): TABLE_OWNED_ELSEWHERE", async () => {
      const h = harness(table(OWN), {
         dropError: coded("42501", "must be owner of table orders_t"),
      });
      await expectReason(h.drop(), "TABLE_OWNED_ELSEWHERE", "not allowed");
      expect(await failures()).toBe(1);
   });

   it("not a table (42809): TABLE_OWNED_ELSEWHERE", async () => {
      const h = harness(table(OWN), {
         dropError: coded("42809", '"orders_t" is not a table'),
      });
      await expectReason(
         h.drop(),
         "TABLE_OWNED_ELSEWHERE",
         "other than a table",
      );
   });

   it("anything else is a server error whose text is redacted", async () => {
      const h = harness(table(OWN), {
         dropError: new Error(
            "could not write s3://secret-bucket/path/data.parquet",
         ),
      });
      const err = await refusal(h.drop());
      expect(err).not.toBeInstanceOf(MaterializedTableDropRefusedError);
      expect(err.message).not.toContain("secret-bucket");
      expect(await failures()).toBe(1);
   });
});

describe("in use", () => {
   it("refuses while any run of the package is active: MATERIALIZATION_CONFLICT", async () => {
      const h = harness(table(OWN), {
         runs: [run("r9", "MANIFEST_ROWS_READY", [])],
      });
      expect(await refusal(h.drop())).toBeInstanceOf(
         MaterializationConflictError,
      );
      expect(h.statements).toEqual([]);
   });

   it("refuses a table a committed run of another version names, by its table segment anywhere", async () => {
      const h = harness(table(OWN), {
         runs: [
            run("r1", "MANIFEST_FILE_READY", [entry("other_schema.ORDERS_T")], {
               version: "2.0.0",
            }),
         ],
      });
      await expectReason(h.drop(), "TABLE_IN_USE", "committed run r1");
      expect(h.statements).toEqual([]);
   });

   it("does not count the named run, unless it is what is served", async () => {
      const older = run("r1", "MANIFEST_FILE_READY", [entry(TABLE)]);
      const newer = run("r2", "MANIFEST_FILE_READY", [
         entry("analytics.next_t"),
      ]);
      const h = harness(table(OWN), { runs: [older, newer] });
      expect(await h.drop({ materializationId: "r1" })).toBe("dropped");

      const served = harness(table(OWN), { runs: [older] });
      await expectReason(
         served.drop({ materializationId: "r1" }),
         "TABLE_IN_USE",
         "newest run",
      );
      expect(served.statements).toEqual([]);
   });

   it("refuses a table a loaded package is bound to, quoted or not", async () => {
      const h = harness(table(OWN), { bound: ['"analytics"."orders_t"'] });
      await expectReason(h.drop(), "TABLE_IN_USE", `package '${PKG}'`);
      expect(h.statements).toEqual([]);
   });

   it("refuses a table a loaded version of another package is bound to", async () => {
      const h = harness(table(OWN), { versionBound: ["orders_t"] });
      await expectReason(
         h.drop(),
         "TABLE_IN_USE",
         "version 1.0.0 of package 'pv'",
      );
   });

   it("refuses a build's staging or backup copy alone, however it is reached: it goes with its table", async () => {
      const recorded = entry(TABLE);
      const id = recorded.sourceEntityId;
      const names = [stagingSuffix, backupSuffix].flatMap((suffix) => [
         `${TABLE}${suffix(id)}`,
         // Another container, or another case: the same copy.
         `public.orders_t${suffix(id)}`,
         `main.ORDERS_T${suffix(id).toUpperCase()}`,
         `orders_t${suffix(id)}`,
      ]);
      for (const name of names) {
         const h = harness(table(OWN), {
            runs: [run("r1", "FAILED", [recorded])],
         });
         const err = await refusal(h.drop({ physicalTableName: name }));
         expect(err).toBeInstanceOf(InvalidArgumentError);
         expect(err.message).toContain(`drop '${TABLE}' instead`);
         expect(h.statements).toEqual([]);
      }
   });

   it("a named run that is not the package's: 404", async () => {
      const h = harness(table(OWN));
      expect(
         await refusal(h.drop({ materializationId: "nope" })),
      ).toBeInstanceOf(MaterializationNotFoundError);
   });
});

describe("quoting, per registered dialect", () => {
   const dialects: DialectName[] = [
      "duckdb",
      "postgres",
      "standardsql",
      "snowflake",
      "databricks",
      "trino",
      "presto",
      "mysql",
      "sqlserver",
   ];
   for (const dialect of dialects) {
      it(`${dialect}: drops the name as the build quoted it`, async () => {
         // SQL Server names take at most a schema.
         const name =
            dialect === "sqlserver"
               ? "Analytics.Orders_T"
               : "my-proj.Analytics.Orders_T";
         // Its own mark, naming it as the build wrote it at this dialect.
         // Presto reads no comment, so its tables are never marked: the
         // named run's record is the evidence there.
         const presto = dialect === "presto";
         const h = harness(
            table(
               presto
                  ? null
                  : ownedTableComment(
                       `${ENV}/${PKG}`,
                       tableIdentityAt(name.split("."), dialect),
                    ),
            ),
            { dialect, runs: [run("r1", "FAILED", [entry(name)])] },
         );
         expect(
            await h.drop({
               physicalTableName: name,
               ...(presto ? { materializationId: "r1" } : {}),
            }),
         ).toBe("dropped");
         expect(h.statements[0]).toBe(
            `DROP TABLE IF EXISTS ${quoteTablePath(name, dialect)}`,
         );
      });
   }

   it("the quoting is the dialect's own", () => {
      expect(quoteTablePath("a.b", "standardsql")).toBe("`a`.`b`");
      expect(quoteTablePath("a.b", "mysql")).toBe("`a`.`b`");
      expect(quoteTablePath('a.b"c', "postgres")).toBe('"a"."b""c"');
   });
});
