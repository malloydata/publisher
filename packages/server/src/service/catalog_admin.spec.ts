// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { afterEach, beforeEach, describe, expect, it, spyOn } from "bun:test";
import { readFileSync } from "fs";
import { resolve } from "path";
import { parse } from "yaml";
import { BadRequestError } from "../errors";
import { loadSlots } from "../query_concurrency";
import { resetInstrumentsForTesting } from "../telemetry";
import {
   startMetricsHarness,
   type MetricsHarness,
} from "../test_helpers/metrics_harness";
import { fakeOf } from "../test_helpers/store_fakes";
import { CatalogAdmin, CatalogAdminError, withLoadSlot } from "./catalog_admin";
import type { Environment } from "./environment";
import type { LoadedEnvironmentsReader } from "./environment_ports";
import * as tableLoad from "./materialization/run/storage_table_load";
import {
   CommittedLoadError,
   LoadRefusalError,
   PostApplyError,
   loadLayout,
   objectUnreadable,
} from "./materialization/run/storage_table_load";
import { DUCKDB_STATEMENT_SEPARATOR } from "./single_select";

const SERVER_SRC = resolve(__dirname, "..");
const REPO_ROOT = resolve(__dirname, "../../../..");

/** The admin with a catalog database nothing listens on: a load that passes validation fails reaching it. */
const admin = new CatalogAdmin({
   catalogDatabase: "host=127.0.0.1 port=1 dbname=none connect_timeout=1",
});

const valid = {
   commitMessage: "nightly charges",
   mode: "merge",
   key: ["id"],
   objects: ["gs://b/p/a.parquet", "gs://b/p/b.parquet"],
};

async function refusal(body: unknown): Promise<CatalogAdminError> {
   const error = await admin.loadTable("c", "s", "t", body).then(
      () => undefined,
      (e) => e,
   );
   expect(error).toBeInstanceOf(CatalogAdminError);
   return error as CatalogAdminError;
}

describe("a schema name", () => {
   it("may not start with the metadata catalogs' prefix, on a create or a drop", async () => {
      for (const attempt of [
         () => admin.createSchema("c", "__ducklake_metadata_x", {}, false),
         () => admin.deleteSchema("c", "__ducklake_metadata_x"),
         () => admin.deleteSchema("c", "__DuckLake_Metadata_x"),
      ]) {
         const error = await attempt().then(
            () => undefined,
            (e) => e,
         );
         expect(error).toBeInstanceOf(CatalogAdminError);
         expect([
            (error as CatalogAdminError).status,
            (error as CatalogAdminError).reason,
         ]).toEqual([400, "SCHEMA_RESERVED"]);
      }
   });
});

const source = { uri: "gs://b/p/a.parquet", format: "parquet" };

/** What an attempt answers, or undefined when it got past every refusal and failed connecting to the catalog database. */
async function refusalOf(attempt: () => Promise<unknown>) {
   const error = await attempt().then(
      () => undefined,
      (e) => e,
   );
   if (error instanceof CatalogAdminError) return [error.status, error.reason];
   // DuckDB's attach (a PUT or DELETE) and node-pg's lock client (a load) word the same refusal differently.
   // Windows retries a refused SYN for about two seconds, so libpq's one-second
   // connect_timeout fires first there and the attach reports a timeout instead.
   expect(String(error?.message)).toMatch(
      /port 1 failed: (Connection refused|timeout expired)|ECONNREFUSED 127\.0\.0\.1:1/,
   );
   return undefined;
}

describe("the main schema", () => {
   it("is refused as the target of a load, in any case, before the catalog database is reached", async () => {
      for (const schema of ["main", "Main", "MAIN"]) {
         const error = await admin.loadTable("c", schema, "t", valid).then(
            () => undefined,
            (e) => e,
         );
         expect(error).toBeInstanceOf(CatalogAdminError);
         expect([
            schema,
            (error as CatalogAdminError).status,
            (error as CatalogAdminError).reason,
         ]).toEqual([schema, 400, "SCHEMA_RESERVED"]);
         // It says where tables go, which a schema create's refusal has no need to.
         expect((error as CatalogAdminError).message).toContain(
            "PUT /catalogs/{catalog}/schemas/{schema}",
         );
      }
      const create = await admin
         .createSchema("c", "main", {}, false)
         .catch((e) => e);
      expect((create as CatalogAdminError).message).not.toContain("PUT");
   });

   it("is not refused as the target of a table PUT or DELETE", async () => {
      for (const schema of ["main", "Main", "MAIN"]) {
         for (const attempt of [
            () => admin.putTable("c", schema, "t", { source }, false),
            () => admin.deleteTable("c", schema, "t"),
         ]) {
            // Past every refusal, it fails reaching the unreachable catalog database.
            expect(await refusalOf(attempt)).toBeUndefined();
         }
      }
   });
});

describe("a storage destination's schema", () => {
   const CATALOG_DATABASE =
      "host=127.0.0.1 port=1 dbname=none connect_timeout=1";
   const lake = (
      name: string,
      metadataSchema: string,
      destinationSchema?: string,
      postgresConnection: object = {
         connectionString: CATALOG_DATABASE,
      },
   ) => ({
      name,
      type: "ducklake",
      ducklakeConnection: {
         catalog: {
            postgresConnection,
            metadataSchema,
         },
         ...(destinationSchema ? { destinationSchema } : {}),
      },
   });
   interface StubEnvironment {
      name: string;
      destinations: ReturnType<typeof lake>[];
      connections?: ReturnType<typeof lake>[];
   }
   const adminOver = (environments: StubEnvironment[]) =>
      new CatalogAdmin(
         { catalogDatabase: CATALOG_DATABASE },
         fakeOf<LoadedEnvironmentsReader>({
            getLoadedEnvironments: () =>
               environments.map((e) =>
                  fakeOf<Environment>({
                     getEnvironmentName: () => e.name,
                     listStorageDestinations: () => e.destinations,
                     listApiConnections: () => e.connections ?? [],
                  }),
               ),
         }),
      );
   const put = (a: CatalogAdmin, schema: string, catalog = "c") =>
      refusalOf(() => a.putTable(catalog, schema, "t", { source }, false));
   const load = (a: CatalogAdmin, schema: string, catalog = "c") =>
      refusalOf(() => a.loadTable(catalog, schema, "t", valid));

   it("is refused as the target of a table PUT and a load, naming the destination, in any case, before the catalog database is reached", async () => {
      const a = adminOver([
         {
            name: "prod",
            destinations: [lake("dest", "c", "materialized")],
         },
      ]);
      for (const schema of ["materialized", "Materialized"]) {
         const expected = [409, "SCHEMA_IS_DESTINATION"];
         expect(await put(a, schema)).toEqual(expected);
         expect(await load(a, schema)).toEqual(expected);
      }
      const error = await a
         .putTable("c", "materialized", "t", { source }, false)
         .catch((e) => e);
      expect((error as CatalogAdminError).message).toContain("prod");
      expect((error as CatalogAdminError).message).toContain("dest");
      expect((error as CatalogAdminError).message).toContain(
         "built by Publisher",
      );
   });

   it("is main when the destination sets no destinationSchema: a PUT is refused as the destination's, a load as reserved", async () => {
      const a = adminOver([
         { name: "prod", destinations: [lake("dest", "c")] },
      ]);
      expect(await put(a, "main")).toEqual([409, "SCHEMA_IS_DESTINATION"]);
      expect(await load(a, "main")).toEqual([400, "SCHEMA_RESERVED"]);
   });

   it("is refused as the target of a schema DELETE, which would cascade over every build table", async () => {
      const a = adminOver([
         {
            name: "prod",
            destinations: [lake("dest", "c", "materialized")],
         },
      ]);
      for (const schema of ["materialized", "Materialized"]) {
         expect(await refusalOf(() => a.deleteSchema("c", schema))).toEqual([
            409,
            "SCHEMA_IS_DESTINATION",
         ]);
      }
      // Another schema, and another catalog's, pass the refusal and fail reaching the unreachable catalog database.
      expect(
         await refusalOf(() => a.deleteSchema("c", "staging")),
      ).toBeUndefined();
      expect(
         await refusalOf(() => a.deleteSchema("other", "materialized")),
      ).toBeUndefined();
   });

   it("leaves a table DELETE in it, and every other schema, alone", async () => {
      const a = adminOver([
         {
            name: "prod",
            destinations: [lake("dest", "c", "materialized")],
         },
      ]);
      expect(
         await refusalOf(() => a.deleteTable("c", "materialized", "t")),
      ).toBeUndefined();
      expect(await put(a, "staging")).toBeUndefined();
      expect(await put(a, "main")).toBeUndefined();
   });

   it("is refused when the destination names the catalog's database as another role, or by its fields", async () => {
      for (const postgresConnection of [
         {
            connectionString:
               "dbname=none user=tier password=other port=1 host=127.0.0.1 sslmode=disable",
         },
         { connectionString: "postgres://tier:other@127.0.0.1:1/none" },
         {
            host: "127.0.0.1",
            port: 1,
            databaseName: "none",
            userName: "tier",
            password: "other",
         },
      ]) {
         const a = adminOver([
            {
               name: "prod",
               destinations: [
                  lake("dest", "c", "materialized", postgresConnection),
               ],
            },
         ]);
         expect(await put(a, "materialized")).toEqual([
            409,
            "SCHEMA_IS_DESTINATION",
         ]);
      }
   });

   it("is not refused when the destination names another database of the same server", async () => {
      const a = adminOver([
         {
            name: "prod",
            destinations: [
               lake("dest", "c", "materialized", {
                  connectionString: "host=127.0.0.1 port=1 dbname=elsewhere",
               }),
            ],
         },
      ]);
      expect(await put(a, "materialized")).toBeUndefined();
   });

   it("is not refused when the destination overlaps the catalog by data path alone", async () => {
      const elsewhere = lake("dest", "c", "materialized", {
         connectionString: "host=127.0.0.1 port=1 dbname=elsewhere",
      });
      const dest = {
         ...elsewhere,
         ducklakeConnection: {
            ...elsewhere.ducklakeConnection,
            storage: { bucketUrl: "s3://bucket/lake/dest" },
         },
      };
      // Only a data path set on the admin's own storage can overlap one; the listener's config types it away.
      const a = new CatalogAdmin(
         {
            catalogDatabase: CATALOG_DATABASE,
            storage: { bucketUrl: "s3://bucket/lake" } as never,
         },
         fakeOf<LoadedEnvironmentsReader>({
            getLoadedEnvironments: () => [
               fakeOf<Environment>({
                  getEnvironmentName: () => "prod",
                  listStorageDestinations: () => [dest as never],
                  listApiConnections: () => [],
               }),
            ],
         }),
      );
      expect(await put(a, "materialized")).toBeUndefined();
   });

   it("is refused only on the catalog the destination uses", async () => {
      const a = adminOver([
         {
            name: "prod",
            destinations: [lake("dest", "c", "materialized")],
         },
      ]);
      expect(await put(a, "materialized", "other")).toBeUndefined();
      expect(await load(a, "materialized", "other")).toBeUndefined();
   });

   it("counts a destination of any loaded environment, read at each write", async () => {
      const environments: StubEnvironment[] = [
         { name: "a", destinations: [] },
         { name: "b", destinations: [] },
      ];
      const a = adminOver(environments);
      expect(await put(a, "materialized")).toBeUndefined();
      environments[1].destinations = [lake("late", "c", "materialized")];
      expect(await put(a, "materialized")).toEqual([
         409,
         "SCHEMA_IS_DESTINATION",
      ]);
   });

   it("counts only a storage destination: a connection's destinationSchema refuses nothing", async () => {
      const a = adminOver([
         {
            name: "prod",
            destinations: [],
            connections: [lake("db_x", "c", "materialized")],
         },
      ]);
      expect(await put(a, "materialized")).toBeUndefined();
      expect(await load(a, "materialized")).toBeUndefined();
   });

   it("refuses nothing when the admin API has no environments to ask", async () => {
      expect(await put(admin, "materialized")).toBeUndefined();
   });
});

describe("a load's request body", () => {
   it("passes validation when every rule is met, and only then reaches the catalog database", async () => {
      const error = await admin
         .loadTable("c", "s", "t", {
            ...valid,
            cursor: "created",
         })
         .then(
            () => undefined,
            (e) => e,
         );
      expect(error).toBeDefined();
      expect((error as CatalogAdminError).reason).not.toBe("INVALID_REQUEST");
      // A keyless replace needs a guard, not a key.
      const replace = await admin
         .loadTable("c", "s", "t", {
            mode: "replace",
            maxDeletedRows: 0,
            objects: ["o"],
         })
         .then(
            () => undefined,
            (e) => e,
         );
      expect((replace as CatalogAdminError).reason).not.toBe("INVALID_REQUEST");
   });

   it.each([
      ["a body that is not an object", [valid], "body must be a JSON object"],
      ["no body", undefined, "body must be a JSON object"],
      ["an unknown key", { ...valid, bogus: 1 }, "Unknown key 'bogus'."],
      ["a loadId, which is gone", { ...valid, loadId: "x" }, "Unknown key"],
      [
         "an empty commitMessage",
         { ...valid, commitMessage: "" },
         "'commitMessage'",
      ],
      [
         "a commitMessage that is not text",
         { ...valid, commitMessage: 1 },
         "'commitMessage'",
      ],
      [
         "a commitMessage over 1024 characters",
         { ...valid, commitMessage: "a".repeat(1025) },
         "'commitMessage'",
      ],
      [
         "a commitMessage with a NUL",
         { ...valid, commitMessage: "a\0b" },
         "no NUL",
      ],
      [
         "a commitMessage holding the statement separator",
         { ...valid, commitMessage: `a ${DUCKDB_STATEMENT_SEPARATOR} b` },
         "statement separator",
      ],
      ["no mode", { ...valid, mode: undefined }, "'mode'"],
      ["a refresh mode", { ...valid, mode: "refresh" }, "'mode'"],
      [
         "a merge with no key",
         { ...valid, key: undefined },
         "A merge needs a 'key'",
      ],
      ["an empty key", { ...valid, key: [] }, "at least one column"],
      [
         "a key of 17 columns",
         { ...valid, key: Array.from({ length: 17 }, (_, i) => `k${i}`) },
         "'key'",
      ],
      ["a repeated key column", { ...valid, key: ["id", "id"] }, "'key'"],
      ["an uppercase key column", { ...valid, key: ["ID"] }, "'key'"],
      ["a key column that is not text", { ...valid, key: [1] }, "'key'"],
      [
         "a merge with maxDeletedRows",
         { ...valid, maxDeletedRows: 1 },
         "only to a replace",
      ],
      [
         "a replace with no maxDeletedRows",
         { ...valid, mode: "replace" },
         "A replace needs 'maxDeletedRows'",
      ],
      [
         "a negative maxDeletedRows",
         { ...valid, mode: "replace", maxDeletedRows: -1 },
         "non-negative integer",
      ],
      [
         "a fractional maxDeletedRows",
         { ...valid, mode: "replace", maxDeletedRows: 1.5 },
         "non-negative integer",
      ],
      [
         "a textual maxDeletedRows",
         { ...valid, mode: "replace", maxDeletedRows: "1" },
         "non-negative integer",
      ],
      ["no objects", { ...valid, objects: undefined }, "at least one object"],
      [
         "an empty objects list",
         { ...valid, objects: [] },
         "at least one object",
      ],
      [
         "1001 objects",
         { ...valid, objects: Array.from({ length: 1001 }, (_, i) => `o${i}`) },
         "'objects'",
      ],
      ["a repeated object", { ...valid, objects: ["o", "o"] }, "'objects'"],
      ["an empty object name", { ...valid, objects: [""] }, "'objects'"],
      ["an object that is not text", { ...valid, objects: [1] }, "'objects'"],
      [
         "a cursor that is not a lowercase name",
         { ...valid, cursor: "Created" },
         "'cursor'",
      ],
      ["a cursor that is not text", { ...valid, cursor: 1 }, "'cursor'"],
   ])(
      "refuses %s as a 400 before taking any lock",
      async (_name, body, message) => {
         const error = await refusal(body);
         expect([error.status, error.reason]).toEqual([400, "INVALID_REQUEST"]);
         expect(error.message).toContain(message);
      },
   );

   it("takes a commitMessage of 1024 characters, or none", async () => {
      for (const commitMessage of [
         "a".repeat(1024),
         "😀".repeat(1024),
         undefined,
      ]) {
         const error = await admin
            .loadTable("c", "s", "t", { ...valid, commitMessage })
            .then(
               () => undefined,
               (e) => e,
            );
         expect((error as CatalogAdminError).reason).not.toBe(
            "INVALID_REQUEST",
         );
      }
   });

   it("accepts 16 key columns and 1000 objects", async () => {
      const error = await admin
         .loadTable("c", "s", "t", {
            ...valid,
            key: Array.from({ length: 16 }, (_, i) => `k${i}`),
            objects: Array.from({ length: 1000 }, (_, i) => `o${i}`),
         })
         .then(
            () => undefined,
            (e) => e,
         );
      expect((error as CatalogAdminError).reason).not.toBe("INVALID_REQUEST");
   });

   it("names the catalog, schema and table as plain identifiers", async () => {
      for (const [c, s, t] of [
         ["c-x", "s", "t"],
         ["c", "s.s", "t"],
         ["c", "s", "t t"],
      ]) {
         const error = await admin.loadTable(c, s, t, valid).then(
            () => undefined,
            (e) => e,
         );
         expect((error as CatalogAdminError).reason).toBe("INVALID_REQUEST");
      }
   });
});

describe("an Idempotency-Key", () => {
   const refusedKey = async (key: string) => {
      const error = await admin.loadTable("c", "s", "t", valid, key).then(
         () => undefined,
         (e) => e,
      );
      return error as CatalogAdminError;
   };

   it("is refused when empty or longer than 255 characters, before taking any lock", async () => {
      for (const key of ["", "k".repeat(256)]) {
         const error = await refusedKey(key);
         expect([error.status, error.reason]).toEqual([400, "INVALID_REQUEST"]);
         expect(error.message).toContain("'Idempotency-Key'");
      }
   });

   it("is taken at 255 characters, and only then reaches the catalog database", async () => {
      expect((await refusedKey("k".repeat(255))).reason).not.toBe(
         "INVALID_REQUEST",
      );
   });
});

describe("a load list's cursor", () => {
   it("is refused as a 400 when it is not one this server gave, before the catalog database is reached", async () => {
      for (const after of ["", "!!", "MQ==x", "bm9wZQ", "LTE"]) {
         const error = await admin.listLoads("c", "s", "t", 10, after).then(
            () => undefined,
            (e) => e,
         );
         expect(error).toBeInstanceOf(CatalogAdminError);
         expect([
            after,
            (error as CatalogAdminError).status,
            (error as CatalogAdminError).reason,
         ]).toEqual([after, 400, "INVALID_REQUEST"]);
      }
   });
});

describe("a table's recorded loads", () => {
   const ID = (n: number) => `aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaa${n}`;
   const record = (n: number, overrides: Record<string, unknown> = {}) =>
      JSON.stringify({
         publisherLoad: 1,
         id: ID(n),
         table: "s.t",
         requestDigest: "d",
         mode: "merge",
         result: { rowCount: n },
         ...overrides,
      });
   const row = (snapshot: number, extra: string) => ({
      snapshot_id: snapshot,
      committed_at: "2026-01-01T00:00:00.000Z",
      author: "publisher-admin",
      commit_message: null,
      commit_extra_info: extra,
   });
   // Newest first, as the catalog database answers: a corrupt record, and one that is another table's, among the real ones.
   const stored = [
      row(9, record(9)),
      row(8, '{"publisherLoad":1,{'),
      row(7, record(7, { table: "s.other" })),
      row(6, record(6)),
   ];
   /** The catalog database as readLoads sees it: it honors the query's `snapshot_id <` bound and LIMIT. */
   const reading = () => {
      const target = new CatalogAdmin({
         catalogDatabase: "host=127.0.0.1 port=1 dbname=none",
      });
      const internals = target as unknown as Record<string, unknown>;
      const pg = async (sql: string) => {
         const below = Number(
            sql.match(/snapshot_id < (\d+)/)?.[1] ?? Infinity,
         );
         const limit = Number(sql.match(/LIMIT (\d+)/)![1]);
         return stored.filter((r) => r.snapshot_id < below).slice(0, limit);
      };
      spyOn(internals, "requireCatalog" as never).mockResolvedValue(
         undefined as never,
      );
      spyOn(internals, "withCatalogDatabase" as never).mockImplementation(((
         fn: (pg: unknown) => unknown,
      ) => fn(pg)) as never);
      return target;
   };

   it("lists only the table's own loads, and a skipped record does not end the pages early", async () => {
      const target = reading();
      const first = await target.listLoads("c", "s", "t", 3);
      expect(first.items.map((l) => l.id)).toEqual([ID(9)]);
      expect(first.nextCursor).toBeDefined();
      const second = await target.listLoads("c", "s", "t", 3, first.nextCursor);
      expect(second.items.map((l) => l.id)).toEqual([ID(6)]);
      expect(second.nextCursor).toBeUndefined();
   });

   it("finds a load by its id in any case, and not by another record's", async () => {
      const target = reading();
      expect(
         (await target.getLoad("c", "s", "t", ID(9).toUpperCase())).id,
      ).toBe(ID(9));
      // The fake database returns every row whatever the id asked for: a record that is not that id's must not come back.
      const missing = await target
         .getLoad("c", "s", "t", ID(5))
         .catch((e) => e);
      expect(missing).toBeInstanceOf(CatalogAdminError);
      expect((missing as CatalogAdminError).reason).toBe("LOAD_NOT_FOUND");
   });
});

describe("a load that committed but whose record cannot be read back", () => {
   const failure = async (key?: string) => {
      const target = new CatalogAdmin({
         catalogDatabase: "host=127.0.0.1 port=1 dbname=none",
      });
      const internals = target as unknown as Record<string, unknown>;
      // The schema exists; no load is recorded, before or after the commit.
      const pg = async (sql: string) =>
         sql.includes("ducklake_schema") ? [{}] : [];
      const spies = [
         spyOn(target, "withTableLock").mockImplementation(((
            _name: string,
            _schema: string,
            _table: string,
            fn: (lock: unknown) => unknown,
         ) =>
            fn({
               pg,
               pgAfterCommit: pg,
               assertLockHeld: async () => undefined,
            })) as never),
         spyOn(internals, "requireCatalog" as never).mockResolvedValue(
            undefined as never,
         ),
         spyOn(internals, "schemaOwner" as never).mockResolvedValue(
            "o" as never,
         ),
         spyOn(tableLoad, "mergeStorageTable").mockResolvedValue({
            result: {
               rowsInserted: 1,
               rowsReplaced: 0,
               rowsUnchanged: 0,
               rowsDeleted: 0,
               rowCount: 1,
               cursorMax: null,
            },
            committedAfter: 5,
         } as never),
      ];
      try {
         return (await target
            .loadTable("c", "s", "t", valid, key)
            .catch((e) => e)) as Error;
      } finally {
         for (const spy of spies) spy.mockRestore();
      }
   };

   it("tells a keyed caller that the same key replays it", async () => {
      const error = await failure("run-1");
      expect(error.message).toContain("committed, but its record could not");
      expect(error.message).toContain("same Idempotency-Key replays it");
      expect(error.message).not.toContain("safe");
   });

   it("tells an unkeyed caller that a retry runs the load again", async () => {
      const error = await failure();
      expect(error.message).toContain("a retry runs the load again");
      expect(error.message).not.toContain("safe");
   });
});

describe("a load's failure as an admin refusal", () => {
   const failure = (error: unknown) =>
      (
         admin as unknown as { loadFailure(error: unknown): unknown }
      ).loadFailure(error);

   it("answers a refusal of the load as a 422 with its detail", () => {
      const mapped = failure(
         new LoadRefusalError("LOAD_DELETE_LIMIT_EXCEEDED", "too many", {
            wouldDelete: 4,
         }),
      ) as CatalogAdminError;
      expect([mapped.status, mapped.reason, mapped.detail]).toEqual([
         422,
         "LOAD_DELETE_LIMIT_EXCEEDED",
         { wouldDelete: 4 },
      ]);
   });

   it("answers DuckLake's commit conflict as a retryable 409", () => {
      const mapped = failure(
         new Error(
            'TransactionContext Error: Failed to commit: Transaction conflict - attempting to insert into table with name "t" - but another transaction has dropped it',
         ),
      ) as CatalogAdminError;
      expect([mapped.status, mapped.reason]).toEqual([409, "LOAD_CONFLICT"]);
   });

   it("answers a table another request created first as a retryable 409", () => {
      const mapped = failure(
         new Error('Catalog Error: Table with name "t" already exists!'),
      ) as CatalogAdminError;
      expect([mapped.status, mapped.reason]).toEqual([409, "LOAD_CONFLICT"]);
   });

   it("answers a schema dropped before the transaction began as a 404", () => {
      const mapped = failure(
         new Error(
            'Binder Error: Schema "s" not found in DuckLakeCatalog "lake"',
         ),
      ) as CatalogAdminError;
      expect([mapped.status, mapped.reason]).toEqual([404, "SCHEMA_NOT_FOUND"]);
   });

   it("keeps the admin refusals a load can meet, and leaves any other failure to answer 500", () => {
      const busy = new CatalogAdminError(503, "LOAD_BUSY", "busy");
      expect(failure(busy)).toBe(busy);
      const outside = failure(
         new BadRequestError(
            "The object must sit under the catalog's data path (/d/).",
         ),
      ) as CatalogAdminError;
      expect([outside.status, outside.reason]).toEqual([
         400,
         "OBJECT_OUTSIDE_DATA_PATH",
      ]);
      const other = new Error("boom");
      expect(failure(other)).toBe(other);
   });

   it("answers DuckDB's out-of-memory and spill-cap failures as a 422 LOAD_TOO_LARGE with the way out", () => {
      for (const message of [
         "Out of Memory Error: failed to pin block of size 256.0 KiB (1.3 GiB/1.3 GiB used)",
         "Out of Memory Error: failed to offload data block of size 256.0 KiB (5.9 GiB/6.0 GiB used).",
         "failed to offload data block of size 256.0 KiB (5.9 GiB/6.0 GiB used)",
         "failed to pin block of size 256.0 KiB",
         "the max_temp_directory_size cap was reached",
      ]) {
         const mapped = failure(new Error(message)) as CatalogAdminError;
         expect(mapped).toBeInstanceOf(CatalogAdminError);
         expect([mapped.status, mapped.reason]).toEqual([
            422,
            "LOAD_TOO_LARGE",
         ]);
         expect(mapped.message).toContain(message.replace(/\.$/, ""));
         expect(mapped.message).toContain("key");
         expect(mapped.message).toContain("admin.memoryLimit");
         expect(mapped.message).toContain("duckdb.memoryLimit");
      }
   });

   it("advises a keyed load to split its objects, not to give a key it has", () => {
      const mapped = (
         admin as unknown as {
            loadFailure(error: unknown, keyed: boolean): CatalogAdminError;
         }
      ).loadFailure(
         new Error("Out of Memory Error: failed to pin block"),
         true,
      );
      expect(mapped.reason).toBe("LOAD_TOO_LARGE");
      expect(mapped.message).toContain("Split the load's objects");
      expect(mapped.message).not.toContain("a key");
   });

   it("quotes only the first line of DuckDB's message, which goes on to advise PRAGMA and SET a caller cannot run", () => {
      const mapped = failure(
         new Error(
            "Out of Memory Error: failed to offload data block of size 256.0 KiB (5.9 GiB/6.0 GiB used).\nThis limit was set by the 'max_temp_directory_size' setting.\nYou can adjust this setting, by using (for example) PRAGMA max_temp_directory_size='10GiB'\n\nPossible solutions:\n* Reducing the number of threads (SET threads=X)",
         ),
      ) as CatalogAdminError;
      expect(mapped.reason).toBe("LOAD_TOO_LARGE");
      expect(mapped.message).toContain("6.0 GiB used)");
      expect(mapped.message).not.toMatch(/PRAGMA|SET threads|\n/);
   });

   it("leaves a transaction conflict after the load committed to answer 500, not as a 409 that says nothing was written", () => {
      const flush = new CommittedLoadError(
         new Error("Transaction conflict - attempting to insert"),
      );
      const mapped = failure(flush);
      expect(mapped).not.toBeInstanceOf(CatalogAdminError);
      expect((mapped as Error).message).toContain("Transaction conflict");
   });

   it("leaves an out-of-memory after the load committed to answer 500, since the rows are written", () => {
      const flush = new CommittedLoadError(
         new Error(
            "Out of Memory Error: failed to pin block of size 256.0 KiB",
         ),
      );
      const mapped = failure(flush);
      expect(mapped).not.toBeInstanceOf(CatalogAdminError);
      expect((mapped as Error).message).toContain("failed to pin block");
   });

   it("leaves a failure in the flush after a load that changed nothing to answer 500, not a 422 or 409 whose advice is for the load", () => {
      for (const message of [
         "Out of Memory Error: failed to pin block of size 256.0 KiB",
         "Transaction conflict - attempting to insert",
      ]) {
         const mapped = failure(new PostApplyError(new Error(message)));
         expect(mapped).not.toBeInstanceOf(CatalogAdminError);
         expect(mapped).not.toBeInstanceOf(CommittedLoadError);
         expect((mapped as Error).message).toBe(message);
      }
   });

   it("leaves a full spill disk, which a retry may cure, to answer 500", () => {
      const full = new Error(
         'IO Error: Could not write file "/tmp/duckdb_temp_storage-0.tmp": No space left on device',
      );
      expect(failure(full)).toBe(full);
   });

   it("maps the memory failures in a load's and a PUT's own failure only, not in redacted(), which reads and snapshot deletes share", () => {
      const oom = new Error("Out of Memory Error: failed to pin block");
      const redacted = (
         admin as unknown as { redacted(error: unknown): unknown }
      ).redacted(oom);
      expect(redacted).toBe(oom);
   });
});

describe("a table PUT that outgrows its write session", () => {
   const put = async (failure: Error) => {
      const target = new CatalogAdmin({
         catalogDatabase: "host=127.0.0.1 port=1 dbname=none",
      });
      const internals = target as unknown as Record<string, unknown>;
      const spies = [
         spyOn(internals, "withCatalogDatabase" as never).mockResolvedValue({
            owner: "o",
            sourceOwner: undefined,
         } as never),
         spyOn(internals, "listTables" as never).mockResolvedValue([] as never),
         spyOn(tableLoad, "loadStorageTable").mockRejectedValue(failure),
      ];
      try {
         return await target.putTable("c", "s", "t", { source }, false).then(
            () => undefined,
            (e) => e,
         );
      } finally {
         for (const spy of spies) spy.mockRestore();
      }
   };

   it("answers 422 LOAD_TOO_LARGE that says to write less, not to give a key", async () => {
      const error = await put(
         new Error(
            "Out of Memory Error: failed to offload data block of size 256.0 KiB (5.9 GiB/6.0 GiB used).\nThis limit was set by the 'max_temp_directory_size' setting.",
         ),
      );
      expect(error).toBeInstanceOf(CatalogAdminError);
      expect([error.status, error.reason]).toEqual([422, "LOAD_TOO_LARGE"]);
      expect(error.message).toContain("Write less");
      expect(error.message).not.toContain("key");
      expect(error.message).toContain("admin.memoryLimit");
   });

   it("answers an out-of-memory after the table was written as a 500, not as a PUT that wrote nothing", async () => {
      const error = await put(
         new CommittedLoadError(
            new Error(
               "Out of Memory Error: failed to pin block of size 256.0 KiB",
            ),
         ),
      );
      expect(error).toBeInstanceOf(CommittedLoadError);
      expect(error).not.toBeInstanceOf(CatalogAdminError);
   });

   it("answers a full spill disk as a 500 to retry", async () => {
      const full = new Error(
         'IO Error: Could not write file "/tmp/duckdb_temp_storage-0.tmp": No space left on device',
      );
      expect(await put(full)).toBe(full);
   });
});

describe("a table PUT's failure as an admin refusal", () => {
   const redacted = (error: unknown) =>
      (admin as unknown as { redacted(error: unknown): unknown }).redacted(
         error,
      );

   it("answers an object the reader refused as 422 OBJECT_UNREADABLE, as a load does", () => {
      const refusal = objectUnreadable(
         new Error(
            "Invalid Input Error: No magic bytes found at end of file 'gs://b/p/a.parquet'",
         ),
         ["gs://b/p/a.parquet"],
         true,
      );
      const mapped = redacted(refusal) as CatalogAdminError;
      expect([mapped.status, mapped.reason, mapped.message]).toEqual([
         422,
         "OBJECT_UNREADABLE",
         "Invalid Input Error: No magic bytes found at end of file 'a.parquet'",
      ]);
   });

   it("does not turn a transaction conflict into a load's 409", () => {
      const conflict = new Error("Transaction conflict - attempting to insert");
      expect(redacted(conflict)).toBe(conflict);
   });
});

describe("a load refused as busy", () => {
   let harness: MetricsHarness;
   beforeEach(async () => {
      harness = await startMetricsHarness();
      resetInstrumentsForTesting();
   });
   afterEach(async () => {
      resetInstrumentsForTesting();
      await harness.shutdown();
   });

   it("answers 503 LOAD_BUSY while this process's slot is held, and counts the refusal", async () => {
      const held = loadSlots.tryAcquire()!;
      try {
         const error = await withLoadSlot(async () => "ran").then(
            () => undefined,
            (e) => e,
         );
         expect([error.status, error.reason]).toEqual([503, "LOAD_BUSY"]);
      } finally {
         held.release();
      }
      expect(
         await harness.collectCounter("publisher_load_rejections_total", {
            cause: "slot",
         }),
      ).toBe(1);
      expect(await withLoadSlot(async () => "ran")).toBe("ran");
      expect(
         await harness.collectCounter("publisher_load_rejections_total"),
      ).toBe(1);
   });
});

describe("the admin API's documented reasons", () => {
   const read = (file: string) => readFileSync(file, "utf8");
   const thrown = new Set<string>();
   for (const [file, pattern] of [
      ["service/catalog_admin.ts", /CatalogAdminError\(\s*\d+,\s*"([A-Z_]+)"/g],
      ["routes/admin.ts", /CatalogAdminError\(\s*\d+,\s*"([A-Z_]+)"/g],
      [
         "service/materialization/run/storage_table_load.ts",
         /LoadRefusalError\(\s*"([A-Z_]+)"/g,
      ],
   ] as const) {
      for (const match of read(resolve(SERVER_SRC, file)).matchAll(pattern)) {
         thrown.add(match[1]);
      }
   }
   // Parsed, not searched: a spec that does not parse documents nothing.
   const doc = parse(read(resolve(REPO_ROOT, "admin-api-doc.yaml")));
   const documented: string =
      doc.components.schemas.Error.properties.reason.description;
   const schemas = doc.components.schemas;

   it("finds the reasons the code throws", () => {
      // A pattern that matched nothing would pass the next check vacuously.
      expect([...thrown]).toEqual(
         expect.arrayContaining([
            "CATALOG_NOT_FOUND",
            "LOAD_BUSY",
            "LOAD_NOT_FOUND",
            "IDEMPOTENCY_KEY_REUSED",
            "LOAD_CONFLICT",
            "LOAD_COLUMN_MISMATCH",
            "LOAD_DELETE_LIMIT_EXCEEDED",
            "OBJECT_UNREADABLE",
            "SCHEMA_IS_DESTINATION",
         ]),
      );
   });

   it("lists every contract violation the code gives, and no other", () => {
      const violations = [
         ...read(
            resolve(
               SERVER_SRC,
               "service/materialization/run/storage_table_load.ts",
            ),
         ).matchAll(/contractRefusal\(\s*"([a-z_]+)"/g),
      ].map((m) => m[1]);
      expect(violations.length).toBeGreaterThan(5);
      expect([...new Set(violations)].sort()).toEqual(
         [...schemas.ColumnMismatchDetail.properties.violation.enum].sort(),
      );
   });

   it("gives the detail keys the spec's schemas require", () => {
      const mismatch = (() => {
         try {
            loadLayout(
               { key: ["nope"] },
               [{ name: "id", type: "BIGINT" }],
               null,
            );
         } catch (error) {
            return (error as LoadRefusalError).detail;
         }
         throw new Error("the load layout accepted a key the objects lack");
      })();
      expect(Object.keys(mismatch!).sort()).toEqual(
         [...schemas.ColumnMismatchDetail.required].sort(),
      );
      const unreadable = objectUnreadable(
         new Error('IO Error: No files found that match the pattern "a"'),
         ["a"],
      );
      expect(Object.keys(unreadable!.detail!).sort()).toEqual(
         [...schemas.ObjectUnreadableDetail.required].sort(),
      );
      const limit = read(
         resolve(
            SERVER_SRC,
            "service/materialization/run/storage_table_load.ts",
         ),
      ).match(/"LOAD_DELETE_LIMIT_EXCEEDED",\s*`[^`]*`,\s*\{([^}]*)\}/);
      expect(limit).not.toBeNull();
      expect(
         [...limit![1].matchAll(/^\s*(\w+)/gm)].map((m) => m[1]).sort(),
      ).toEqual([...schemas.DeleteLimitDetail.required].sort());
   });

   it("lists every reason the code throws in the spec", () => {
      // The list itself: the prose below it names each reason again, which would keep a stale list entry passing.
      const listed = documented
         .match(/Machine-readable:([^.]*)\./)![1]
         .split(",")
         .map((reason) => reason.trim());
      expect(listed.length).toBeGreaterThan(10);
      expect([...thrown].filter((reason) => !listed.includes(reason))).toEqual(
         [],
      );
   });
});

describe("the admin API's load schemas", () => {
   const doc = parse(
      readFileSync(resolve(REPO_ROOT, "admin-api-doc.yaml"), "utf8"),
   );
   const loads =
      doc.paths[
         "/catalogs/{catalogName}/schemas/{schemaName}/tables/{tableName}/loads"
      ];
   const schemaRef = (response: {
      content: Record<string, { schema: { $ref: string } }>;
   }) => response.content["application/json"].schema.$ref;
   /** A schema's required fields, through its allOf parts. */
   const required = (name: string): string[] => {
      const schema = doc.components.schemas[name];
      return [
         ...(schema.required ?? []),
         ...(schema.allOf ?? []).flatMap(
            (part: { $ref?: string; required?: string[] }) =>
               part.$ref
                  ? required(part.$ref.split("/").pop()!)
                  : (part.required ?? []),
         ),
      ];
   };

   it("answers a no-op with counts alone, a commit with its id, snapshot and time, and a read with its author and message", () => {
      expect(required("LoadNoOp")).toContain("mode");
      for (const field of ["id", "snapshotId", "committedAt"]) {
         expect(required("LoadNoOp")).not.toContain(field);
         expect(required("Load")).toContain(field);
      }
      expect(required("Load")).toContain("mode");
      for (const field of ["author", "commitMessage"]) {
         expect(required("Load")).not.toContain(field);
         expect(required("LoadRead")).toContain(field);
      }
   });

   it("gives each response the schema the route answers with", () => {
      expect(schemaRef(loads.post.responses["201"])).toBe(
         "#/components/schemas/Load",
      );
      expect(schemaRef(loads.post.responses["200"])).toBe(
         "#/components/schemas/LoadNoOp",
      );
      expect(
         schemaRef(
            doc.paths[
               "/catalogs/{catalogName}/schemas/{schemaName}/tables/{tableName}/loads/{loadId}"
            ].get.responses["200"],
         ),
      ).toBe("#/components/schemas/LoadRead");
      expect(doc.components.schemas.LoadList.properties.items.items.$ref).toBe(
         "#/components/schemas/LoadRead",
      );
   });

   it("declares the list limit once, for every list that takes it", () => {
      expect(JSON.stringify(doc).match(/"name":"limit"/g)).toHaveLength(1);
      expect(loads.get.parameters).toContainEqual({
         $ref: "#/components/parameters/limit",
      });
      expect(
         doc.paths["/catalogs/{catalogName}/snapshots"].get.parameters,
      ).toContainEqual({ $ref: "#/components/parameters/limit" });
   });
});
