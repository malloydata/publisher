// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { afterEach, beforeEach, describe, expect, it } from "bun:test";
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
import {
   LoadRefusalError,
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

/** What an attempt answers, or undefined when it got past every refusal and failed reaching the catalog database. */
async function refusalOf(attempt: () => Promise<unknown>) {
   const error = await attempt().then(
      () => undefined,
      (e) => e,
   );
   return error instanceof CatalogAdminError
      ? [error.status, error.reason]
      : undefined;
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

   it("leaves a DELETE in it, and every other schema, alone", async () => {
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
         expect(mapped.message).toContain(message);
         expect(mapped.message).toContain("key");
         expect(mapped.message).toContain("admin.memoryLimit");
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
