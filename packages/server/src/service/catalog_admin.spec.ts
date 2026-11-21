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
import { CatalogAdmin, CatalogAdminError, withLoadSlot } from "./catalog_admin";
import { LoadRefusalError } from "./materialization/run/storage_table_load";

const SERVER_SRC = resolve(__dirname, "..");
const REPO_ROOT = resolve(__dirname, "../../../..");

/** The admin with a catalog database nothing listens on: a load that passes validation fails reaching it. */
const admin = new CatalogAdmin({
   catalogDatabase: "host=127.0.0.1 port=1 dbname=none connect_timeout=1",
});

const valid = {
   loadId: "run_1:charges",
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
            loadId: "x",
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
      ["no loadId", { ...valid, loadId: undefined }, "'loadId'"],
      ["a loadId with a space", { ...valid, loadId: "a b" }, "'loadId'"],
      [
         "a loadId over 128 characters",
         { ...valid, loadId: "a".repeat(129) },
         "'loadId'",
      ],
      ["an empty loadId", { ...valid, loadId: "" }, "'loadId'"],
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

describe("a load's failure as an admin refusal", () => {
   const failure = (error: unknown) =>
      (
         admin as unknown as { loadFailure(error: unknown): unknown }
      ).loadFailure(error);

   it("answers a refusal of the load as a 422 with its detail", () => {
      const mapped = failure(
         new LoadRefusalError("LOAD_ROW_DROP_GUARD", "too many", {
            wouldDelete: 4,
         }),
      ) as CatalogAdminError;
      expect([mapped.status, mapped.reason, mapped.detail]).toEqual([
         422,
         "LOAD_ROW_DROP_GUARD",
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
      const other = new Error("Out of Memory Error: x");
      expect(failure(other)).toBe(other);
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

   it("finds the reasons the code throws", () => {
      // A pattern that matched nothing would pass the next check vacuously.
      expect([...thrown]).toEqual(
         expect.arrayContaining([
            "CATALOG_NOT_FOUND",
            "LOAD_BUSY",
            "LOAD_CONFLICT",
            "LOAD_SCHEMA_CONTRACT",
            "LOAD_ROW_DROP_GUARD",
            "LOAD_OBJECT_UNREADABLE",
         ]),
      );
   });

   it("lists every contract detail the code gives", () => {
      const details = [
         ...read(
            resolve(
               SERVER_SRC,
               "service/materialization/run/storage_table_load.ts",
            ),
         ).matchAll(/contractRefusal\(\s*"([a-z_]+)"/g),
      ].map((m) => m[1]);
      expect(details.length).toBeGreaterThan(5);
      expect(
         details.filter((detail) => !documented.includes(`\`${detail}\``)),
      ).toEqual([]);
   });

   it("lists every reason the code throws in the spec", () => {
      expect(
         [...thrown].filter((reason) => !documented.includes(reason)),
      ).toEqual([]);
   });
});
