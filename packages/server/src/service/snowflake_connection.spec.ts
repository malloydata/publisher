// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * F-16: every Snowflake connection Publisher builds reads integers and
 * timestamps as a stored (DuckDB) copy answers them, so a live answer equals
 * the served one. The driver values are snowflake-sdk's own
 * (test_helpers/fake_snowflake.ts).
 */

import { DuckDBConnection } from "@malloydata/db-duckdb";
import { SingleConnectionRuntime } from "@malloydata/malloy";
import { afterEach, beforeEach, describe, expect, it } from "bun:test";
import fs from "fs/promises";
import os from "os";
import path from "path";
import snowflake from "snowflake-sdk";
import type { components } from "../api";
import { assertSnowflakeApplicationNameConfig } from "../config";
import { bigIntReplacer } from "../json_utils";
import { driverValue, fakeSnowflake } from "../test_helpers/fake_snowflake";
import { installSettings } from "../test_helpers/settings_env";
import {
   createEnvironmentConnections,
   buildEnvironmentMalloyConfig,
} from "./connection";
import {
   buildSnowflakeConnection,
   buildSnowflakeConnOptions,
   normalizeSnowflakeValue,
   PublisherSnowflakeConnection,
} from "./snowflake_connection";

type ApiConnection = components["schemas"]["Connection"];

const KEY = "-----BEGIN PRIVATE KEY-----MIIB-----END PRIVATE KEY-----";
const BASE = {
   account: "test-account",
   username: "test-user",
   warehouse: "test-warehouse",
};

// 2026-01-15T18:00:00Z; a TIMESTAMP_TZ's wire value carries its offset as
// an index, 1440 + minutes (-08:00 is 960).
const EPOCH = "1768500000.000";
const TZ_MINUS_8 = `${EPOCH} 960`;
const BIG = "4530000000000225187";

describe("buildSnowflakeConnOptions", () => {
   it("reads integers as the connector's default does, on the password path", () => {
      const options = buildSnowflakeConnOptions("sf", {
         ...BASE,
         password: "pw",
      });
      expect(options).toEqual({
         ...BASE,
         password: "pw",
         jsTreatIntegerAsBigInt: true,
      });
   });

   it("reads integers as the connector's default does, on the key-pair path", () => {
      const options = buildSnowflakeConnOptions("sf", {
         ...BASE,
         privateKey: KEY,
         privateKeyPass: "phrase",
         role: "r",
         database: "db",
         schema: "s",
      });
      expect(options).toMatchObject({
         ...BASE,
         authenticator: "SNOWFLAKE_JWT",
         privateKeyPass: "phrase",
         role: "r",
         database: "db",
         schema: "s",
         jsTreatIntegerAsBigInt: true,
      });
      expect(options.privateKey).toContain("BEGIN PRIVATE KEY");
   });

   it("uses the key pair when both a key and a password are set", () => {
      const options = buildSnowflakeConnOptions("sf", {
         ...BASE,
         privateKey: KEY,
         password: "pw",
      });
      expect(options.authenticator).toBe("SNOWFLAKE_JWT");
      expect(options.jsTreatIntegerAsBigInt).toBe(true);
   });

   it("drops explicit nulls, which would break the connection's digest", () => {
      const options = buildSnowflakeConnOptions("sf", {
         ...BASE,
         password: "pw",
         privateKey: null,
         privateKeyPass: null,
         database: null,
         schema: null,
         role: null,
      } as unknown as components["schemas"]["SnowflakeConnection"]);
      for (const field of [
         "privateKey",
         "privateKeyPass",
         "database",
         "schema",
         "role",
         "authenticator",
      ]) {
         expect(field in options).toBe(false);
      }
   });

   it("refuses a connection with neither a password nor a key", () => {
      expect(() => buildSnowflakeConnOptions("sf", { ...BASE })).toThrow(
         /privateKey or password is required for connection sf/,
      );
      expect(() =>
         buildSnowflakeConnOptions("sf", { ...BASE, warehouse: "" }),
      ).toThrow(/warehouse is required/);
   });
});

describe("the client application a Snowflake session reports", () => {
   let restoreSettings: (() => void) | undefined;
   const installEnv = (env: Record<string, string>): void => {
      restoreSettings?.();
      restoreSettings = installSettings({ env }).restore;
   };
   afterEach(() => {
      restoreSettings?.();
      restoreSettings = undefined;
   });

   it("is left to the driver when unset", () => {
      installEnv({});
      const options = buildSnowflakeConnOptions("sf", {
         ...BASE,
         password: "pw",
      });
      expect("application" in options).toBe(false);
   });

   it("is PUBLISHER_SNOWFLAKE_APPLICATION_NAME on the password and key-pair paths", () => {
      installEnv({ PUBLISHER_SNOWFLAKE_APPLICATION_NAME: "Credible_Credible" });
      for (const auth of [{ password: "pw" }, { privateKey: KEY }]) {
         const options = buildSnowflakeConnOptions("sf", { ...BASE, ...auth });
         expect(options.application).toBe("Credible_Credible");
      }
   });

   it("reaches every Snowflake connection an environment builds", async () => {
      installEnv({ PUBLISHER_SNOWFLAKE_APPLICATION_NAME: "Credible_Credible" });
      const root = await fs.mkdtemp(path.join(os.tmpdir(), "sf-app-spec-"));
      const configs: ApiConnection[] = [
         {
            name: "sf_password",
            type: "snowflake",
            snowflakeConnection: { ...BASE, password: "pw" },
         },
         {
            name: "sf_key_pair",
            type: "snowflake",
            snowflakeConnection: { ...BASE, privateKey: KEY },
         },
      ];
      const { malloyConnections, releaseConnections } =
         await createEnvironmentConnections(configs, root);
      try {
         for (const config of configs) {
            const { connOptions } = malloyConnections.get(
               config.name!,
            ) as unknown as { connOptions: Record<string, unknown> };
            expect(connOptions.application).toBe("Credible_Credible");
         }
      } finally {
         await releaseConnections();
         await fs.rm(root, { recursive: true, force: true });
      }
   });

   it("accepts exactly the names snowflake-sdk accepts", () => {
      // snowflake-sdk validates the name when a connection is created, with
      // no network, so a value it refuses would fail every Snowflake
      // connection. The boot check must refuse the same values, no more.
      snowflake.configure({ logLevel: "OFF" });
      for (const name of [
         "Credible_Credible",
         "DagsterLabs_Dagster",
         "Ab",
         "A" + "b".repeat(50),
         "a.b-c_d",
         "A",
         "A" + "b".repeat(51),
         "Credible Data",
         "1Credible",
         "_Credible",
         "Credible/Data",
      ]) {
         let sdkAccepts = true;
         try {
            snowflake.createConnection({
               ...BASE,
               password: "pw",
               application: name,
            });
         } catch {
            sdkAccepts = false;
         }
         installEnv({ PUBLISHER_SNOWFLAKE_APPLICATION_NAME: name });
         let publisherAccepts = true;
         try {
            assertSnowflakeApplicationNameConfig();
         } catch {
            publisherAccepts = false;
         }
         expect({ name, publisherAccepts }).toEqual({
            name,
            publisherAccepts: sdkAccepts,
         });
      }
   });
});

describe("every Snowflake connection an environment builds", () => {
   let root: string;
   beforeEach(async () => {
      root = await fs.mkdtemp(path.join(os.tmpdir(), "sf-conn-spec-"));
   });
   afterEach(async () => {
      await fs.rm(root, { recursive: true, force: true });
   });

   const configs: [string, ApiConnection][] = [
      [
         "password",
         {
            name: "sf_password",
            type: "snowflake",
            snowflakeConnection: { ...BASE, password: "pw" },
         },
      ],
      [
         "key pair",
         {
            name: "sf_key_pair",
            type: "snowflake",
            snowflakeConnection: { ...BASE, privateKey: KEY },
         },
      ],
   ];

   for (const [label, config] of configs) {
      it(`is Publisher's, with the connector's integer handling (${label})`, async () => {
         const { malloyConnections, releaseConnections } =
            await createEnvironmentConnections([config], root);
         try {
            const connection = malloyConnections.get(config.name!);
            expect(connection).toBeInstanceOf(PublisherSnowflakeConnection);
            const { connOptions } = connection as unknown as {
               connOptions: Record<string, unknown>;
            };
            expect(connOptions.jsTreatIntegerAsBigInt).toBe(true);
         } finally {
            await releaseConnections();
         }
      });

      it(`is Publisher's on a connection test's lookup too (${label})`, async () => {
         const environment = buildEnvironmentMalloyConfig([config], root);
         try {
            const connection =
               await environment.malloyConfig.connections.lookupConnection(
                  config.name,
               );
            expect(connection).toBeInstanceOf(PublisherSnowflakeConnection);
         } finally {
            await environment.releaseConnections();
         }
      });
   }

   it("builds directly with the same options", async () => {
      const connection = buildSnowflakeConnection("sf", {
         ...BASE,
         password: "pw",
         responseTimeoutMilliseconds: 1234,
      });
      try {
         const internals = connection as unknown as {
            connOptions: Record<string, unknown>;
            timeoutMs: number;
         };
         expect(internals.connOptions.jsTreatIntegerAsBigInt).toBe(true);
         expect(internals.timeoutMs).toBe(1234);
      } finally {
         await connection.close();
      }
   });
});

describe("normalizeSnowflakeValue, over snowflake-sdk's own values", () => {
   it("keeps every digit of an integer past 2^53, as a BigInt", () => {
      const value = driverValue("fixed", BIG);
      // The driver's wrapper serializes as a string, whatever its size.
      expect(JSON.stringify(value)).toBe(`"${BIG}"`);
      expect(normalizeSnowflakeValue(value)).toBe(BigInt(BIG));
      expect(normalizeSnowflakeValue(driverValue("fixed", `-${BIG}`))).toBe(
         -BigInt(BIG),
      );
   });

   it("reads a safe integer as a number, as before", () => {
      expect(normalizeSnowflakeValue(driverValue("fixed", "5"))).toBe(5);
      expect(
         normalizeSnowflakeValue(driverValue("fixed", "9007199254740991")),
      ).toBe(Number.MAX_SAFE_INTEGER);
      expect(
         normalizeSnowflakeValue(driverValue("fixed", "9007199254740992")),
      ).toBe(9007199254740992n);
   });

   it("leaves a scaled NUMBER and a float alone", () => {
      expect(normalizeSnowflakeValue(driverValue("fixed", "5.25", 2))).toBe(
         5.25,
      );
      expect(normalizeSnowflakeValue(driverValue("real", "1.5"))).toBe(1.5);
   });

   it("renders a TIMESTAMP_TZ as its instant in UTC ISO form", () => {
      const value = driverValue("timestamp_tz", TZ_MINUS_8, 3);
      // As live rendered it before: the account's format, the value's offset.
      expect(JSON.stringify(value)).toBe('"2026-01-15 10:00:00.000 -0800"');
      const normalized = normalizeSnowflakeValue(value);
      expect(normalized).toBeInstanceOf(Date);
      expect(JSON.stringify(normalized)).toBe('"2026-01-15T18:00:00.000Z"');
   });

   it("renders TIMESTAMP_NTZ, TIMESTAMP_LTZ and DATE as DuckDB does", () => {
      for (const type of ["timestamp_ntz", "timestamp_ltz"]) {
         const value = driverValue(type, EPOCH, 3);
         expect(JSON.stringify(value)).toBe('"2026-01-15T18:00:00.000+00:00"');
         expect(JSON.stringify(normalizeSnowflakeValue(value))).toBe(
            '"2026-01-15T18:00:00.000Z"',
         );
      }
      const date = driverValue("date", "20468");
      expect(JSON.stringify(date)).toBe('"2026-01-15"');
      expect(JSON.stringify(normalizeSnowflakeValue(date))).toBe(
         '"2026-01-15T00:00:00.000Z"',
      );
   });

   it("passes other values through and walks arrays and plain objects", () => {
      const plain = new Date("2026-01-15T18:00:00Z");
      expect(normalizeSnowflakeValue(plain)).toBe(plain);
      expect(normalizeSnowflakeValue(null)).toBeNull();
      expect(normalizeSnowflakeValue("x")).toBe("x");
      expect(normalizeSnowflakeValue(true)).toBe(true);
      expect(
         normalizeSnowflakeValue({
            a: [driverValue("fixed", BIG)],
            b: { c: driverValue("timestamp_tz", TZ_MINUS_8, 3) },
         }),
      ).toEqual({
         a: [BigInt(BIG)],
         b: { c: new Date("2026-01-15T18:00:00.000Z") },
      });
      // A plain object's own `value` is data, not a driver wrapper.
      expect(normalizeSnowflakeValue({ value: 5n })).toEqual({ value: 5n });
   });
});

/** A live Snowflake connection whose one row holds each kind of value. */
function fakeLiveSnowflake(): PublisherSnowflakeConnection {
   return fakeSnowflake(
      () => [
         {
            total: driverValue("fixed", BIG),
            n: driverValue("fixed", "5"),
            at: driverValue("timestamp_tz", TZ_MINUS_8, 3),
            day: driverValue("date", "20468"),
         },
      ],
      [
         { name: "total", type: "NUMBER(38,0)" },
         { name: "n", type: "NUMBER(38,0)" },
         { name: "at", type: "TIMESTAMP_TZ(9)" },
         { name: "day", type: "DATE" },
      ],
   );
}

describe("a live Snowflake answer equals the served one", () => {
   it("gives the same compact JSON as the DuckDB copy of the same row", async () => {
      const live = fakeLiveSnowflake();
      const liveRuntime = new SingleConnectionRuntime({ connection: live });
      const liveResult = await liveRuntime
         .loadQuery(
            `run: snowflake.sql("""SELECT total, n, at, day FROM t""") -> { select: * }`,
         )
         .run();

      const duckdb = new DuckDBConnection("duckdb", ":memory:");
      try {
         // A storage= copy is DuckDB: a HUGEINT read of the stored integer,
         // the instant as TIMESTAMPTZ, the DATE as DATE.
         const servedResult = await new SingleConnectionRuntime({
            connection: duckdb,
         })
            .loadQuery(
               `run: duckdb.sql("""SELECT ${BIG}::HUGEINT AS total, 5::BIGINT AS n, TIMESTAMPTZ '2026-01-15 10:00:00-08' AS "at", DATE '2026-01-15' AS "day" """) -> { select: * }`,
            )
            .run();

         const liveJson = JSON.stringify(liveResult.data.value, bigIntReplacer);
         const servedJson = JSON.stringify(
            servedResult.data.value,
            bigIntReplacer,
         );
         // Column order is the fake DESCRIBE's, so compare as objects.
         expect(JSON.parse(liveJson)).toEqual([
            {
               total: BIG,
               n: 5,
               at: "2026-01-15T18:00:00.000Z",
               day: "2026-01-15T00:00:00.000Z",
            },
         ]);
         expect(JSON.parse(liveJson)).toEqual(JSON.parse(servedJson));

         // The full result keeps every digit of the bigint field too.
         const cell = (liveResult.data.value[0] as Record<string, unknown>)
            .total;
         expect(cell).toBe(BigInt(BIG));
      } finally {
         await duckdb.close();
         await live.close();
      }
   });

   it("normalizes a streamed row as it does a batch", async () => {
      const connection = fakeSnowflake(() => [
         { total: driverValue("fixed", BIG) },
      ]);
      try {
         const rows = [];
         for await (const row of connection.runSQLStream("SELECT 1")) {
            rows.push(row);
         }
         expect(rows).toEqual([{ total: BigInt(BIG) }]);
      } finally {
         await connection.close();
      }
   });
});
