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
import type { components } from "../api";
import { bigIntReplacer } from "../json_utils";
import { driverValue, fakeSnowflake } from "../test_helpers/fake_snowflake";
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

/**
 * R-08: a `.sql()` source's schema is read by creating a session-scoped TEMP
 * VIEW and describing it. The fake pool below behaves as generic-pool's FIFO
 * pool does once it holds two idle sessions: each `use()` takes the session
 * idle the longest, so consecutive statements land on different sessions.
 * Each session sees only the temp views it created, as Snowflake's do.
 */
describe("a .sql() source's schema read on a pooled Snowflake connection", () => {
   type Session = { id: number; views: Set<string> };

   function pooledFake() {
      const connection = new PublisherSnowflakeConnection("snowflake", {
         connOptions: { ...BASE, password: "pw" },
         poolOptions: { min: 0, max: 1 },
      });
      const idle: Session[] = [
         { id: 1, views: new Set() },
         { id: 2, views: new Set() },
      ];
      const log: { session: number; sql: string }[] = [];
      const destroyed: number[] = [];
      const pool_ = {
         acquire: async () => idle.shift() as Session,
         release: async (s: Session) => {
            idle.push(s);
         },
         destroy: async (s: Session) => {
            destroyed.push(s.id);
         },
      };
      const _execute = async (sql: string, s: Session) => {
         log.push({ session: s.id, sql });
         if (sql.includes("missing_table")) {
            throw new Error("Object 'MISSING_TABLE' does not exist.");
         }
         const create = /CREATE OR REPLACE TEMP VIEW (\w+)/i.exec(sql);
         if (create) {
            s.views.add(create[1].toUpperCase());
            return [];
         }
         const describe = /^\s*DESCRIBE TABLE (\w+)/i.exec(sql);
         if (describe) {
            const name = describe[1].toUpperCase();
            if (!s.views.has(name)) {
               throw new Error(
                  `SQL compilation error:\nTable '${name}' does not exist or not authorized.`,
               );
            }
            return [
               { name: "id", type: "NUMBER(38,0)" },
               { name: "label", type: "VARCHAR(16777216)" },
            ];
         }
         return [];
      };
      const executor = {
         pool_,
         ensureSessionInitialized: async () => {},
         _execute,
         // The connector's own pooled calls: one session per statement.
         async batch(sql: string) {
            const s = await pool_.acquire();
            try {
               const rows = await _execute(sql, s);
               await pool_.release(s);
               return rows;
            } catch (e) {
               await pool_.destroy(s);
               throw e;
            }
         },
         async tryBatch(sql: string) {
            const s = await pool_.acquire();
            try {
               return await _execute(sql, s);
            } catch {
               return undefined;
            } finally {
               await pool_.release(s);
            }
         },
         done: async () => {},
      };
      (connection as unknown as { executor: unknown }).executor = executor;
      return { connection, log, idle, destroyed };
   }

   const REF = {
      connection: "snowflake",
      selectStr: "SELECT id, label FROM looker.sf_types",
   };

   it("fails the upstream way: the DESCRIBE lands on another session", async () => {
      const { connection, log } = pooledFake();
      const upstream = Object.getPrototypeOf(
         PublisherSnowflakeConnection.prototype,
      ) as PublisherSnowflakeConnection;
      await expect(
         upstream.fetchSelectSchema.call(connection, REF),
      ).rejects.toThrow(/Table 'TT[0-9A-F]+' does not exist or not authorized/);
      expect(log.map((l) => l.session)).toEqual([1, 2]);
   });

   it("reads the schema on the session that created the temp view, and returns it", async () => {
      const { connection, log, idle, destroyed } = pooledFake();
      const def = await connection.fetchSelectSchema(REF);
      expect(def.fields.map((f) => [f.name, f.type])).toEqual([
         ["id", "number"],
         ["label", "string"],
      ]);
      expect(log.map((l) => l.session)).toEqual([1, 1]);
      expect(log[0].sql).toMatch(/^CREATE OR REPLACE TEMP VIEW tt/);
      expect(log[1].sql).toMatch(/^DESCRIBE TABLE tt/);
      // The session goes back to the pool; nothing was destroyed.
      expect(idle.map((s) => s.id).sort()).toEqual([1, 2]);
      expect(destroyed).toEqual([]);
   });

   it("destroys the borrowed session when the read fails, as pool.use() would", async () => {
      const { connection, idle, destroyed } = pooledFake();
      await expect(
         connection.fetchSelectSchema({
            ...REF,
            selectStr: "SELECT * FROM missing_table",
         }),
      ).rejects.toThrow(/MISSING_TABLE/);
      expect(destroyed).toEqual([1]);
      expect(idle.map((s) => s.id)).toEqual([2]);
   });

   it("keeps a connector without the pool internals on the upstream path", async () => {
      const connection = fakeSnowflake(
         () => [],
         [{ name: "id", type: "NUMBER(38,0)" }],
      );
      try {
         const def = await connection.fetchSelectSchema(REF);
         expect(def.fields.map((f) => f.name)).toEqual(["id"]);
      } finally {
         await connection.close();
      }
   });
});
