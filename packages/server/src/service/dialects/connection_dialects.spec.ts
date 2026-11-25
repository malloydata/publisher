// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { describe, expect, it } from "bun:test";
import { getStaticConnectionAttributes } from "../connection_config";
import { registeredDialect } from "../materialization/domain/dialect";
import {
   CONNECTION_DIALECTS,
   connectionDialectOf,
   registeredConnectionType,
   type ConnectionType,
} from ".";

/**
 * The attributes the API reported for every connection type before the
 * registry existed (`getStaticConnectionAttributes`' switch), pinned: a
 * registry entry that changes one changes what clients read.
 */
const ATTRIBUTES_SNAPSHOT: Record<ConnectionType, unknown> = {
   postgres: {
      dialectName: "postgres",
      isPool: true,
      canPersist: true,
      canStream: true,
   },
   bigquery: {
      dialectName: "standardsql",
      isPool: false,
      canPersist: true,
      canStream: true,
   },
   snowflake: {
      dialectName: "snowflake",
      isPool: true,
      canPersist: true,
      canStream: true,
   },
   trino: {
      dialectName: "trino",
      isPool: false,
      canPersist: true,
      canStream: false,
   },
   databricks: {
      dialectName: "databricks",
      isPool: false,
      canPersist: true,
      canStream: false,
   },
   mysql: {
      dialectName: "mysql",
      isPool: false,
      canPersist: true,
      canStream: false,
   },
   sqlserver: {
      dialectName: "sqlserver",
      isPool: true,
      canPersist: false,
      canStream: true,
   },
   duckdb: {
      dialectName: "duckdb",
      isPool: false,
      canPersist: true,
      canStream: true,
   },
   motherduck: {
      dialectName: "duckdb",
      isPool: false,
      canPersist: true,
      canStream: true,
   },
   ducklake: {
      dialectName: "duckdb",
      isPool: false,
      canPersist: true,
      canStream: true,
   },
   publisher: undefined,
};

const TYPES = Object.keys(CONNECTION_DIALECTS) as ConnectionType[];

describe("the connection-type registry", () => {
   it("has an entry for every type, keyed by its own type", () => {
      expect(TYPES.sort()).toEqual(
         (Object.keys(ATTRIBUTES_SNAPSHOT) as ConnectionType[]).sort(),
      );
      for (const type of TYPES) {
         expect(CONNECTION_DIALECTS[type].type).toBe(type);
      }
   });

   for (const type of Object.keys(ATTRIBUTES_SNAPSHOT) as ConnectionType[]) {
      it(`reports ${type}'s attributes as before`, () => {
         expect(getStaticConnectionAttributes(type)).toEqual(
            ATTRIBUTES_SNAPSHOT[type] as never,
         );
      });
   }

   it("hands each caller a fresh attributes object", () => {
      for (const type of TYPES) {
         const first = getStaticConnectionAttributes(type);
         if (first === undefined) continue;
         first.canPersist = !first.canPersist;
         expect(getStaticConnectionAttributes(type)).toEqual(
            ATTRIBUTES_SNAPSHOT[type] as never,
         );
      }
   });

   it("maps every type with attributes to a registered Malloy dialect", () => {
      for (const type of TYPES) {
         const attributes = CONNECTION_DIALECTS[type].attributes;
         if (attributes === undefined) continue;
         expect({
            type,
            dialect: registeredDialect(attributes.dialectName),
         }).toEqual({ type, dialect: attributes.dialectName as never });
      }
   });

   it("knows no type that is not registered, prototype members included", () => {
      for (const type of [
         undefined,
         "",
         "standardsql",
         "redshift",
         "gcs",
         "constructor",
         "toString",
         "__proto__",
         "hasOwnProperty",
         42,
      ]) {
         expect(registeredConnectionType(type)).toBeUndefined();
         expect(connectionDialectOf(type)).toBeUndefined();
         expect(getStaticConnectionAttributes(type as never)).toBeUndefined();
      }
   });
});
