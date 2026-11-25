// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { describe, expect, it } from "bun:test";
import type { components } from "../../api";
import { isSameCatalog } from "../materialization/domain/same_catalog_build";
import {
   PASSTHROUGH_SOURCE_TYPES,
   storageBuildReadsConnectionType,
   unreadableSourceConnectionMessage,
} from "../materialization/domain/source_connection_type";
import { READ_ONLY_DIALECTS } from "../materialization/plan/materialization_eligibility";
import { passthroughBuildOf } from "../materialization/run/passthrough_build";
import { CONNECTION_DIALECTS, connectionDialectOf } from ".";

/**
 * The connection-type facts materialization's `domain/` and `plan/` keep as
 * data (they import nothing from the service, so they cannot read the
 * registry) agree with the registry, for every connection type and for type
 * strings nothing registers: which types a `storage=` build reads, which is
 * a catalog, which proxies another server's connection, and which dialects'
 * connections cannot hold a table.
 */

type ApiConnection = components["schemas"]["Connection"];

const TYPES = [
   ...Object.keys(CONNECTION_DIALECTS),
   "constructor",
   "toString",
   "__proto__",
   "nope",
   "",
];

const passthroughTypes = () =>
   TYPES.filter((t) => connectionDialectOf(t)?.passthrough !== undefined);
const isCatalog = (t: string) =>
   connectionDialectOf(t)?.destination !== undefined;

describe("materialization's connection-type data agrees with the registry", () => {
   it("PASSTHROUGH_SOURCE_TYPES are the types with a passthrough, and each has a passthrough build", () => {
      expect([...PASSTHROUGH_SOURCE_TYPES].sort() as string[]).toEqual(
         passthroughTypes().sort(),
      );
      for (const t of TYPES) {
         expect({ t, build: passthroughBuildOf(t) !== undefined }).toEqual({
            t,
            build: connectionDialectOf(t)?.passthrough !== undefined,
         });
      }
   });

   it("a storage= build reads a passthrough type or a catalog", () => {
      for (const t of TYPES) {
         expect({ t, reads: storageBuildReadsConnectionType(t) }).toEqual({
            t,
            reads:
               connectionDialectOf(t)?.passthrough !== undefined ||
               isCatalog(t),
         });
      }
   });

   it("only a catalog type is the same catalog as a destination", () => {
      const config = {
         catalog: {
            postgresConnection: { host: "pg", port: 5432, databaseName: "c" },
         },
      };
      const of = (type: string) =>
         ({ name: "c", type, ducklakeConnection: config }) as ApiConnection;
      for (const t of TYPES) {
         expect({ t, same: isSameCatalog(of(t), of(t)) }).toEqual({
            t,
            same: isCatalog(t),
         });
         expect({ t, same: isSameCatalog(of(t), of("ducklake")) }).toEqual({
            t,
            same: isCatalog(t),
         });
      }
   });

   it("the refusal names a catalog, and a proxy to another server, by the registry", () => {
      const source = { name: "s", connectionName: "c" };
      for (const t of TYPES.filter((x) => x !== "")) {
         const message = unreadableSourceConnectionMessage(source, t);
         const registered = connectionDialectOf(t);
         expect({
            t,
            catalog: message.includes("is a DuckLake catalog other than"),
            proxy: message.includes("is a 'publisher' connection, a proxy"),
         }).toEqual({
            t,
            catalog: isCatalog(t),
            // The one registered type whose attributes are only known once
            // it resolves: it names another server's connection.
            proxy:
               registered !== undefined && registered.attributes === undefined,
         });
      }
   });

   it("READ_ONLY_DIALECTS are the dialects whose connections cannot hold a table", () => {
      const readOnly = new Set<string>();
      const writable = new Set<string>();
      for (const entry of Object.values(CONNECTION_DIALECTS)) {
         if (entry.attributes === undefined) continue;
         (entry.attributes.canPersist ? writable : readOnly).add(
            String(entry.attributes.dialectName),
         );
      }
      expect([...READ_ONLY_DIALECTS].sort()).toEqual([...readOnly].sort());
      // A dialect is read-only only when no connection type of it persists.
      for (const d of READ_ONLY_DIALECTS) expect(writable.has(d)).toBe(false);
   });
});
