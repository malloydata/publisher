// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { DuckDBConnection } from "@malloydata/db-duckdb";
import { makeDigest } from "@malloydata/malloy";
import { afterEach, beforeEach, describe, expect, it } from "bun:test";
import * as fs from "fs";
import * as os from "os";
import * as path from "path";
import { components } from "../api";
import {
   applyConnectionFingerprint,
   buildDuckLakeConnection,
} from "./connection";
import { assembleEnvironmentConnections } from "./connection_config";
import {
   installDuckDBInstanceSharing,
   setDuckDBLockedReopenWaitMsForTesting,
} from "./duckdb_instance_sharing";

installDuckDBInstanceSharing();

type ApiConnection = components["schemas"]["Connection"];

function lake(name: string, bucket = "lake"): ApiConnection {
   return {
      name,
      type: "ducklake",
      ducklakeConnection: {
         catalog: {
            postgresConnection: {
               host: "catalog.internal",
               port: 5432,
               databaseName: "ducklake",
               userName: "publisher",
               password: "catalog-secret",
            },
         },
         storage: { bucketUrl: `gs://managed-tier/${bucket}` },
      },
   };
}

let envPath: string;
const opened: DuckDBConnection[] = [];

/** A DuckLake connection as the environment builds it, before any attach. */
function open(connection: ApiConnection) {
   const assembled = assembleEnvironmentConnections([connection], envPath);
   const built = buildDuckLakeConnection(
      assembled.metadata.get(connection.name!)!,
      assembled.pojo.connections[connection.name!],
   );
   opened.push(built);
   return { built, assembled };
}

const instanceOf = (connection: DuckDBConnection) =>
   DuckDBConnection.activeDBs[
      (connection as unknown as { shareKey: string }).shareKey
   ]?.instance;

beforeEach(() => {
   envPath = fs.realpathSync(fs.mkdtempSync(path.join(os.tmpdir(), "lake-")));
});

afterEach(async () => {
   setDuckDBLockedReopenWaitMsForTesting(undefined);
   for (const connection of opened.splice(0)) {
      await connection.close().catch(() => {});
   }
   fs.rmSync(envPath, { recursive: true, force: true });
});

describe("a DuckLake connection's in-memory primary", () => {
   it("keeps no database file and lists none for the lockdown allowlist", async () => {
      const { built, assembled } = open(lake("a"));
      await built.runSQL("SELECT 1");

      expect(assembled.pojo.connections.a.databasePath).toBe(":memory:");
      expect(assembled.metadata.get("a")!.databasePath).toBeUndefined();
      expect(fs.readdirSync(envPath)).toEqual([]);
   });

   it("opens two lakes of identical config on two instances", async () => {
      const a = open(lake("a")).built;
      const b = open(lake("b")).built;
      await Promise.all([a.runSQL("SELECT 1"), b.runSQL("SELECT 1")]);

      expect(instanceOf(a)).toBeDefined();
      expect(instanceOf(b)).toBeDefined();
      expect(instanceOf(a)).not.toBe(instanceOf(b));
   });
});

describe("a DuckLake connection's digest", () => {
   const legacyDigest = (name: string) =>
      makeDigest(
         "duckdb",
         path.join(envPath, `${name}_ducklake.duckdb`),
         envPath,
         undefined,
      );

   it("is the one Malloy computed for its old database file", () => {
      const a = open(lake("a")).built;
      const b = open(lake("b")).built;

      expect(a.getDigest()).toBe(legacyDigest("a"));
      expect(b.getDigest()).toBe(legacyDigest("b"));
      expect(a.getDigest()).not.toBe(b.getDigest());
   });

   it("is replaced by the API fingerprint when one is set", () => {
      const { built, assembled } = open(lake("a"));
      const unset = applyConnectionFingerprint(
         built,
         assembled.metadata.get("a"),
      );
      expect(unset.getDigest()).toBe(legacyDigest("a"));

      const metadata = assembled.metadata.get("a")!;
      const fingerprinted = applyConnectionFingerprint(built, {
         ...metadata,
         apiConnection: { ...metadata.apiConnection, fingerprint: "fp-1" },
      });
      expect(fingerprinted.getDigest()).toBe("fp-1");
   });
});

describe("a DuckLake connection edit", () => {
   it("opens the new generation at once while the old one keeps answering", async () => {
      // Were the primary a file, the new generation would wait for the old to
      // release it (duckdb_instance_sharing.ts); a wait this long fails the bound.
      setDuckDBLockedReopenWaitMsForTesting(20_000);
      const before = open(lake("a", "v1")).built;
      await before.runSQL("SELECT 1");
      const inFlight = before.runSQL(
         "SELECT count(*)::INTEGER AS n FROM range(20000) a, range(20000) b",
      );

      const started = Date.now();
      const after = open(lake("a", "v2")).built;
      const now = await after.runSQL("SELECT 7 AS v");

      expect(Date.now() - started).toBeLessThan(5_000);
      expect(now.rows).toEqual([{ v: 7 }]);
      expect(instanceOf(after)).not.toBe(instanceOf(before));
      expect((await inFlight).rows).toEqual([{ n: 400_000_000 }]);
   });
});
