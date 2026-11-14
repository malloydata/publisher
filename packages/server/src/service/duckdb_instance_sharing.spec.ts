// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { DuckDBConnection } from "@malloydata/db-duckdb";
import { afterEach, beforeEach, describe, expect, it, mock } from "bun:test";
import * as fs from "fs";
import * as os from "os";
import * as path from "path";
import {
   canonicalDuckDBFile,
   dbDuckDBInternalsMatch,
   DuckDBInstanceSettingsChangedError,
   installDuckDBInstanceSharing,
   instanceKeyOf,
   openDuckDBFilesForTesting,
   recordAttachments,
   recordedAttachments,
   serializeAttachRun,
   setDuckDBCloseWaitMsForTesting,
   withThreadCap,
} from "./duckdb_instance_sharing";
import { attachmentDigest } from "./connection";
import { Environment } from "./environment";
import { availableCpus, cgroupCpuLimit } from "../settings/registry/storage";
import { installSettings } from "../test_helpers/settings_env";

installDuckDBInstanceSharing();

type Internals = { shareKey: string };
const shareKeyOf = (connection: DuckDBConnection) =>
   (connection as unknown as Internals).shareKey;
/**
 * Whether this process can open a file again after closing it. Not on Windows:
 * db-duckdb's close is a refcount decrement that keeps the OS file lock while
 * its C++ connections live (its own note in `detachInstance`), so a reopen in
 * the same process is refused there until they are collected. Sharing means an
 * edit never needs that reopen; it is checked where the platform allows it.
 */
const canReopenAfterClose = process.platform !== "win32";
const instanceOf = (connection: DuckDBConnection) =>
   DuckDBConnection.activeDBs[shareKeyOf(connection)]?.instance;
/** The open files under this test's directory; other specs share the process. */
const openFilesHere = () =>
   openDuckDBFilesForTesting().filter((file) =>
      file.startsWith(canonicalDuckDBFile(dir)!),
   );

let dir: string;
const opened: DuckDBConnection[] = [];

function open(options: Record<string, unknown>): DuckDBConnection {
   const connection = new DuckDBConnection({ name: "duck", ...options });
   opened.push(connection);
   return connection;
}

/**
 * Hold every later `connect()` to `connection`'s shared instance until the
 * test releases or fails it, and report whether the instance was closed. A
 * held connect never reaches DuckDB once the instance is closed, so a
 * regression fails an assertion instead of crashing the process.
 */
function holdConnects(connection: DuckDBConnection) {
   const instance = instanceOf(connection)! as unknown as {
      connect(): Promise<unknown>;
      closeSync(): void;
   };
   const connect = instance.connect.bind(instance);
   const closeSync = instance.closeSync.bind(instance);
   let closed = false;
   instance.closeSync = () => {
      closed = true;
      closeSync();
   };
   let settle!: (error?: Error) => void;
   const gate = new Promise<Error | undefined>((resolve) => {
      settle = resolve;
   });
   instance.connect = async () => {
      const error = await gate;
      if (error) throw error;
      if (closed) throw new Error("connect on a closed instance");
      return connect();
   };
   return {
      closed: () => closed,
      release: () => settle(),
      fail: (error: Error) => settle(error),
   };
}

async function waitFor(condition: () => boolean): Promise<void> {
   for (let i = 0; i < 100 && !condition(); i++) {
      await new Promise((resolve) => setTimeout(resolve, 10));
   }
   expect(condition()).toBe(true);
}

beforeEach(() => {
   dir = fs.realpathSync(fs.mkdtempSync(path.join(os.tmpdir(), "duck-share-")));
});

afterEach(async () => {
   for (const connection of opened.splice(0)) {
      await connection.close().catch(() => {});
   }
   // Nothing this spec opened may outlive its test: in a serial run every later
   // spec file shares this process.
   expect(openFilesHere()).toEqual([]);
   fs.rmSync(dir, { recursive: true, force: true });
});

describe("canonicalDuckDBFile", () => {
   it("names no file for in-memory and MotherDuck databases", () => {
      expect(canonicalDuckDBFile(undefined)).toBeUndefined();
      expect(canonicalDuckDBFile(":memory:")).toBeUndefined();
      expect(canonicalDuckDBFile("md:analytics")).toBeUndefined();
      expect(canonicalDuckDBFile("motherduck:analytics")).toBeUndefined();
   });

   it("spells one file one way through a symlinked directory", () => {
      const real = path.join(dir, "real");
      fs.mkdirSync(real);
      fs.writeFileSync(path.join(real, "a.duckdb"), "");
      const link = path.join(dir, "link");
      fs.symlinkSync(real, link, "dir");
      expect(canonicalDuckDBFile(path.join(link, "a.duckdb"))).toBe(
         canonicalDuckDBFile(path.join(real, "a.duckdb")),
      );
   });

   it("resolves a file not created yet through its directory", () => {
      const expected = path.join(dir, "new.duckdb");
      expect(canonicalDuckDBFile(expected)).toBe(
         process.platform === "win32" ? expected.toLowerCase() : expected,
      );
   });

   it("case-folds on Windows, whose file system is case-insensitive", () => {
      const file = path.join(dir, "Mixed.DuckDB");
      expect(canonicalDuckDBFile(file, "win32")).toBe(file.toLowerCase());
      expect(canonicalDuckDBFile(file, "linux")).toBe(file);
   });
});

describe("instanceKeyOf", () => {
   const base = { databasePath: "/x.duckdb", threads: 2, readOnly: false };

   it("ignores setupSQL, which each connection runs on its own session", () => {
      expect(instanceKeyOf({ ...base, setupSQL: "SELECT 1" })).toBe(
         instanceKeyOf({ ...base, setupSQL: "SELECT 2" }),
      );
   });

   it("differs on a setting DuckDB fixes when it opens the file", () => {
      expect(instanceKeyOf({ ...base, threads: 4 })).not.toBe(
         instanceKeyOf(base),
      );
   });
});

describe("one DuckDB instance per file", () => {
   it("shares one instance between connections that differ only in setupSQL", async () => {
      const file = path.join(dir, "shared.duckdb");
      const a = open({ databasePath: file, setupSQL: "SELECT 1" });
      await a.runSQL("CREATE TABLE t AS SELECT 42 AS v");
      const b = open({ databasePath: file, setupSQL: "SELECT 2" });
      const rows = await b.runSQL("SELECT v FROM t");

      expect(shareKeyOf(a)).not.toBe(shareKeyOf(b));
      expect(instanceOf(b)).toBe(instanceOf(a));
      expect(rows.rows).toEqual([{ v: 42 }]);
      expect(openFilesHere()).toEqual([canonicalDuckDBFile(file)!]);
   });

   it("keeps the instance open until the last connection on it closes", async () => {
      const file = path.join(dir, "refcount.duckdb");
      const a = open({ databasePath: file, setupSQL: "SELECT 1" });
      await a.runSQL("CREATE TABLE t AS SELECT 1 AS v");
      const b = open({ databasePath: file, setupSQL: "SELECT 2" });
      await b.runSQL("SELECT 1");

      await a.close();
      expect(
         (await b.runSQL("SELECT count(*)::INTEGER AS n FROM t")).rows,
      ).toEqual([{ n: 1 }]);

      await b.close();
      expect(openFilesHere()).toEqual([]);
      expect(
         Object.keys(DuckDBConnection.activeDBs).filter(
            (key) => key === shareKeyOf(a) || key === shareKeyOf(b),
         ),
      ).toEqual([]);

      // A later connection opens the file afresh, not a closed instance.
      if (!canReopenAfterClose) return;
      const c = open({ databasePath: file, setupSQL: "SELECT 3" });
      expect(
         (await c.runSQL("SELECT count(*)::INTEGER AS n FROM t")).rows,
      ).toEqual([{ n: 1 }]);
   });

   it("keeps the instance open while a joining connection connects, even if the last other one closes meanwhile", async () => {
      const file = path.join(dir, "race.duckdb");
      const a = open({ databasePath: file, setupSQL: "SELECT 1" });
      await a.runSQL("CREATE TABLE t AS SELECT 5 AS v");
      // b joins a's instance; its connect is still pending when a closes.
      const b = open({ databasePath: file, setupSQL: "SELECT 2" });
      await a.close();
      expect((await b.runSQL("SELECT v FROM t")).rows).toEqual([{ v: 5 }]);
      await b.close();
      expect(openFilesHere()).toEqual([]);
   });

   it("opens one instance when two connections to a file open at once", async () => {
      const file = path.join(dir, "concurrent.duckdb");
      const a = open({ databasePath: file, setupSQL: "SELECT 1" });
      const b = open({ databasePath: file, setupSQL: "SELECT 2" });
      // Neither open has finished when the other starts.
      await Promise.all([a.runSQL("SELECT 1"), b.runSQL("SELECT 1")]);
      expect(instanceOf(b)).toBe(instanceOf(a));
      expect(openFilesHere()).toEqual([canonicalDuckDBFile(file)!]);
   });

   it("does not share between different files", async () => {
      const a = open({ databasePath: path.join(dir, "a.duckdb") });
      const b = open({ databasePath: path.join(dir, "b.duckdb") });
      await a.runSQL("SELECT 1");
      await b.runSQL("SELECT 1");
      expect(instanceOf(a)).not.toBe(instanceOf(b));
   });

   it("refuses a second instance with different open-time settings, rather than opening one", async () => {
      const file = path.join(dir, "settings.duckdb");
      const a = open({ databasePath: file, threads: 1 });
      await a.runSQL("SELECT 1");
      const b = open({ databasePath: file, threads: 2 });
      await expect(b.runSQL("SELECT 1")).rejects.toBeInstanceOf(
         DuckDBInstanceSettingsChangedError,
      );

      // Once the file is released, the new settings open it.
      await a.close();
      if (!canReopenAfterClose) return;
      const c = open({ databasePath: file, threads: 2 });
      expect((await c.runSQL("SELECT 1 AS one")).rows).toEqual([{ one: 1 }]);
   });
});

describe("one shared in-memory DuckDB instance", () => {
   // db-duckdb shares an in-memory instance between connections with the same
   // settings, as it does a file's, so the same race applies: closing the last
   // registered connection while another is still connecting to the instance
   // freed the database under the connect, and the process segfaulted (the
   // Windows unit-suite crash). It crashed within two rounds when unguarded.
   it("keeps the instance open while a joining connection connects, even if the last other one closes meanwhile", async () => {
      for (let round = 0; round < 20; round++) {
         const a = open({ databasePath: ":memory:", workingDirectory: dir });
         await a.runSQL("SELECT 1");
         const b = open({ databasePath: ":memory:", workingDirectory: dir });
         await a.close();
         expect((await b.runSQL("SELECT 42 AS v")).rows).toEqual([{ v: 42 }]);
         await b.close();
         expect(DuckDBConnection.activeDBs[shareKeyOf(b)]).toBeUndefined();
      }
   });

   it("closes the instance of a connection closed before its first query", async () => {
      const connection = open({
         databasePath: ":memory:",
         workingDirectory: dir,
      });
      // The constructor's init() is still opening the instance.
      await connection.close();
      await connection.connecting;
      expect(
         DuckDBConnection.activeDBs[shareKeyOf(connection)],
      ).toBeUndefined();
   });

   it("never closes the instance under a pending connect", async () => {
      const a = open({ databasePath: ":memory:", workingDirectory: dir });
      await a.runSQL("SELECT 1");
      const held = holdConnects(a);
      const b = open({ databasePath: ":memory:", workingDirectory: dir });
      await a.close();
      // Deterministic, unlike the round loop above: b's connect is held open
      // in JavaScript, so a premature close fails this assertion rather than
      // segfaulting the whole test process.
      expect(held.closed()).toBe(false);
      held.release();
      expect((await b.runSQL("SELECT 42 AS v")).rows).toEqual([{ v: 42 }]);
      await b.close();
      expect(held.closed()).toBe(true);
      expect(DuckDBConnection.activeDBs[shareKeyOf(b)]).toBeUndefined();
   });

   it("releases the instance when a join's connect fails after the last other connection closed", async () => {
      const a = open({ databasePath: ":memory:", workingDirectory: dir });
      await a.runSQL("SELECT 1");
      const held = holdConnects(a);
      const b = open({ databasePath: ":memory:", workingDirectory: dir });
      await a.close();
      held.fail(new Error("connect failed"));
      await expect(b.runSQL("SELECT 1")).rejects.toThrow("connect failed");
      expect(held.closed()).toBe(true);
      expect(DuckDBConnection.activeDBs[shareKeyOf(b)]).toBeUndefined();
      await b.close();
   });

   it("leaves no placeholder behind when a join's connect fails while another connection holds the instance", async () => {
      const a = open({ databasePath: ":memory:", workingDirectory: dir });
      await a.runSQL("SELECT 1");
      const held = holdConnects(a);
      const b = open({ databasePath: ":memory:", workingDirectory: dir });
      // b closes during its own failing connect.
      const closing = b.close();
      held.fail(new Error("connect failed"));
      await closing;
      expect(
         DuckDBConnection.activeDBs[shareKeyOf(a)]!.connections,
      ).toHaveLength(1);
      expect(held.closed()).toBe(false);
      // No pin is left, so a's close is the last and closes the instance.
      await a.close();
      expect(held.closed()).toBe(true);
      expect(DuckDBConnection.activeDBs[shareKeyOf(a)]).toBeUndefined();
   });

   it("closes once when closed twice before its first query", async () => {
      const connection = open({
         databasePath: ":memory:",
         workingDirectory: dir,
      });
      await Promise.all([connection.close(), connection.close()]);
      await connection.close();
      expect(
         DuckDBConnection.activeDBs[shareKeyOf(connection)],
      ).toBeUndefined();
      await expect(connection.runSQL("SELECT 1")).rejects.toThrow(/closed/);
   });

   it("finishes closing a connection whose connect never settles, and releases the instance once it does", async () => {
      setDuckDBCloseWaitMsForTesting(50);
      try {
         const a = open({ databasePath: ":memory:", workingDirectory: dir });
         await a.runSQL("SELECT 1");
         const held = holdConnects(a);
         const b = open({ databasePath: ":memory:", workingDirectory: dir });
         await a.close();
         // A connect that hangs (a remote database that never answers) must
         // not hold an environment delete or a shutdown forever.
         const outcome = await Promise.race([
            b.close().then(() => "closed"),
            new Promise((resolve) => setTimeout(resolve, 2_000, "hung")),
         ]);
         expect(outcome).toBe("closed");
         // The pending connect is still using the instance: it stays open.
         expect(held.closed()).toBe(false);
         held.release();
         await b.connecting;
         await waitFor(() => held.closed());
         expect(DuckDBConnection.activeDBs[shareKeyOf(b)]).toBeUndefined();
      } finally {
         setDuckDBCloseWaitMsForTesting(undefined);
      }
   });
});

describe("a file-backed connection closed before its first query", () => {
   it("closes its instance and releases the file", async () => {
      const file = path.join(dir, "early.duckdb");
      const connection = open({ databasePath: file });
      await connection.close();
      await connection.connecting;
      expect(
         DuckDBConnection.activeDBs[shareKeyOf(connection)],
      ).toBeUndefined();
      expect(openFilesHere()).toEqual([]);
      if (canReopenAfterClose) {
         const again = open({ databasePath: file });
         expect((await again.runSQL("SELECT 1 AS v")).rows).toEqual([{ v: 1 }]);
      }
   });
});

describe("attach runs of one connection on one instance", () => {
   it("run one at a time", async () => {
      const a = open({ databasePath: path.join(dir, "runs.duckdb") });
      await a.runSQL("SELECT 1");
      const order: string[] = [];
      let release!: () => void;
      const held = new Promise<void>((resolve) => (release = resolve));
      const first = serializeAttachRun(a, "duck", async () => {
         order.push("first started");
         await held;
         order.push("first done");
      });
      const second = serializeAttachRun(a, "duck", async () => {
         order.push("second started");
      });
      await Promise.resolve();
      release();
      await Promise.all([first, second]);
      expect(order).toEqual(["first started", "first done", "second started"]);
   });

   it("still runs the next after one fails", async () => {
      const a = open({ databasePath: path.join(dir, "runs.duckdb") });
      await a.runSQL("SELECT 1");
      const first = serializeAttachRun(a, "duck", async () => {
         throw new Error("unreachable host");
      });
      const second = serializeAttachRun(a, "duck", async () => "ran");
      await expect(first).rejects.toThrow("unreachable host");
      expect(await second).toBe("ran");
   });
});

it("digests an attached database's settings whatever order its keys arrive in", () => {
   const stored = {
      name: "warehouse",
      type: "postgres",
      postgresConnection: { host: "h", password: "p", port: 5432 },
   };
   // As an echoed update arrives: the withheld password put back last.
   const echoed = {
      type: "postgres",
      name: "warehouse",
      postgresConnection: { port: 5432, host: "h", password: "p" },
   };
   expect(attachmentDigest(echoed as never)).toBe(
      attachmentDigest(stored as never),
   );
   expect(
      attachmentDigest({
         ...stored,
         postgresConnection: { ...stored.postgresConnection, host: "other" },
      } as never),
   ).not.toBe(attachmentDigest(stored as never));
});

it("finds the db-duckdb internals it patches", () => {
   // False skips sharing and the thread cap with only a warning, so an
   // upgrade that reshapes them must fail here instead.
   expect(dbDuckDBInternalsMatch()).toBe(true);
});

describe("the per-instance thread cap", () => {
   let restore: (() => void) | undefined;
   afterEach(() => {
      restore?.();
      restore = undefined;
   });

   it("defaults each instance to four threads, or fewer on a smaller host", () => {
      restore = installSettings({ env: {} }).restore;
      expect(withThreadCap({})).toEqual({
         threads: String(Math.min(4, availableCpus())),
      });
   });

   it("reads a cgroup CPU quota as whole CPUs, and no quota as none", () => {
      expect(cgroupCpuLimit("200000", "100000")).toBe(2);
      expect(cgroupCpuLimit("50000", "100000")).toBe(1);
      expect(cgroupCpuLimit("150000", "100000")).toBe(2);
      // v2 and v1 spellings of "no quota", and a file that held nothing.
      expect(cgroupCpuLimit("max", "100000")).toBeUndefined();
      expect(cgroupCpuLimit("-1", "100000")).toBeUndefined();
      expect(cgroupCpuLimit("", undefined)).toBeUndefined();
      expect(availableCpus()).toBeGreaterThanOrEqual(1);
   });

   it("keeps a connection's own threads setting", () => {
      restore = installSettings({
         env: { PUBLISHER_DUCKDB_THREADS: "8" },
      }).restore;
      expect(withThreadCap({ threads: "1" })).toEqual({ threads: "1" });
      expect(withThreadCap({})).toEqual({ threads: "8" });
   });

   it("leaves DuckDB's one-per-core default under off", () => {
      restore = installSettings({
         env: { PUBLISHER_DUCKDB_THREADS: "off" },
      }).restore;
      expect(withThreadCap({})).toEqual({});
   });

   it("caps an instance whose connection sets no threads", async () => {
      // Through db-duckdb's own open: an upgrade that stops building instance
      // options in buildInstanceOptions would drop the cap silently. The test
      // preload's cap applies here, ahead of the production one.
      const a = open({ databasePath: path.join(dir, "capped.duckdb") });
      expect(
         (await a.runSQL("SELECT current_setting('threads')::INTEGER AS n"))
            .rows,
      ).toEqual([
         { n: Number(process.env.PUBLISHER_TEST_DUCKDB_THREADS ?? 2) },
      ]);
   });

   it("opens an instance with the threads its connection asks for", async () => {
      const a = open({ databasePath: path.join(dir, "t.duckdb"), threads: 1 });
      expect(
         (await a.runSQL("SELECT current_setting('threads')::INTEGER AS n"))
            .rows,
      ).toEqual([{ n: 1 }]);
   });
});

describe("a connection edit to a file-backed DuckDB connection", () => {
   let restoreSettings: () => void;
   let envPath: string;

   beforeEach(() => {
      restoreSettings = installSettings({
         env: { PUBLISHER_ALLOW_DUCKDB_SETUP_SQL: "true" },
      }).restore;
      envPath = path.join(dir, "env");
      fs.mkdirSync(envPath);
   });

   afterEach(() => {
      restoreSettings();
   });

   const duck = (setupSQL: string) =>
      ({
         name: "duck",
         type: "duckdb",
         duckdbConnection: { setupSQL },
      }) as never;

   it("opens the edited connection on the same instance, and the draining one keeps answering", async () => {
      const env = await Environment.create("testEnv", envPath, [
         duck("SELECT 1"),
      ]);
      try {
         const before = (await env.getMalloyConnection(
            "duck",
         )) as unknown as DuckDBConnection;
         await before.runSQL("CREATE TABLE t AS SELECT 7 AS v");

         await env.update({
            name: "testEnv",
            connections: [duck("SELECT 2")],
         } as never);
         const after = (await env.getMalloyConnection(
            "duck",
         )) as unknown as DuckDBConnection;

         expect(after).not.toBe(before);
         expect(instanceOf(after)).toBe(instanceOf(before));
         // Both generations answer: the edited one at once, the retired one
         // for the rest of its drain.
         const [now, draining] = await Promise.all([
            after.runSQL("SELECT v FROM t"),
            before.runSQL("SELECT v FROM t"),
         ]);
         expect(now.rows).toEqual([{ v: 7 }]);
         expect(draining.rows).toEqual([{ v: 7 }]);
      } finally {
         await env.closeAllConnections();
      }
   });

   it("detaches a database an earlier generation attached and the edit removed", async () => {
      const env = await Environment.create("testEnv", envPath, [
         duck("SELECT 1"),
      ]);
      try {
         const before = (await env.getMalloyConnection(
            "duck",
         )) as unknown as DuckDBConnection;
         // Stands in for an attached warehouse: attachments belong to the
         // instance, so they outlive the generation that made them.
         await before.runSQL("ATTACH ':memory:' AS warehouse");
         recordAttachments(before, "duck", {
            generation: 0,
            databases: new Map([
               ["warehouse", { digest: "digest", type: "postgres" }],
            ]),
         });

         await env.update({
            name: "testEnv",
            connections: [duck("SELECT 2")],
         } as never);
         const after = (await env.getMalloyConnection(
            "duck",
         )) as unknown as DuckDBConnection;

         expect(instanceOf(after)).toBe(instanceOf(before));
         const databases = await after.runSQL(
            "SELECT database_name FROM duckdb_databases()",
         );
         expect(
            databases.rows.map((row) => row["database_name"]),
         ).not.toContain("warehouse");
         expect(recordedAttachments(after, "duck")?.databases.size).toBe(0);
      } finally {
         await env.closeAllConnections();
      }
   });

   it("drops the secret of a storage entry the edit removed", async () => {
      const env = await Environment.create("testEnv", envPath, [
         duck("SELECT 1"),
      ]);
      try {
         const before = (await env.getMalloyConnection(
            "duck",
         )) as unknown as DuckDBConnection;
         // A `gcs`, `s3` or `azure` entry creates only a secret, which is
         // instance-wide: detaching a catalog by its name leaves it in force.
         await before.runSQL(
            "CREATE SECRET secret_s3_bucket (TYPE s3, KEY_ID 'k', SECRET 's')",
         );
         recordAttachments(before, "duck", {
            generation: 0,
            databases: new Map([["bucket", { digest: "digest", type: "s3" }]]),
         });

         await env.update({
            name: "testEnv",
            connections: [duck("SELECT 2")],
         } as never);
         const after = (await env.getMalloyConnection(
            "duck",
         )) as unknown as DuckDBConnection;

         const secrets = await after.runSQL(
            "SELECT name FROM duckdb_secrets()",
         );
         expect(secrets.rows.map((row) => row["name"])).not.toContain(
            "secret_s3_bucket",
         );
      } finally {
         await env.closeAllConnections();
      }
   });

   it("leaves a newer generation's attachments to it", async () => {
      const env = await Environment.create("testEnv", envPath, [
         duck("SELECT 1"),
      ]);
      try {
         const before = (await env.getMalloyConnection(
            "duck",
         )) as unknown as DuckDBConnection;
         await before.runSQL("ATTACH ':memory:' AS warehouse");
         // Written by a generation newer than any this test builds: a
         // draining older one must not undo it.
         recordAttachments(before, "duck", {
            generation: Number.MAX_SAFE_INTEGER,
            databases: new Map([
               ["warehouse", { digest: "digest", type: "postgres" }],
            ]),
         });

         await env.update({
            name: "testEnv",
            connections: [duck("SELECT 2")],
         } as never);
         const after = (await env.getMalloyConnection(
            "duck",
         )) as unknown as DuckDBConnection;

         const databases = await after.runSQL(
            "SELECT database_name FROM duckdb_databases()",
         );
         expect(databases.rows.map((row) => row["database_name"])).toContain(
            "warehouse",
         );
         expect(
            recordedAttachments(after, "duck")?.databases.has("warehouse"),
         ).toBe(true);
      } finally {
         await env.closeAllConnections();
      }
   });

   it("keeps the record of a database a failed run did not reach, so a retry replaces it", async () => {
      const env = await Environment.create("testEnv", envPath, [
         duck("SELECT 1"),
      ]);
      try {
         const before = (await env.getMalloyConnection(
            "duck",
         )) as unknown as DuckDBConnection;
         await before.runSQL("ATTACH ':memory:' AS first");
         await before.runSQL("ATTACH ':memory:' AS second");
         recordAttachments(before, "duck", {
            generation: 0,
            databases: new Map([
               ["first", { digest: "old", type: "postgres" }],
               ["second", { digest: "old", type: "postgres" }],
            ]),
         });

         // Both changed; replacing `first` fails, so the run never reaches
         // `second`, which still holds its old settings.
         await env.update({
            name: "testEnv",
            connections: [
               {
                  name: "duck",
                  type: "duckdb",
                  duckdbConnection: {
                     setupSQL: "SELECT 2",
                     attachedDatabases: [
                        { name: "first", type: "unsupported" },
                        { name: "second", type: "postgres" },
                     ],
                  },
               },
            ],
         } as never);
         await env.getMalloyConnection("duck").catch(() => undefined);

         const record = recordedAttachments(before, "duck");
         expect(record?.databases.has("first")).toBe(false);
         expect(record?.databases.get("second")?.digest).toBe("old");
      } finally {
         await env.closeAllConnections();
      }
   });
});

describe("build-plan retries after an environment closes", () => {
   it("cancels a pending retry and queues nothing more once the environment closes", async () => {
      const envPath = path.join(dir, "retry-env");
      fs.mkdirSync(envPath);
      const env = await Environment.create("retryEnv", envPath, []);
      const refreshBuildPlan = mock(async () => true);
      const pkg = {
         refreshBuildPlan,
         isBuildPlanUnavailable: () => true,
         markBuildPlanStale: () => {},
         getPackageName: () => "p",
         getVersionId: () => undefined,
      };
      const internals = env as unknown as {
         schedulePlanRetry(p: unknown): void;
         schedulePlanRefresh(p: unknown): void;
         planRefreshBackoff: Map<unknown, { timer?: unknown }>;
      };

      internals.schedulePlanRetry(pkg);
      expect(internals.planRefreshBackoff.get(pkg)?.timer).toBeDefined();

      await env.closeAllConnections();
      expect(internals.planRefreshBackoff.size).toBe(0);

      // Nothing queues or runs after close.
      internals.schedulePlanRefresh(pkg);
      internals.schedulePlanRetry(pkg);
      expect(internals.planRefreshBackoff.size).toBe(0);
      expect(refreshBuildPlan).not.toHaveBeenCalled();
   });

   it("cancels every open environment's retries on shutdown", async () => {
      const envPath = path.join(dir, "shutdown-env");
      fs.mkdirSync(envPath);
      const env = await Environment.create("shutdownEnv", envPath, []);
      const internals = env as unknown as {
         schedulePlanRetry(p: unknown): void;
         planRefreshBackoff: Map<unknown, unknown>;
      };
      internals.schedulePlanRetry({
         refreshBuildPlan: async () => true,
         isBuildPlanUnavailable: () => true,
         getPackageName: () => "p",
      });
      expect(internals.planRefreshBackoff.size).toBe(1);

      Environment.cancelAllPlanRefreshes();
      expect(internals.planRefreshBackoff.size).toBe(0);
   });
});

describe("package-load workers", () => {
   // Workers are threads of this process, and the registry above lives in the
   // main thread's isolate: a worker that opened a DuckDB file would be a
   // second instance of it. They never do: every connection a worker looks up
   // is a proxy whose statements run on the main thread's connection
   // (package_load_worker.ts, buildWorkerMalloyConfig). This pins it, by
   // checking nothing the worker imports loads db-duckdb.
   it("never import db-duckdb, so they never open a DuckDB file", () => {
      const root = path.resolve(
         __dirname,
         "../package_load/package_load_worker.ts",
      );
      const seen = new Set<string>();
      const bare = new Set<string>();
      const visit = (file: string) => {
         if (seen.has(file)) return;
         seen.add(file);
         const text = fs.readFileSync(file, "utf-8");
         const specifiers = [
            ...text.matchAll(
               /^\s*import\s+(?!type\b)[^;]*?from\s+["']([^"']+)["']/gms,
            ),
            ...text.matchAll(
               /^\s*export\s+(?!type\b)[^;]*?from\s+["']([^"']+)["']/gms,
            ),
            ...text.matchAll(/^\s*import\s+["']([^"']+)["']/gm),
         ].map((m) => m[1]);
         for (const spec of specifiers) {
            if (!spec.startsWith(".")) {
               bare.add(spec);
               continue;
            }
            const base = path.resolve(path.dirname(file), spec);
            const candidates = [
               base,
               `${base}.ts`,
               path.join(base, "index.ts"),
               base.replace(/\.js$/, ".ts"),
            ];
            const found = candidates.find(
               (c) => fs.existsSync(c) && fs.statSync(c).isFile(),
            );
            if (found && found.endsWith(".ts")) visit(found);
         }
      };
      visit(root);
      expect(seen.size).toBeGreaterThan(10);
      expect(
         [...bare].filter((s) => s.startsWith("@malloydata/db-duckdb")),
      ).toEqual([]);
      expect([...bare].filter((s) => s.startsWith("@duckdb/"))).toEqual([]);
   });
});
