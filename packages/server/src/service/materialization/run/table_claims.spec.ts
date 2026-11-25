// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

// The window an auto-run spends loading its manifest (MANIFEST_ROWS_READY with
// the manifest recorded): the tables it just built must stay claimed, so an
// orchestrated run of another version cannot overwrite one meanwhile.
import { describe, expect, it } from "bun:test";
import type { Materialization } from "../../../storage/DatabaseInterface";
import {
   internalErrorToHttpError,
   MaterializationConflictError,
} from "../../../errors";
import {
   makeInstruction,
   makeMaterialization,
} from "../../../test_helpers/materialization_test_fixtures";
import {
   SUPERSEDED_TABLES_KEY,
   WRITES_TABLES_KEY,
   WRITES_TARGETS_KEY,
} from "../../versions/materialization_scope";
import { TableClaims } from "./table_claims";
import { LockOrderError, withOwnedNameLock } from "./table_ownership";

const SHARED = "summary";

/** A `scope: package` auto-run, loading the manifest of the table it built. */
function loadingAutoRun(metadata: Record<string, unknown>): Materialization {
   return {
      id: "a",
      environmentId: "env",
      packageName: "pkg",
      status: "MANIFEST_ROWS_READY",
      version: "1.0.0",
      manifest: {
         strict: false,
         builtAt: "2026-11-15T00:00:00Z",
         entries: {
            s: { sourceEntityId: "s", physicalTableName: SHARED },
         },
      },
      metadata: {
         mode: "auto",
         scope: "package",
         versionId: "1.0.0",
         ...metadata,
      },
      startedAt: null,
      completedAt: null,
      error: null,
      createdAt: new Date(),
      updatedAt: new Date(),
   } as unknown as Materialization;
}

function claims(runs: Materialization[]): TableClaims {
   return new TableClaims(
      () =>
         ({
            listMaterializations: async () => runs,
         }) as never,
   );
}

describe("assertTablesFree while an auto-run loads its manifest", () => {
   it("refuses an orchestrated run's table the loading run still claims", async () => {
      const a = loadingAutoRun({ writesTables: [SHARED] });
      const refused = await claims([a])
         .assertTablesFree("env", "pkg", [SHARED], "b", true)
         .catch((error: unknown) => error as Error);
      expect(refused?.message).toMatch(/being written by materialization a/);
      expect(internalErrorToHttpError(refused as Error).json).toMatchObject({
         code: 409,
         reason: "MATERIALIZATION_CONFLICT",
      });
   });

   it("refuses it as a shared table too, since the loading run recorded its manifest", async () => {
      const a = loadingAutoRun({});
      const refused = await claims([a])
         .assertTablesFree("env", "pkg", [SHARED], "b", true)
         .catch((error: unknown) => error as Error);
      expect(refused?.message).toMatch(/one the versions of package pkg share/);
      expect(internalErrorToHttpError(refused as Error).json).toMatchObject({
         code: 409,
         reason: "MATERIALIZATION_CONFLICT",
      });
   });
});

// ---------------------------------------------------------------------------
// Characterization: the claim, release, conflict and concurrency rules,
// against an in-memory run store. Pinned before the table-identity
// unification, so a change to how a name is compared shows up here.
// ---------------------------------------------------------------------------

/** An in-memory run store holding `runs`, recording every metadata write. */
function store(runs: Materialization[]) {
   const byId = new Map(runs.map((r) => [r.id, r]));
   const writes: { id: string; metadata: Record<string, unknown> }[] = [];
   let failWritesTo: string | undefined;
   const repository = {
      listMaterializations: async (environmentId: string, pkg: string) =>
         [...byId.values()].filter(
            (r) => r.environmentId === environmentId && r.packageName === pkg,
         ),
      getMaterializationById: async (id: string) => byId.get(id) ?? null,
      updateMaterialization: async (
         id: string,
         update: { metadata: Record<string, unknown> },
      ) => {
         // A tick, so a write is never atomic with the read before it.
         await Promise.resolve();
         if (id === failWritesTo) throw new Error(`run ${id} was deleted`);
         const run = byId.get(id)!;
         byId.set(id, { ...run, metadata: update.metadata });
         writes.push({ id, metadata: update.metadata });
         return byId.get(id);
      },
   };
   return {
      claims: new TableClaims(() => repository as never),
      run: (id: string) => byId.get(id)!,
      writes,
      failWritesTo: (id: string) => (failWritesTo = id),
   };
}

/** A run of `env`/`pkg`, active (PENDING) unless a status is given. */
const run = (overrides: Partial<Materialization>): Materialization =>
   makeMaterialization({
      environmentId: "env",
      packageName: "pkg",
      ...overrides,
   });

/** A committed run whose manifest holds `entries` (address -> table). */
const committed = (
   id: string,
   entries: Record<string, string>,
   overrides: Partial<Materialization> = {},
): Materialization =>
   run({
      id,
      status: "MANIFEST_FILE_READY",
      manifest: {
         strict: false,
         builtAt: "2026-11-15T00:00:00Z",
         entries: Object.fromEntries(
            Object.entries(entries).map(([address, table]) => [
               address,
               {
                  sourceEntityId: address,
                  physicalTableName: table,
                  connectionName: "duckdb",
               },
            ]),
         ),
      } as never,
      ...overrides,
   });

const conflict = (p: Promise<unknown>) =>
   p.then(
      () => undefined,
      (e: unknown) => e as Error,
   );

describe("TableClaims.assertTablesFree", () => {
   it("refuses a table an active run writes, compared by what the name names (case, quotes and schema folded)", async () => {
      const { claims } = store([
         run({
            id: "a",
            metadata: { [WRITES_TABLES_KEY]: ['"Main"."Summary"'] },
         }),
      ]);
      for (const spelling of [
         "summary",
         "SUMMARY",
         "other.summary",
         '"summary"',
      ]) {
         const err = await conflict(
            claims.assertTablesFree("env", "pkg", [spelling], "b", false),
         );
         expect(err).toBeInstanceOf(MaterializationConflictError);
         expect(err?.message).toContain(
            'Table "Main"."Summary" is being written by materialization a',
         );
      }
      expect(
         await conflict(
            claims.assertTablesFree("env", "pkg", ["summary2"], "b", false),
         ),
      ).toBeUndefined();
   });

   it("names the run's version in the refusal", async () => {
      const { claims } = store([
         run({
            id: "a",
            version: "1.2.0",
            metadata: { [WRITES_TABLES_KEY]: ["t"] },
         }),
      ]);
      const err = await conflict(
         claims.assertTablesFree("env", "pkg", ["t"], "b", false),
      );
      expect(err?.message).toBe(
         "Table t is being written by materialization a of version 1.2.0 of package pkg; two runs never write one table at once.",
      );
   });

   it("ignores the asking run itself, terminal runs, and other packages", async () => {
      const writes = { [WRITES_TABLES_KEY]: ["t"] };
      const { claims } = store([
         run({ id: "self", metadata: writes }),
         run({ id: "done", status: "MANIFEST_FILE_READY", metadata: writes }),
         run({ id: "failed", status: "FAILED", metadata: writes }),
         run({ id: "cancelled", status: "CANCELLED", metadata: writes }),
         run({ id: "elsewhere", packageName: "other", metadata: writes }),
      ]);
      expect(
         await conflict(
            claims.assertTablesFree("env", "pkg", ["t"], "self", false),
         ),
      ).toBeUndefined();
   });

   it("counts a run loading its manifest (MANIFEST_ROWS_READY) as active", async () => {
      const { claims } = store([
         run({
            id: "a",
            status: "MANIFEST_ROWS_READY",
            metadata: { [WRITES_TABLES_KEY]: ["t"] },
         }),
      ]);
      expect(
         await conflict(
            claims.assertTablesFree("env", "pkg", ["t"], "b", false),
         ),
      ).toBeInstanceOf(MaterializationConflictError);
   });

   it("checks the versions' shared tables only for a run with instructions", async () => {
      const { claims } = store([committed("old", { e: "shared_t" })]);
      expect(
         await conflict(
            claims.assertTablesFree("env", "pkg", ["shared_t"], "b", false),
         ),
      ).toBeUndefined();
      const err = await conflict(
         claims.assertTablesFree("env", "pkg", ["SHARED_T"], "b", true),
      );
      expect(err?.message).toBe(
         "Table shared_t is one the versions of package pkg share, which only an auto-run of its latest version rebuilds; name tables of your own in buildInstructions.",
      );
   });

   it("does not count a version's own run, or a versioned orchestrated run, as shared", async () => {
      const { claims } = store([
         committed(
            "own",
            { e: "t1" },
            { version: "1.0.0", metadata: { scope: "version" } },
         ),
         committed(
            "orch",
            { e: "t2" },
            { version: "1.0.0", metadata: { mode: "orchestrated" } },
         ),
         // An orchestrated run from before the first versioned publish is.
         committed(
            "unversioned",
            { e: "t3" },
            { version: null, metadata: { mode: "orchestrated" } },
         ),
      ]);
      expect(
         await conflict(
            claims.assertTablesFree("env", "pkg", ["t1"], "b", true),
         ),
      ).toBeUndefined();
      expect(
         await conflict(
            claims.assertTablesFree("env", "pkg", ["t2"], "b", true),
         ),
      ).toBeUndefined();
      expect(
         await conflict(
            claims.assertTablesFree("env", "pkg", ["t3"], "b", true),
         ),
      ).toBeInstanceOf(MaterializationConflictError);
   });

   it("reads nothing for an empty list of tables", async () => {
      const claims = new TableClaims(
         () =>
            ({
               listMaterializations: async () => {
                  throw new Error("read");
               },
            }) as never,
      );
      expect(
         await conflict(claims.assertTablesFree("env", "pkg", [], "b", true)),
      ).toBeUndefined();
   });
});

describe("TableClaims.claimTables", () => {
   it("records the run's tables (deduplicated by spelling) and targets beside its other metadata", async () => {
      const s = store([run({ id: "me", metadata: { mode: "auto" } })]);
      const targets = [
         {
            sourceEntityId: "e",
            physicalTableName: "t",
            connectionName: "duckdb",
         },
      ];
      await s.claims.claimTables(
         "env",
         "pkg",
         "me",
         [
            makeInstruction({ physicalTableName: "t" }),
            makeInstruction({ physicalTableName: "t" }),
            makeInstruction({ physicalTableName: "T" }),
         ],
         { supersede: false, targets },
      );
      expect(s.run("me").metadata).toEqual({
         mode: "auto",
         [WRITES_TABLES_KEY]: ["t", "T"],
         [WRITES_TARGETS_KEY]: targets,
      });
   });

   it("refuses, and records nothing, when another active run writes one of them", async () => {
      const s = store([
         run({ id: "me" }),
         run({ id: "other", metadata: { [WRITES_TABLES_KEY]: ["t"] } }),
      ]);
      const err = await conflict(
         s.claims.claimTables("env", "pkg", "me", [
            makeInstruction({ physicalTableName: "T" }),
         ]),
      );
      expect(err).toBeInstanceOf(MaterializationConflictError);
      expect(s.writes).toEqual([]);
   });

   it("supersedes other runs' entries for the tables it writes from another definition, not the same one, and not twice", async () => {
      const s = store([
         run({ id: "me" }),
         committed("other_def", { "addr-old": "t" }),
         committed("same_def", { "addr-new": "t" }),
         committed(
            "marked",
            { "addr-old": "t" },
            { metadata: { [SUPERSEDED_TABLES_KEY]: ["T"] } },
         ),
         committed("unrelated", { "addr-old": "u" }),
      ]);
      await s.claims.claimTables("env", "pkg", "me", [
         makeInstruction({
            physicalTableName: "t",
            sourceEntityId: "addr-new",
         }),
      ]);
      expect(s.writes.map((w) => w.id).sort()).toEqual(["me", "other_def"]);
      expect(s.run("other_def").metadata).toEqual({
         [SUPERSEDED_TABLES_KEY]: ["t"],
      });
   });

   it("marks nothing superseded with supersede off", async () => {
      const s = store([
         run({ id: "me" }),
         committed("other_def", { "addr-old": "t" }),
      ]);
      await s.claims.claimTables(
         "env",
         "pkg",
         "me",
         [
            makeInstruction({
               physicalTableName: "t",
               sourceEntityId: "addr-new",
            }),
         ],
         { supersede: false },
      );
      expect(s.writes.map((w) => w.id)).toEqual(["me"]);
   });

   it("keeps going when a superseded run cannot be written (deleted meanwhile)", async () => {
      const s = store([
         run({ id: "me" }),
         committed("gone", { "addr-old": "t" }),
         committed("kept", { "addr-old": "t" }),
      ]);
      s.failWritesTo("gone");
      await s.claims.claimTables("env", "pkg", "me", [
         makeInstruction({
            physicalTableName: "t",
            sourceEntityId: "addr-new",
         }),
      ]);
      expect(s.run("kept").metadata).toEqual({
         [SUPERSEDED_TABLES_KEY]: ["t"],
      });
   });

   it("lets exactly one of two concurrent claims of one table through, the first to take the lock", async () => {
      const s = store([run({ id: "a" }), run({ id: "b" })]);
      const [a, b] = await Promise.all([
         conflict(
            s.claims.claimTables("env", "pkg", "a", [
               makeInstruction({ physicalTableName: "t" }),
            ]),
         ),
         conflict(
            s.claims.claimTables("env", "pkg", "b", [
               makeInstruction({ physicalTableName: "T" }),
            ]),
         ),
      ]);
      expect(a).toBeUndefined();
      expect(b).toBeInstanceOf(MaterializationConflictError);
      expect(s.run("b").metadata).toBeNull();
   });

   it("does not serialize claims of different packages against each other", async () => {
      const s = store([
         run({ id: "a" }),
         run({ id: "b", packageName: "other" }),
      ]);
      await Promise.all([
         s.claims.claimTables("env", "pkg", "a", [
            makeInstruction({ physicalTableName: "t" }),
         ]),
         s.claims.claimTables("env", "other", "b", [
            makeInstruction({ physicalTableName: "t" }),
         ]),
      ]);
      expect(s.run("a").metadata?.[WRITES_TABLES_KEY]).toEqual(["t"]);
      expect(s.run("b").metadata?.[WRITES_TABLES_KEY]).toEqual(["t"]);
   });

   it("releases a table once its run is terminal", async () => {
      const s = store([run({ id: "a" }), run({ id: "b" })]);
      await s.claims.claimTables("env", "pkg", "a", [
         makeInstruction({ physicalTableName: "t" }),
      ]);
      expect(
         await conflict(
            s.claims.claimTables("env", "pkg", "b", [
               makeInstruction({ physicalTableName: "t" }),
            ]),
         ),
      ).toBeInstanceOf(MaterializationConflictError);
      // The run store moves `a` to a terminal status; nothing is unclaimed.
      Object.assign(s.run("a"), { status: "FAILED" });
      expect(
         await conflict(
            s.claims.claimTables("env", "pkg", "b", [
               makeInstruction({ physicalTableName: "t" }),
            ]),
         ),
      ).toBeUndefined();
   });
});

describe("TableClaims.markHeldElsewhere", () => {
   it("supersedes only entries naming that exact table, connection and destination", async () => {
      const s = store([
         run({ id: "me" }),
         committed("exact", { e: "t" }),
         // Another spelling of one table on a case-folding engine: kept,
         // where markSuperseded's identity rule would have marked it.
         committed("cased", { e: "T" }),
      ]);
      await s.claims.markHeldElsewhere("env", "pkg", "me", {
         physicalTableName: "t",
         connectionName: "duckdb",
      });
      expect(s.run("exact").metadata).toEqual({
         [SUPERSEDED_TABLES_KEY]: ["t"],
      });
      expect(s.run("cased").metadata).toBeNull();
      await s.claims.markHeldElsewhere("env", "pkg", "me", {
         physicalTableName: "t",
         connectionName: "other",
      });
      expect(s.writes.map((w) => w.id)).toEqual(["exact"]);
   });
});

describe("TableClaims lock order", () => {
   it("refuses, under test, the table lock taken inside an owned-name lock", async () => {
      const { claims } = store([]);
      const err = await conflict(
         withOwnedNameLock("t", () =>
            claims.withTableLock("env", "pkg", async () => 1),
         ),
      );
      expect(err).toBeInstanceOf(LockOrderError);
   });
});
