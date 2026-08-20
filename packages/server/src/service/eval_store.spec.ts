import { afterEach, beforeEach, describe, expect, it } from "bun:test";
import fs from "fs";
import os from "os";
import path from "path";
import { DuckDBConnection } from "../storage/duckdb/DuckDBConnection";
import { initializeSchema } from "../storage/duckdb/schema";
import { EvalStore } from "./eval_store";

describe("EvalStore", () => {
   let tempDir: string;
   let db: DuckDBConnection;
   let store: EvalStore;

   beforeEach(async () => {
      tempDir = fs.mkdtempSync(path.join(os.tmpdir(), "eval-store-"));
      db = new DuckDBConnection(path.join(tempDir, "test.db"));
      await db.initialize();
      await initializeSchema(db, true);
      store = new EvalStore(db);
   });

   afterEach(async () => {
      await db.close();
      fs.rmSync(tempDir, { recursive: true, force: true });
   });

   it("round-trips a set, case, run, and events", async () => {
      const set = await store.createSet({
         name: "synthetic",
         description: "calibration",
      });
      const created = await store.createCase(set.id, {
         qid: "q1",
         question: "How many orders?",
         split: "train",
         state: "selected",
         golden: { status: "verified", kind: "scalar", value: "32" },
      });
      expect(created.goldenRevision).toBe(1);

      const updated = await store.updateCase(created.id, {
         golden: { status: "verified", kind: "scalar", value: "33" },
      });
      expect(updated.goldenRevision).toBe(2);
      // A case with no tags/selection must still accept a golden patch.
      // DuckDB rejects untyped NULL binds on UPDATE without CAST.
      const bare = await store.createCase(set.id, {
         qid: "q2",
         question: "Orders by status",
      });
      const patchedBare = await store.updateCase(bare.id, {
         golden: { status: "verified", kind: "rows", path: "gold/s5.csv" },
      });
      expect(patchedBare.goldenRevision).toBe(1);
      expect(patchedBare.golden).toMatchObject({ path: "gold/s5.csv" });

      const held = await store.updateCase(created.id, {
         golden: {
            status: "ambiguous",
            reason: "Two honest replays disagree; hold the key.",
            path: "gold/q1.csv",
         },
      });
      expect(held.goldenRevision).toBe(3);
      expect(held.golden).toMatchObject({ status: "ambiguous" });

      // REST PATCH sends omitted fields as undefined; those must not wipe the row.
      const restShaped = await store.updateCase(created.id, {
         question: undefined,
         split: undefined,
         state: undefined,
         golden: {
            status: "ambiguous",
            reason: "REST-shaped patch",
            path: "gold/q1.csv",
         },
      });
      expect(restShaped.question).toBe(created.question);
      expect(restShaped.state).toBe("selected");
      expect(restShaped.goldenRevision).toBe(4);

      const run = await store.createRun({
         setId: set.id,
         config: { answerer: "opus" },
      });
      await store.appendEvent({
         runId: run.id,
         caseId: created.id,
         kind: "score",
         payload: { pass: true, golden_revision: updated.goldenRevision },
      });
      const events = await store.listEvents(run.id);
      expect(events).toHaveLength(1);
      expect(events[0].kind).toBe("score");
   });

   it("patches metadata on a set whose optional columns are null", async () => {
      // DuckDB rejects untyped NULL binds on UPDATE without CAST.
      const set = await store.createSet({ name: "bare-set" });
      const patched = await store.updateSet(set.id, {
         metadata: { status: "active", version: 2 },
      });
      expect(patched.metadata).toEqual({ status: "active", version: 2 });
      expect(patched.draftRevision).toBe(set.draftRevision + 1);
   });

   it("exports and re-imports a package snapshot", async () => {
      const set = await store.createSet({ name: "pilot" });
      await store.createCase(set.id, {
         qid: "q1",
         question: "How many orders?",
         state: "selected",
         golden: { status: "verified", kind: "scalar", value: "32" },
      });
      const dest = path.join(tempDir, "export-root");
      fs.mkdirSync(dest);
      const { directory } = await store.writeSnapshotToDirectory(set.id, dest);
      expect(fs.existsSync(path.join(directory, "eval.json"))).toBe(true);
      expect(fs.existsSync(path.join(directory, "cases.jsonl"))).toBe(true);

      await expect(store.importFromDirectory(directory)).rejects.toThrow(
         /already exists/,
      );
      await store.updateSet(set.id, { status: "archived" });
      const imported = await store.importFromDirectory(directory);
      expect(imported.name).toBe("pilot");
      expect(imported.status).toBe("active");
      const cases = await store.listCases(imported.id);
      expect(cases).toHaveLength(1);
      expect(cases[0].question).toBe("How many orders?");
      expect(cases[0].golden).toMatchObject({ status: "verified", value: "32" });
   });

   it("lists active sets by default and refuses a second live name", async () => {
      const live = await store.createSet({ name: "synthetic" });
      expect(live.status).toBe("active");
      await expect(store.createSet({ name: "synthetic" })).rejects.toThrow(
         /already exists/,
      );
      await store.updateSet(live.id, { status: "archived" });
      const replacement = await store.createSet({ name: "synthetic" });
      const listed = await store.listSets();
      expect(listed.map((s) => s.id)).toEqual([replacement.id]);
      const all = await store.listSets({ status: "all", name: "synthetic" });
      expect(all).toHaveLength(2);
   });

   it("refuses a path-escaping set name", async () => {
      await expect(store.createSet({ name: "../escape" })).rejects.toThrow(
         /Invalid eval set name/,
      );
   });

   it("round-trips a checkpoint and omits file bytes from the list", async () => {
      const set = await store.createSet({ name: "beaver-train" });
      const run = await store.createRun({
         setId: set.id,
         config: { mode: "improve" },
      });
      const created = await store.createCheckpoint({
         label: "after-fclt-rooms-default",
         runId: run.id,
         environmentName: "local",
         packageName: "facilities",
         modelPath: "facilities.malloy",
         servedRevision: "rev-1",
         sourceContentSha: "sha-1",
         issueIds: ["iss-5694"],
         files: [
            {
               path: "facilities.malloy",
               content: "source: rooms is duckdb.table('fclt_rooms')\n",
            },
         ],
      });
      expect(created.files[0].sha256).toHaveLength(64);
      expect(created.files[0].content).toContain("fclt_rooms");

      const listed = await store.listCheckpoints({ runId: run.id });
      expect(listed).toHaveLength(1);
      expect(listed[0].id).toBe(created.id);
      expect(listed[0].files[0].content).toBe("");
      expect(listed[0].files[0].sha256).toBe(created.files[0].sha256);

      const fetched = await store.getCheckpoint(created.id);
      expect(fetched.files[0].content).toContain("fclt_rooms");
      expect(fetched.servedRevision).toBe("rev-1");
      expect(fetched.issueIds).toEqual(["iss-5694"]);
   });
});
