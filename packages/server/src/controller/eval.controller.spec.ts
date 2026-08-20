import { afterEach, beforeEach, describe, expect, it } from "bun:test";
import fs from "fs";
import os from "os";
import path from "path";
import { PUBLISHER_DATA_DIR } from "../constants";
import { DuckDBConnection } from "../storage/duckdb/DuckDBConnection";
import { initializeSchema } from "../storage/duckdb/schema";
import { EvalController } from "./eval.controller";

describe("EvalController checkpoints", () => {
   let tempDir: string;
   let db: DuckDBConnection;
   let controller: EvalController;
   let reloads: Array<{ env: string; pkg: string }>;

   beforeEach(async () => {
      process.env.PUBLISHER_EVAL_STORE = "on";
      tempDir = fs.mkdtempSync(path.join(os.tmpdir(), "eval-controller-"));
      db = new DuckDBConnection(path.join(tempDir, "test.db"));
      await db.initialize();
      await initializeSchema(db, true);
      reloads = [];
      const pkgDir = path.join(
         tempDir,
         PUBLISHER_DATA_DIR,
         "local",
         "facilities",
      );
      fs.mkdirSync(pkgDir, { recursive: true });
      fs.writeFileSync(
         path.join(pkgDir, "facilities.malloy"),
         "source: rooms is duckdb.table('fclt_rooms')\n",
      );
      controller = new EvalController(() => db, {
         serverRoot: tempDir,
         reloadPackage: async (environmentName, packageName) => {
            reloads.push({ env: environmentName, pkg: packageName });
            return {
               metadata: {
                  servedRevision: "rev-restored",
                  sourceContentSha: "sha-restored",
               },
               mode: "in-place",
            };
         },
      });
   });

   afterEach(async () => {
      delete process.env.PUBLISHER_EVAL_STORE;
      await db.close();
      fs.rmSync(tempDir, { recursive: true, force: true });
   });

   it("snapshots on-disk model bytes and restores them after a later edit", async () => {
      const set = await controller.createSet({ name: "beaver-train" });
      const run = await controller.createRun({
         setId: set.id,
         config: { mode: "improve" },
      });
      const created = await controller.createCheckpoint({
         label: "before-next-batch",
         runId: run.id,
         environmentName: "local",
         packageName: "facilities",
         modelPath: "facilities.malloy",
         servedRevision: "rev-1",
         issueIds: ["iss-5694"],
      });
      expect(created.files[0].content).toContain("fclt_rooms");
      expect(
         fs.existsSync(
            path.join(
               tempDir,
               ".eval_checkpoints",
               created.id,
               "facilities.malloy",
            ),
         ),
      ).toBe(true);

      const pkgFile = path.join(
         tempDir,
         PUBLISHER_DATA_DIR,
         "local",
         "facilities",
         "facilities.malloy",
      );
      fs.writeFileSync(pkgFile, "source: rooms is duckdb.table('fac_rooms')\n");

      const restored = await controller.restoreCheckpoint(created.id);
      expect(fs.readFileSync(pkgFile, "utf-8")).toContain("fclt_rooms");
      expect(reloads).toEqual([{ env: "local", pkg: "facilities" }]);
      expect(restored.reload).toEqual({
         mode: "in-place",
         servedRevision: "rev-restored",
         sourceContentSha: "sha-restored",
      });
      const events = await controller.listEvents(run.id);
      expect(events.map((e: { kind: string }) => e.kind)).toEqual([
         "checkpoint",
         "checkpoint",
      ]);
      expect(events[1].payload).toMatchObject({
         action: "restored",
         checkpointId: created.id,
      });
   });
});
