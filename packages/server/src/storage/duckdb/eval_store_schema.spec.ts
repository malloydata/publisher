import { afterEach, beforeEach, describe, expect, it } from "bun:test";
import fs from "fs";
import os from "os";
import path from "path";
import { DuckDBConnection } from "./DuckDBConnection";
import { initializeSchema } from "./schema";

describe("eval store schema and --init", () => {
   let tempDir: string;
   let db: DuckDBConnection;

   beforeEach(async () => {
      tempDir = fs.mkdtempSync(path.join(os.tmpdir(), "eval-store-schema-"));
      db = new DuckDBConnection(path.join(tempDir, "test.db"));
      await db.initialize();
      await initializeSchema(db, true);
   });

   afterEach(async () => {
      await db.close();
      fs.rmSync(tempDir, { recursive: true, force: true });
   });

   it("creates eval tables and mcp_traces", async () => {
      const tables = await db.all<{ name: string }>(
         `SELECT name FROM sqlite_master WHERE type='table' AND name LIKE 'eval_%' OR name = 'mcp_traces'`,
      );
      const names = new Set(tables.map((t) => t.name));
      expect(names.has("eval_sets")).toBe(true);
      expect(names.has("eval_cases")).toBe(true);
      expect(names.has("eval_evidence")).toBe(true);
      expect(names.has("eval_runs")).toBe(true);
      expect(names.has("eval_events")).toBe(true);
      expect(names.has("eval_checkpoints")).toBe(true);
      expect(names.has("mcp_traces")).toBe(true);
      const setCols = await db.all<{ name: string }>(
         `SELECT name FROM pragma_table_info('eval_sets')`,
      );
      expect(setCols.map((c) => c.name)).toContain("status");
      const valueTables = await db.all<{ name: string }>(
         `SELECT name FROM sqlite_master WHERE type='table' AND name LIKE 'dimension_value%'`,
      );
      const valueNames = new Set(valueTables.map((t) => t.name));
      expect(valueNames.has("dimension_values")).toBe(true);
      expect(valueNames.has("dimension_value_generations")).toBe(true);
   });

   it("preserves eval tables across --init and drops traces", async () => {
      await db.run(
         `INSERT INTO eval_sets (
            id, name, description, target_model_path, environment_name,
            package_name, draft_revision, export_revision, metadata_json,
            created_at, updated_at
          ) VALUES (?, ?, NULL, NULL, NULL, NULL, 1, NULL, NULL, ?, ?)`,
         ["set-1", "pilot", "2026-01-01T00:00:00.000Z", "2026-01-01T00:00:00.000Z"],
      );
      await db.run(
         `INSERT INTO mcp_traces (
            trace_id, tool_name, mode, request_json, response_json,
            request_hash, response_hash, ranked_summary_json, result_count,
            environment_name, package_name, retrieval_config_hash,
            referenced, created_at
          ) VALUES (?, ?, ?, NULL, NULL, ?, ?, NULL, 0, NULL, NULL, NULL, 0, ?)`,
         [
            "trace-1",
            "malloy_getContext",
            "retrieval",
            "h1",
            "h2",
            "2026-01-01T00:00:00.000Z",
         ],
      );

      await initializeSchema(db, true);

      const set = await db.get<{ name: string }>(
         `SELECT name FROM eval_sets WHERE id = ?`,
         ["set-1"],
      );
      expect(set?.name).toBe("pilot");

      const trace = await db.get<{ trace_id: string }>(
         `SELECT trace_id FROM mcp_traces WHERE trace_id = ?`,
         ["trace-1"],
      );
      expect(trace).toBeNull();
   });

   it("preserves eval checkpoints across --init", async () => {
      await db.run(
         `INSERT INTO eval_checkpoints (
            id, label, run_id, environment_name, package_name, model_path,
            served_revision, source_content_sha, issue_ids_json, files_json,
            created_at
          ) VALUES (?, ?, NULL, ?, ?, ?, NULL, NULL, ?, ?, ?)`,
         [
            "cp-1",
            "after-join",
            "local",
            "facilities",
            "facilities.malloy",
            "[]",
            JSON.stringify([
               {
                  path: "facilities.malloy",
                  content: "source: x is duckdb.table('t')",
                  sha256: "abc",
               },
            ]),
            "2026-01-01T00:00:00.000Z",
         ],
      );
      await initializeSchema(db, true);
      const row = await db.get<{ label: string }>(
         `SELECT label FROM eval_checkpoints WHERE id = ?`,
         ["cp-1"],
      );
      expect(row?.label).toBe("after-join");
   });

   it("drops dimension-value index tables on --init", async () => {
      await db.run(
         `INSERT INTO dimension_value_generations (
            environment_name, package_name, generation, served_revision,
            status, truncated_count, value_count, updated_at
          ) VALUES (?, ?, ?, NULL, ?, 0, 1, ?)`,
         ["env", "pkg", 1, "ready", "2026-01-01T00:00:00.000Z"],
      );
      await initializeSchema(db, true);
      const row = await db.get<{ generation: number }>(
         `SELECT generation FROM dimension_value_generations WHERE package_name = ?`,
         ["pkg"],
      );
      expect(row).toBeNull();
   });
});
