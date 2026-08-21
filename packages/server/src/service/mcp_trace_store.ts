import { createHash } from "crypto";
import { getMcpTraceMode, getMcpTraceRetention, type McpTraceMode } from "../config";
import { logger } from "../logger";
import type { DuckDBConnection } from "../storage/duckdb/DuckDBConnection";

export interface McpTraceRecord {
   traceId: string;
   toolName: string;
   mode: McpTraceMode;
   request?: unknown;
   response?: unknown;
   requestHash?: string;
   responseHash?: string;
   rankedSummary?: unknown;
   resultCount?: number;
   environmentName?: string;
   packageName?: string;
   retrievalConfigHash?: string;
   createdAt: string;
}

function hashPayload(value: unknown): string {
   return createHash("sha256").update(JSON.stringify(value ?? null)).digest("hex");
}

function nowIso(): string {
   return new Date().toISOString();
}

export class McpTraceStore {
   constructor(private db: DuckDBConnection) {}

   async record(input: {
      toolName: string;
      request: unknown;
      response: unknown;
      rankedSummary?: unknown;
      resultCount?: number;
      environmentName?: string;
      packageName?: string;
      retrievalConfigHash?: string;
   }): Promise<string | undefined> {
      const mode = getMcpTraceMode();
      if (mode === "off") return undefined;

      const traceId = crypto.randomUUID();
      const requestHash = hashPayload(input.request);
      const responseHash = hashPayload(input.response);
      const persistExact = mode === "retrieval";

      if (mode === "metadata") {
         logger.info("[MCP Trace] recorded", {
            traceId,
            toolName: input.toolName,
            requestHash,
            responseHash,
            resultCount: input.resultCount ?? 0,
            environmentName: input.environmentName,
            packageName: input.packageName,
         });
      }

      await this.db.run(
         `INSERT INTO mcp_traces (
            trace_id, tool_name, mode, request_json, response_json,
            request_hash, response_hash, ranked_summary_json, result_count,
            environment_name, package_name, retrieval_config_hash,
            created_at
          ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
         [
            traceId,
            input.toolName,
            mode,
            persistExact ? JSON.stringify(input.request ?? null) : null,
            persistExact ? JSON.stringify(input.response ?? null) : null,
            requestHash,
            responseHash,
            input.rankedSummary ? JSON.stringify(input.rankedSummary) : null,
            input.resultCount ?? 0,
            input.environmentName ?? null,
            input.packageName ?? null,
            input.retrievalConfigHash ?? null,
            nowIso(),
         ],
      );
      await this.evictIfNeeded();
      return traceId;
   }

   async get(traceId: string): Promise<McpTraceRecord | null> {
      const row = await this.db.get<{
         trace_id: string;
         tool_name: string;
         mode: McpTraceMode;
         request_json: string | null;
         response_json: string | null;
         request_hash: string | null;
         response_hash: string | null;
         ranked_summary_json: string | null;
         result_count: number | null;
         environment_name: string | null;
         package_name: string | null;
         retrieval_config_hash: string | null;
         created_at: string;
      }>(
         `SELECT trace_id, tool_name, mode, request_json, response_json,
                 request_hash, response_hash, ranked_summary_json, result_count,
                 environment_name, package_name, retrieval_config_hash, created_at
          FROM mcp_traces WHERE trace_id = ?`,
         [traceId],
      );
      return row ? this.toRecord(row) : null;
   }

   async listRecent(limit = 20): Promise<McpTraceRecord[]> {
      const rows = await this.db.all<{
         trace_id: string;
         tool_name: string;
         mode: McpTraceMode;
         request_json: string | null;
         response_json: string | null;
         request_hash: string | null;
         response_hash: string | null;
         ranked_summary_json: string | null;
         result_count: number | null;
         environment_name: string | null;
         package_name: string | null;
         retrieval_config_hash: string | null;
         created_at: string;
      }>(
         `SELECT trace_id, tool_name, mode, request_json, response_json,
                 request_hash, response_hash, ranked_summary_json, result_count,
                 environment_name, package_name, retrieval_config_hash, created_at
          FROM mcp_traces ORDER BY created_at DESC LIMIT ?`,
         [Math.min(Math.max(limit, 1), 100)],
      );
      return rows.map((row) => this.toRecord(row));
   }

   async clear(): Promise<void> {
      await this.db.run(`DELETE FROM mcp_traces`);
   }

   private async evictIfNeeded(): Promise<void> {
      const retention = getMcpTraceRetention();
      const countRow = await this.db.get<{ n: number }>(
         `SELECT CAST(COUNT(*) AS INTEGER) AS n FROM mcp_traces`,
      );
      const count = countRow?.n ?? 0;
      if (count <= retention) return;
      const overflow = count - retention;
      await this.db.run(
         `DELETE FROM mcp_traces WHERE trace_id IN (
            SELECT trace_id FROM mcp_traces
            ORDER BY created_at ASC
            LIMIT ?
          )`,
         [overflow],
      );
   }

   private toRecord(row: {
      trace_id: string;
      tool_name: string;
      mode: McpTraceMode;
      request_json: string | null;
      response_json: string | null;
      request_hash: string | null;
      response_hash: string | null;
      ranked_summary_json: string | null;
      result_count: number | null;
      environment_name: string | null;
      package_name: string | null;
      retrieval_config_hash: string | null;
      created_at: string;
   }): McpTraceRecord {
      return {
         traceId: row.trace_id,
         toolName: row.tool_name,
         mode: row.mode,
         request: row.request_json ? JSON.parse(row.request_json) : undefined,
         response: row.response_json ? JSON.parse(row.response_json) : undefined,
         requestHash: row.request_hash ?? undefined,
         responseHash: row.response_hash ?? undefined,
         rankedSummary: row.ranked_summary_json
            ? JSON.parse(row.ranked_summary_json)
            : undefined,
         resultCount: row.result_count ?? undefined,
         environmentName: row.environment_name ?? undefined,
         packageName: row.package_name ?? undefined,
         retrievalConfigHash: row.retrieval_config_hash ?? undefined,
         createdAt: row.created_at,
      };
   }
}
