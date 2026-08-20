import { createHash } from "crypto";
import { getEmbeddingConfig } from "../config";

/**
 * Retrieval contract version for the current (legacy) get_context
 * surface. Bumped when the request/response shape changes.
 */
export const RETRIEVAL_VERSION = "legacy-1";
export const TYPED_RETRIEVAL_VERSION = "typed-1";

export interface RetrievalIndexReadiness {
   lexical: "ready" | "unavailable";
   semantic: "ready" | "indexing" | "cooldown" | "oversize" | "unavailable";
   dimensionalValues?: "ready" | "indexing" | "unavailable" | "off";
   generation?: number;
}

export interface RetrievalEvidence {
   servedRevision?: string;
   sourceContentSha?: string;
   retrievalVersion: string;
   retrievalConfigHash: string;
   index: RetrievalIndexReadiness;
}

export function retrievalConfigHash(options?: {
   cutoff?: number;
   resultLimit?: number;
   grouping?: boolean;
   retrievalVersion?: string;
   dimensionValueIndex?: string;
}): string {
   let embedding: ReturnType<typeof getEmbeddingConfig> = null;
   try {
      embedding = getEmbeddingConfig();
   } catch {
      embedding = null;
   }
   const payload = {
      version: options?.retrievalVersion ?? RETRIEVAL_VERSION,
      lexical: "lunr",
      embeddingModel: embedding?.model ?? null,
      embeddingBaseUrl: embedding?.baseUrl ?? null,
      embeddingDimensions: embedding?.dimensions ?? null,
      cutoff: options?.cutoff ?? null,
      resultLimit: options?.resultLimit ?? null,
      grouping: options?.grouping ?? true,
      faceting: true,
      chunking: true,
      dimensionValueIndex: options?.dimensionValueIndex ?? "off",
   };
   return createHash("sha256")
      .update(JSON.stringify(payload))
      .digest("hex")
      .slice(0, 16);
}

export function compactRankedSummary(results: unknown[]): {
   entityIds: string[];
   ranks: number[];
   resultCount: number;
} {
   const entityIds: string[] = [];
   const ranks: number[] = [];
   for (const [index, result] of results.entries()) {
      if (!result || typeof result !== "object") continue;
      const row = result as {
         kind?: string;
         name?: string;
         source?: string;
      };
      const typedId = (row as { entityId?: string }).entityId;
      const id =
         typedId ??
         [row.kind, row.source, row.name].filter(Boolean).join(":");
      if (id.length === 0) continue;
      entityIds.push(id);
      ranks.push(index + 1);
   }
   return { entityIds, ranks, resultCount: entityIds.length };
}
