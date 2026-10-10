// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * The engine's embedding rows: one per (node, facet), stored in
 * `catalog_embeddings` with the hash of the exact text embedded, and held in
 * memory as unit vectors for the search.
 *
 * Facets: `derived` (the node's derived text, see derived_text.ts), `q1` to
 * `q3` (generated questions) and `values` (the value list its doc gives). A
 * node's closeness to a phrase is its best row.
 *
 * The sync is the shipped index's rule, applied to its own table: a row whose
 * text hash and embedding model match is reused, everything else is embedded
 * in batches through the configured provider, and rows the package no longer
 * wants are deleted. A separate table, because the shipped sync deletes every
 * row of `entity_embeddings` it did not write.
 *
 * Brute force in memory rather than a scan in DuckDB: the vectors are needed
 * for every target of every request, and a few hundred thousand 512-float
 * rows is tens of megabytes.
 */

import type { EmbeddingModel } from "../../providers/types";
import {
   EMBEDDING_BATCH_TIMEOUT_MS,
   prepareEmbeddingInput,
} from "../../service/embedding_provider";
import type { DuckDBConnection } from "../../storage/duckdb/DuckDBConnection";
import { sha256 } from "./llm_cache";

export interface VectorRow {
   node: string;
   facet: string;
   text: string;
}

/** One stored row, unit length, ready for a dot product. */
export interface Facet {
   facet: string;
   vec: Float32Array;
}

export interface VectorSyncOutcome {
   vectors: Map<string, Facet[]>;
   rows: number;
   embedded: number;
   reused: number;
   deleted: number;
   /** Characters sent to the provider; a cost estimate is chars / 4 tokens. */
   charsEmbedded: number;
   dims: number;
}

function unit(v: readonly number[]): Float32Array {
   let n = 0;
   for (const x of v) n += x * x;
   const s = n > 0 ? 1 / Math.sqrt(n) : 0;
   const out = new Float32Array(v.length);
   for (let i = 0; i < v.length; i++) out[i] = v[i] * s;
   return out;
}

export function dot(a: Float32Array, b: Float32Array): number {
   const n = Math.min(a.length, b.length);
   let s = 0;
   for (let i = 0; i < n; i++) s += a[i] * b[i];
   return s;
}

/** Unit vector of a raw embedding; exported for the search, which embeds the phrase. */
export const toUnit = unit;

interface StoredRow {
   node_key: string;
   facet: string;
   content_hash: string;
   embedding_model: string;
   embedding: number[] | string;
}

function parseVector(v: number[] | string): number[] {
   if (Array.isArray(v)) return v.map(Number);
   return JSON.parse(v) as number[];
}

export async function syncVectors(args: {
   db: DuckDBConnection;
   provider: EmbeddingModel;
   environmentName: string;
   packageName: string;
   rows: readonly VectorRow[];
   shouldStop?: () => boolean;
}): Promise<VectorSyncOutcome> {
   const { db, provider, environmentName, packageName } = args;
   const prefix = provider.documentPrefix ?? "";
   const wanted = new Map<
      string,
      VectorRow & { input: string; hash: string }
   >();
   for (const r of args.rows) {
      const input = prepareEmbeddingInput(r.text);
      wanted.set(`${r.node}\u0000${r.facet}`, {
         ...r,
         input,
         hash: sha256(prefix + input),
      });
   }
   const stored = await db.all<StoredRow>(
      `SELECT node_key, facet, content_hash, embedding_model, embedding
       FROM catalog_embeddings WHERE environment_name = ? AND package_name = ?`,
      [environmentName, packageName],
   );
   const vectors = new Map<string, Facet[]>();
   const add = (node: string, facet: string, v: number[]) => {
      const list = vectors.get(node) ?? [];
      list.push({ facet, vec: unit(v) });
      vectors.set(node, list);
   };
   let reused = 0;
   let deleted = 0;
   const kept = new Set<string>();
   let dims = 0;
   for (const row of stored) {
      const key = `${row.node_key}\u0000${row.facet}`;
      const want = wanted.get(key);
      if (
         want &&
         row.content_hash === want.hash &&
         row.embedding_model === provider.model
      ) {
         const v = parseVector(row.embedding);
         if (dims === 0) dims = v.length;
         if (v.length === dims) {
            add(row.node_key, row.facet, v);
            kept.add(key);
            reused += 1;
            continue;
         }
      }
      if (!want) {
         await db.run(
            `DELETE FROM catalog_embeddings WHERE environment_name = ? AND package_name = ? AND node_key = ? AND facet = ?`,
            [environmentName, packageName, row.node_key, row.facet],
         );
         deleted += 1;
      }
   }
   const toEmbed = [...wanted.entries()].filter(([k]) => !kept.has(k));
   let embedded = 0;
   let chars = 0;
   const batchSize = Math.max(1, Math.min(provider.maxBatch, 256));
   for (let i = 0; i < toEmbed.length; i += batchSize) {
      if (args.shouldStop?.()) break;
      const batch = toEmbed.slice(i, i + batchSize);
      const out = await provider.embedBatch(
         batch.map(([, r]) => prefix + r.input),
         EMBEDDING_BATCH_TIMEOUT_MS,
      );
      const now = new Date().toISOString();
      const params: unknown[] = [];
      batch.forEach(([, r], j) => {
         const v = out[j];
         if (dims === 0) dims = v.length;
         chars += r.input.length;
         add(r.node, r.facet, v);
         params.push(
            environmentName,
            packageName,
            r.node,
            r.facet,
            r.hash,
            provider.model,
            v.length,
            JSON.stringify(v),
            now,
         );
      });
      await db.run(
         `INSERT INTO catalog_embeddings (environment_name, package_name, node_key, facet, content_hash, embedding_model, dims, embedding, updated_at)
          VALUES ${batch.map(() => "(?, ?, ?, ?, ?, ?, ?, CAST(? AS FLOAT[]), ?)").join(", ")}
          ON CONFLICT (environment_name, package_name, node_key, facet) DO UPDATE SET
            content_hash = EXCLUDED.content_hash, embedding_model = EXCLUDED.embedding_model,
            dims = EXCLUDED.dims, embedding = EXCLUDED.embedding, updated_at = EXCLUDED.updated_at`,
         params,
      );
      embedded += batch.length;
   }
   return {
      vectors,
      rows: wanted.size,
      embedded,
      reused,
      deleted,
      charsEmbedded: chars,
      dims,
   };
}

/** Best dot product of `q` against a node's rows, and which facet gave it. */
export function bestFacet(
   q: Float32Array,
   facets: readonly Facet[] | undefined,
): { score: number; facet: string } | undefined {
   if (!facets || facets.length === 0) return undefined;
   let best = facets[0];
   let bestScore = dot(q, best.vec);
   for (let i = 1; i < facets.length; i++) {
      const s = dot(q, facets[i].vec);
      if (s > bestScore) {
         bestScore = s;
         best = facets[i];
      }
   }
   return { score: bestScore, facet: best.facet };
}
