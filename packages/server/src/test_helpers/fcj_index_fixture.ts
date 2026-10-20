// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * The fcj fixture with its index built: the bag-of-words stub embedder (texts
 * sharing words are close, so scores are stable and readable), no LLM, and a
 * stand-in warehouse for the two `#(index)` dimensions.
 */

import {
   DEFAULT_FCJ_SETTINGS,
   type FcjSettings,
} from "../service/package_retrieval";

/**
 * The engine's settings as most specs below were written against: clusters
 * (not the list mode that is now the default), no union of the meaning and
 * keyword lists, and no readings rule. A spec that tests a default says so by
 * using DEFAULT_FCJ_SETTINGS itself.
 */
export const CLUSTER_SETTINGS: FcjSettings = {
   ...DEFAULT_FCJ_SETTINGS,
   connect: "clusters",
   listUnion: 0,
   readingsRule: false,
};
import { createCatalogTables } from "../storage/duckdb/schema";
import { buildFcjIndex, type FcjIndex } from "../mcp/context/index_build";
import type { IndexValueFetcher } from "../mcp/context/values_index";
import { compileFcjFixture, openTempDb, type FcjFixture } from "./fcj_fixture";
import { stubEmbeddingProvider } from "./get_context_llm_harness";

/** Stands in for the warehouse: the fixture's two #(index) dimensions. */
export const FCJ_VALUES: Record<
   string,
   Array<{ value: string; count: number }>
> = {
   "cities.city": [
      { value: "Denver", count: 3 },
      { value: "Boulder", count: 1 },
   ],
   "flights.arrives_at": [
      { value: "Denver International (DIA)", count: 10 },
      { value: "Chicago O'Hare (ORD)", count: 5 },
   ],
};

export const fcjValueFetcher: IndexValueFetcher = async (req) => {
   const all = FCJ_VALUES[`${req.source}.${req.dimension}`] ?? [];
   return {
      values: all.slice(0, req.limit),
      truncated: all.length > req.limit,
   };
};

export interface FcjIndexFixture extends FcjFixture {
   index: FcjIndex;
   close: () => Promise<void>;
}

/** `files` replaces the default fixture (and its imports) with other model files. */
export async function buildFcjIndexFixture(
   settings = CLUSTER_SETTINGS,
   files?: Record<string, string>,
): Promise<FcjIndexFixture> {
   const fixture = files
      ? await compileFcjFixture(files, {})
      : await compileFcjFixture();
   const { db, close } = await openTempDb(createCatalogTables);
   const index = await buildFcjIndex({
      pkgIndex: fixture.pkgIndex,
      db,
      provider: stubEmbeddingProvider(),
      environmentName: "env",
      packageName: "fixture",
      settings,
      valueRead: { environmentStore: {} as never, fetcher: fcjValueFetcher },
   });
   return { ...fixture, index, close };
}
