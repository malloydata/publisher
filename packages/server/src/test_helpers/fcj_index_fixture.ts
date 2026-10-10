// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * The fcj fixture with its index built: the bag-of-words stub embedder (texts
 * sharing words are close, so scores are stable and readable), no LLM, and a
 * stand-in warehouse for the two `#(index)` dimensions.
 */

import { DEFAULT_FCJ_SETTINGS } from "../service/package_retrieval";
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

export async function buildFcjIndexFixture(
   settings = DEFAULT_FCJ_SETTINGS,
): Promise<FcjIndexFixture> {
   const fixture = await compileFcjFixture();
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
