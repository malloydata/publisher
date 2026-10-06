// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { modelDefToModelInfo, type ModelDef } from "@malloydata/malloy";
import type * as Malloy from "@malloydata/malloy-interfaces";

type Givens = NonNullable<ModelDef["givens"]>;

/**
 * `modelDefToModelInfo` compiles every named and anonymous query to SQL just to
 * read its output schema, binding no givens, so a query that bakes a default-less
 * given throws. The SQL is discarded, so a placeholder default yields the same
 * schema; the retry runs on a copy and the caller's `modelDef` is never touched.
 */
export function modelInfoOf(modelDef: ModelDef): Malloy.ModelInfo {
   try {
      return modelDefToModelInfo(modelDef);
   } catch (err) {
      if (!isUnboundGiven(err) || !modelDef.givens) throw err;
      try {
         return modelDefToModelInfo({
            ...modelDef,
            givens: withPlaceholderDefaults(modelDef.givens),
         });
      } catch {
         throw err;
      }
   }
}

function isUnboundGiven(err: unknown): boolean {
   return (err as { code?: unknown })?.code === "compiler-given-no-value";
}

function withPlaceholderDefaults(givens: Givens): Givens {
   const out: Givens = {};
   for (const [id, given] of Object.entries(givens)) {
      if (given.default !== undefined) {
         out[id] = given;
         continue;
      }
      // A filter given's SQL emit needs a filter literal, not a null.
      const isFilter = given.type.type === "filter expression";
      out[id] = {
         ...given,
         default: isFilter
            ? { node: "filterLiteral", filterSrc: "" }
            : { node: "null" },
      };
   }
   return out;
}
