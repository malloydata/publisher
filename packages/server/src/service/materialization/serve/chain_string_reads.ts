// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * The strings a `storage=` build over its upstreams' stored tables compares
 * ({@link StringReads}), read at plan from the author's compile: a pure walk
 * of the build's query, which no query or run changes. The plan records
 * them; the build judges them against the upstreams' recorded contracts
 * (`domain/string_reads.ts`), never by probing a collation.
 */

import type { QueryMaterializer } from "@malloydata/malloy";
import { quotedIdentifier, quotedPath } from "../../malloy_text";
import type { ChainStringReadsRequest } from "../domain/chain_build";
import type { StringRead, StringReads } from "../domain/string_reads";
import { queryFilterReadsOfQuery } from "./gate_reads";
import type { GateReads } from "./gate_reads_types";
import { chainBuildBaseOf, chainBuildReadsOfQuery } from "./result_reads";

/**
 * What the build `request` names compares, read in the model `load`
 * compiles queries in: a query-shaped source's own query over its base, a
 * rollup's grouping of its base by its grain with its measures, or a view's
 * or named query's `run:`. Each is read with the upstreams `stored` names
 * (by sourceID) as the tables it reads.
 */
export async function chainBuildStringReads(
   load: (query: string) => QueryMaterializer,
   request: ChainStringReadsRequest,
): Promise<StringReads> {
   const stored = new Set(request.stored);
   let runnable: QueryMaterializer;
   let downstream: string | undefined;
   if ("run" in request) {
      runnable = load(request.run);
   } else {
      const { rollup } = request;
      let base = rollup?.baseName;
      if (rollup === undefined) {
         downstream = request.downstream;
         base = await chainBuildBaseOf(
            load(`run: ${quotedIdentifier(downstream)} -> { select: * }`),
            downstream,
         );
      }
      if (base === undefined)
         return { refused: "the base its strings are read on is not named" };
      const clauses: string[] = [];
      if (rollup && rollup.grain.length > 0) {
         clauses.push(`group_by: ${rollup.grain.map(quotedPath).join(", ")}`);
      }
      clauses.push(
         rollup
            ? `aggregate: ${rollup.measures.map(quotedPath).join(", ")}`
            : "select: *",
      );
      runnable = load(
         `run: ${quotedIdentifier(base)} -> { ${clauses.join("; ")} }`,
      );
   }
   const reads = await chainBuildReadsOfQuery(runnable, stored, downstream);
   if (reads === undefined)
      return { refused: "the strings it compares could not be read" };
   // What its stages' filters compare: a `where:`, a `having:`, a filtered
   // measure's own or one it names.
   const filters = await queryFilterReadsOfQuery(runnable, stored, {
      fromStage: 0,
      namedMeasures: true,
      pipeline: reads.stages,
   });
   if (filters === undefined)
      return { refused: "the strings its filters compare could not be read" };
   const out: StringRead[] = [];
   for (const [gate, need] of [
      [reads.ordered, "exact"],
      [reads.grouped, "equality"],
      [filters, filters.equalityOnly ? "equality" : "exact"],
   ] as const) {
      const judged = readsOf(gate, need);
      if (judged === undefined)
         return {
            refused: "a string it compares could not be followed to a column",
         };
      out.push(...judged);
   }
   return { reads: out };
}

/** One walk's reads at `need`, or undefined when one could not be followed. */
function readsOf(
   reads: GateReads,
   need: StringRead["need"],
): StringRead[] | undefined {
   if (reads.unresolved) return undefined;
   if (reads.comparesNoString) return [];
   // Each read is judged against its own upstream's contract: one not
   // followed to a stored upstream refuses there.
   return [
      ...reads.columns.map((names, i) => ({
         upstream: reads.columnUpstreams?.[i],
         names: [...names],
         need,
      })),
      // A read through a query computed when the tables are read (an inner
      // query's base): its column is compared by the build all the same.
      ...(reads.elsewhereColumns ?? []).map((c) => ({
         upstream: c.upstream,
         names: [...c.names],
         need,
      })),
   ];
}
