// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * What a compiled model serves, and the load-time checks that refuse it. One
 * function for both load paths (the package-load worker and `Model.create`),
 * so the two cannot drift. Worker-safe: it logs nothing, and returns the
 * non-fatal findings for the caller to report.
 */
import {
   isSourceDef,
   type Model as MalloyModel,
   type ModelMaterializer,
} from "@malloydata/malloy";
import * as Malloy from "@malloydata/malloy-interfaces";
import {
   recordAuthorizeAdmitAllGate,
   recordRowLevelGateRejected,
} from "../authorize_metrics";
import {
   assertNoLegacyStringGate,
   assertNoMisplacedAuthorizeAnnotations,
   findLegacyStringGates,
   validateAuthorizeProbes,
} from "./authorize";
import { type FilterDefinition } from "./filter";
import {
   assertAuthorizeGrammarValid,
   assertNoRetiredRouteMarkers,
   collectRetiredRouteMarkers,
   computeGivenDeclaredTypes,
} from "./gate_classification";
import {
   validateSourceLineGateGivenUsage,
   type ExpandableRefSummary,
} from "./gate_dimension";
import {
   attachSuggestGivenNames,
   gateGivenSource,
   malloyGivenToApi,
   suggestGivenLookup,
   type MalloyGiven,
   type MalloyGivenApi,
} from "./given";
import {
   collectSourceInfos,
   extractQueriesFromModelDef,
   extractSourcesFromModelDef,
   type ExtractedQuery,
   type ExtractedSource,
} from "./source_extraction";

export interface CheckedModel {
   sources: ExtractedSource[];
   queries: ExtractedQuery[];
   filterMap: Map<string, FilterDefinition[]>;
   givens: MalloyGivenApi[] | undefined;
   /** Every source this file can resolve; see `collectSourceInfos`. */
   sourceInfos: Malloy.SourceInfo[];
   /** Non-fatal gate findings: the model still serves, the caller reports them. */
   warnings: { sourceName: string; message: string }[];
}

/**
 * Extract `compiled`'s served shape and run every load-time check on it.
 * Throws on a check that refuses the model (it becomes the model's
 * compilation error); records the gate metrics as it goes.
 *
 * Order matters: a retired marker and a misplaced `#(authorize)` are checked
 * before the gates themselves, so each mistake gets its own message.
 */
export async function extractCheckedModel(
   materializer: ModelMaterializer,
   compiled: MalloyModel,
   onFilterParseError?: (sourceName: string, err: unknown) => void,
): Promise<CheckedModel> {
   const modelDef = compiled._modelDef;
   // Malloy's `Model.givens` already collapses import inheritance.
   const malloyGivens = Array.from(compiled.givens.values());
   const givens =
      malloyGivens.length > 0
         ? malloyGivens.map((given) => malloyGivenToApi(given as MalloyGiven))
         : undefined;

   const extracted = extractSourcesFromModelDef(
      modelDef,
      givens,
      onFilterParseError,
   );
   const { sources, authorizeMap, authorizeOwnNotes } = extracted;
   // Now that each source's EFFECTIVE gate is known, say which givens each
   // `suggest` needs in its request. In place, so the copies on `sources` see it.
   attachSuggestGivenNames(
      givens,
      suggestGivenLookup(
         modelDef,
         (name) => gateGivenSource(sources, name),
         new Set((givens ?? []).map((given) => given.name)),
      ),
   );
   const { queries, misplacedAuthorize: misplacedOnQueries } =
      extractQueriesFromModelDef(modelDef);

   // A retired route marker (e.g. `#(partition)`) is enforced by nothing.
   assertNoRetiredRouteMarkers(collectRetiredRouteMarkers(modelDef));
   // A misplaced `#(authorize)` (on a `query:` or a field) is enforced by
   // nothing and would fail OPEN.
   assertNoMisplacedAuthorizeAnnotations([
      ...extracted.misplacedAuthorize,
      ...misplacedOnQueries,
   ]);
   // Presence-based `authorizeOwnNotes`, not the attributed map: a refusal
   // must not narrow (see `extractSourcesFromModelDef`).
   const legacyStringGates = findLegacyStringGates(authorizeOwnNotes);
   legacyStringGates.forEach(() =>
      recordRowLevelGateRejected("legacy_string_gate"),
   );
   assertNoLegacyStringGate(legacyStringGates);
   assertAuthorizeGrammarValid(
      modelDef,
      authorizeMap,
      authorizeOwnNotes,
      computeGivenDeclaredTypes(givens),
      (_sourceName, route) => recordAuthorizeAdmitAllGate(route),
      extracted.attributedAuthorizeOwnNotes,
   );

   const warnings: CheckedModel["warnings"] = [];
   // Probe-compiles every entry point's gate, so an unknown given or field
   // fails at load, not on first request. An inherited gate this entry point
   // cannot express only warns: every query against it is denied.
   await validateAuthorizeProbes(materializer, {
      authorizeMap,
      authorizeOwnNotes: extracted.attributedAuthorizeOwnNotes,
      onRowLevelGateRejected: recordRowLevelGateRejected,
      onRowLevelGateUnexpressible: (sourceName, detail) => {
         warnings.push({
            sourceName,
            message: `Row-level #(access_filter) gate not expressible at this entry point ("${sourceName}"); every query against it will be denied: ${detail}`,
         });
      },
      // Runs at EVERY entry point whose probe compiled, so `sourceName` may be
      // an inheritor; `modelDef.contents[sourceName]` is the struct the probe
      // was grafted onto.
      onOwnRowLevelConditionCompiled: (sourceName, condition, route) => {
         const struct = modelDef.contents[sourceName];
         if (!struct || !isSourceDef(struct)) return;
         validateSourceLineGateGivenUsage(
            sourceName,
            route,
            struct,
            condition.refSummary as ExpandableRefSummary | undefined,
            condition.e,
            modelDef,
            (cause, detail) => {
               recordRowLevelGateRejected(cause);
               warnings.push({
                  sourceName,
                  message: `#(${route}) gate warning on "${sourceName}" (${cause}): ${detail}`,
               });
            },
         );
      },
   });

   return {
      sources,
      queries,
      filterMap: extracted.filterMap,
      givens,
      sourceInfos: collectSourceInfos(modelDef),
      warnings,
   };
}
