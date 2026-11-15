// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * The compiler-internal slots of a compiled Malloy object, read in one
 * place: a `Model`'s `ModelDef`, a persist source's own `SourceDef`, its
 * explore and the model it was planned in, and the facts read off a compiled
 * source that more than one reader needs (its declared primary key, the time
 * zone it sets).
 *
 * Malloy keeps these slots internal (`_modelDef`, `_sourceDef`, `_explore`),
 * so a Malloy upgrade that moves one breaks this module alone. Every reader
 * outside `governance/ir` goes through it; materialization's IR ratchet
 * (`layer_rules.spec.ts`) holds that.
 *
 * Pure: reads only what it is handed.
 */

/** The `ModelDef` a compiled `Model` (or anything carrying one) holds. */
export function modelDefOf<T extends { _modelDef: unknown }>(
   model: T,
): T["_modelDef"] {
   return model._modelDef;
}

/** {@link modelDefOf}, or undefined for no model, or one that holds none. */
export function modelDefIfAny<T extends { _modelDef?: unknown }>(
   model: T | undefined,
): T["_modelDef"] | undefined {
   return model?._modelDef;
}

/** A persist source's own compiled `SourceDef`. */
export function sourceDefOf<T extends { _sourceDef: unknown }>(
   source: T,
): T["_sourceDef"] {
   return source._sourceDef;
}

/** A persist source's compiled explore (its fields as Malloy presents them). */
export function exploreOf<T extends { _explore: unknown }>(
   source: T,
): T["_explore"] {
   return source._explore;
}

/** The `ModelDef` of the model a persist source was planned in, if it holds one. */
export function plannedModelDefOf(source: {
   _model?: { _modelDef?: unknown };
}): unknown {
   return source._model?._modelDef;
}

/** A compiled source's declared `primary_key:`, if it has one. */
export function primaryKeyOf(def: unknown): string | undefined {
   const key = (def as { primaryKey?: unknown } | undefined)?.primaryKey;
   return typeof key === "string" ? key : undefined;
}

/**
 * The time zone a compiled source or query stage sets itself, exactly as it
 * is recorded (`queryTimezone`): for a comparison of two that must agree
 * whatever either holds.
 */
export function recordedTimezoneOf(def: unknown): unknown {
   return (def as { queryTimezone?: unknown } | undefined)?.queryTimezone;
}

/** The time zone a compiled source or query stage sets, when it names one. */
export function timezoneOf(def: unknown): string | undefined {
   const zone = recordedTimezoneOf(def);
   return typeof zone === "string" && zone !== "" ? zone : undefined;
}

/**
 * The query time zone a compiled source's rows are computed in: its own,
 * or, for a query-shaped source, the last stage of its pipeline that sets
 * one, as Malloy applies them.
 */
export function queryTimezoneOf(structDef: unknown): string | undefined {
   const def = structDef as
      | {
           queryTimezone?: unknown;
           query?: { pipeline?: readonly { queryTimezone?: unknown }[] };
        }
      | undefined;
   let tz: unknown = def?.queryTimezone;
   for (const stage of def?.query?.pipeline ?? []) {
      if (typeof stage?.queryTimezone === "string") tz = stage.queryTimezone;
   }
   return typeof tz === "string" && tz !== "" ? tz : undefined;
}
