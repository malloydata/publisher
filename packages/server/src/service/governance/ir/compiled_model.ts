// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * The compiler-internal slots of a compiled Malloy object, read in one
 * place: a `Model`'s `ModelDef`, a persist source's own `SourceDef`, its
 * explore and the model it was planned in, and the facts read off a compiled
 * source that more than one reader needs (its declared primary key, the time
 * zone it sets, a field's access modifier).
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

/**
 * A persist source's output columns, by name (its `as` name when it has one),
 * IR type and numeric subtype (`numberType`, for a number), in order: what
 * its table holds.
 */
export function outputColumnsOf(source: {
   _explore: { structDef: unknown };
}): [string | undefined, string | undefined, string | undefined][] {
   const fields =
      (source._explore.structDef as { fields?: unknown[] }).fields ?? [];
   return fields.map((f) => {
      const field = f as {
         name?: string;
         as?: string;
         type?: string;
         numberType?: string;
      };
      return [field.as ?? field.name, field.type, field.numberType];
   });
}

/** The `ModelDef` of the model a persist source was planned in, if it holds one. */
export function plannedModelDefOf(source: {
   _model?: { _modelDef?: unknown };
}): unknown {
   return source._model?._modelDef;
}

/**
 * A compiled field's access modifier when it is not public, or undefined. Fails
 * closed, as `isHidden` in preaggregation_synthesis.ts does: a modifier it does
 * not know reads as private. The one reader of `accessModifier` for P3: the
 * plan, the build copy's selectable set and the bind guard's access proof.
 */
export function hiddenAccess(field: {
   accessModifier?: unknown;
}): "private" | "internal" | undefined {
   if (field.accessModifier == null || field.accessModifier === "public") {
      return undefined;
   }
   return field.accessModifier === "internal" ? "internal" : "private";
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

/** Max IR depth to walk; deep enough for real sources, a hang backstop. */
const MAX_WALK_DEPTH = 200;

/**
 * Whether a slice of compiled IR reads a given, by EITHER a `{node:'given'}`
 * reference or a non-empty `givenUsage` summary, so a compiler that prunes one
 * of the two signals cannot make a given read clean. Throws past a depth
 * bound, which every caller treats as a read.
 */
export function readsAGiven(
   node: unknown,
   seen: WeakSet<object> = new WeakSet(),
   depth = 0,
): boolean {
   if (depth > MAX_WALK_DEPTH) {
      throw new Error("given walk exceeded max depth");
   }
   if (node === null || typeof node !== "object") return false;
   if (seen.has(node as object)) return false;
   seen.add(node as object);

   if (Array.isArray(node)) {
      return node.some((item) => readsAGiven(item, seen, depth + 1));
   }
   const record = node as Record<string, unknown>;
   if (record.node === "given" || record.node === "givenReference") return true;
   if (record.givenRef !== undefined) return true;
   const summary = record.refSummary as { givenUsage?: unknown[] } | undefined;
   if (summary?.givenUsage && summary.givenUsage.length > 0) return true;
   if (Array.isArray(record.givenUsage) && record.givenUsage.length > 0) {
      return true;
   }
   return Object.values(record).some((v) => readsAGiven(v, seen, depth + 1));
}

/**
 * The verbatim text of each of a compiled source's `where:` terms that reads
 * a given ({@link readsAGiven}), sorted: what a routed compile that writes
 * them again must carry, compared with live's by text. Undefined when the
 * filter list, or a given-reading term's text, cannot be read (fail closed).
 */
export function givenWhereCodes(def: unknown): string[] | undefined {
   const list = (def as { filterList?: unknown } | undefined)?.filterList;
   if (list === undefined) return [];
   if (!Array.isArray(list)) return undefined;
   const codes: string[] = [];
   try {
      for (const term of list) {
         if (!readsAGiven(term)) continue;
         const code = (term as { code?: unknown } | null)?.code;
         if (typeof code !== "string" || code === "") return undefined;
         codes.push(code);
      }
   } catch {
      return undefined;
   }
   return codes.sort();
}
