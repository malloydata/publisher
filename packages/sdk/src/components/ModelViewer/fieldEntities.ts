// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * What a host knows about a model's fields beyond what the compiled model says.
 *
 * The viewer's own reads give it every field's name, type and annotations, and
 * the model file's text gives it the code of whatever the file declares. A host
 * with a fuller record of the model (an index of every field the model compiles,
 * say) can pass that record as entities, and the viewer then shows what only it
 * knows: an inherited table column's origin, the table a source reads, a join's
 * target source and its declaration.
 */
export interface FieldEntity {
   /** The field's name within its declaring source. */
   name: string;
   /** The source that declares the field. An entity without one is dropped:
    *  field names are unique only within a source. */
   source?: string | null;
   /** `dimension`, `measure`, `view`, `join`, or `column` for a table column the
    *  source passes through unchanged. */
   fieldType?: string | null;
   /** The Malloy that defines the field, verbatim. */
   code?: string | null;
   /** The table the declaring source reads, when it reads one. */
   table?: FieldEntityTable | null;
   /** For a join, the source it joins: the alias can differ from its name. */
   joinTarget?: { modelPath: string; sourceName: string } | null;
}

export interface FieldEntityTable {
   connectionName: string;
   catalogName?: string | null;
   schemaName?: string | null;
   tableName: string;
}

/** One source's fields, keyed by field name. */
export type EntitiesByField = ReadonlyMap<string, FieldEntity>;

/**
 * Every source declared by one model file, keyed by SOURCE name, never a flat
 * field map: `aircraft` and `aircraft_models` can both declare `id`, and a flat
 * map would show one source's definition on the other's field.
 */
export type EntitiesBySource = ReadonlyMap<string, EntitiesByField>;

/** A model's entities grouped by declaring source; one with no source is dropped rather than guessed. */
export function groupEntitiesBySource(
   entities: ReadonlyArray<FieldEntity>,
): EntitiesBySource {
   const bySource = new Map<string, Map<string, FieldEntity>>();
   for (const entity of entities) {
      if (!entity.source) continue;
      let fields = bySource.get(entity.source);
      if (!fields) {
         fields = new Map();
         bySource.set(entity.source, fields);
      }
      fields.set(entity.name, entity);
   }
   return bySource;
}
