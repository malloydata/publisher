// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import type { EntitiesByField, FieldEntity } from "./fieldEntities";
import type { TextDeclaration } from "./modelTextDeclarations";

/**
 * Per-field Malloy-definition lookup, supplied by the owner (SchemaView) when
 * code snippets are available for this row's source. `code` stays undefined
 * while loading or when the field has no definition of its own (an inherited
 * column).
 */
export interface FieldCodeLookup {
   code?: string;
   loading: boolean;
   /** The definitions read failed: kept distinct from "this field has no
    *  definition" (an error carries no evidence of absence), and the one
    *  absence the row still opens to report. */
   error?: boolean;
   /** The field is an inherited table column (entity `fieldType` "column"):
    *  its stored code is the SOURCE's declaring line, which reads as nonsense
    *  for the field, so it's suppressed. Such a row doesn't open; without a
    *  description it says "inherited" beside its type instead. */
   inherited?: boolean;
   /** For an inherited column, where it comes from: the table expression a
    *  table column is read from (`fromTable`), or the passthrough line of the
    *  query source it is an output of. Shown as its Code in place of a
    *  definition of its own. */
   inheritedFrom?: { code: string; fromTable: boolean };
   /** For an inherited column: a host's description of that warehouse column,
    *  so a passthrough dimension the modeler never documented still says what
    *  it holds. Never set for a defined field. */
   columnDoc?: string;
}

/**
 * Builds the per-field code lookup for ONE source's fields.
 *
 * Shared by the active source and by each joined source rendered inline: a
 * joined source is the same source seen from another page, so its fields must
 * offer the same definition/"inherited" affordance. Keeping one factory is what
 * makes that identical rather than merely similar.
 *
 * `entities` must be the fields of the source that DECLARES these fields —
 * for a join, the target source's map, not the host's. Field names are only
 * unique within a source, so passing the wrong map silently renders another
 * source's definition on a same-named field.
 */
export function makeGetFieldCode(params: {
   entities: EntitiesByField | undefined;
   loading: boolean;
   error: boolean;
   /**
    * Generated descriptions of the base TABLE's columns (column name → text),
    * for the table this source reads. Only ever applied to inherited columns —
    * a field with a Malloy definition is the modeler's expression, and its name
    * colliding with a warehouse column's says nothing about what it computes.
    *
    * Must be the table behind THESE entities. A joined source reads its own
    * table, so the host's map would describe the wrong columns — SchemaJoin
    * passes none.
    */
   columnDescriptions?: ReadonlyMap<string, string>;
   /**
    * The same source's declarations read from the model file's text
    * (`fieldDeclarations`), for a field the entities don't carry: without a
    * host's entities, every field's code comes from here. Used only once the entities read has settled with no entity for the
    * field, and only for a declaration of the field's own kind.
    */
   declarations?: ReadonlyMap<string, TextDeclaration>;
   /** The model file is still being parsed for `declarations`. */
   declarationsLoading?: boolean;
}): (field: { name: string; kind?: string }) => FieldCodeLookup {
   const {
      entities,
      loading,
      error,
      columnDescriptions,
      declarations,
      declarationsLoading,
   } = params;
   return (field) => {
      const entity: FieldEntity | undefined = entities?.get(field.name);
      const declared = declarations?.get(field.name);
      if (!entity && !loading && declared && declared.kind === field.kind) {
         return {
            code: declared.code,
            loading: false,
            error: false,
            inherited: false,
         };
      }
      if (!entity && !loading && !error && declarationsLoading) {
         return { loading: true, error: false, inherited: false };
      }
      // An inherited table column's stored code slice is the SOURCE's
      // declaring line (e.g. `duckdb.table('…')`) — misleading as a field
      // definition, so it's suppressed and the row says "inherited".
      const inherited = entity?.fieldType === "column";
      return {
         code: inherited ? undefined : (entity?.code ?? undefined),
         // Where an inherited column comes from, shown in its place: a table's
         // column carries the source's table expression (and the table itself);
         // a query source's passthrough carries its own line (`category`).
         inheritedFrom:
            inherited && entity?.code
               ? {
                    code: entity.code,
                    fromTable: !!entity.table || entity.code !== field.name,
                 }
               : undefined,
         loading: !error && loading,
         error,
         inherited,
         columnDoc: inherited ? columnDescriptions?.get(field.name) : undefined,
      };
   };
}
