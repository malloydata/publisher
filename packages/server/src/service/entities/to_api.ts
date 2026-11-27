// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import type { components } from "../../api";
import { compareText } from "../../compare_text";
import type {
   Entity,
   Given,
   ModelCompileResult,
   SourceInfoField,
   SourceMetadata,
} from "./internal_types";

/** The generated types are read-only (`readOnly` in the spec); built here. */
type Mutable<T> = { -readonly [K in keyof T]: T[K] };
type ApiModelEntities = components["schemas"]["ModelEntities"];
type ApiEntityField = Mutable<components["schemas"]["EntityField"]>;
type ApiSourceEntities = Mutable<components["schemas"]["SourceEntities"]>;
type ApiSourceSchemaField = components["schemas"]["SourceSchemaField"];
type ApiGiven = components["schemas"]["Given"];

/** The `GET …/entities` models for one package's extraction results. */
export function toApiEntities(
   results: readonly ModelCompileResult[],
): ApiModelEntities[] {
   return results
      .map(toApiModelEntities)
      .sort((a, b) => compareText(a.modelPath, b.modelPath));
}

function toApiModelEntities(result: ModelCompileResult): ApiModelEntities {
   if (result.compileStatus !== "SUCCESS") {
      return {
         modelPath: result.modelFilePath,
         error: result.errorMessage ?? "The model did not compile.",
         errorKind: result.errorKind ?? "compile",
      };
   }
   const givens = result.givens ?? modelGivensOf(result.sourceMetadataEntries);
   return {
      modelPath: result.modelFilePath,
      fields: result.entities.map(toApiField),
      sources: toApiSources(result),
      givens: givens.map(toApiGiven),
      sourceSchemas: (result.sourceInfos ?? []).map((info) => ({
         name: info.name,
         annotations: info.annotations.map((a) => a.value),
         fields: info.schema.fields.map(toApiSchemaField),
      })),
   };
}

function toApiField(entity: Entity): ApiEntityField {
   const table = entity.table_info ?? {};
   const field: ApiEntityField = {
      name: entity.name,
      source: entity.source ?? "",
      kind: entity.field_type === "unknown" ? "hidden" : entityKind(entity),
      code: entity.code ?? "",
      annotations: entity.tags ?? [],
      table: {
         ...(table.connection_name != null && {
            connection: table.connection_name,
         }),
         ...(table.catalog_name != null && { catalog: table.catalog_name }),
         ...(table.schema_name != null && { schema: table.schema_name }),
         ...(table.table_name != null && { name: table.table_name }),
      },
   };
   if (entity.field_data_type != null) field.dataType = entity.field_data_type;
   if (entity.description !== undefined) field.doc = entity.description;
   const join = entity.join_source_metadata;
   if (join) {
      field.join = {
         nested: join.is_nested_index === true,
         ...(join.original_source_name != null && {
            sourceName: join.original_source_name,
         }),
         ...(join.original_source_path != null && {
            sourcePath: join.original_source_path,
         }),
         ...(join.relationship && { relationship: join.relationship }),
      };
   }
   if (entity.dim_index_fingerprint) {
      field.indexFingerprint = {
         sqlHash: entity.dim_index_fingerprint.sql_fingerprint,
         requiredFilterDimensions:
            entity.dim_index_fingerprint.required_filter_dimensions,
      };
   }
   return field;
}

function entityKind(entity: Entity): ApiEntityField["kind"] {
   switch (entity.field_type) {
      case "dimension":
      case "column":
      case "measure":
      case "view":
      case "join":
         return entity.field_type;
      default:
         return "hidden";
   }
}

function toApiSchemaField(field: SourceInfoField): ApiSourceSchemaField {
   return {
      kind: field.kind as ApiSourceSchemaField["kind"],
      name: field.name,
      ...(field.type !== undefined && { type: field.type }),
      ...(field.annotations !== undefined && {
         annotations: field.annotations.map((a) => a.value),
      }),
      ...(field.schema !== undefined && {
         fields: field.schema.fields.map(toApiSchemaField),
      }),
   };
}

function toApiGiven(given: Given): ApiGiven {
   return given as ApiGiven;
}

/**
 * Every GIVENS row of a model carries the model's whole given list, so the
 * first one is the list.
 */
function modelGivensOf(rows: readonly SourceMetadata[]): Given[] {
   return rows.find((r) => r.metadataType === "GIVENS")?.givens ?? [];
}

/**
 * Fold a model's metadata rows into one entry per (file, source), in the order
 * the rows first name each, then add every source the model surfaces that no
 * row names.
 */
function toApiSources(result: ModelCompileResult): ApiSourceEntities[] {
   const bySource = new Map<string, ApiSourceEntities>();
   const entryFor = (row: SourceMetadata): ApiSourceEntities => {
      const key = `${row.modelFilePath}\u0000${row.sourceName}`;
      let entry = bySource.get(key);
      if (!entry) {
         entry = {
            modelPath: row.modelFilePath,
            name: row.sourceName,
            joinOnly: true,
            agentHidden: false,
            queryable: true,
         };
         bySource.set(key, entry);
      }
      return entry;
   };
   const topLevel = new Set(
      (result.sourceInfos ?? []).map(
         (info) => `${result.modelFilePath}\u0000${info.name}`,
      ),
   );
   for (const row of result.sourceMetadataEntries) {
      const entry = entryFor(row);
      if (topLevel.has(`${row.modelFilePath}\u0000${row.sourceName}`)) {
         entry.joinOnly = false;
      }
      switch (row.metadataType) {
         case "DOCS":
            if (row.metadata !== "") entry.doc = row.metadata;
            break;
         case "AGENT_HIDDEN":
            entry.agentHidden = true;
            break;
         case "NOT_QUERYABLE":
            entry.queryable = false;
            if (row.servedFrom) {
               entry.servedFrom = {
                  modelPath: row.servedFrom.modelFilePath,
                  sourceName: row.servedFrom.sourceName,
               };
            }
            break;
         case "FILTERS":
            entry.filters = row.filters ?? [];
            break;
         case "AUTHORIZE":
            entry.authorize = (row.authorize ?? []).map((gate) => ({
               expression: gate.expression,
               givenNames: gate.given_names,
               ...(gate.route !== undefined && { route: gate.route }),
               ...(gate.terms !== undefined && {
                  terms: gate.terms.map((t) => ({
                     fieldPath: t.field_path,
                     given: t.given,
                     op: t.op,
                  })),
               }),
            }));
            break;
         case "GIVENS":
            // Reported once, on the model.
            break;
      }
   }
   for (const info of result.sourceInfos ?? []) {
      const key = `${result.modelFilePath}\u0000${info.name}`;
      if (!bySource.has(key)) {
         bySource.set(key, {
            modelPath: result.modelFilePath,
            name: info.name,
            joinOnly: false,
            agentHidden: false,
            queryable: true,
         });
      }
   }
   return [...bySource.values()];
}
