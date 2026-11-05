// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * Serializes Malloy Explore objects into the SourceInfo JSON format (a model's
 * `sourceSchemas` on `Package.entities`), which an indexer can use to
 * summarize or describe a source.
 *
 * SourceInfo shape:
 *   {
 *     name: string,
 *     annotations: [{value: string}, ...],
 *     schema: {
 *       fields: [
 *         {
 *           kind: "dimension" | "measure" | "calculate" | "join" | "view" | "column",
 *           name: string,
 *           type?: string,
 *           annotations?: [{value: string}, ...],
 *           schema?: { fields: [...] }
 *         }
 *       ]
 *     }
 *   }
 */

import { AccessModifierEnum, EntityTypeEnum } from "./internal_types";
import {
   getAccessModifier,
   getDirectTaglines,
   inferFieldDataType,
   inferType,
   isFieldHidden,
} from "./malloy_indexing_helpers";
import type { Explore, Field } from "@malloydata/malloy";
import type {
   SourceInfo,
   SourceInfoAnnotation,
   SourceInfoField,
} from "./internal_types";

function toAnnotations(taglines: string[]): SourceInfoAnnotation[] {
   return taglines
      .map((t) => t.trim())
      .filter((t) => t.length > 0)
      .map((value) => ({ value }));
}

function getAnnotations(entity: Field | Explore): SourceInfoAnnotation[] {
   try {
      return toAnnotations(entity.getTaglines());
   } catch {
      return [];
   }
}

// Top-level source annotations must be the ones genuinely declared on the
// source: `getTaglines()` walks/copies an extended base's notes, which would
// make `source: child is documented_base extend {}` inherit the base's
// `#(doc)` into its summary and description (see getDirectTaglines). Fields
// keep `getAnnotations` — a field inherited unchanged legitimately keeps the
// doc declared on it.
function getExploreAnnotations(explore: Explore): SourceInfoAnnotation[] {
   try {
      return toAnnotations(getDirectTaglines(explore));
   } catch {
      return [];
   }
}

const ENTITY_TYPE_TO_KIND: Record<string, string> = {
   [EntityTypeEnum.Measure]: "measure",
   [EntityTypeEnum.Dimension]: "dimension",
   [EntityTypeEnum.Column]: "column",
   [EntityTypeEnum.Join]: "join",
   [EntityTypeEnum.View]: "view",
};

function getFieldKind(field: Field, docString: string): string | null {
   if (isFieldHidden(field)) {
      return null;
   }
   const entityType = inferType(field, docString);
   return ENTITY_TYPE_TO_KIND[entityType] ?? null;
}

function serializeField(
   field: Field,
   docString: string,
): SourceInfoField | null {
   const kind = getFieldKind(field, docString);
   if (!kind) {
      return null;
   }
   const result: SourceInfoField = {
      kind,
      name: field.name,
   };

   const fieldType = inferFieldDataType(field);
   if (fieldType) {
      result.type = fieldType;
   }

   const annotations = getAnnotations(field);
   if (annotations.length > 0) {
      result.annotations = annotations;
   }

   if (field.isExploreField()) {
      const nestedExplore = field as unknown as Explore;
      const nestedFields = serializeFields(nestedExplore, docString);
      if (nestedFields.length > 0) {
         result.schema = { fields: nestedFields };
      }
   }

   if (field.isQueryField()) {
      try {
         const queryField = field as unknown as Explore;
         if (queryField.allFields) {
            const nestedFields = serializeFields(queryField, docString);
            if (nestedFields.length > 0) {
               result.schema = { fields: nestedFields };
            }
         }
      } catch {
         // Not all query fields expose nested structure
      }
   }

   return result;
}

function serializeFields(
   explore: Explore,
   docString: string,
): SourceInfoField[] {
   const fields: SourceInfoField[] = [];
   for (const field of explore.allFields) {
      if (getAccessModifier(field) !== AccessModifierEnum.Public) {
         continue;
      }
      const serialized = serializeField(field, docString);
      if (serialized) {
         fields.push(serialized);
      }
   }
   return fields;
}

/**
 * Serialize a Malloy Explore into the SourceInfo JSON format.
 */
export function serializeExploreToSourceInfo(
   explore: Explore,
   docString: string,
): SourceInfo {
   const annotations = getExploreAnnotations(explore);
   const fields = serializeFields(explore, docString);

   return {
      name: explore.name,
      annotations,
      schema: { fields },
   };
}
