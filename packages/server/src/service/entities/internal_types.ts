// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import type { MalloyGivenApi } from "../given";

/**
 * The shapes the entities extraction builds internally, one model file at a
 * time. `to_api.ts` turns a package's worth of them into the public
 * `GET …/entities` payload. Field names here are the extraction's own
 * (snake_case on an entity, camelCase on a source row) and never reach the
 * wire as is.
 */

export enum AccessModifierEnum {
   Public = "public",
   Private = "private",
   Internal = "internal",
}

export enum EntityTypeEnum {
   Join = "join",
   View = "view",
   Measure = "measure",
   Dimension = "dimension",
   Column = "column",
   Unknown = "unknown",
}

export interface TableInfo {
   connection_name?: string | null;
   catalog_name?: string | null;
   schema_name?: string | null;
   table_name?: string | null;
}

export interface JoinSourceMetadata {
   original_source_name?: string | null;
   original_source_path?: string | null;
   is_nested_index?: boolean;
}

export interface DimIndexFingerprint {
   sql_fingerprint: string;
   required_filter_dimensions: string[];
}

/** One field of a source, as the extraction reports it. */
export interface Entity {
   name: string;
   field_type: string;
   field_data_type?: string | null;
   source?: string | null;
   code?: string | null;
   /** The field's annotation lines, trimmed. */
   tags?: string[];
   description?: string;
   table_info?: TableInfo | null;
   join_source_metadata?: JoinSourceMetadata | null;
   dim_index_fingerprint?: DimIndexFingerprint | null;
}

export interface Filter {
   name: string;
   type: "equal" | "in" | "like" | "greater_than" | "less_than";
   dimension?: string;
   required?: boolean;
   implicit?: boolean;
}

/** A given as the server serializes it everywhere ({@link malloyGivenToApi}). */
export type Given = MalloyGivenApi;

export interface AuthorizeTerm {
   field_path: string;
   given: string;
   op: "=" | "in";
}

export interface Authorize {
   expression: string;
   given_names: string[];
   route?: "authorize" | "access_filter";
   terms?: AuthorizeTerm[];
}

export interface SourceAddress {
   modelFilePath: string;
   sourceName: string;
}

export type SourceMetadataType =
   | "DOCS"
   | "FILTERS"
   | "GIVENS"
   | "AUTHORIZE"
   | "AGENT_HIDDEN"
   | "NOT_QUERYABLE";

/** One fact about one source, keyed by the model file it is filed under. */
export interface SourceMetadata {
   modelFilePath: string;
   sourceName: string;
   metadata: string;
   metadataType: SourceMetadataType;
   agentHidden?: boolean;
   servedFrom?: SourceAddress;
   filters?: Filter[];
   givens?: Given[];
   authorize?: Authorize[];
}

/** A source's schema: its public fields, recursively, with their annotations. */
export interface SourceInfoAnnotation {
   value: string;
}

export interface SourceInfoField {
   kind: string;
   name: string;
   type?: string;
   annotations?: SourceInfoAnnotation[];
   schema?: { fields: SourceInfoField[] };
}

export interface SourceInfo {
   name: string;
   annotations: SourceInfoAnnotation[];
   schema: { fields: SourceInfoField[] };
}

/** The extraction's result for one model file. */
export interface ModelCompileResult {
   modelFilePath: string;
   entities: Entity[];
   sourceMetadataEntries: SourceMetadata[];
   compileStatus: "SUCCESS" | "ERROR";
   /** The model's own givens, whether or not any source carries them. */
   givens?: Given[];
   /** On an ERROR: the author's compile error, or the server's failure to extract. */
   errorKind?: "compile" | "extraction";
   errorMessage?: string;
   sourceInfos?: SourceInfo[];
}
