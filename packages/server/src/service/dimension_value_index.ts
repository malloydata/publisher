import { logger } from "../logger";
import {
   getDimensionValueIndexCap,
   getDimensionValueIndexMode,
} from "../config";
import { DuckDBConnection } from "../storage/duckdb/DuckDBConnection";
import { parseAuthorizeAnnotation } from "./authorize";
import { parseFilterAnnotation } from "./filter";
import type { Package } from "./package";

const INDEX_TAG = /#\(\s*index\s*\)/;

export type DimensionValueIndexStatus =
   | "ready"
   | "indexing"
   | "unavailable"
   | "off";

export interface IndexableDimension {
   modelPath: string;
   sourceName: string;
   dimensionName: string;
}

export interface DimensionValueHit {
   entityId: string;
   kind: "dimensional_value";
   name: string;
   source: string;
   dimension: string;
   modelPath: string;
   rank: number;
}

export interface DimensionValueIndexMeta {
   generation: number;
   status: DimensionValueIndexStatus;
   truncatedCount: number;
   valueCount: number;
   servedRevision?: string;
}

type AnnotationLike = string | { value: string };

function annotationLines(annotations: AnnotationLike[] | undefined): string[] {
   if (!annotations) return [];
   return annotations.map((a) => (typeof a === "string" ? a : a.value));
}

/** True when a dimension (or source) is explicitly opted into value indexing. */
export function hasIndexAnnotation(
   annotations: AnnotationLike[] | undefined,
): boolean {
   return annotationLines(annotations).some((line) => INDEX_TAG.test(line));
}

/**
 * A source whose row visibility depends on the caller. Indexing it would
 * either leak gated values or store an empty scan.
 */
export function sourceIsProtected(source: {
   authorize?: string[] | undefined;
   filters?: unknown[] | undefined;
   annotations?: AnnotationLike[] | undefined;
}): boolean {
   if ((source.authorize?.length ?? 0) > 0) return true;
   if ((source.filters?.length ?? 0) > 0) return true;
   const lines = annotationLines(source.annotations);
   for (const line of lines) {
      if (parseAuthorizeAnnotation(line) !== null) return true;
      try {
         if (parseFilterAnnotation(line) !== null) return true;
      } catch {
         // Malformed filter still means the author intended a filter.
         if (/#\(\s*filter\s*\)/.test(line)) return true;
      }
   }
   return false;
}

export function malloyIdent(name: string): string {
   if (/^[A-Za-z_][A-Za-z0-9_]*$/.test(name)) return name;
   return `\`${name.replace(/`/g, "``")}\``;
}

export function dimensionValueEntityId(
   source: string,
   dimension: string,
   value: string,
): string {
   return `dimensional_value:${source}:${dimension}:${value}`;
}

export async function getDimensionValueIndexMeta(
   db: DuckDBConnection,
   environmentName: string,
   packageName: string,
): Promise<DimensionValueIndexMeta | undefined> {
   if (getDimensionValueIndexMode() === "off") {
      return {
         generation: 0,
         status: "off",
         truncatedCount: 0,
         valueCount: 0,
      };
   }
   const row = await db.get<{
      generation: number;
      status: string;
      truncated_count: number;
      value_count: number;
      served_revision: string | null;
   }>(
      `SELECT generation, status, truncated_count, value_count, served_revision
       FROM dimension_value_generations
       WHERE environment_name = ? AND package_name = ?`,
      [environmentName, packageName],
   );
   if (!row) return undefined;
   return {
      generation: row.generation,
      status: row.status as DimensionValueIndexStatus,
      truncatedCount: row.truncated_count,
      valueCount: row.value_count,
      servedRevision: row.served_revision ?? undefined,
   };
}

/**
 * Walk a package for dimensions that may be indexed: tagged `#(index)`,
 * on a source with no authorize/filter visibility.
 */
export async function collectSourceMeta(
   pkg: Package,
): Promise<Map<string, { givens?: unknown; authorize?: string[] }>> {
   const meta = new Map<string, { givens?: unknown; authorize?: string[] }>();
   const models = await pkg.listModels();
   for (const apiModel of models) {
      const modelPath = apiModel.path;
      if (!modelPath) continue;
      const model = pkg.getModel(modelPath);
      if (!model || typeof model.getSources !== "function") continue;
      for (const source of model.getSources() ?? []) {
         if (!source.name) continue;
         meta.set(source.name, {
            ...(source.givens ? { givens: source.givens } : {}),
            ...(source.authorize ? { authorize: source.authorize } : {}),
         });
      }
   }
   return meta;
}

export async function collectIndexableDimensionsAsync(
   pkg: Package,
): Promise<IndexableDimension[]> {
   const models = await pkg.listModels();
   const found: IndexableDimension[] = [];
   for (const apiModel of models) {
      const modelPath = apiModel.path;
      if (!modelPath) continue;
      const model = pkg.getModel(modelPath);
      if (!model) continue;
      const apiSources = (
         typeof model.getSources === "function" ? model.getSources() : []
      ) as Array<{
         name?: string;
         authorize?: string[];
         filters?: unknown[];
         annotations?: AnnotationLike[];
      }>;
      const protectedByName = new Map<string, boolean>();
      for (const source of apiSources) {
         if (!source.name) continue;
         protectedByName.set(source.name, sourceIsProtected(source));
      }
      const sourceInfos = model.getSourceInfos() ?? [];
      for (const sourceInfo of sourceInfos) {
         const sourceName = sourceInfo.name;
         const protectedSource =
            protectedByName.get(sourceName) ??
            sourceIsProtected({
               annotations: sourceInfo.annotations as AnnotationLike[],
            });
         if (protectedSource) continue;
         for (const field of sourceInfo.schema.fields ?? []) {
            if (field.kind !== "dimension") continue;
            if (!hasIndexAnnotation(field.annotations as AnnotationLike[])) {
               continue;
            }
            found.push({
               modelPath,
               sourceName,
               dimensionName: field.name,
            });
         }
      }
   }
   return found;
}

export type FetchDimensionValues = (
   dim: IndexableDimension,
   cap: number,
) => Promise<string[]>;

/**
 * Default fetcher: one grouped query per dimension, clipped at cap+1 so
 * truncation is visible. Values stay on this machine.
 */
export function fetchDimensionValuesFromPackage(
   pkg: Package,
): FetchDimensionValues {
   return async (dim, cap) => {
      const model = pkg.getModel(dim.modelPath);
      if (!model || typeof model.getQueryResults !== "function") return [];
      const source = malloyIdent(dim.sourceName);
      const field = malloyIdent(dim.dimensionName);
      const query = `run: ${source} -> { group_by: ${field}; limit: ${cap + 1} }`;
      try {
         const { compactResult } = await model.getQueryResults(
            undefined,
            undefined,
            query,
            undefined,
            undefined,
            undefined,
            undefined,
            undefined,
            "compact",
         );
         const rows = Array.isArray(compactResult) ? compactResult : [];
         const values: string[] = [];
         for (const row of rows) {
            if (!row || typeof row !== "object") continue;
            const record = row as Record<string, unknown>;
            const raw =
               record[dim.dimensionName] ?? Object.values(record)[0];
            if (raw === null || raw === undefined) continue;
            values.push(String(raw));
         }
         return values;
      } catch (error) {
         logger.debug("[dimension-value-index] fetch failed", {
            source: dim.sourceName,
            dimension: dim.dimensionName,
            error: error instanceof Error ? error.message : String(error),
         });
         return [];
      }
   };
}

/**
 * Replace the package's value index atomically: write generation N+1,
 * point the package at it, then delete the previous generation.
 */
export async function refreshDimensionValueIndex(args: {
   db: DuckDBConnection;
   environmentName: string;
   packageName: string;
   servedRevision?: string;
   dimensions: IndexableDimension[];
   fetchValues: FetchDimensionValues;
   cap?: number;
}): Promise<DimensionValueIndexMeta> {
   const cap = args.cap ?? getDimensionValueIndexCap();
   const current = await getDimensionValueIndexMeta(
      args.db,
      args.environmentName,
      args.packageName,
   );
   const nextGeneration = (current?.generation ?? 0) + 1;
   const now = new Date().toISOString();

   await args.db.run(
      `INSERT INTO dimension_value_generations (
          environment_name, package_name, generation, served_revision,
          status, truncated_count, value_count, updated_at
        ) VALUES (?, ?, ?, ?, 'indexing', 0, 0, ?)
        ON CONFLICT (environment_name, package_name) DO UPDATE SET
          status = 'indexing',
          updated_at = excluded.updated_at`,
      [
         args.environmentName,
         args.packageName,
         current?.generation ?? 0,
         args.servedRevision ?? null,
         now,
      ],
   );

   let valueCount = 0;
   let truncatedCount = 0;
   for (const dim of args.dimensions) {
      const fetched = await args.fetchValues(dim, cap);
      const truncated = fetched.length > cap;
      const values = truncated ? fetched.slice(0, cap) : fetched;
      if (truncated) truncatedCount += 1;
      for (const value of values) {
         await args.db.run(
            `INSERT INTO dimension_values (
                environment_name, package_name, generation, model_path,
                source_name, dimension_name, value, truncated, updated_at
              ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
              ON CONFLICT (
                environment_name, package_name, generation, model_path,
                source_name, dimension_name, value
              ) DO UPDATE SET
                truncated = excluded.truncated,
                updated_at = excluded.updated_at`,
            [
               args.environmentName,
               args.packageName,
               nextGeneration,
               dim.modelPath,
               dim.sourceName,
               dim.dimensionName,
               value,
               truncated ? 1 : 0,
               now,
            ],
         );
         valueCount += 1;
      }
   }

   await args.db.run(
      `INSERT INTO dimension_value_generations (
          environment_name, package_name, generation, served_revision,
          status, truncated_count, value_count, updated_at
        ) VALUES (?, ?, ?, ?, 'ready', ?, ?, ?)
        ON CONFLICT (environment_name, package_name) DO UPDATE SET
          generation = excluded.generation,
          served_revision = excluded.served_revision,
          status = 'ready',
          truncated_count = excluded.truncated_count,
          value_count = excluded.value_count,
          updated_at = excluded.updated_at`,
      [
         args.environmentName,
         args.packageName,
         nextGeneration,
         args.servedRevision ?? null,
         truncatedCount,
         valueCount,
         now,
      ],
   );

   if (current && current.generation > 0) {
      await args.db.run(
         `DELETE FROM dimension_values
          WHERE environment_name = ? AND package_name = ? AND generation = ?`,
         [args.environmentName, args.packageName, current.generation],
      );
   }

   return {
      generation: nextGeneration,
      status: "ready",
      truncatedCount,
      valueCount,
      servedRevision: args.servedRevision,
   };
}

export async function searchDimensionValues(args: {
   db: DuckDBConnection;
   environmentName: string;
   packageName: string;
   searchText?: string | null;
   sourceName?: string;
   dimensionName?: string;
   limit: number;
}): Promise<DimensionValueHit[]> {
   if (getDimensionValueIndexMode() === "off") return [];
   const meta = await getDimensionValueIndexMeta(
      args.db,
      args.environmentName,
      args.packageName,
   );
   if (!meta || meta.status !== "ready") return [];

   const needle = (args.searchText ?? "").trim().toLowerCase();
   const rows = await args.db.all<{
      model_path: string;
      source_name: string;
      dimension_name: string;
      value: string;
   }>(
      `SELECT model_path, source_name, dimension_name, value
       FROM dimension_values
       WHERE environment_name = ?
         AND package_name = ?
         AND generation = ?
         AND (? IS NULL OR source_name = ?)
         AND (? IS NULL OR dimension_name = ?)
       ORDER BY source_name, dimension_name, value`,
      [
         args.environmentName,
         args.packageName,
         meta.generation,
         args.sourceName ?? null,
         args.sourceName ?? null,
         args.dimensionName ?? null,
         args.dimensionName ?? null,
      ],
   );

   const matched = (rows ?? []).filter((row) => {
      if (!needle) return true;
      return (
         row.value.toLowerCase().includes(needle) ||
         row.dimension_name.toLowerCase().includes(needle)
      );
   });

   return matched.slice(0, args.limit).map((row, index) => ({
      entityId: dimensionValueEntityId(
         row.source_name,
         row.dimension_name,
         row.value,
      ),
      kind: "dimensional_value" as const,
      name: row.value,
      source: row.source_name,
      dimension: row.dimension_name,
      modelPath: row.model_path,
      rank: index + 1,
   }));
}

/**
 * Refresh the package index when it is missing or the served model moved.
 * No-op when the feature is off.
 */
export async function ensureDimensionValueIndex(args: {
   db: DuckDBConnection;
   pkg: Package;
   environmentName: string;
   packageName: string;
}): Promise<DimensionValueIndexMeta | undefined> {
   if (getDimensionValueIndexMode() === "off") {
      return {
         generation: 0,
         status: "off",
         truncatedCount: 0,
         valueCount: 0,
      };
   }
   const servedRevision =
      typeof args.pkg.getServedRevision === "function"
         ? args.pkg.getServedRevision()
         : undefined;
   const meta = await getDimensionValueIndexMeta(
      args.db,
      args.environmentName,
      args.packageName,
   );
   if (
      meta?.status === "ready" &&
      meta.servedRevision === servedRevision
   ) {
      return meta;
   }
   const dimensions = await collectIndexableDimensionsAsync(args.pkg);
   return refreshDimensionValueIndex({
      db: args.db,
      environmentName: args.environmentName,
      packageName: args.packageName,
      servedRevision,
      dimensions,
      fetchValues: fetchDimensionValuesFromPackage(args.pkg),
   });
}
