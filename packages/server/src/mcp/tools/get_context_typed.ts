import { z } from "zod";
import type { Relationship } from "@malloydata/malloy-interfaces";

export const TARGET_TYPES = [
   "source",
   "dimension",
   "measure",
   "view",
   "dimensional_value",
] as const;
export type TargetType = (typeof TARGET_TYPES)[number];

export const searchTargetSchema = z.object({
   target_type: z.enum(TARGET_TYPES),
   search_text: z.string().max(500).nullable().optional(),
});

export const scopeSchema = z.object({
   environment: z.string(),
   package: z.string(),
   version: z.string().nullable().optional(),
   model_path: z.string().nullable().optional(),
   source: z.string().nullable().optional(),
   entity_name: z.string().nullable().optional(),
});

export type SearchTarget = z.infer<typeof searchTargetSchema>;
export type ContextScope = z.infer<typeof scopeSchema>;

export interface ResolvedScope {
   environmentName?: string;
   packageName?: string;
   modelPath?: string;
   sourceName?: string;
   entityName?: string;
}

export interface TypedEntity {
   entityId: string;
   kind: string;
   name: string;
   source: string | undefined;
   environmentName: string;
   packageName: string;
   modelPath: string;
   doc: string;
   rank: number;
   relationship?: Relationship;
   aliases?: string[];
   alsoIn?: string[];
   score?: number;
   dimension?: string;
}

export interface SourceCard {
   name: string;
   modelPath: string;
   doc: string;
   joins: Array<{ name: string; relationship: Relationship; doc?: string }>;
   resource_id: {
      environment: string;
      package: string;
      model_path: string;
      source: string;
   };
   givens?: unknown;
   authorize?: string[];
   score?: number;
   entities?: {
      dimensions?: TypedEntity[];
      measures?: TypedEntity[];
      views?: TypedEntity[];
      dimensional_values?: TypedEntity[];
   };
}

export interface TypedEnvelope {
   ranking: "relevance" | "prominence";
   total_available: number;
   returned: number;
   next_offset?: number;
   sources: SourceCard[];
   targets: Array<{
      target_type: TargetType;
      search_text: string | null;
      results: TypedEntity[];
   }>;
}

export function isTypedRequest(params: {
   search_targets?: SearchTarget[] | undefined;
}): boolean {
   return Array.isArray(params.search_targets);
}

export function kindsForTarget(type: TargetType): string[] {
   if (type === "view") return ["view", "query"];
   if (type === "dimensional_value") return [];
   return [type];
}

export function typedKind(kind: string): string {
   return kind === "query" ? "view" : kind;
}

export function stableEntityId(
   kind: string,
   source: string | undefined,
   name: string,
): string {
   return `${typedKind(kind)}:${source ?? ""}:${name}`;
}

export function resolveScope(params: {
   environmentName?: string;
   packageName?: string;
   sourceName?: string;
   scopes?: ContextScope[];
}): ResolvedScope {
   const scope = params.scopes?.[0];
   return {
      environmentName: scope?.environment ?? params.environmentName,
      packageName: scope?.package ?? params.packageName,
      modelPath: scope?.model_path ?? undefined,
      sourceName: scope?.source ?? params.sourceName,
      entityName: scope?.entity_name ?? undefined,
   };
}

/**
 * Returns an error message when the typed call is invalid, otherwise undefined.
 */
export function validateTypedCall(args: {
   targets: SearchTarget[];
   offset?: number | null;
}): string | undefined {
   const kinds = new Set(args.targets.map((t) => t.target_type));
   const hasSource = kinds.has("source");
   const hasOther = [...kinds].some((k) => k !== "source");
   if (hasSource && hasOther) {
      return "Do not mix source targets with other target types in the same call. Discover sources first, then drill down.";
   }
   const offset = args.offset ?? 0;
   if (offset > 0) {
      const listingOnly =
         args.targets.length > 0 &&
         args.targets.every(
            (t) =>
               t.target_type === "source" &&
               (t.search_text === undefined ||
                  t.search_text === null ||
                  t.search_text.trim() === ""),
         );
      if (!listingOnly) {
         return "offset is only valid on a pure source listing (source targets with no search_text).";
      }
   }
   return undefined;
}

export function isProminenceListing(targets: SearchTarget[]): boolean {
   return (
      targets.length > 0 &&
      targets.every(
         (t) =>
            t.search_text === undefined ||
            t.search_text === null ||
            t.search_text.trim() === "",
      )
   );
}

export function pageSlice<T>(
   items: T[],
   offset: number | undefined,
   limit: number,
): { page: T[]; total: number; nextOffset?: number } {
   const start = offset ?? 0;
   const page = items.slice(start, start + limit);
   const nextOffset =
      start + page.length < items.length ? start + page.length : undefined;
   return { page, total: items.length, nextOffset };
}

export function toTypedEntity(
   row: {
      kind: string;
      name: string;
      source: string | undefined;
      environmentName: string;
      packageName: string;
      modelPath: string;
      doc: string;
      relationship?: Relationship;
      aliases?: string[];
      alsoIn?: string[];
      score?: number;
      dimension?: string;
   },
   rank: number,
): TypedEntity {
   return {
      entityId: stableEntityId(row.kind, row.source, row.name),
      kind: typedKind(row.kind),
      name: row.name,
      source: row.source,
      environmentName: row.environmentName,
      packageName: row.packageName,
      modelPath: row.modelPath,
      doc: row.doc,
      rank,
      ...(row.relationship ? { relationship: row.relationship } : {}),
      ...(row.aliases ? { aliases: row.aliases } : {}),
      ...(row.alsoIn ? { alsoIn: row.alsoIn } : {}),
      ...(row.score !== undefined ? { score: row.score } : {}),
      ...(row.dimension ? { dimension: row.dimension } : {}),
   };
}

export function buildSourceCards(args: {
   entities: TypedEntity[];
   environmentName: string;
   packageName: string;
   sourceContext: Map<
      string,
      {
         name: string;
         modelPath: string;
         doc: string;
         joins: Array<{
            name: string;
            relationship: Relationship;
            doc?: string;
         }>;
      }
   >;
   sourceMeta?: Map<string, { givens?: unknown; authorize?: string[] }>;
}): SourceCard[] {
   const bySource = new Map<string, TypedEntity[]>();
   for (const entity of args.entities) {
      const key = entity.source ?? entity.name;
      const list = bySource.get(key);
      if (list) list.push(entity);
      else bySource.set(key, [entity]);
   }
   const cards: SourceCard[] = [];
   for (const [sourceName, entities] of bySource) {
      const context = args.sourceContext.get(sourceName);
      const meta = args.sourceMeta?.get(sourceName);
      const modelPath = context?.modelPath ?? entities[0]?.modelPath ?? "";
      const dimensions = entities.filter((e) => e.kind === "dimension");
      const measures = entities.filter((e) => e.kind === "measure");
      const views = entities.filter((e) => e.kind === "view");
      const values = entities.filter((e) => e.kind === "dimensional_value");
      const hasEntities =
         dimensions.length + measures.length + views.length + values.length > 0;
      const bestScore = entities
         .map((e) => e.score)
         .filter((s): s is number => typeof s === "number")
         .sort((a, b) => b - a)[0];
      cards.push({
         name: sourceName,
         modelPath,
         doc: context?.doc ?? "",
         joins: context?.joins ?? [],
         resource_id: {
            environment: args.environmentName,
            package: args.packageName,
            model_path: modelPath,
            source: sourceName,
         },
         ...(meta?.givens ? { givens: meta.givens } : {}),
         ...(meta?.authorize ? { authorize: meta.authorize } : {}),
         ...(bestScore !== undefined ? { score: bestScore } : {}),
         ...(hasEntities
            ? {
                 entities: {
                    ...(dimensions.length > 0 ? { dimensions } : {}),
                    ...(measures.length > 0 ? { measures } : {}),
                    ...(views.length > 0 ? { views } : {}),
                    ...(values.length > 0 ? { dimensional_values: values } : {}),
                 },
              }
            : {}),
      });
   }
   return cards;
}

export function buildTypedEnvelope(args: {
   ranking: "relevance" | "prominence";
   sources: SourceCard[];
   targets: TypedEnvelope["targets"];
   offset?: number;
   limit: number;
   pageSources?: boolean;
}): TypedEnvelope {
   const { page, total, nextOffset } = args.pageSources
      ? pageSlice(args.sources, args.offset, args.limit)
      : {
           page: args.sources.slice(0, args.limit),
           total: args.sources.length,
           nextOffset: undefined,
        };
   return {
      ranking: args.ranking,
      total_available: total,
      returned: page.length,
      ...(nextOffset !== undefined ? { next_offset: nextOffset } : {}),
      sources: page,
      targets: args.targets,
   };
}
