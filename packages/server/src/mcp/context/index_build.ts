// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * Stage 0 of find, connect, judge: build everything the request-time stages
 * search, once per package load.
 *
 *   catalog -> doc parts -> derived text -> value index -> questions ->
 *   vectors -> keyword index
 *
 * The build runs in the same process-wide queue as the shipped embedding
 * sync (one provider, one rate limit) and its result is held per Package
 * instance, so a reload builds a new one and the old one is dropped with its
 * Package. Every step that calls a model is cached by content, so a reload
 * that changed nothing costs no model call and no embedding.
 *
 * A step that needs the chat model and cannot get it (none configured, or its
 * calls fail) is skipped and named in the build report; the index is still
 * built and still answers. Only an embedding failure fails the build, because
 * without vectors the closeness ranking the find stage leans on is gone.
 */

import type { ChatModel, EmbeddingModel } from "../../providers/types";
import type { FcjSettings } from "../../service/package_retrieval";
import type { EnvironmentStore } from "../../service/environment_store";
import type { DuckDBConnection } from "../../storage/duckdb/DuckDBConnection";
import { logger } from "../../logger";
import { humanizeName } from "../tools/embedding_index";
import { SyncRequestBudget } from "../tools/get_context_llm";
import type { PackageIndex } from "../tools/get_context_tool";
import type { StoredSourceSummary } from "../tools/source_summaries";
import { summaryKey } from "../tools/source_summaries";
import {
   buildCatalog,
   catalogInputOf,
   type Catalog,
   type CatalogNode,
   type CatalogReport,
} from "./catalog";
import { catalogReportLine, saveCatalog } from "./catalog_store";
import { derivedText } from "./derived_text";
import {
   DEFAULT_DOC_PARTS_INSTRUCTIONS,
   partsOf,
   resolveDocParts,
   type DocParts,
} from "./doc_parts";
import { KeywordIndex } from "./keyword";
import {
   promptShowsValues,
   questionsInstructions,
} from "../../prompts/questions";
import {
   QUESTION_LINE_MAX_VALUES,
   QUESTIONS_PER_NODE,
   resolveQuestions,
   wantsQuestions,
} from "./questions";
import {
   buildValueIndex,
   ValueIndex,
   type IndexValueFetcher,
   type ValueBuildReport,
} from "./values_index";
import { syncVectors, type Facet, type VectorRow } from "./vectors";

/** What the build did, for the load log and the index-status endpoint. */
export interface FcjBuildReport {
   catalog: CatalogReport;
   summaries: { stored: number; sources: number };
   docParts: {
      longDocs: number;
      byModel: number;
      byRules: number;
      skipped?: string;
      failure?: string;
   };
   questions: {
      nodes: number;
      written: number;
      reused: number;
      missing: number;
      skipped?: string;
      failure?: string;
   };
   vectors: {
      rows: number;
      embedded: number;
      reused: number;
      deleted: number;
      charsEmbedded: number;
      dims: number;
      skipped?: string;
   };
   keywordDocs: number;
   values: ValueBuildReport | { skipped: string };
   ms: number;
}

export interface FcjIndex {
   catalog: Catalog;
   /** Derived search text by node key. */
   derived: Map<string, string>;
   /** Doc parts by node key. */
   parts: Map<string, DocParts>;
   /** Generated questions by node key; absent nodes have none. */
   questions: Map<string, string[]>;
   /** Embedding rows by node key, unit length. Empty with no provider. */
   vectors: Map<string, Facet[]>;
   keyword: KeywordIndex;
   /** Stored values of `#(index)` dimensions and doc value lists. */
   values: ValueIndex;
   /** One-line summary by source key, when stored. */
   summaries: Map<string, string>;
   /** The provider the vectors were made with; null when there is none. */
   provider: EmbeddingModel | null;
   report: FcjBuildReport;
}

/** The model-facing steps, resolved by the caller from the package and the operator's LLM. */
export interface FcjLlm {
   chat: ChatModel;
   concurrency: number;
   maxCallsPerSync: number;
   docPartsInstructions?: string;
   questionsInstructions?: string;
}

/**
 * The values each dimension's question line shows (the `values` wording): the
 * list its doc gives, then its most frequent stored values, distinct, at most
 * QUESTION_LINE_MAX_VALUES. Dimensions with neither are absent.
 */
export function questionLineValues(
   index: ValueIndex,
   parts: ReadonlyMap<string, DocParts>,
): Map<string, string[]> {
   const stored = new Map<string, Array<{ value: string; count: number }>>();
   for (const v of index.values) {
      if (v.origin !== "data") continue;
      const list = stored.get(v.dimension) ?? [];
      list.push({ value: v.value, count: v.count });
      stored.set(v.dimension, list);
   }
   const out = new Map<string, string[]>();
   for (const key of new Set([...parts.keys(), ...stored.keys()])) {
      const fromData = (stored.get(key) ?? [])
         .sort((a, b) => b.count - a.count || (a.value < b.value ? -1 : 1))
         .map((v) => v.value);
      const seen = new Set<string>();
      const list: string[] = [];
      for (const v of [...(parts.get(key)?.values ?? []), ...fromData]) {
         const k = v.trim().toLowerCase();
         if (!k || seen.has(k)) continue;
         seen.add(k);
         list.push(v.trim());
         if (list.length >= QUESTION_LINE_MAX_VALUES) break;
      }
      if (list.length > 0) out.set(key, list);
   }
   return out;
}

/** The text a node's `values` facet embeds, from the list its doc gives. */
export function valuesFacetText(node: CatalogNode, values: readonly string[]) {
   return `${humanizeName(node.name) || node.name} values: ${values.join(", ")}`;
}

export async function buildFcjIndex(args: {
   pkgIndex: PackageIndex;
   db: DuckDBConnection;
   provider: EmbeddingModel | null;
   environmentName: string;
   packageName: string;
   settings: FcjSettings;
   llm?: FcjLlm;
   /** Stored source summaries by summaryKey(modelPath, source); see source_summaries.ts. */
   storedSummaries?: ReadonlyMap<string, StoredSourceSummary>;
   /** Where `#(index)` dimensions are read from; absent: doc value lists only. */
   valueRead?: {
      environmentStore: EnvironmentStore;
      fetcher?: IndexValueFetcher;
   };
   shouldStop?: () => boolean;
}): Promise<FcjIndex> {
   const started = Date.now();
   const { db, environmentName, packageName, settings } = args;
   const catalog = buildCatalog({
      ...(await catalogInputOf(args.pkgIndex)),
      collapseCopies: settings.collapseCopies,
      joinedCopies: settings.joinedCopies ?? "indexed",
      unservedCopies: settings.unservedCopies ?? 0,
   });
   // A folded copy is never searched, so it gets no row and no keyword text.
   const searched = [...catalog.nodes.values()].filter((n) => !n.copyOf);

   // Summaries: any model path of the source may carry the stored row.
   const summaries = new Map<string, string>();
   for (const source of catalog.sources.values()) {
      for (const modelPath of source.modelPaths) {
         const stored = args.storedSummaries?.get(
            summaryKey(modelPath, source.name),
         );
         if (stored?.oneLineSummary) {
            summaries.set(source.key, stored.oneLineSummary);
            break;
         }
      }
   }

   const budget = new SyncRequestBudget(args.llm?.maxCallsPerSync ?? 0);
   const llmFor = (enabled: FcjSettings["docParts"]) =>
      enabled !== false && args.llm ? args.llm : undefined;

   // Doc parts.
   const docLlm = llmFor(settings.docParts);
   const docOutcome = await resolveDocParts({
      docs: [...catalog.nodes.values()].map((n) => n.doc).filter(Boolean),
      ...(docLlm
         ? {
              llm: {
                 db,
                 environmentName,
                 packageName,
                 chat: docLlm.chat,
                 instructions:
                    docLlm.docPartsInstructions ??
                    DEFAULT_DOC_PARTS_INSTRUCTIONS,
                 concurrency: docLlm.concurrency,
                 budget,
                 ...(args.shouldStop ? { shouldStop: args.shouldStop } : {}),
              },
           }
         : {}),
   });
   const parts = new Map<string, DocParts>();
   for (const node of catalog.nodes.values()) {
      parts.set(node.key, partsOf(docOutcome.parts, node.doc));
   }

   // Derived text.
   const textCtx = {
      catalog,
      whatOf: (n: CatalogNode) => parts.get(n.key)?.what ?? n.doc,
      summaryOf: (k: string) => summaries.get(k),
   };
   const derived = new Map<string, string>();
   for (const node of catalog.nodes.values()) {
      derived.set(node.key, derivedText(node, textCtx));
   }

   // Value index. Before the questions, because the `values` wording shows a
   // field's stored values on its line.
   let values = new ValueIndex([]);
   let valueReport: FcjBuildReport["values"] = {
      skipped: "retrieval.fcj.values is false",
   };
   if (settings.values) {
      const built = await buildValueIndex({
         catalog,
         parts,
         cap: settings.valueCap,
         db,
         ...(args.valueRead
            ? {
                 read: {
                    environmentStore: args.valueRead.environmentStore,
                    environmentName,
                    packageName,
                    ...(args.valueRead.fetcher
                       ? { fetcher: args.valueRead.fetcher }
                       : {}),
                 },
              }
            : {}),
         ...(args.shouldStop ? { shouldStop: args.shouldStop } : {}),
      });
      values = built.index;
      valueReport = built.report;
   }

   // Questions.
   const perNode = settings.questionsPerNode ?? QUESTIONS_PER_NODE;
   const variant = settings.questionsPrompt ?? "questions";
   const lineValues = promptShowsValues(variant)
      ? questionLineValues(values, parts)
      : undefined;
   const questions = new Map<string, string[]>();
   const qLlm = llmFor(settings.questions);
   const questionNodes = searched.filter(wantsQuestions);
   let questionReport: FcjBuildReport["questions"] = {
      nodes: questionNodes.length,
      written: 0,
      reused: 0,
      missing: questionNodes.length,
      ...(qLlm
         ? {}
         : {
              skipped:
                 settings.questions === false
                    ? "retrieval.fcj.questions is false"
                    : "no LLM is configured",
           }),
   };
   if (qLlm && questionNodes.length > 0) {
      const run = await resolveQuestions({
         nodes: questionNodes,
         derivedOf: (n) => derived.get(n.key) ?? "",
         ...(lineValues
            ? { valuesOf: (n: CatalogNode) => lineValues.get(n.key) ?? [] }
            : {}),
         perNode,
         db,
         environmentName,
         packageName,
         chat: qLlm.chat,
         // A package prompt file wins over the variant setting.
         instructions:
            qLlm.questionsInstructions ??
            questionsInstructions(variant, perNode),
         concurrency: qLlm.concurrency,
         budget,
         ...(args.shouldStop ? { shouldStop: args.shouldStop } : {}),
      });
      for (const [k, v] of run.output) questions.set(k, v);
      questionReport = {
         nodes: questionNodes.length,
         written: run.generated,
         reused: run.reused,
         missing: run.missing,
         ...(run.failure ? { failure: run.failure } : {}),
         ...(run.capped
            ? { skipped: "the per-sync LLM call limit ran out" }
            : {}),
      };
   }

   // Vectors.
   const rows: VectorRow[] = [];
   for (const node of searched) {
      rows.push({
         node: node.key,
         facet: "derived",
         text: derived.get(node.key)!,
      });
      (questions.get(node.key) ?? [])
         .slice(0, perNode)
         .forEach((q, i) =>
            rows.push({ node: node.key, facet: `q${i + 1}`, text: q }),
         );
      const values = parts.get(node.key)?.values ?? [];
      if (values.length > 0) {
         rows.push({
            node: node.key,
            facet: "values",
            text: valuesFacetText(node, values),
         });
      }
   }
   let vectors = new Map<string, Facet[]>();
   let vectorReport: FcjBuildReport["vectors"] = {
      rows: rows.length,
      embedded: 0,
      reused: 0,
      deleted: 0,
      charsEmbedded: 0,
      dims: 0,
      ...(args.provider
         ? {}
         : { skipped: "no embedding provider is configured" }),
   };
   if (args.provider) {
      const out = await syncVectors({
         db,
         provider: args.provider,
         environmentName,
         packageName,
         rows,
         ...(args.shouldStop ? { shouldStop: args.shouldStop } : {}),
      });
      vectors = out.vectors;
      vectorReport = {
         rows: out.rows,
         embedded: out.embedded,
         reused: out.reused,
         deleted: out.deleted,
         charsEmbedded: out.charsEmbedded,
         dims: out.dims,
      };
   }

   // Keyword index, over the same texts.
   const keyword = new KeywordIndex(
      searched.map((node) => ({
         key: node.key,
         text: [
            derived.get(node.key) ?? "",
            ...(questions.get(node.key) ?? []),
            ...(parts.get(node.key)?.values ?? []),
         ].join(" "),
      })),
   );

   try {
      await saveCatalog(
         db,
         environmentName,
         packageName,
         catalog,
         (k) => derived.get(k) ?? "",
      );
   } catch (error) {
      logger.warn(
         "[get_context fcj] The catalog could not be written to its tables",
         {
            environmentName,
            packageName,
            error: error instanceof Error ? error.message : String(error),
         },
      );
   }

   const report: FcjBuildReport = {
      catalog: catalog.report,
      summaries: { stored: summaries.size, sources: catalog.sources.size },
      docParts: {
         longDocs: docOutcome.longDocs,
         byModel: docOutcome.byModel,
         byRules: docOutcome.byRules,
         ...(docLlm
            ? {}
            : {
                 skipped:
                    settings.docParts === false
                       ? "retrieval.fcj.docParts is false: split by rules"
                       : "no LLM is configured: split by rules",
              }),
         ...(docOutcome.run?.failure
            ? { failure: docOutcome.run.failure }
            : {}),
      },
      questions: questionReport,
      vectors: vectorReport,
      keywordDocs: keyword.size,
      values: valueReport,
      ms: Date.now() - started,
   };
   logger.info("[get_context fcj] Built the search index", {
      environmentName,
      packageName,
      catalog: catalogReportLine(catalog.report),
      report,
   });
   return {
      catalog,
      derived,
      parts,
      questions,
      vectors,
      keyword,
      values,
      summaries,
      provider: args.provider,
      report,
   };
}
