// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * Three short questions per field and per view that the node answers, written
 * by the chat model from the node's derived text at index time (the count is
 * `retrieval.fcj.questionsPerNode`, and the wording a variant of
 * prompts/questions.ts). Each question
 * is embedded as its own row (facets `q1` to `q3`), and the best row of a node
 * is its closeness at search time, so a node can be found by how a person
 * asks for it as well as by what it is called.
 *
 * They replace keyphrases, which compress a doc and lose its qualifiers;
 * questions sit beside the doc and lose nothing. The modeller never sees or
 * edits them. Cached by content (see llm_cache.ts), so an unchanged node costs
 * no call on a restart or a reload.
 */

import type { ChatModel } from "../../providers/types";
import type { DuckDBConnection } from "../../storage/duckdb/DuckDBConnection";
import type { SyncRequestBudget } from "../tools/get_context_llm";
import {
   DEFAULT_QUESTIONS_PER_NODE,
   questionsInstructions,
} from "../../prompts/questions";
import type { CatalogNode } from "./catalog";
import {
   generateCached,
   resultsByIndex,
   sha256,
   stringList,
   type CachedRun,
} from "./llm_cache";

/** Rows a node gets unless `retrieval.fcj.questionsPerNode` says otherwise. */
export const QUESTIONS_PER_NODE = DEFAULT_QUESTIONS_PER_NODE;

/** Nodes per LLM call. */
export const QUESTIONS_BATCH_SIZE = 15;

/** The node kinds that get questions. */
export function wantsQuestions(node: CatalogNode): boolean {
   return (
      node.kind === "dimension" ||
      node.kind === "measure" ||
      node.kind === "view" ||
      node.kind === "query"
   );
}

/** The `questions` variant at three per node; prompts/questions.ts has the others. */
export const DEFAULT_QUESTIONS_INSTRUCTIONS =
   questionsInstructions("questions");

function questionsReply(perNode: number): string {
   const slots = Array.from({ length: perNode }, () => '"..."').join(", ");
   return `Reply with one JSON object {"results": [...]} and nothing else. Each element is {"index": <field number>, "questions": [${slots}]}.`;
}

export function renderQuestionsPrompt(
   lines: readonly string[],
   perNode: number = QUESTIONS_PER_NODE,
): string {
   return [
      "<fields>",
      ...lines.map((l, i) => `[${i + 1}] ${l}`),
      "</fields>",
      "",
      questionsReply(perNode),
   ].join("\n");
}

function validateQuestions(
   value: unknown,
   count: number,
   perNode: number,
): Map<number, string[]> {
   const out = new Map<number, string[]>();
   for (const [index, r] of resultsByIndex(value, count)) {
      const qs = stringList(r.questions, perNode).filter(
         (q) => q.split(" ").length <= 25,
      );
      if (qs.length > 0) out.set(index, qs);
   }
   if (out.size === 0 && count > 0) {
      throw new Error("no field in the reply had questions");
   }
   return out;
}

/** Most values a node's line shows (the `values` variant). */
export const QUESTION_LINE_MAX_VALUES = 8;

/**
 * The line a node is shown as: its kind, type and derived text, then its
 * values when the variant uses them and the node has any. With no values the
 * line is what it has always been, so the default variant's rows stay cached.
 */
export function questionLine(
   node: CatalogNode,
   derived: string,
   values: readonly string[] = [],
): string {
   const kind = node.dataType ? `${node.kind}, ${node.dataType}` : node.kind;
   const shown = values.slice(0, QUESTION_LINE_MAX_VALUES);
   return shown.length > 0
      ? `(${kind}) ${derived} Values: ${shown.join(", ")}.`
      : `(${kind}) ${derived}`;
}

export async function resolveQuestions(args: {
   nodes: readonly CatalogNode[];
   derivedOf: (node: CatalogNode) => string;
   /** The values a node's line shows; absent: none (every variant but `values`). */
   valuesOf?: (node: CatalogNode) => readonly string[];
   /** Rows per node; QUESTIONS_PER_NODE when absent. */
   perNode?: number;
   db: DuckDBConnection;
   environmentName: string;
   packageName: string;
   chat: ChatModel;
   instructions: string;
   concurrency: number;
   budget: SyncRequestBudget;
   shouldStop?: () => boolean;
}): Promise<CachedRun<string[]>> {
   const perNode = args.perNode ?? QUESTIONS_PER_NODE;
   const items = args.nodes.filter(wantsQuestions).map((node) => {
      const line = questionLine(
         node,
         args.derivedOf(node),
         args.valuesOf?.(node) ?? [],
      );
      return { key: node.key, inputHash: sha256(line), line };
   });
   return generateCached<string[]>({
      db: args.db,
      table: "node_questions",
      environmentName: args.environmentName,
      packageName: args.packageName,
      items,
      chat: args.chat,
      instructions: args.instructions,
      render: (lines) => renderQuestionsPrompt(lines, perNode),
      validate: (value, count) => validateQuestions(value, count, perNode),
      batchSize: QUESTIONS_BATCH_SIZE,
      concurrency: args.concurrency,
      maxTokensPerItem: 30 * perNode,
      budget: args.budget,
      ...(args.shouldStop ? { shouldStop: args.shouldStop } : {}),
   });
}
