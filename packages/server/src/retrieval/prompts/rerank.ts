// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * The source-rerank prompt, ported from Credible's
 * retrieval/prompts/get_context_rerank_sources.txt with its wording intact.
 * See prompts/refine.ts for why: a change is a new version, never an edit.
 */

export const RERANK_PROMPT_VERSIONS = ["v1"] as const;

export const RERANK_ROLE = `You are an expert at matching natural language queries to data sources. You evaluate how well each data source could address the concepts in a query. You always respond with the requested JSON format.`;

const RERANK_BODY = `You are tasked with ranking data sources by how well they match a natural language query. The query may be a question, statement, or description of data needs.

# Task

Given a natural language query and a list of data sources with their metadata, rank the sources from best to worst match. For each source, assign a relevance score:

- 0: No relevance - the source has no useful fields for the query
- 1: Low relevance - the source has some tangentially related fields
- 2: Medium relevance - the source covers some key concepts but is missing others
- 3: High relevance - the source covers most or all key concepts in the query

# Evaluation Criteria

When evaluating a source, consider:
- Does the source have fields (dimensions/measures) that match the concepts in the query?
- Could this source be used to write a query that addresses the user's intent?
- How completely does the source cover the query's requirements?
- Pay attention to the source description if available - it often indicates the purpose and scope of the data
- If two sources have equally relevant entities, use the source name and description to determine which is more relevant to the query

# Input

## Data Sources

##SOURCES##

## Natural Language Query

##NL_TEXT##

# Output Format

Provide a JSON array where each element has:
- "source": the source name
- "index": the original index number of the source (from the input)
- "score": score for how relevant the source is to the natural language query (0, 1, 2, or 3)

Order the array from best match (highest relevance) to worst match. When sources have the same score, order them by how well they match the natural language query.`;

const JSON_MODE_NOTE = `

Because the reply must be a JSON object, wrap the array like this: {"results": [ ... ]}.`;

export interface RerankEntityLine {
   name: string;
   entityType: string;
   description: string;
   /** Sample values of a value-indexed dimension, best first. */
   values?: string[];
}

export interface RerankSource {
   source: string;
   modelPath: string;
   pkg: string;
   /** The source's own doc, already capped by the caller. */
   docs?: string;
   /** An LLM-written summary, when one exists and may be sent. */
   summary?: string;
   entities: RerankEntityLine[];
}

const flatten = (s: string): string => s.replace(/\s+/g, " ").trim();

/** The `[idx] Source: ...` block for every source, one per record. */
export function formatRerankSources(sources: RerankSource[]): string {
   const lines: string[] = [];
   sources.forEach((s, idx) => {
      lines.push(
         `[${idx}] Source: ${s.source}, Model: ${s.modelPath}, Package: ${s.pkg || "unknown"}`,
      );
      if (s.docs) lines.push(`    Description: ${flatten(s.docs)}`);
      if (s.summary) lines.push(`    Summary: ${flatten(s.summary)}`);
      for (const e of s.entities) {
         let line = `      - ${e.name} (${e.entityType})`;
         if (e.description) line += `: ${flatten(e.description)}`;
         lines.push(line);
         if (e.values && e.values.length > 0) {
            lines.push(`            Values: ${e.values.join(", ")}`);
         }
      }
   });
   return lines.join("\n");
}

export function buildRerankPrompt(args: {
   sources: RerankSource[];
   nlText: string;
   wrapForJsonMode: boolean;
}): { system: string; user: string } {
   const user = RERANK_BODY.replace("##SOURCES##", () =>
      formatRerankSources(args.sources),
   ).replace("##NL_TEXT##", () => args.nlText);
   return {
      system: RERANK_ROLE,
      user: args.wrapForJsonMode ? user + JSON_MODE_NOTE : user,
   };
}
