// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

// GENERATED from Credible's indexing/packages/packages/llms/prompts/source_summary.txt by
// the port script; do not edit by hand. The wording is kept exactly because a
// keyphrase or summary here should mean what one there means. A change goes in
// as a new prompt version, never an edit: the version is part of every cache
// key and every recorded run.

export const SUMMARY_PROMPT_VERSIONS = ["v1"] as const;

export const SUMMARY_ROLE = `You are a data modeling expert who writes clear, concise, faithful descriptions of data sources for other language models to understand. Always respond with valid JSON only, no other text.`;

export const SUMMARY_BODY = `Describe the following Malloy data source so that another language model can quickly understand what data it contains and what kinds of questions it can answer.

## Source Name

##SOURCE_NAME##

## Source Annotations / Documentation

##SOURCE_DOCS##

## Serialized Source Metadata (including fields reachable through declared joins)

##ENTITIES##

## Instructions

Produce both a summary and a one-line summary. Every claim in either field must be grounded in facts explicitly present in the source documentation or serialized source metadata above.

Shared faithfulness rules:
- Use only source, field, measure, view, and joined-source names that appear in the input.
- Do not infer semantics from naming conventions alone. In particular, similarly named fields or names ending in \`_id\`, \`_key\`, or \`KEY\` do not establish a relationship.
- State the source's grain ("one row per ...") only when the source documentation explicitly declares it. Never infer grain from fields, measures, views, or joined metadata.
- A joined source serialized in the metadata may be named as content reachable from this source. Do not describe or infer the join relationship, join condition, cardinality, or how records correspond unless those facts are explicitly documented in the input.
- Do not add domain assumptions, likely use cases, or business meaning not stated in the input.

Summary requirements:
- Write a dense, readable reference paragraph in plain prose (no markdown headers, bullet lists, or bold text).
- Explain the documented subject or purpose, important documented usage instructions or caveats, key dimensions, measures, and pre-built views.
- You may mention joined sources and their fields as reachable content, subject to the shared faithfulness rules above.
- Aim for 200-500 tokens when the input contains enough documented detail. Do not pad sparse input with guesses.
- Use backticks around all source, field, measure, and view names.

One-line-summary requirements:
- Exactly one sentence and at most 120 characters.
- State only the source's documented subject or purpose.
- If the input does not explicitly document a subject or purpose, use exactly this grounded fallback based only on the source name: "The \`##SOURCE_NAME##\` source." Do not interpret the source name.
- Never mention joins, relationships, other sources, or grain.
- Do not list fields merely to fill space.

## Output Format

Return a JSON object with exactly these two fields:
- "summary": the summary text
- "one_line_summary": the one-sentence summary`;

export function buildSummaryPrompt(args: {
   sourceName: string;
   sourceDocs: string;
   /** The serialized source and the fields reachable through its joins. */
   entities: string;
}): { system: string; user: string } {
   return {
      system: SUMMARY_ROLE,
      user: SUMMARY_BODY.replaceAll("##SOURCE_NAME##", () => args.sourceName)
         .replaceAll("##SOURCE_DOCS##", () => args.sourceDocs)
         .replaceAll("##ENTITIES##", () => args.entities),
   };
}
