// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * The dimension-value refine prompt, ported from Credible's
 * retrieval/prompts/get_context_refine_dim_value_entities.txt. The wording is
 * kept as it is so a score here means what a score there means. Unlike the
 * entity refine prompt it shows the model only the phrase, not the whole
 * query, and asks for no reason. Changes go in as a new version, never an edit:
 * the version is part of every cache key.
 */

export const VALUE_REFINE_PROMPT_VERSIONS = ["v1"] as const;

export const VALUE_REFINE_ROLE = `You are an expert at assessing the relevance of string value matches to concepts referenced in a query.`;

const BODY = `Rate how relevant each candidate dimensional value is to a specific phrase from the user's query.

You are evaluating candidate values that were retrieved because they look similar to the phrase. For each candidate, decide whether it is a value the user likely cares about based on the phrase.

PHRASE:
##PHRASE##

CANDIDATE VALUES (each line is \`- [<index>] <value>: <source>.<dimension>\`):
##ENTITIES##

Return a JSON array containing ONLY candidates that have some relevance to the phrase. Omitting a candidate means it is not relevant. If no candidates are relevant, return an empty array [].

Each included object must have exactly two keys: "index" and "score".
"index" must be the integer index from the candidate's line (the number in brackets).
"score" must be one of the strings "LOW", "MEDIUM", or "HIGH" (always quoted).

Schema:
{"index": <int>, "score": "LOW" | "MEDIUM" | "HIGH"}

Score definitions:
- "LOW" — only loosely or indirectly related to the phrase.
- "MEDIUM" — related to the phrase with a reasonable chance of being the value the user wants, but not an exact match.
- "HIGH" — high-confidence match; this is clearly a value the user is asking about.

Example output:
[{"index": 2, "score": "HIGH"}, {"index": 0, "score": "MEDIUM"}]

Rules:
- "index" must exactly match one of the indices listed above.
- Only include candidates that have some relevance to the phrase; omit clearly irrelevant ones.
- Score each candidate independently against the phrase. Do not compare candidates to each other.`;

/** Appended when the endpoint is asked for JSON mode, which forces an object. */
const JSON_MODE_NOTE = `

Because the reply must be a JSON object, wrap the array like this: {"results": [ ... ]}.`;

/** One candidate as the prompt shows it: `- [i] value: source.dimension`. */
export function valueRefineLine(
   index: number,
   c: { value: string; source: string; dimension: string },
): string {
   return `- [${index}] ${c.value}: ${c.source}.${c.dimension}`;
}

export function buildValueRefinePrompt(args: {
   phrase: string;
   lines: string[];
   wrapForJsonMode: boolean;
}): { system: string; user: string } {
   const user = BODY.replace(
      "##PHRASE##",
      () => `Text: "${args.phrase}"`,
   ).replace("##ENTITIES##", () => args.lines.join("\n"));
   return {
      system: VALUE_REFINE_ROLE,
      user: args.wrapForJsonMode ? user + JSON_MODE_NOTE : user,
   };
}
