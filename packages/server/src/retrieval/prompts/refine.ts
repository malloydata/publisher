// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * The entity-refine prompt, ported from Credible's
 * retrieval/prompts/get_context_refine_entities.txt. The wording is kept as it
 * is on purpose: the point of the port is that a score here means what a score
 * there means, and the prompt is most of what a score is. Changes go in as a
 * new version, never an edit, because the version is part of every cache key
 * and every recorded run.
 */

export const REFINE_PROMPT_VERSIONS = ["v1"] as const;

export const REFINE_ROLE = `You are an expert at evaluating how well database entities (dimensions, measures, views) match a specific search phrase. You always respond with the requested JSON format.`;

const REFINE_BODY = `Rate how well each candidate entity matches a specific search phrase. Score strictly against the PHRASE; use the QUERY only as background context for the agent's overall intent (it's a concatenation of all phrases the agent is searching in parallel).

QUERY:
##QUERY##

PHRASE:
##PHRASE##

CANDIDATE ENTITIES (each line is \`- [<index>] <name> (<entity_type> / <data_type>, source: <source>): <description>\`):
##ENTITIES##

Return a JSON array containing ONLY entities that have some relevance to the phrase. Skip any entity that is clearly not relevant — omitting it from the array means it is not relevant. If no entities are relevant, return an empty array [].

Each included object must have exactly these three keys in this order: "index", "score", "reason".
The value of "index" must be the integer shown in square brackets at the start of the candidate's line.
The value of "score" must be one of the strings "LOW", "MEDIUM", or "HIGH" (always quoted; never used as a key).

Schema:
  {"index": <int>, "score": "LOW" | "MEDIUM" | "HIGH", "reason": "<one sentence>"}

Score definitions:
- "LOW" — only loosely or indirectly related to the phrase. When in doubt about relevance, prefer "LOW" to maintain high recall.
- "MEDIUM" — related to the phrase with a reasonable chance of being useful, but not an obvious or direct match.
- "HIGH" — high-confidence match. The entity is clearly and directly relevant.

Example output (for a phrase like "top-performing drama shows"):
[{"index": 3, "score": "HIGH", "reason": "Measures total viewership per program, directly answering which shows are top-performing."}, {"index": 0, "score": "LOW", "reason": "Program duration could loosely relate to engagement."}]

Rules:
- "index" must be an integer that appears in square brackets at the start of one of the candidate lines above.
- "reason" must be exactly one sentence.
- In the reason, prefer saying "this dimension/measure/view" instead of "this entity".
- Do not include ID columns/dimensions unless the user explicitly asked for them.
- Only skip entities that have absolutely NO relevance to the phrase; use LOW or MEDIUM for somewhat relevant entities.
- Only use HIGH if the entity EXACTLY matches what is needed.
- Score each entity independently against the phrase according to the above criteria. Do not compare entities to each other or adjust scores based on what else is in the list.`;

/** Appended when the endpoint is asked for JSON mode, which forces an object. */
const JSON_MODE_NOTE = `

Because the reply must be a JSON object, wrap the array like this: {"results": [ ... ]}.`;

/** One candidate as the prompt shows it. */
export interface RefineLine {
   name: string;
   /** dimension | measure | view | query | join | source */
   entityType: string;
   dataType?: string;
   source: string;
   description: string;
}

const flatten = (s: string): string => s.replace(/\s+/g, " ").trim();

/** `- [i] name (type / dtype, source: S): description`, on one line. */
export function refineLine(index: number, e: RefineLine): string {
   const typePart = e.dataType ? `${e.entityType} / ${e.dataType}` : e.entityType;
   return `- [${index}] ${e.name} (${typePart}, source: ${e.source}): ${flatten(e.description)}`;
}

export function buildRefinePrompt(args: {
   query: string;
   phrase: string;
   lines: string[];
   wrapForJsonMode: boolean;
}): { system: string; user: string } {
   const user = REFINE_BODY.replace("##QUERY##", () => args.query)
      .replace("##PHRASE##", () => `Text: "${args.phrase}"`)
      .replace("##ENTITIES##", () => args.lines.join("\n"));
   return {
      system: REFINE_ROLE,
      user: args.wrapForJsonMode ? user + JSON_MODE_NOTE : user,
   };
}

/** Appended to the prompt for the single repair retry. */
export const REPAIR_NOTE = `

Your previous reply could not be used. Reply with ONLY the JSON, nothing else: no prose, no code fence.`;
