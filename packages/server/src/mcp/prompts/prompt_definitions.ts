import { z } from "zod";

/**
 * Helper to build fully-qualified prompt IDs combining name + semver.
 */
export const withVersion = (name: string, v: string) => `${name}@${v}`;

/* --------------------------------------------------------------------------
 * Argument Schemas (Zod) – one per prompt
 * ----------------------------------------------------------------------- */

export const ExplainArgsSchema = z
   .object({
      query_code: z.string(),
      model_context_uri: z.string().url().optional(),
   })
   .describe(
      "Explain a Malloy query: purpose, transformations, output structure.",
   );

export const GenerateQueryArgsSchema = z
   .object({
      natural_language_goal: z.string(),
      target_model_uri: z.string().url(),
      data_source_hints: z.string().optional(),
   })
   .describe(
      "Generate a Malloy query from a natural-language goal and model context.",
   );

export const TranslateSqlArgsSchema = z
   .object({
      sql_query: z.string(),
      target_model_uri: z.string().url(),
      sql_dialect: z.string().default("standard_sql"),
   })
   .describe("Translate a SQL query to Malloy using model schema context.");

export const SummarizeModelArgsSchema = z
   .object({
      model_uri: z.string().url(),
   })
   .describe("Summarize a Malloy model's purpose, sources, and joins.");

/* --------------------------------------------------------------------------
 * PromptDefinition
 * ----------------------------------------------------------------------- */

export interface PromptDefinition {
   id: string;
   argsSchema: z.ZodTypeAny;
   template: string; // Handlebars syntax
}

/* --------------------------------------------------------------------------
 * Prompt Templates  (Handlebars strings)
 * ----------------------------------------------------------------------- */

const explainTemplate = `
<instructions>
You are an expert Malloy data analyst.
Explain the following Malloy query. Provide details on:
• Purpose and intended insight
• Key sources involved
• Main transformations / calculations
• Expected result structure (dimensions/measures)
</instructions>

<malloy_query>
{{query_code}}
</malloy_query>

{{#if schema_context}}
<model_schema_context uri="{{model_context_uri}}">
{{schema_context}}
</model_schema_context>
{{/if}}
`;

const generateTemplate = `
<instructions>
You are an expert Malloy modeler and query writer.
Generate a Malloy query that satisfies the goal below using the model at {{target_model_uri}}.

Goal: {{natural_language_goal}}

{{#if data_source_hints}}
Hints to consider: {{data_source_hints}}
{{/if}}

Return ONLY the Malloy query code.
</instructions>

<malloy_model_context uri="{{target_model_uri}}" />
{{#if schema_context}}
<fetched_model_schema_context>
{{schema_context}}
</fetched_model_schema_context>
{{/if}}
`;

const translateTemplate = `
<instructions>
You are fluent in both {{sql_dialect}} SQL and Malloy.
Translate the SQL below into Malloy using the model at {{target_model_uri}} for schema context. Return ONLY Malloy code.
</instructions>

<sql_query dialect="{{sql_dialect}}">{{sql_query}}</sql_query>

<malloy_model_context uri="{{target_model_uri}}" />
{{#if schema_context}}
<fetched_model_schema_context>
{{schema_context}}
</fetched_model_schema_context>
{{/if}}
`;

const summarizeTemplate = `
<instructions>
You are an expert Malloy modeler.
Provide a concise summary of the model at {{model_uri}} covering purpose, key sources, dimensions, measures & joins.
</instructions>

<malloy_model_content uri="{{model_uri}}" />
{{#if model_def_content}}
<fetched_model_definition>
{{model_def_content}}
</fetched_model_definition>
{{/if}}
`;

/* --------------------------------------------------------------------------
 * PROMPTS registry
 * ----------------------------------------------------------------------- */

export const PROMPTS: Record<string, PromptDefinition> = {
   [withVersion("explain-malloy-query", "1.0.0")]: {
      id: withVersion("explain-malloy-query", "1.0.0"),
      argsSchema: ExplainArgsSchema,
      template: explainTemplate,
   },
   [withVersion("generate-malloy-query-from-description", "1.0.0")]: {
      id: withVersion("generate-malloy-query-from-description", "1.0.0"),
      argsSchema: GenerateQueryArgsSchema,
      template: generateTemplate,
   },
   [withVersion("translate-sql-to-malloy", "1.0.0")]: {
      id: withVersion("translate-sql-to-malloy", "1.0.0"),
      argsSchema: TranslateSqlArgsSchema,
      template: translateTemplate,
   },
   [withVersion("summarize-malloy-model", "1.0.0")]: {
      id: withVersion("summarize-malloy-model", "1.0.0"),
      argsSchema: SummarizeModelArgsSchema,
      template: summarizeTemplate,
   },
};

// Backwards-compat alias until the old handlers are removed (step 4).
export const MALLOY_PROMPTS = PROMPTS;
