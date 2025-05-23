import { describe, it, expect } from "bun:test";
import {
   MALLOY_PROMPTS,
   ExplainArgsSchema,
   GenerateQueryArgsSchema,
   TranslateSqlArgsSchema,
   SummarizeModelArgsSchema,
} from "../../../src/mcp/prompts/prompt_definitions";
import type { z } from "zod";

describe("Prompt Definitions", () => {
   it("should have correctly defined MALLOY_PROMPTS", () => {
      expect(MALLOY_PROMPTS).toBeDefined();
      expect(Object.keys(MALLOY_PROMPTS).length).toBeGreaterThan(0);
   });

   const expectedPrompts: Array<{
      id: string;
      schema: z.ZodTypeAny;
      description: string;
   }> = [
      {
         id: "explain-malloy-query@1.0.0",
         schema: ExplainArgsSchema,
         description:
            "Explain a Malloy query: purpose, transformations, output structure.",
      },
      {
         id: "generate-malloy-query-from-description@1.0.0",
         schema: GenerateQueryArgsSchema,
         description:
            "Generate a Malloy query from a natural-language goal and model context.",
      },
      {
         id: "translate-sql-to-malloy@1.0.0",
         schema: TranslateSqlArgsSchema,
         description:
            "Translate a SQL query to Malloy using model schema context.",
      },
      {
         id: "summarize-malloy-model@1.0.0",
         schema: SummarizeModelArgsSchema,
         description: "Summarize a Malloy model's purpose, sources, and joins.",
      },
   ];

   for (const expected of expectedPrompts) {
      describe(`Prompt: ${expected.id}`, () => {
         const promptDef = MALLOY_PROMPTS[expected.id];

         it("should exist in MALLOY_PROMPTS", () => {
            expect(promptDef).toBeDefined();
         });

         it("should have correct id", () => {
            expect(promptDef.id).toBe(expected.id);
         });

         it("should have the correct argsSchema", () => {
            expect(promptDef.argsSchema).toBe(expected.schema);
         });

         it("should have the correct overall description in argsSchema", () => {
            // Zod stores the description on the _def property for object schemas
            expect(promptDef.argsSchema._def.description).toBe(
               expected.description,
            );
         });

         it("should have a non-empty template", () => {
            expect(promptDef.template).toBeDefined();
            expect(promptDef.template.trim()).not.toBe("");
         });
      });
   }

   // Test that schemas have the expected shape
   describe("Schema shapes", () => {
      it("ExplainArgsSchema should have query_code and model_context_uri", () => {
         expect(ExplainArgsSchema.shape.query_code).toBeDefined();
         expect(ExplainArgsSchema.shape.model_context_uri).toBeDefined();
      });

      it("GenerateQueryArgsSchema should have expected fields", () => {
         expect(
            GenerateQueryArgsSchema.shape.natural_language_goal,
         ).toBeDefined();
         expect(GenerateQueryArgsSchema.shape.target_model_uri).toBeDefined();
         expect(GenerateQueryArgsSchema.shape.data_source_hints).toBeDefined();
      });

      it("TranslateSqlArgsSchema should have expected fields", () => {
         expect(TranslateSqlArgsSchema.shape.sql_query).toBeDefined();
         expect(TranslateSqlArgsSchema.shape.target_model_uri).toBeDefined();
         expect(TranslateSqlArgsSchema.shape.sql_dialect).toBeDefined();
      });

      it("SummarizeModelArgsSchema should have model_uri", () => {
         expect(SummarizeModelArgsSchema.shape.model_uri).toBeDefined();
      });
   });
});
