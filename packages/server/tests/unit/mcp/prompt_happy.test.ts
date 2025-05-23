/* eslint-disable @typescript-eslint/no-explicit-any */
import { describe, it, expect } from "bun:test";
import { promptHandlerMap } from "../../../src/mcp/prompts/handlers";

describe("Prompt Handlers – happy paths", () => {
   const projectStore = undefined; // no ProjectStore needed for minimal tests

   it("explain handler returns text message", async () => {
      const handler = promptHandlerMap["explain-malloy-query@1.0.0"];
      const res = await handler(
         { query_code: "run: t -> { aggregate: c is count() }" },
         projectStore as any,
      );
      expect(res.messages.length).toBe(1);
      expect(res.messages[0].role).toBe("user");
   });

   it("generate handler returns message", async () => {
      const handler =
         promptHandlerMap["generate-malloy-query-from-description@1.0.0"];
      const res = await handler(
         {
            natural_language_goal: "total sales by day",
            target_model_uri: "malloy://project/p/pkg/models/m.malloy",
         },
         projectStore as any,
      );
      expect(res.messages[0].role).toBe("user");
   });

   it("translate handler returns message", async () => {
      const handler = promptHandlerMap["translate-sql-to-malloy@1.0.0"];
      const res = await handler(
         {
            sql_query: "SELECT 1",
            target_model_uri: "malloy://project/p/pkg/models/m.malloy",
         },
         projectStore as any,
      );
      expect(res.messages[0].content.type).toBe("text");
   });

   it("summarize handler returns message", async () => {
      const handler = promptHandlerMap["summarize-malloy-model@1.0.0"];
      const res = await handler(
         { model_uri: "malloy://project/p/pkg/models/m.malloy" },
         projectStore as any,
      );
      expect(res.messages[0].content.type).toBe("text");
   });
});
