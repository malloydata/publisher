import * as Handlebars from "handlebars";
import { z } from "zod";
import {
   PromptMessageSchema,
   GetPromptResultSchema,
} from "@modelcontextprotocol/sdk/types.js";
import { PROMPTS } from "./prompt_definitions";
import { getCompiledModel } from "./utils";
import { ProjectStore } from "../../service/project_store";

/* eslint-disable @typescript-eslint/no-explicit-any */

/* --------------------------------------------------------------------------
 * Helper – create a simple text prompt message
 * ----------------------------------------------------------------------- */

const textMsg = (text: string): z.infer<typeof PromptMessageSchema> => ({
   role: "user",
   content: { type: "text", text },
});

/* --------------------------------------------------------------------------
 * makePromptHandler – higher-order factory
 * ----------------------------------------------------------------------- */

export function makePromptHandler(id: string) {
   const def = PROMPTS[id];
   if (!def) throw new Error(`Unknown prompt id: ${id}`);

   // pre-compile Handlebars template
   const templateFn = Handlebars.compile(def.template, { noEscape: true });

   // eslint-disable-next-line @typescript-eslint/no-explicit-any
   return async (params: any, ps: ProjectStore) => {
      const data: Record<string, unknown> = { ...params };

      // Dynamically add model content / schema context if the args include URIs
      if (ps) {
         if (typeof params?.model_uri === "string") {
            const compiled = await getCompiledModel(params.model_uri, ps);
            // eslint-disable-next-line @typescript-eslint/no-explicit-any
            (data as any).model_def_content = (compiled as any).modelDef;
         }
         if (typeof params?.target_model_uri === "string") {
            const compiled = await getCompiledModel(
               params.target_model_uri,
               ps,
            );
            // eslint-disable-next-line @typescript-eslint/no-explicit-any
            (data as any).schema_context = (compiled as any).modelInfo;
         }
         if (typeof params?.model_context_uri === "string") {
            const compiled = await getCompiledModel(
               params.model_context_uri,
               ps,
            );
            // eslint-disable-next-line @typescript-eslint/no-explicit-any
            (data as any).schema_context = (compiled as any).modelInfo;
         }
      }

      const rendered = templateFn(data);

      return {
         messages: [textMsg(rendered)],
         // Ensure shape matches SDK schema
      } as z.infer<typeof GetPromptResultSchema>;
   };
}

/* --------------------------------------------------------------------------
 * Registry – id -> handler
 * ----------------------------------------------------------------------- */

export const promptHandlerMap = Object.fromEntries(
   Object.keys(PROMPTS).map((id) => [id, makePromptHandler(id)]),
) as Record<
   string,
   // eslint-disable-next-line @typescript-eslint/no-explicit-any
   (
      params: any,
      ps: ProjectStore,
   ) => Promise<z.infer<typeof GetPromptResultSchema>>
>;
