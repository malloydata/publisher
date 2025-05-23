import { McpServer } from "@modelcontextprotocol/sdk/server/mcp.js";
import { ProjectStore } from "../../service/project_store";
import { MALLOY_PROMPTS } from "./prompt_definitions";
import { promptHandlerMap } from "./handlers";
import { z } from "zod";

/**
 * Registers all defined Malloy prompts with the MCP server.
 *
 * @param mcpServer The McpServer instance.
 * @param projectStore The ProjectStore instance for handlers to access project data.
 */
export function registerPromptCapability(
   mcpServer: McpServer,
   projectStore: ProjectStore,
): void {
   console.log("[MCP Init] Registering prompt capability...");
   const startTime = Date.now();
   let registeredCount = 0;

   for (const promptKey in MALLOY_PROMPTS) {
      const promptDefinition = MALLOY_PROMPTS[promptKey];
      const handler = promptHandlerMap[promptDefinition.id];

      if (handler && promptDefinition.argsSchema instanceof z.ZodObject) {
         // Prepare a handler that injects the projectStore
         const preparedHandler = (
            args: z.infer<typeof promptDefinition.argsSchema>,
         ) => handler(args, projectStore);

         // Register using prompt ID, the Zod schema shape, and the prepared handler.
         mcpServer.prompt(
            promptDefinition.id,
            (
               promptDefinition.argsSchema as z.ZodObject<
                  z.ZodRawShape,
                  z.UnknownKeysParam,
                  z.ZodTypeAny,
                  object,
                  object
               >
            ).shape,
            preparedHandler,
         );

         registeredCount++;
         console.log(
            `[MCP Prompt Reg] Registered prompt: ${promptDefinition.id}`,
         );
      } else if (!handler) {
         console.warn(
            `Handler not found for prompt: ${promptDefinition.id}. It will not be registered.`,
         );
      } else if (!(promptDefinition.argsSchema instanceof z.ZodObject)) {
         console.warn(
            `Prompt definition for ${promptDefinition.id} does not have a ZodObject schema. It will not be registered.`,
         );
      }
   }

   const endTime = Date.now();
   console.log(
      `[MCP Init] Finished registering prompts. Registered: ${registeredCount}. Duration: ${
         endTime - startTime
      }ms`,
   );
}
