import { McpServer } from "@modelcontextprotocol/sdk/server/mcp.js";
import { z } from "zod";
import { ProjectStore } from "../../service/project_store";
import { buildMalloyUri } from "../handler_utils";

const listPackagesShape = {
   // projectName is required; other fields mirror SDK expectations
   projectName: z
      .string()
      .describe(
         "Environment name. Names are listed in the malloy_projectList tool.",
      ),
};
type listPackagesParams = z.infer<z.ZodObject<typeof listPackagesShape>>;

const getModelsShape = {
   // projectName is required; other fields mirror SDK expectations
   projectName: z
      .string()
      .describe(
         "Environment name. Names are listed in the malloy_projectList tool.",
      ),
   packageName: z
      .string()
      .describe(
         "Package name. Package names are listed in the listPackages tool.",
      ),
};
type getModelsParams = z.infer<z.ZodObject<typeof getModelsShape>>;

const getModelTextShape = {
   projectName: z
      .string()
      .describe(
         "Environment name. Names are listed in the malloy_projectList tool.",
      ),
   packageName: z
      .string()
      .describe(
         "Package name. Package names are listed in the listPackages tool.",
      ),
   modelPath: z
      .string()
      .describe(
         "Model path. Model paths are listed in the malloy_packageGet tool.",
      ),
};
type getModelTextParams = z.infer<z.ZodObject<typeof getModelTextShape>>;

/**
 * Registers Malloy discovery tools with the MCP server.
 * Lists environments and packages for navigation.
 */
export function registerTools(
   mcpServer: McpServer,
   projectStore: ProjectStore,
): void {
   mcpServer.tool(
      "malloy_projectList",
      "Lists all Malloy environments",
      {},
      async () => {
         console.log(
            "[MCP LOG] Entering ListResources (environment) handler (listing ALL packages)...",
         );
         const allProjects = await Promise.all(
            (await projectStore.listProjects())
               .filter((project) => project.name)
               .map((project) => ({
                  name: project.name,
                  project: projectStore.getProject(project.name!, false),
               })),
         );

         console.log(
            `[MCP LOG] Found ${allProjects.length} environments defined.`,
         );

         const mappedResources = await Promise.all(
            allProjects.map(async (project) => {
               const name = project.name;
               const projectInstance = await project.project;
               const metadata = await projectInstance.reloadProjectMetadata();
               const readme = metadata.readme;
               return {
                  name,
                  type: "project",
                  description: readme || "NO Description available",
               };
            }),
         );
         // console.log(mappedResources);
         console.log(
            `[MCP LOG] ListResources (environment): Returning ${mappedResources.length} package resources.`,
         );
         return {
            content: [
               {
                  type: "resource",
                  resource: {
                     type: "application/json",
                     uri: buildMalloyUri({}, "project"),
                     text: JSON.stringify(mappedResources),
                  },
               },
            ],
         };
      },
   );

   mcpServer.tool(
      "malloy_packageList",
      "Lists all Malloy packages within an environment",
      listPackagesShape,
      async (params: listPackagesParams) => {
         const { projectName } = params;
         console.log(
            "[MCP LOG] Entering ListResources (environment) handler (listing ALL packages)...",
         );
         const project = await projectStore.getProject(projectName, false);
         const packages = await project.listPackages();
         console.log(`[MCP LOG] Found ${packages.length} packages defined.`);
         const mappedResources = packages.map((pkg) => ({
            modelPath: pkg.name,
            type: "package",
            description: pkg.description,
         }));
         console.log(
            `[MCP LOG] ListResources (environment): Returning ${mappedResources.length} package resources.`,
         );
         return {
            content: [
               {
                  type: "resource",
                  resource: {
                     type: "application/json",
                     uri: buildMalloyUri({ project: projectName }, "package"),
                     text: JSON.stringify(mappedResources),
                  },
               },
            ],
         };
      },
   );

   mcpServer.tool(
      "malloy_packageGet",
      "Lists resources within a package",
      getModelsShape,
      async (params: getModelsParams) => {
         const { projectName, packageName } = params;
         console.log(
            "[MCP LOG] Entering GetResources (environment) handler (listing ALL packages)...",
         );
         const project = await projectStore.getProject(projectName, false);
         const pkg = await project.getPackage(packageName, false);
         const models = await pkg.listModels();
         console.log(`[MCP LOG] Found ${models.length} models defined.`);
         const mappedResources = models.map((model) => ({
            name: model.path,
            type: "model",
         }));
         console.log(
            `[MCP LOG] ListResources (environment): Returning ${mappedResources.length} package resources.`,
         );
         return {
            content: [
               {
                  type: "resource",
                  resource: {
                     type: "application/json",
                     uri: buildMalloyUri(
                        { project: projectName, package: packageName },
                        "model",
                     ),
                     text: JSON.stringify(mappedResources),
                  },
               },
            ],
         };
      },
   );

   mcpServer.tool(
      "malloy_modelGetText",
      "Gets the raw text content of a model file",
      getModelTextShape,
      async (params: getModelTextParams) => {
         const { projectName, packageName, modelPath } = params;
         console.log(
            `[MCP LOG] Entering GetModelText handler for ${projectName}/${packageName}/${modelPath}...`,
         );

         try {
            const project = await projectStore.getProject(projectName, false);
            const pkg = await project.getPackage(packageName, false);
            const model = pkg.getModel(modelPath);

            if (!model) {
               console.log(
                  "model not found",
                  modelPath,
                  "in ",
                  pkg.listModels(),
               );
               throw new Error(`Model not found: ${modelPath}`);
            }

            // Use the new getModelFileText method
            const fileText = await pkg.getModelFileText(modelPath);

            console.log(
               `[MCP LOG] Successfully retrieved model text for ${modelPath}`,
            );
            return {
               content: [
                  {
                     type: "resource",
                     resource: {
                        type: "text/plain",
                        uri: buildMalloyUri(
                           {
                              project: projectName,
                              package: packageName,
                              model: modelPath,
                           },
                           "model-text",
                        ),
                        text: fileText,
                     },
                  },
               ],
            };
         } catch (error) {
            console.error(`[MCP LOG] Error retrieving model text: ${error}`);
            const errorMessage =
               error instanceof Error ? error.message : "Unknown error";
            return {
               content: [
                  {
                     type: "resource",
                     resource: {
                        type: "text/plain",
                        uri: buildMalloyUri(
                           {
                              project: projectName,
                              package: packageName,
                              model: modelPath,
                           },
                           "model-text",
                        ),
                        text: `Error: ${errorMessage}`,
                     },
                  },
               ],
            };
         }
      },
   );
}
