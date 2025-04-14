import {
    McpServer,
    ResourceTemplate
} from "@modelcontextprotocol/sdk/server/mcp.js";
import { z } from "zod";
import { PackageService } from "../service/package.service";
import { PackageNotFoundError, ModelNotFoundError } from "../errors";
import { MCP_ERROR_MESSAGES } from './mcp_constants';

export const testServerInfo = {
  name: "malloy-publisher-mcp-server",
  version: "0.0.1",
  displayName: "Malloy Publisher MCP Server",
  description: "Provides access to Malloy models and query execution via MCP.",
};

/**
 * Initializes and returns the MCP Server instance.
 * @param packageService Instance of PackageService
 */
export function initializeMcpServer(packageService: PackageService): McpServer {
    const mcpServer = new McpServer(testServerInfo);

    // Register Project Resource
    mcpServer.resource(
        'project',
        new ResourceTemplate('malloy://project/{projectName}', {
            list: async () => ({
                resources: [{
                    uri: 'malloy://project/home',
                    name: 'home',
                    description: 'Home project containing Malloy packages'
                }]
            })
        }),
        async (uri, { projectName }) => ({
            contents: [{
                type: 'text',
                uri: uri.href,
                text: `Project: ${projectName}`
            }]
        })
    );

    // Register Package Resource
    mcpServer.resource(
        'package',
        new ResourceTemplate('malloy://project/{projectName}/package/{packageName}', {
            list: async () => {
                 console.log("[MCP Server Debug] Entering package list callback...");
                const packages = await packageService.listPackages();
                console.log(`[MCP Server Debug] packageService.listPackages returned ${packages.length} packages.`);
                const mappedResources = packages.map(pkg => ({
                    uri: `malloy://project/home/package/${pkg.name || 'unknown'}`,
                    name: pkg.name || 'unknown',
                    description: `Package: ${pkg.name || 'unknown'}`
                }));
                console.log("[MCP Server Debug] Finished mapping packages.");
                return {
                    resources: mappedResources
                };
            }
        }),
        async (uri, { projectName, packageName }) => {
            try {
                const pkg = await packageService.getPackage(packageName as string);
                return {
                    contents: [{
                        type: 'text',
                        uri: uri.href,
                        text: `Package: ${packageName}`
                    }]
                };
            } catch (error) {
                if (error instanceof PackageNotFoundError) {
                    return {
                        isError: true,
                        contents: [{
                            type: 'text',
                            uri: uri.href,
                            text: MCP_ERROR_MESSAGES.PACKAGE_NOT_FOUND(packageName as string)
                        }]
                    };
                }
                throw error;
            }
        }
    );

    // 1. Resource for READING a SPECIFIC model
    mcpServer.resource(
        'model',
        new ResourceTemplate('malloy://project/{projectName}/package/{packageName}/models/{modelPath}', { list: undefined }),
        async (uri, { projectName, packageName, modelPath }) => {
            try {
                console.log(`[MCP Server Debug] Reading specific model: project=${projectName}, package=${packageName}, model=${modelPath}`);
                const pkg = await packageService.getPackage(packageName as string);
                const modelInstance = pkg.getModel(modelPath as string);
                if (!modelInstance) {
                     console.log(`[MCP Server Debug] Model not found: ${modelPath}`);
                    return {
                        isError: true,
                        contents: [{
                            type: 'application/json',
                            uri: uri.href,
                            text: JSON.stringify({ error: MCP_ERROR_MESSAGES.MODEL_NOT_FOUND(packageName as string, modelPath as string) })
                        }]
                    };
                }

                const compiledModel = await modelInstance.getModel();
                console.log(`[MCP Server Debug] Successfully retrieved compiled model for: ${modelPath}`);

                return {
                    contents: [{
                        type: 'application/json',
                        uri: uri.href,
                        text: JSON.stringify(compiledModel)
                    }]
                };
            } catch (error) {
                 console.error(`[MCP Server Error] Error reading model ${modelPath} in package ${packageName}:`, error);
                if (error instanceof PackageNotFoundError) {
                    return {
                        isError: true,
                        contents: [{
                            type: 'application/json',
                            uri: uri.href,
                            text: JSON.stringify({ error: MCP_ERROR_MESSAGES.PACKAGE_NOT_FOUND(packageName as string) })
                        }]
                    };
                }
                throw error;
            }
        }
    );

    // 2. NEW Resource for LISTING models within a package (using READ)
    mcpServer.resource(
        'packageModels',
        new ResourceTemplate('malloy://project/{projectName}/package/{packageName}/models', { list: undefined }),
        async (uri, { projectName, packageName }) => {
            try {
                console.log(`[MCP Server Debug] Listing models for package: project=${projectName}, package=${packageName}`);
                const pkg = await packageService.getPackage(packageName as string);
                const models = await pkg.listModels();
                console.log(`[MCP Server Debug] Found ${models.length} models in package ${packageName}`);

                const modelContents = models.map(model => ({
                    type: 'application/json',
                    uri: `malloy://project/${projectName}/package/${packageName}/models/${model.path || ''}`,
                    text: JSON.stringify({
                       path: model.path,
                       type: model.type,
                       name: model.path || 'Unnamed Model',
                       description: `Malloy ${model.type || 'model'} file: ${model.path || ''}`
                    })
                }));

                return {
                    contents: modelContents
                };
            } catch (error) {
                console.error(`[MCP Server Error] Error listing models for package ${packageName}:`, error);
                if (error instanceof PackageNotFoundError) {
                     console.log(`[MCP Server Debug] Package not found while listing models: ${packageName}`);
                    return {
                        isError: true,
                        contents: [{
                            type: 'application/json',
                            uri: uri.href,
                            text: JSON.stringify({ error: MCP_ERROR_MESSAGES.PACKAGE_NOT_FOUND(packageName as string) })
                        }]
                    };
                }
                throw error;
            }
        }
    );

    // Register malloy/executeQuery Tool
    mcpServer.tool(
        'malloy/executeQuery',
        {
            projectName: z.string().default("home"),
            packageName: z.string(),
            modelPath: z.string(),
            query: z.string().optional(),
            sourceName: z.string().optional(),
            queryName: z.string().optional()
        },
        async ({ packageName, modelPath, query, sourceName, queryName }) => {
            try {
                const pkg = await packageService.getPackage(packageName);
                const model = pkg.getModel(modelPath);
                if (!model) {
                    return {
                        isError: true,
                        content: [{
                            type: 'text',
                            text: MCP_ERROR_MESSAGES.MODEL_NOT_FOUND(packageName, modelPath)
                        }]
                    };
                }

                try {
                    const { queryResults } = await model.getQueryResults(sourceName, queryName, query);
                    return {
                        content: [{
                            type: 'text',
                            text: JSON.stringify(queryResults)
                        }]
                    };
                } catch (queryError) {
                    // Handle query execution errors (syntax errors, invalid queries, etc.) as application errors
                    return {
                        isError: true,
                        content: [{
                            type: 'text',
                            text: MCP_ERROR_MESSAGES.COMPILATION_ERROR(queryError instanceof Error ? queryError.message : 'Invalid query')
                        }]
                    };
                }
            } catch (error) {
                if (error instanceof PackageNotFoundError) {
                    return {
                        isError: true,
                        content: [{
                            type: 'text',
                            text: MCP_ERROR_MESSAGES.PACKAGE_NOT_FOUND(packageName)
                        }]
                    };
                }
                // Rethrow unexpected errors to let MCP framework handle it
                throw error;
            }
        }
    );

    console.log("MCP Resources and Tools registered.");
    return mcpServer;
} 