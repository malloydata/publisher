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
                const packages = await packageService.listPackages();
                return {
                    resources: packages.map(pkg => ({
                        uri: `malloy://project/home/package/${pkg.name || 'unknown'}`,
                        name: pkg.name || 'unknown',
                        description: `Package: ${pkg.name || 'unknown'}`
                    }))
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

    // Register Model Resource
    mcpServer.resource(
        'model',
        new ResourceTemplate('malloy://project/{projectName}/package/{packageName}/models/{modelPath}', {
            list: async () => {
                try {
                    const pkg = await packageService.getPackage('home');
                    const models = await pkg.listModels();
                    return {
                        resources: models.map(model => ({
                            uri: `malloy://project/home/package/home/models/${model.path || ''}`,
                            name: model.path || '',
                            description: `Model: ${model.path || ''}`
                        }))
                    };
                } catch (error) {
                    if (error instanceof PackageNotFoundError) {
                        return { resources: [] };
                    }
                    throw error;
                }
            }
        }),
        async (uri, { projectName, packageName, modelPath }) => {
            try {
                const pkg = await packageService.getPackage(packageName as string);
                const model = pkg.getModel(modelPath as string);
                if (!model) {
                    return {
                        isError: true,
                        contents: [{
                            type: 'text',
                            uri: uri.href,
                            text: MCP_ERROR_MESSAGES.MODEL_NOT_FOUND(packageName as string, modelPath as string)
                        }]
                    };
                }
                return {
                    contents: [{
                        type: 'text',
                        uri: uri.href,
                        text: `Model: ${modelPath}`
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
                throw error;
            }
        }
    );

    console.log("MCP Resources and Tools registered.");
    return mcpServer;
} 