import { EnvironmentStore } from "../../service/environment_store";
import { McpError, ErrorCode } from "@modelcontextprotocol/sdk/types.js";

// Type alias for compiled model pulled from OpenAPI-generated types.
// Using `any` fallback in case the exact type path changes.
// eslint-disable-next-line @typescript-eslint/no-explicit-any
export type ApiCompiledModel = any;

export interface ParsedMalloyUri {
   environmentName: string;
   packageName: string;
   modelPath: string;
}

/**
 * Parse a canonical Malloy model URI of the form:
 *   malloy://environment/{environment}/package/{package}/models/{modelPath}
 *
 * Throws an McpError(InvalidParams) if the URI is malformed.
 */
export function parseMalloyUri(uri: string): ParsedMalloyUri {
   const match = uri.match(
      /^malloy:\/\/environment\/([^/]+)\/package\/([^/]+)\/models\/(.+)$/,
   );

   if (!match) {
      throw new McpError(ErrorCode.InvalidParams, `Invalid Malloy URI: ${uri}`);
   }

   return {
      environmentName: match[1],
      packageName: match[2],
      modelPath: match[3],
   };
}

/**
 * Retrieve the compiled model for the given Malloy URI, using the provided
 * EnvironmentStore. Results are memoised in `compiledModelCache`.
 */
export async function getCompiledModel(
   uri: string,
   environmentStore: EnvironmentStore,
): Promise<ApiCompiledModel> {
   const { environmentName, packageName, modelPath } = parseMalloyUri(uri);

   // Look up the model via the EnvironmentStore hierarchy.
   const model = await environmentStore
      .getEnvironment(environmentName, false)
      .then((e) => e.getPackage(packageName, false))
      .then((pkg) => pkg.getModel(modelPath));

   if (!model || model.getModelType() === "notebook") {
      throw new McpError(
         ErrorCode.InvalidParams,
         `Model ${uri} not found or refers to a notebook`,
      );
   }

   const compiled = await model.getModel();
   return compiled;
}
