import { check } from "k6";
import { getModelsClient, getPackagesClient } from "./client_factory.ts";
import type {
   CompiledModel,
   Model,
   Package,
} from "./clients/malloyPublisherSemanticModelServingAPI.schemas.ts";

export const PUBLISHER_URL = __ENV.PUBLISHER_URL || "http://localhost:4000";
export const PROJECT_NAME = __ENV.PROJECT_NAME || "malloy-samples";
export const AUTH_TOKEN = __ENV.AUTH_TOKEN || "";

export const WHITELISTED_PACKAGES = __ENV.WHITELISTED_PACKAGES
   ? (JSON.parse(__ENV.WHITELISTED_PACKAGES) as Array<string>)
   : ["ecommerce", "faa", "imdb", "bigquery-hackernews"];

export const USE_VERSION_ID = __ENV.USE_VERSION_ID === "true" ? true : false;
/**
 * Available packages in the k6-tests/packages directory
 * These are the actual package directories that exist
 * Can be set via AVAILABLE_PACKAGES environment variable (JSON array)
 * If not set, defaults to common packages
 *
 * Note: Since k6 can't read directories at runtime, this should be set via
 * a pre-build script that scans the packages directory, or manually via env var
 */
const AVAILABLE_PACKAGES = __ENV.AVAILABLE_PACKAGES
   ? (JSON.parse(__ENV.AVAILABLE_PACKAGES) as Array<string>)
   : ["ecommerce", "faa", "imdb", "bigquery-hackernews"];

/**
 * Single source of truth for getting available packages for testing
 *
 * This function:
 * 1. Reads from AVAILABLE_PACKAGES (env var or default list)
 * 2. Filters by WHITELISTED_PACKAGES
 * 3. Filters BigQuery packages based on credential availability:
 *    - If HAS_BIGQUERY_CREDENTIALS is true: includes all BigQuery packages
 *    - If HAS_BIGQUERY_CREDENTIALS is false: excludes BigQuery packages
 *
 * @returns Array of package names that are available, whitelisted, and compatible with current credentials
 */
export function getAvailablePackages(): Array<string> {
   let packages = AVAILABLE_PACKAGES;

   // Step 1: Filter by whitelist if provided
   if (WHITELISTED_PACKAGES.length > 0) {
      packages = packages.filter((pkg) => WHITELISTED_PACKAGES.includes(pkg));
   }

   // Step 2: Filter BigQuery packages based on credential availability
   if (!HAS_BIGQUERY_CREDENTIALS) {
      // Remove all BigQuery packages if credentials are not available
      packages = packages.filter((pkg) => !pkg.startsWith("bigquery-"));
   }
   // If credentials are available, keep all packages (including BigQuery ones)

   return packages;
}

// Initialize clients with base URL including /api/v0 prefix
export const BASE_URL = `${PUBLISHER_URL}/api/v0`;
const packagesClient = getPackagesClient(BASE_URL, AUTH_TOKEN);
const modelsClient = getModelsClient(BASE_URL, AUTH_TOKEN);

// Check if BigQuery credentials are available
export const HAS_BIGQUERY_CREDENTIALS =
   __ENV.GOOGLE_APPLICATION_CREDENTIALS !== undefined &&
   __ENV.GOOGLE_APPLICATION_CREDENTIALS !== "";

export const getPackages = (): Array<{
   name: string;
   description: string;
   versionId?: string;
}> => {
   const { response, data } = packagesClient.listPackages(PROJECT_NAME, {
      tags: { name: "list_packages" },
   });
   check(response, {
      "packages list request successful": (r) => r.status === 200,
   });

   if (response.status !== 200) {
      console.error(
         `Failed to get packages: ${response.status} - ${response.body}`,
      );
      return [];
   }

   if (!Array.isArray(data)) {
      console.error(`Expected array but got: ${typeof data}`, data);
      return [];
   }

   return data.map((pkg: Package) => ({
      name: pkg.name || "",
      description: pkg.description || "",
      versionId: USE_VERSION_ID ? "latest" : undefined,
   }));
};

export const loadPackage = (packageName: string, location: string): Package => {
   const apiPackage: Package = {
      name: packageName,
      description: "",
      location: location,
   };

   const { response } = packagesClient.createPackage(PROJECT_NAME, apiPackage);

   check(response, {
      [`package ${packageName} can upload`]: (r) => r.status === 200,
      [`package ${packageName} upload time < 500ms`]: (r) =>
         r.timings.duration < 500,
   });

   return apiPackage;
};

export const queryPackage = (packageName: string): Package => {
   const { response, data } = packagesClient.getPackage(
      PROJECT_NAME,
      packageName,
   );

   check(response, {
      "package query successful": (r) => r.status === 200,
      "package query response time < 500ms": (r) => r.timings.duration < 500,
   });

   return data;
};

export const unloadPackage = (packageName: string, packageId: string) => {
   // Note: The generated client uses projectName/packageName pattern
   // If packageId is different from packageName, this may need adjustment
   const { response } = packagesClient.deletePackage(PROJECT_NAME, packageId);

   check(response, {
      [`package ${packageName} can unload`]: (r) =>
         r.status === 200 || r.status === 204,
      [`package ${packageName} unload time < 500ms`]: (r) =>
         r.timings.duration < 500,
   });
};

export const getModels = (
   packageName: string,
   versionId?: string,
): Array<{
   path: string;
   type: string;
}> => {
   const params = USE_VERSION_ID && versionId ? { versionId } : {};
   const { response, data } = modelsClient.listModels(
      PROJECT_NAME,
      packageName,
      params,
      { tags: { name: "list_models" } },
   );

   check(response, {
      "list models request successful": (r) => r.status === 200,
      "list models response time < 2s": (r) => r.timings.duration < 2000,
   });

   const code = (data as { code?: number })["code"];
   if (code !== undefined && code !== 200) {
      console.error(`Failed to list models: ${code} - ${JSON.stringify(data)}`);
      return [];
   }
   if (!Array.isArray(data)) {
      console.log(`Data: ${JSON.stringify(data)}`);
      console.error(`Expected array but got: ${typeof data}`, data);
      return [];
   }

   return data.map((model: Model) => ({
      path: model.path || "",
      type: model.error ? "error" : "source", // Infer type from presence of error
   }));
};

type ModelData = {
   type: "notebook" | "source";
   sources?: Array<{
      name: string;
      views: Array<{
         name: string;
      }>;
   }>;
};

export const getModelData = (
   packageName: string,
   modelPath: string,
   versionId?: string,
): ModelData => {
   const params = USE_VERSION_ID && versionId ? { versionId } : {};
   const { response, data } = modelsClient.getModel(
      PROJECT_NAME,
      packageName,
      modelPath,
      params,
      { tags: { name: "get_model" } },
   );

   check(response, {
      "get model data request successful": (r) => r.status === 200,
      "get model data response time < 2s": (r) => r.timings.duration < 2000,
   });

   // Transform CompiledModel to ModelData format expected by getViews

   // The API returns CompiledModel with sourceInfos array containing JSON strings
   // Each string contains source information with views in schema.fields
   const compiledModel = data as CompiledModel & {
      sourceInfos?: string[];
   };

   const sources: Array<{
      name: string;
      views: Array<{ name: string }>;
   }> = [];

   // Parse sourceInfos array (primary method - more reliable than sources)
   if (compiledModel.sourceInfos && Array.isArray(compiledModel.sourceInfos)) {
      for (const sourceInfoJson of compiledModel.sourceInfos) {
         try {
            const sourceInfo = JSON.parse(sourceInfoJson) as {
               kind?: string;
               name?: string;
               schema?: {
                  fields?: Array<{
                     kind?: string;
                     name?: string;
                  }>;
               };
            };

            // Only process source entries
            if (sourceInfo.kind === "source" && sourceInfo.name) {
               const views: Array<{ name: string }> = [];

               // Extract views from schema.fields where kind === "view"
               if (
                  sourceInfo.schema?.fields &&
                  Array.isArray(sourceInfo.schema.fields)
               ) {
                  for (const field of sourceInfo.schema.fields) {
                     if (field.kind === "view" && field.name) {
                        views.push({ name: field.name });
                     }
                  }
               }

               // Only include sources that have views (matching original behavior)
               if (views.length > 0) {
                  sources.push({
                     name: sourceInfo.name,
                     views: views,
                  });
               }
            }
         } catch (error) {
            console.warn(
               `Failed to parse sourceInfo for ${packageName}/${modelPath}:`,
               error,
            );
         }
      }
   }

   // Fallback to sources array only if sourceInfos is empty or failed to parse anything
   const compiledModelWithSources = data as CompiledModel & {
      sources?: Array<{
         name?: string;
         views?: Array<{ name?: string }>;
      }>;
   };

   if (sources.length === 0 && compiledModelWithSources.sources) {
      for (const source of compiledModelWithSources.sources) {
         if (source && source.name) {
            const views: Array<{ name: string }> = [];

            // Extract views from source
            if (source.views && Array.isArray(source.views)) {
               for (const view of source.views) {
                  if (view && view.name) {
                     views.push({ name: view.name });
                  }
               }
            }

            if (views.length > 0) {
               sources.push({
                  name: source.name,
                  views: views,
               });
            }
         }
      }
   }

   // Note: We exclude queries from views - queries are named queries that can be executed,
   // but they are not views. Views are defined in the sourceInfos array.

   return {
      type: "source",
      sources: sources.length > 0 ? sources : undefined,
   };
};

export function* getViews(modelData: ModelData) {
   if (
      modelData.type === "source" &&
      modelData.sources &&
      modelData.sources.length > 0
   ) {
      for (const source of modelData.sources) {
         if (source && source.name && source.views && source.views.length > 0) {
            for (const view of source.views) {
               if (view && view.name) {
                  yield {
                     sourceName: source.name,
                     viewName: view.name,
                  };
               }
            }
         }
      }
   }
}

export const queryModelView = (
   packageName: string,
   modelPath: string,
   sourceName: string,
   queryName: string,
   versionId?: string,
) => {
   // For top-level queries (empty sourceName), only pass queryName
   // For queries on sources, pass both sourceName and queryName
   const queryRequest: {
      sourceName?: string;
      queryName: string;
      versionId?: string;
   } = {
      queryName: queryName,
   };

   // Only include sourceName if it's not empty (top-level queries have empty sourceName)
   if (sourceName && sourceName.trim() !== "") {
      queryRequest.sourceName = sourceName;
   }

   // Only include versionId if USE_VERSION_ID flag is enabled
   if (USE_VERSION_ID && versionId) {
      queryRequest.versionId = versionId;
   }

   const { response } = modelsClient.executeQueryModel(
      PROJECT_NAME,
      packageName,
      modelPath,
      queryRequest,
      { tags: { name: "execute_query" } },
   );

   // Log error details if request failed
   if (response.status >= 400) {
      console.error(
         `Query failed for ${packageName}/${modelPath}:`,
         JSON.stringify({
            status: response.status,
            statusText: response.status_text,
            error: response.error,
            errorCode: response.error_code,
            body: response.body,
            request: queryRequest,
         }),
      );
   }

   return response;
};

// ============================================================================
// Helper Functions
// ============================================================================

/**
 * Generate unique test resource name
 */
export function generateTestName(prefix: string): string {
   return `${prefix}-${Date.now()}-${Math.random().toString(36).substring(7)}`;
}
