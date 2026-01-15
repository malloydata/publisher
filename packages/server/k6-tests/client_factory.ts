import type { Params } from "k6/http";
import { ConnectionsTestClient } from "./clients/connections-test.ts";
import { ConnectionsClient } from "./clients/connections.ts";
import { DatabasesClient } from "./clients/databases.ts";
import { ModelsClient } from "./clients/models.ts";
import { NotebooksClient } from "./clients/notebooks.ts";
import { PackagesClient } from "./clients/packages.ts";
import { ProjectsClient } from "./clients/projects.ts";
import { PublisherClient } from "./clients/publisher.ts";
import { WatchModeClient } from "./clients/watch-mode.ts";

/**
 * Creates client options with base URL and authorization header
 */
function createClientOptions(
   baseUrl: string,
   authToken?: string,
): {
   baseUrl: string;
   commonRequestParameters?: Params;
} {
   const options: {
      baseUrl: string;
      commonRequestParameters?: Params;
   } = {
      baseUrl,
   };

   if (authToken) {
      options.commonRequestParameters = {
         headers: {
            Authorization: authToken,
         },
      };
   }

   return options;
}

/**
 * Get PackagesClient instance
 */
export function getPackagesClient(
   baseUrl: string,
   authToken?: string,
): PackagesClient {
   return new PackagesClient(createClientOptions(baseUrl, authToken));
}

/**
 * Get ModelsClient instance
 */
export function getModelsClient(
   baseUrl: string,
   authToken?: string,
): ModelsClient {
   return new ModelsClient(createClientOptions(baseUrl, authToken));
}

/**
 * Get NotebooksClient instance
 */
export function getNotebooksClient(
   baseUrl: string,
   authToken?: string,
): NotebooksClient {
   return new NotebooksClient(createClientOptions(baseUrl, authToken));
}

/**
 * Get DatabasesClient instance
 */
export function getDatabasesClient(
   baseUrl: string,
   authToken?: string,
): DatabasesClient {
   return new DatabasesClient(createClientOptions(baseUrl, authToken));
}

/**
 * Get ConnectionsClient instance
 */
export function getConnectionsClient(
   baseUrl: string,
   authToken?: string,
): ConnectionsClient {
   return new ConnectionsClient(createClientOptions(baseUrl, authToken));
}

/**
 * Get ConnectionsTestClient instance
 */
export function getConnectionsTestClient(
   baseUrl: string,
   authToken?: string,
): ConnectionsTestClient {
   return new ConnectionsTestClient(createClientOptions(baseUrl, authToken));
}

/**
 * Get ProjectsClient instance
 */
export function getProjectsClient(
   baseUrl: string,
   authToken?: string,
): ProjectsClient {
   return new ProjectsClient(createClientOptions(baseUrl, authToken));
}

/**
 * Get PublisherClient instance
 */
export function getPublisherClient(
   baseUrl: string,
   authToken?: string,
): PublisherClient {
   return new PublisherClient(createClientOptions(baseUrl, authToken));
}

/**
 * Get WatchModeClient instance
 */
export function getWatchModeClient(
   baseUrl: string,
   authToken?: string,
): WatchModeClient {
   return new WatchModeClient(createClientOptions(baseUrl, authToken));
}
