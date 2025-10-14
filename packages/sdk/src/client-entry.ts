// Lightweight client entry point for better code splitting
// This module contains essential client functionality without heavy UI components

// Export OpenAPI generated client APIs
export * from "./client/api";
export * from "./client/configuration";

// Export server provider and hooks for React integration
export { ServerProvider, useServer } from "./components/ServerProvider";
export type {
   ApiClients,
   ServerContextValue,
   ServerProviderProps,
} from "./components/ServerProvider";

// Export the query client for users who need direct access
export { globalQueryClient } from "./utils/queryClient";
