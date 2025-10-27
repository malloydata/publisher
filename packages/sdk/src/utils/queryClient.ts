import { QueryClient } from "@tanstack/react-query";

// Global QueryClient instance - isolated to avoid circular dependencies
export const globalQueryClient = new QueryClient({
   defaultOptions: {
      queries: {
         retry: false,
         throwOnError: false,
      },
      mutations: {
         retry: false,
         throwOnError: false,
      },
   },
});
