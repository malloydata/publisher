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

// Refetch policy for chart/query-RESULT queries. A Malloy query result is a
// pure function of the query text + filters, which are already in each query
// key, so re-executing the query on tab refocus or reconnect only repaints the
// same result and can cause a visible chart flicker. Treat results as fresh
// for a few minutes and don't auto-refetch on focus/reconnect. Kept scoped to
// the result queries (spread into their useQuery options) rather than set on
// the global client, so other SDK queries (metadata lists, status) keep
// react-query's default freshness behavior.
export const CHART_RESULT_QUERY_OPTIONS = {
   staleTime: 5 * 60 * 1000,
   refetchOnWindowFocus: false,
   refetchOnReconnect: false,
} as const;
