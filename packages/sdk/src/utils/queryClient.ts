// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { QueryClient } from "@tanstack/react-query";

// Global QueryClient instance - isolated to avoid circular dependencies
export const globalQueryClient = new QueryClient({
   defaultOptions: {
      queries: {
         // Not retried: `useQueryWithApiError` surfaces a failure the moment it
         // happens, and the SDK's specs and error states are written for that.
         retry: false,
         throwOnError: false,
         // Returning to the tab is not a reason to re-read: navigation, a
         // mutation's invalidation or a hook's own polling is. Without this,
         // every query on the page (a dashboard's tiles among them, billed by
         // the warehouse) refetched on each refocus once stale. A query whose
         // data changes while the reader is away opts back in.
         refetchOnWindowFocus: false,
         // Off for the same reason, and because the "online" event is not
         // proof of a network. A laptop waking from sleep fires it before
         // Wi-Fi or a VPN is back, so every stale query on the page refetched
         // into a network error at once.
         refetchOnReconnect: false,
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
// for a few minutes and don't auto-refetch on focus/reconnect. The staleTime is
// kept scoped to the result queries (spread into their useQuery options) rather
// than set on the global client, so other SDK queries (metadata lists, status)
// are stale at once and re-read on the next mount. Focus and reconnect are off
// globally as well; they are repeated here so the policy reads whole.
export const CHART_RESULT_QUERY_OPTIONS = {
   staleTime: 5 * 60 * 1000,
   refetchOnWindowFocus: false,
   refetchOnReconnect: false,
} as const;
