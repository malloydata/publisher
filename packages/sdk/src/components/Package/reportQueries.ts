// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import type { QueryClient } from "@tanstack/react-query";

/**
 * Drop every cached read of a package's reports after a write changed them:
 * each report's manifest and the package's report listing.
 */
export function invalidateReportQueries(queryClient: QueryClient): void {
   for (const queryKey of [["report"], ["reports"]])
      void queryClient.invalidateQueries({ queryKey });
}
