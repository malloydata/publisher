// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import type { QueryClient } from "@tanstack/react-query";
import { legacyReportQueryKeys } from "../legacy/queryKeys";

/**
 * Drop every cached read of a package's reports after a write changed them:
 * each report's manifest, the listing, and the deprecated readers' copies,
 * whose listings a layout switch moves a report between.
 */
export function invalidateReportQueries(queryClient: QueryClient): void {
   for (const queryKey of [["report"], ["reports"], ...legacyReportQueryKeys()])
      void queryClient.invalidateQueries({ queryKey });
}
