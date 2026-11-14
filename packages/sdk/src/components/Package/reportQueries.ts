// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import type { QueryClient } from "@tanstack/react-query";
import { legacyReportQueryKeys } from "../legacy/queryKeys";

/**
 * Drop every cached read of a package's reports after a write changed them:
 * each report's manifest, the listing, and the deprecated readers' copies,
 * whose listings a layout switch moves a report between. With `modelPath`,
 * the one report's own cached reads too.
 */
export function invalidateReportQueries(
   queryClient: QueryClient,
   environmentName: string,
   packageName: string,
   modelPath?: string,
): void {
   for (const queryKey of [
      ["report"],
      ["reports"],
      ...legacyReportQueryKeys(environmentName, packageName, modelPath),
   ])
      void queryClient.invalidateQueries({ queryKey });
}
