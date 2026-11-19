// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import type { QueryKey } from "@tanstack/react-query";

/**
 * The cache entries the deprecated report readers keep: the removed
 * `Dashboard` wrapper's manifest and the old dashboard listing a host may
 * still key on.
 */
export function legacyReportQueryKeys(): QueryKey[] {
   return [["dashboards"], ["dashboard"]];
}
