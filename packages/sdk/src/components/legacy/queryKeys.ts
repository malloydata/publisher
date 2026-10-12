// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import type { QueryKey } from "@tanstack/react-query";
import { encodeResourceUri } from "../../utils/formatting";

/**
 * The cache entries the deprecated report readers keep: the `Dashboard`
 * wrapper's manifest, the old dashboard and notebook listings a host may
 * still key on, and the `Notebook` wrapper's read, which it keys on the
 * report's resource URI.
 */
export function legacyReportQueryKeys(
   environmentName: string,
   packageName: string,
   modelPath?: string,
): QueryKey[] {
   return [
      ["dashboards"],
      ["notebooks"],
      ["dashboard"],
      ...(modelPath === undefined
         ? []
         : [[encodeResourceUri({ environmentName, packageName, modelPath })]]),
   ];
}
