// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import type { QueryGovernor } from "../service/governance/query_governor";
import type { Model } from "../service/model";

/**
 * The {@link QueryGovernor} a `Model` delegates its authorization to, for
 * specs that drive governance internals (formerly private `Model` methods).
 */
export function governorOf(model: Model): QueryGovernor {
   return (model as unknown as { governor: QueryGovernor }).governor;
}
