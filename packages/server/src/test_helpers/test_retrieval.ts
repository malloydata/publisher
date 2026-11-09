// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { NO_CATALOG, RetrievalService } from "../retrieval/service";
import type { EnvironmentStore } from "../service/environment_store";

/**
 * A RetrievalService over `store` with no catalog database, for a spec that
 * builds a controller or MCP tool by hand. Production passes the composition
 * root's one service; a spec gets its own, with a fresh registry.
 */
export function testRetrieval(store: unknown): RetrievalService {
   return new RetrievalService(store as EnvironmentStore, NO_CATALOG);
}
