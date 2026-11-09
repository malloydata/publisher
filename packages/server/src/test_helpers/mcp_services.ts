// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import type { McpServices } from "../app_services";
import { CompileController } from "../controller/compile.controller";
import { ConnectionController } from "../controller/connection.controller";
import { PackageController } from "../controller/package.controller";
import { QuerySlots } from "../query_concurrency";
import { RetrievalService } from "../retrieval/service";
import type { EnvironmentStore } from "../service/environment_store";
import { QueryUseCase } from "../usecases/query";

/**
 * The services initializeMcpServer takes, as the composition root builds them
 * (real controllers), over a fake store. A spec that only stubs the store
 * exercises the same controller code a running server does.
 */
export function mcpServicesFor(store: unknown): McpServices {
   const environmentStore = store as EnvironmentStore;
   // Read lazily, as the root's catalog is: a fake store with no storage
   // fails only when a build or purge reaches for it.
   const retrieval = new RetrievalService(environmentStore, {
      getDuckDbConnection: () =>
         environmentStore.storageManager.getDuckDbConnection(),
   });
   const querySlots = new QuerySlots();
   return {
      environmentStore,
      retrieval,
      querySlots,
      queryUseCase: new QueryUseCase(environmentStore, querySlots),
      compileController: new CompileController(environmentStore),
      packageController: new PackageController(environmentStore, retrieval),
      connectionController: new ConnectionController(environmentStore),
   };
}
