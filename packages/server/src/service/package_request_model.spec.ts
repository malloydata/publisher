// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { afterAll, beforeAll, describe, expect, it, mock } from "bun:test";
import type { MalloyConfig } from "@malloydata/malloy";
import {
   type LoadedTempPackage,
   loadTempPackage,
   usePackageLoadPool,
} from "../test_helpers/temp_package";
import type { Model } from "./model";

describe("Package.prepareRequestModel", () => {
   usePackageLoadPool();
   let loaded: LoadedTempPackage;

   beforeAll(async () => {
      loaded = await loadTempPackage({
         "orders.malloy":
            'source: orders is duckdb.sql("select 1 as id") extend { measure: n is count() }\n',
      });
   }, 120_000);
   afterAll(async () => {
      await loaded?.cleanup();
   });

   it("gives a request's model the served models' serve setup, and no pre-aggregation companion", () => {
      const destinations = (() => ({})) as unknown as () => MalloyConfig;
      loaded.pkg.setServeDestinationConfig(destinations);
      const model = {
         setServeBindings: mock(),
         setServeDestinationConfig: mock(),
         setFreshnessResolver: mock(),
         setPreaggregateEntityIdResolver: mock(),
         buildPreaggregateServeModel: mock(),
      };

      const prepared = loaded.pkg.prepareRequestModel(
         model as unknown as Model,
      );

      expect(prepared).toBe(model as unknown as Model);
      expect(model.setServeBindings).toHaveBeenCalledTimes(1);
      expect(model.setServeDestinationConfig).toHaveBeenCalledWith(
         destinations,
      );
      expect(model.setFreshnessResolver).toHaveBeenCalledTimes(1);
      expect(model.setPreaggregateEntityIdResolver).toHaveBeenCalledTimes(1);
      // The companion compiles from the package's files, which an overlay replaces.
      expect(model.buildPreaggregateServeModel).not.toHaveBeenCalled();
   });
});
