// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { afterEach, beforeEach, describe, expect, it } from "bun:test";
import { QuerySlots } from "../query_concurrency";
import type { EnvironmentStore } from "../service/environment_store";
import type { Package } from "../service/package";
import { fcjStateOf } from "./index/engine_state";
import { _setChatModelForTests, getChatModel } from "./llm/active";
import type { ChatModel } from "./llm/types";
import {
   installRetrievalRegistry,
   RetrievalRegistry,
   retrievalRegistry,
} from "./registry";
import { RetrievalService } from "./service";

let previous: RetrievalRegistry | undefined;
beforeEach(() => {
   previous = installRetrievalRegistry(new RetrievalRegistry());
});
afterEach(() => {
   installRetrievalRegistry(previous);
});

const store = {} as EnvironmentStore;

describe("RetrievalRegistry", () => {
   it("is the registry retrieval reads once a RetrievalService is given it", () => {
      const registry = new RetrievalRegistry();
      const service = new RetrievalService(store, registry);
      expect(service.registry).toBe(registry);
      expect(retrievalRegistry()).toBe(registry);
   });

   it("is adopted, not replaced, by a RetrievalService given none", () => {
      const inForce = retrievalRegistry();
      expect(new RetrievalService(store).registry).toBe(inForce);
      expect(retrievalRegistry()).toBe(inForce);
   });

   it("takes the slot pool and resolver it is built with, or a pool of its own", () => {
      const querySlots = new QuerySlots();
      const resolver = async () => {
         throw new Error("unused");
      };
      const built = new RetrievalRegistry({
         querySlots,
         queryModelResolver: resolver,
      });
      expect(built.querySlots).toBe(querySlots);
      expect(built.queryModelResolver).toBe(resolver);
      expect(new RetrievalRegistry().querySlots).toBeInstanceOf(QuerySlots);
      expect(new RetrievalRegistry().queryModelResolver).toBeUndefined();
   });

   it("holds what a test seam sets, so a fresh registry starts clean", () => {
      const model = { id: "fake" } as unknown as ChatModel;
      _setChatModelForTests(model);
      expect(getChatModel()).toBe(model);
      installRetrievalRegistry(new RetrievalRegistry());
      // No override on the new registry: back to the configured model (none).
      expect(getChatModel()).toBeNull();
   });

   it("keys a package's index state by the Package instance, so a reload starts over", () => {
      const loaded = {} as Package;
      const reloaded = {} as Package;
      retrievalRegistry().engineStates.set(loaded, {
         status: "building",
         startedAt: 1,
      });
      expect(fcjStateOf(loaded)?.status).toBe("building");
      expect(fcjStateOf(reloaded)).toBeUndefined();
   });
});
