// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { describe, expect, it } from "bun:test";
import { QuerySlots } from "../query_concurrency";
import type { PackageLookup } from "../service/environment_ports";
import type { Package } from "../service/package";
import { fcjStateOf } from "./index/engine_state";
import type { ChatModel } from "./llm/types";
import { RetrievalRegistry } from "./registry";
import { NO_CATALOG, RetrievalService } from "./service";

const store = {} as PackageLookup;

describe("RetrievalRegistry", () => {
   it("is the registry a RetrievalService given it holds", () => {
      const registry = new RetrievalRegistry();
      const service = new RetrievalService(store, NO_CATALOG, registry);
      expect(service.registry).toBe(registry);
   });

   it("is built fresh for a RetrievalService given none, so two such services share nothing", () => {
      const one = new RetrievalService(store, NO_CATALOG).registry;
      const two = new RetrievalService(store, NO_CATALOG).registry;
      expect(one).toBeInstanceOf(RetrievalRegistry);
      expect(two).not.toBe(one);
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
      const registry = new RetrievalRegistry();
      registry.chat.override(model);
      expect(registry.chat.model()).toBe(model);
      // No override on a new registry: the configured model (none).
      expect(new RetrievalRegistry().chat.model()).toBeNull();
   });

   it("keys a package's index state by the Package instance, so a reload starts over", () => {
      const registry = new RetrievalRegistry();
      const loaded = {} as Package;
      const reloaded = {} as Package;
      registry.engineStates.set(loaded, {
         status: "building",
         startedAt: 1,
      });
      expect(fcjStateOf(registry, loaded)?.status).toBe("building");
      expect(fcjStateOf(registry, reloaded)).toBeUndefined();
   });
});
