// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * Mount a component that calls `useServer()` without a real server.
 *
 * `mock.module` is process-global and there is no way to undo it, so every
 * spec that stubs `ServerProvider` stubs it for the whole `bun test` run. One
 * shared stub is what keeps that survivable: the shape a later spec inherits
 * is this one, whatever it registered. See test/README.md.
 */
import { QueryClientProvider } from "@tanstack/react-query";
import { mock } from "bun:test";
import type { ReactNode } from "react";
import { globalQueryClient } from "../src/utils/queryClient";

/** Stands in for the base URL `useQueryWithApiError` appends to every key. */
export const TEST_SERVER = "http://localhost/api/v0";

/**
 * Replace `ServerProvider` with a stub serving `apiClients`.
 *
 * Call it at the top of a spec, BEFORE importing whatever is under test:
 * a static import of the component would hoist above this and bind the real
 * module. `ServerProvider` itself is stubbed alongside `useServer` because
 * `components/index.ts` re-exports both, and the barrel is in the graph.
 */
export function mockServerProvider(apiClients: unknown) {
   mock.module("../src/components/ServerProvider", () => ({
      ServerProvider: () => null,
      useServer: () => ({ server: TEST_SERVER, apiClients }),
   }));
}

/**
 * The client `ServerProvider` hands down in production, which is also the one
 * `useQueryWithApiError` reaches for directly. Passing the same instance means
 * a spec can read every query out of {@link cacheKeys}, whether the code under
 * test went through the hook or through plain `useQueries`.
 */
export const serverWrapper = ({ children }: { children: ReactNode }) => (
   <QueryClientProvider client={globalQueryClient}>
      {children}
   </QueryClientProvider>
);

/**
 * Every query key in the cache as a string, so a spec can assert on one slot.
 *
 * Stringified rather than compared structurally because the interesting
 * assertion is almost always "is this value in the key at all": a key that
 * carries a version in the wrong slot is still a key that cannot collide.
 */
export function cacheKeys(prefix?: string): string[] {
   const keys = globalQueryClient
      .getQueryCache()
      .getAll()
      .map((query) => JSON.stringify(query.queryKey));
   return prefix === undefined
      ? keys
      : keys.filter((key) => key.startsWith(`["${prefix}"`));
}

/** Between tests: a key left behind is a key the next test can mistake. */
export function clearCache() {
   globalQueryClient.clear();
}

/** A request that never settles, so nothing renders past its loading state. */
export function pending<T = never>(): Promise<T> {
   return new Promise<T>(() => {});
}
