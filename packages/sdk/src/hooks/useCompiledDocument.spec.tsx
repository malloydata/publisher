// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { renderHook, waitFor } from "@testing-library/react";
import { beforeEach, describe, expect, it, mock } from "bun:test";
import {
   clearCache,
   mockServerProvider,
   serverWrapper,
} from "../../test/serverProvider";

const compileModelSource = mock(
   (
      _env: string,
      _pkg: string,
      _path: string,
      _body: { source?: string; scope?: string; givens?: object },
   ) => Promise.resolve({ data: { status: "success", problems: [] } }),
);

mockServerProvider({ models: { compileModelSource } });

const { useCompiledDocument } = await import("./useCompiledDocument");

const SOURCE = '## artifact { tiles=["orders_secured -> by_status"] }\n';

const render = (givens?: Record<string, unknown>) =>
   renderHook(
      () =>
         useCompiledDocument({
            environmentName: "env",
            packageName: "pkg",
            modelPath: "index.malloy",
            source: SOURCE,
            givens,
         }),
      { wrapper: serverWrapper },
   );

const bodyOfLastCall = () => compileModelSource.mock.calls.at(-1)?.[3];

beforeEach(() => {
   clearCache();
   compileModelSource.mockClear();
});

describe("useCompiledDocument: the givens a gate reads", () => {
   it("sends no givens key when the host sets none", async () => {
      const { result } = render();
      await waitFor(() => expect(result.current.isSuccess).toBe(true));
      expect(bodyOfLastCall()).toEqual({ source: SOURCE, scope: "append" });
   });

   it("sends a list-valued given as the list, which a `$TENANTS`-style gate reads", async () => {
      const { result } = render({ TENANTS: ["acme", "globex"] });
      await waitFor(() => expect(result.current.isSuccess).toBe(true));
      expect(bodyOfLastCall()).toEqual({
         source: SOURCE,
         scope: "append",
         givens: { TENANTS: ["acme", "globex"] },
      });
   });

   it("compiles again when a given's value changes, so a stale restricted verdict is not reused", async () => {
      const { result, rerender } = renderHook(
         ({ givens }: { givens: Record<string, unknown> }) =>
            useCompiledDocument({
               environmentName: "env",
               packageName: "pkg",
               modelPath: "index.malloy",
               source: SOURCE,
               givens,
            }),
         {
            wrapper: serverWrapper,
            initialProps: { givens: { TENANTS: ["acme"] } },
         },
      );
      await waitFor(() => expect(result.current.isSuccess).toBe(true));
      rerender({ givens: { TENANTS: ["acme", "globex"] } });
      await waitFor(() => expect(compileModelSource).toHaveBeenCalledTimes(2));
      expect(bodyOfLastCall()?.givens).toEqual({
         TENANTS: ["acme", "globex"],
      });
   });
});
