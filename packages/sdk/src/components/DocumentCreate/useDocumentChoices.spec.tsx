// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { act, renderHook, waitFor } from "@testing-library/react";
import { beforeEach, describe, expect, it, mock } from "bun:test";
import {
   clearCache,
   mockServerProvider,
   serverWrapper,
} from "../../../test/serverProvider";

const modelInfo = JSON.stringify({
   entries: [{ kind: "source", name: "orders" }],
});
const model = {
   data: {
      modelInfo,
      sources: [{ name: "orders", views: [{ name: "by_day" }] }],
   },
};

const getModel = mock(
   (_env: string, _pkg: string, _path: string, _version?: string) =>
      Promise.resolve(model),
);

mockServerProvider({ models: { getModel } });

const { useDocumentChoices } = await import("./useDocumentChoices");

const render = (models: string[], enabled = true) =>
   renderHook(
      () =>
         useDocumentChoices({
            environmentName: "env",
            packageName: "pkg",
            models,
            enabled,
         }),
      { wrapper: serverWrapper },
   );

const callsFor = (path: string) =>
   getModel.mock.calls.filter((c) => c[2] === path).length;

beforeEach(() => {
   clearCache();
   getModel.mockReset();
   getModel.mockImplementation(() => Promise.resolve(model));
});

describe("useDocumentChoices: which lookups failed", () => {
   it("reports no failures when every model loads", async () => {
      const { result } = render(["a.malloy", "b.malloy"]);
      await waitFor(() => expect(result.current.isSuccess).toBe(true));
      expect([...result.current.choices.keys()]).toEqual([
         "a.malloy",
         "b.malloy",
      ]);
      expect(result.current.failed).toEqual([]);
   });

   it("keeps the other models' choices and names the one that failed", async () => {
      getModel.mockImplementation((_e, _p, path) =>
         path === "b.malloy"
            ? Promise.reject(new Error("boom"))
            : Promise.resolve(model),
      );
      const { result } = render(["a.malloy", "b.malloy", "c.malloy"]);
      await waitFor(() => expect(result.current.isSuccess).toBe(true));
      expect([...result.current.choices.keys()]).toEqual([
         "a.malloy",
         "c.malloy",
      ]);
      expect(result.current.failed).toEqual(["b.malloy"]);
   });

   it("lists every model, in order, when all lookups fail", async () => {
      getModel.mockImplementation(() => Promise.reject(new Error("down")));
      const { result } = render(["a.malloy", "b.malloy"]);
      await waitFor(() => expect(result.current.isSuccess).toBe(true));
      expect(result.current.choices.size).toBe(0);
      expect(result.current.failed).toEqual(["a.malloy", "b.malloy"]);
   });

   it("retry refetches only the failed lookups and clears failed on success", async () => {
      let healthy = false;
      getModel.mockImplementation((_e, _p, path) =>
         path === "b.malloy" && !healthy
            ? Promise.reject(new Error("boom"))
            : Promise.resolve(model),
      );
      const { result } = render(["a.malloy", "b.malloy"]);
      await waitFor(() => expect(result.current.failed).toEqual(["b.malloy"]));
      expect(callsFor("a.malloy")).toBe(1);
      healthy = true;
      act(() => result.current.retry());
      await waitFor(() => expect(result.current.failed).toEqual([]));
      expect([...result.current.choices.keys()]).toEqual([
         "a.malloy",
         "b.malloy",
      ]);
      expect(callsFor("a.malloy")).toBe(1);
      expect(callsFor("b.malloy")).toBe(2);
   });

   it("offers a retry only when a failed lookup could succeed on a second attempt", async () => {
      const status = (code: number) =>
         Promise.reject(
            Object.assign(new Error("x"), { response: { status: code } }),
         );
      getModel.mockImplementation((_e, _p, path) =>
         path === "a.malloy" ? status(403) : status(404),
      );
      const denied = render(["a.malloy", "b.malloy"]);
      await waitFor(() =>
         expect(denied.result.current.failed).toEqual(["a.malloy", "b.malloy"]),
      );
      expect(denied.result.current.canRetry).toBe(false);

      clearCache();
      getModel.mockImplementation((_e, _p, path) =>
         path === "a.malloy" ? status(404) : status(503),
      );
      const flaky = render(["a.malloy", "b.malloy"]);
      await waitFor(() =>
         expect(flaky.result.current.failed).toEqual(["a.malloy", "b.malloy"]),
      );
      expect(flaky.result.current.canRetry).toBe(true);
   });

   it("makes no calls while disabled", () => {
      const { result } = render(["a.malloy"], false);
      expect(getModel).not.toHaveBeenCalled();
      expect(result.current.failed).toEqual([]);
      expect(result.current.isSuccess).toBe(false);
   });
});
