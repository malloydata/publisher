// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { act, renderHook, waitFor } from "@testing-library/react";
import { afterEach, describe, expect, it, mock } from "bun:test";
import { QueryClientProvider } from "@tanstack/react-query";
import type { ReactNode } from "react";
import { globalQueryClient } from "../utils/queryClient";
import {
   setConsoleEventHandler,
   type ConsoleEvent,
} from "../telemetry/consoleEvents";
import { useCrudMutation } from "./useCrudMutation";

/**
 * The shape every create, edit and delete dialog in the Console now runs
 * through. Its job is four things in one order — run the write, close the
 * dialog, refetch what changed, and say what happened — and the fourth used
 * to be written out per file in two different ways.
 */

const wrapper = ({ children }: { children: ReactNode }) => (
   <QueryClientProvider client={globalQueryClient}>
      {children}
   </QueryClientProvider>
);

const mount = (
   mutationFn: () => Promise<unknown>,
   onSettled = () => {},
   invalidates: string[][] = [["packages", "examples"]],
) =>
   renderHook(
      () =>
         useCrudMutation({
            mutationFn,
            success: "Package deleted",
            invalidates,
            onSettled,
            resource: "package",
            action: "delete",
         }),
      { wrapper },
   );

describe("useCrudMutation", () => {
   afterEach(() => setConsoleEventHandler(undefined));

   it("closes the dialog and reports success once the write lands", async () => {
      const events: ConsoleEvent[] = [];
      setConsoleEventHandler((e) => events.push(e));
      const onSettled = mock(() => {});
      const view = mount(() => Promise.resolve({ ok: true }), onSettled);

      act(() => view.result.current.mutate());
      await waitFor(() => expect(onSettled).toHaveBeenCalledTimes(1));
      await waitFor(() => expect(events).toHaveLength(1));
      expect(events[0]).toMatchObject({
         type: "console.mutation",
         resource: "package",
         action: "delete",
         ok: true,
      });
      expect(events[0].reason).toBeUndefined();
   });

   it("reports a failure with the reason the operator was shown", async () => {
      // The gap this closes: a delete that failed left no trace at all once
      // its six-second snackbar had gone.
      const events: ConsoleEvent[] = [];
      setConsoleEventHandler((e) => events.push(e));
      const view = mount(() => Promise.reject(new Error("Package is in use")));

      act(() => view.result.current.mutate());
      await waitFor(() => expect(events).toHaveLength(1));
      expect(events[0]).toMatchObject({
         ok: false,
         reason: "Package is in use",
      });
   });

   it("does not close the dialog when the write fails", async () => {
      // There is something to correct and retry, so the form has to stay.
      const onSettled = mock(() => {});
      const view = mount(() => Promise.reject(new Error("nope")), onSettled);
      act(() => view.result.current.mutate());
      await waitFor(() => expect(view.result.current.isPending).toBe(false));
      expect(onSettled).not.toHaveBeenCalled();
   });

   it("falls back to a sentence for a rejection that is not an Error", async () => {
      // Every dialog had its own copy of this, and two of them did not have
      // it at all — they read `error.message` off whatever was thrown.
      const events: ConsoleEvent[] = [];
      setConsoleEventHandler((e) => events.push(e));
      const view = mount(() => Promise.reject("a bare string"));
      act(() => view.result.current.mutate());
      await waitFor(() => expect(events).toHaveLength(1));
      expect(events[0].reason).toBe("An unknown error occurred");
   });

   it("invalidates the keys it was given", async () => {
      const invalidate = mock(() => Promise.resolve());
      const original = globalQueryClient.invalidateQueries;
      globalQueryClient.invalidateQueries =
         invalidate as unknown as typeof original;
      try {
         const view = mount(
            () => Promise.resolve({}),
            () => {},
            [["packages", "examples"], ["environments"]],
         );
         act(() => view.result.current.mutate());
         await waitFor(() => expect(invalidate).toHaveBeenCalledTimes(2));
      } finally {
         globalQueryClient.invalidateQueries = original;
      }
   });
});
