// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import {
   focusManager,
   QueryClient,
   QueryObserver,
   type QueryObserverResult,
} from "@tanstack/react-query";
import { describe, expect, it } from "bun:test";
import { modelPageOf, modelPageState } from "./modelPage";

describe("modelPageOf", () => {
   it("is the page /status names, the Explorer unless it says viewer", () => {
      expect(modelPageOf({ modelPage: "viewer" }, false)).toBe("viewer");
      expect(modelPageOf({ modelPage: "explorer" }, false)).toBe("explorer");
      // A server that predates the setting sends no field.
      expect(modelPageOf({}, false)).toBe("explorer");
      expect(modelPageOf({ modelPage: "something new" }, false)).toBe(
         "explorer",
      );
   });

   // A deployment that chose viewer has chosen not to show the Explorer, and
   // the viewer writes no value into Malloy on any server.
   it("is the viewer when /status could not be read", () => {
      expect(modelPageOf(undefined, true)).toBe("viewer");
   });

   it("is the Explorer while /status has not answered", () => {
      expect(modelPageOf(undefined, false)).toBe("explorer");
   });
});

describe("modelPageState", () => {
   it("settles once /status answers or fails", () => {
      expect(
         modelPageState({
            data: { modelPage: "viewer" },
            isPending: false,
            isError: false,
         }),
      ).toEqual({ modelPage: "viewer", isLoadingStatus: false });
      expect(
         modelPageState({ data: undefined, isPending: false, isError: true }),
      ).toEqual({ modelPage: "viewer", isLoadingStatus: false });
   });

   // The first /status fails and its retry pauses, as it does in a hidden tab
   // or offline: nothing has answered, yet react-query's isLoading is false,
   // so a page that waited on it would draw the Explorer on a viewer server.
   it("waits while a retry is paused", async () => {
      const client = new QueryClient();
      focusManager.setFocused(false);
      try {
         const observer = new QueryObserver(client, {
            queryKey: ["status"],
            queryFn: () => Promise.reject(new Error("500")),
            retry: 3,
            retryDelay: 0,
         });
         const paused = await new Promise<QueryObserverResult>((resolve) => {
            const unsubscribe = observer.subscribe((result) => {
               if (result.fetchStatus !== "paused") return;
               unsubscribe();
               resolve(result);
            });
         });
         expect(paused.isLoading).toBe(false);
         expect(modelPageState(paused)).toEqual({
            modelPage: "explorer",
            isLoadingStatus: true,
         });
      } finally {
         focusManager.setFocused(undefined);
         client.clear();
      }
   });
});
