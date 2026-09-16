// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { afterEach, describe, expect, it, mock } from "bun:test";
import {
   reportConsoleEvent,
   setConsoleEventHandler,
   type ConsoleEvent,
} from "./consoleEvents";

const event = (over: Partial<ConsoleEvent> = {}): ConsoleEvent => ({
   type: "console.mutation",
   resource: "package",
   action: "delete",
   ok: true,
   durationMs: 12,
   ...over,
});

describe("console event sink", () => {
   afterEach(() => setConsoleEventHandler(undefined));

   it("delivers to the installed handler", () => {
      const sink = mock((_e: ConsoleEvent) => {});
      setConsoleEventHandler(sink);
      reportConsoleEvent(event());
      expect(sink).toHaveBeenCalledTimes(1);
      expect(sink.mock.calls[0][0]).toMatchObject({
         resource: "package",
         action: "delete",
         ok: true,
      });
   });

   it("is a no-op when no host installed one", () => {
      // An SDK embedded in someone else's app reports to nobody, and that has
      // to be silent rather than a crash on every write.
      expect(() => reportConsoleEvent(event())).not.toThrow();
   });

   it("replaces the handler rather than fanning out", () => {
      const first = mock((_e: ConsoleEvent) => {});
      const second = mock((_e: ConsoleEvent) => {});
      setConsoleEventHandler(first);
      setConsoleEventHandler(second);
      reportConsoleEvent(event());
      expect(first).not.toHaveBeenCalled();
      expect(second).toHaveBeenCalledTimes(1);
   });

   it("swallows a sink that throws, so telemetry cannot break a write", () => {
      // This is called from the success path of a delete. A sink bug that
      // surfaced as a failed delete would be worse than the line it lost.
      setConsoleEventHandler(() => {
         throw new Error("the sink is broken");
      });
      expect(() => reportConsoleEvent(event())).not.toThrow();
   });

   it("carries the reason on a failure", () => {
      const sink = mock((_e: ConsoleEvent) => {});
      setConsoleEventHandler(sink);
      reportConsoleEvent(event({ ok: false, reason: "Package is in use" }));
      expect(sink.mock.calls[0][0]).toMatchObject({
         ok: false,
         reason: "Package is in use",
      });
   });
});
