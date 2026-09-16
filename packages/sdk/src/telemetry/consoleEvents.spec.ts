// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { afterEach, describe, expect, it, mock } from "bun:test";
import {
   reportConsoleEvent,
   reporting,
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

/**
 * The wrapper both shapes of Console write go through. Its contract is that
 * nothing downstream can tell it is there: same value out, same rejection out,
 * and an event either way.
 */
describe("reporting()", () => {
   afterEach(() => setConsoleEventHandler(undefined));

   it("returns the write's own value and reports a success", async () => {
      const seen: ConsoleEvent[] = [];
      setConsoleEventHandler((e) => seen.push(e));
      const write = reporting("connection", "create", async (name: string) => ({
         created: name,
      }));

      expect(await write("bigquery")).toEqual({ created: "bigquery" });
      expect(seen).toHaveLength(1);
      expect(seen[0]).toMatchObject({
         resource: "connection",
         action: "create",
         ok: true,
      });
      expect(typeof seen[0].durationMs).toBe("number");
   });

   it("re-throws the write's own error, unchanged, and reports the failure", async () => {
      const seen: ConsoleEvent[] = [];
      setConsoleEventHandler((e) => seen.push(e));
      const boom = new Error("Connection is in use");
      const write = reporting("connection", "delete", async () => {
         throw boom;
      });

      // The SAME error object: a caller that matches on it must still match.
      await expect(write(undefined as never)).rejects.toBe(boom);
      expect(seen).toHaveLength(1);
      expect(seen[0]).toMatchObject({
         ok: false,
         reason: "Connection is in use",
      });
   });

   it("reports a rejection that is not an Error without inventing a message", async () => {
      const seen: ConsoleEvent[] = [];
      setConsoleEventHandler((e) => seen.push(e));
      const write = reporting("package", "update", async () => {
         throw "a bare string";
      });
      await expect(write(undefined as never)).rejects.toBe("a bare string");
      expect(seen[0].reason).toBe("An unknown error occurred");
   });

   it("is transparent when no sink is installed", async () => {
      const write = reporting("scope", "update", async (n: number) => n * 2);
      expect(await write(21)).toBe(42);
   });
});
