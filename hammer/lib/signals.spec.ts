// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { describe, expect, it } from "bun:test";
import { installSignalTeardown, type SignalHost } from "./signals";

type Signal = "SIGINT" | "SIGTERM" | "SIGHUP";

function fakeHost() {
   const listeners = new Map<Signal, Set<() => void>>();
   const exits: number[] = [];
   const host: SignalHost = {
      on: (s, l) => {
         if (!listeners.has(s)) listeners.set(s, new Set());
         listeners.get(s)!.add(l);
      },
      off: (s, l) => listeners.get(s)?.delete(l),
      exit: (code) => exits.push(code),
   };
   const send = (s: Signal) => {
      for (const l of [...(listeners.get(s) ?? [])]) l();
   };
   return { host, send, exits, listeners };
}

const quiet = () => {};
const tick = () => new Promise((r) => setTimeout(r, 0));

describe("tearing a run down on a signal", () => {
   it("runs the teardown on Ctrl-C, then exits 130", async () => {
      const { host, send, exits } = fakeHost();
      let tornDown = 0;
      installSignalTeardown(async () => void tornDown++, host, quiet);
      send("SIGINT");
      await tick();
      expect(tornDown).toBe(1);
      expect(exits).toEqual([130]);
   });

   it("exits 143 on SIGTERM", async () => {
      const { host, send, exits } = fakeHost();
      installSignalTeardown(async () => {}, host, quiet);
      send("SIGTERM");
      await tick();
      expect(exits).toEqual([143]);
   });

   it("exits 129 on SIGHUP, when the terminal running it closes", async () => {
      const { host, send, exits } = fakeHost();
      let tornDown = 0;
      installSignalTeardown(async () => void tornDown++, host, quiet);
      send("SIGHUP");
      await tick();
      expect(tornDown).toBe(1);
      expect(exits).toEqual([129]);
   });

   it("exits at once on a second Ctrl-C while the teardown hangs", async () => {
      const { host, send, exits } = fakeHost();
      installSignalTeardown(() => new Promise(() => {}), host, quiet);
      send("SIGINT");
      send("SIGINT");
      expect(exits).toEqual([1]);
   });

   it("still exits when the teardown throws", async () => {
      const { host, send, exits } = fakeHost();
      installSignalTeardown(
         async () => {
            throw new Error("docker gone");
         },
         host,
         quiet,
      );
      send("SIGINT");
      await tick();
      await tick();
      expect(exits).toEqual([130]);
   });

   it("removes its handlers when the run finishes normally", () => {
      const { host, listeners } = fakeHost();
      const remove = installSignalTeardown(async () => {}, host, quiet);
      remove();
      expect(listeners.get("SIGINT")?.size).toBe(0);
      expect(listeners.get("SIGTERM")?.size).toBe(0);
      expect(listeners.get("SIGHUP")?.size).toBe(0);
   });
});
