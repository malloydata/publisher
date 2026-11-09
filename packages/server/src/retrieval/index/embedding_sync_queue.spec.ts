// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { describe, expect, it } from "bun:test";
import { SerialQueue } from "./embedding_sync_queue";

describe("SerialQueue", () => {
   it("runs jobs one after another, in the order queued", async () => {
      const queue = new SerialQueue();
      const events: string[] = [];
      let releaseFirst!: () => void;
      const firstGate = new Promise<void>((resolve) => {
         releaseFirst = resolve;
      });
      const first = queue.enqueue(async () => {
         events.push("first:start");
         await firstGate;
         events.push("first:end");
      });
      const second = queue.enqueue(async () => {
         events.push("second:start");
         events.push("second:end");
      });
      // Let the event loop turn (every pending promise job runs first): the
      // second job must not start while the first is held.
      await new Promise((resolve) => setImmediate(resolve));
      expect(events).toEqual(["first:start"]);
      expect(queue.size).toBe(2);

      releaseFirst();
      await Promise.all([first, second]);
      expect(events).toEqual([
         "first:start",
         "first:end",
         "second:start",
         "second:end",
      ]);
      expect(queue.size).toBe(0);
   });

   it("keeps going after a job throws", async () => {
      const queue = new SerialQueue();
      const events: string[] = [];
      const failed = queue.enqueue(async () => {
         throw new Error("boom");
      });
      const after = queue.enqueue(async () => {
         events.push("after");
      });
      await Promise.all([failed, after]);
      expect(events).toEqual(["after"]);
   });
});
