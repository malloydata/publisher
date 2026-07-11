import { describe, expect, it } from "bun:test";

import { RpcWaitAccountant } from "./rpc_wait_accountant";

/** Accountant driven by a settable fake clock. */
function withClock(): { acc: RpcWaitAccountant; at: (ms: number) => void } {
   let t = 0;
   const acc = new RpcWaitAccountant(() => t);
   return { acc, at: (ms) => (t = ms) };
}

describe("RpcWaitAccountant", () => {
   it("brackets a single fetch's wait", () => {
      const { acc, at } = withClock();
      at(0);
      acc.begin("A");
      at(10);
      acc.noteStart("A");
      at(60);
      acc.noteSettle("A");
      expect(acc.waitMs).toBe(50);
   });

   it("counts overlapping fetches as their union, not the sum", () => {
      const { acc, at } = withClock();
      at(0);
      acc.begin("A");
      at(0);
      acc.noteStart("A"); // window opens
      at(10);
      acc.noteStart("A"); // second fetch, window stays open
      at(40);
      acc.noteSettle("A"); // one still outstanding
      at(60);
      acc.noteSettle("A"); // window closes
      expect(acc.waitMs).toBe(60); // union [0,60], not 30+50
   });

   it("accumulates disjoint fetch windows", () => {
      const { acc, at } = withClock();
      at(0);
      acc.begin("A");
      at(0);
      acc.noteStart("A");
      at(20);
      acc.noteSettle("A");
      at(50);
      acc.noteStart("A");
      at(70);
      acc.noteSettle("A");
      expect(acc.waitMs).toBe(40);
   });

   it("ignores a straggler fetch from a prior reused-worker load", () => {
      const { acc, at } = withClock();
      at(0);
      acc.begin("A");
      at(0);
      acc.noteStart("A"); // load A opens a fetch and never settles it (failed)

      at(100);
      acc.begin("B"); // worker reused for load B
      at(110);
      acc.noteStart("B");
      at(130);
      acc.noteSettle("B");

      at(140);
      acc.noteSettle("A"); // A's late response — must be a no-op

      expect(acc.waitMs).toBe(20); // only B's [110,130]
   });

   it("ignores a stray start from a non-active load", () => {
      const { acc, at } = withClock();
      at(0);
      acc.begin("B");
      at(10);
      acc.noteStart("A"); // wrong load
      at(50);
      acc.noteSettle("A");
      expect(acc.waitMs).toBe(0);
   });

   it("counts active-load fetches only", () => {
      const { acc, at } = withClock();
      at(0);
      acc.begin("B");
      acc.noteStart("B");
      acc.noteSettle("B");
      acc.noteStart("B");
      acc.noteStart("A"); // straggler — not counted
      expect(acc.fetches).toBe(2);
   });

   it("begin() discards prior-load state", () => {
      const { acc, at } = withClock();
      at(0);
      acc.begin("A");
      at(0);
      acc.noteStart("A");
      at(50);
      acc.noteSettle("A");
      expect(acc.waitMs).toBe(50);
      at(60);
      acc.begin("B");
      expect(acc.waitMs).toBe(0);
   });
});
