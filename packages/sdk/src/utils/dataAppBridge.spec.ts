// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { afterEach, beforeEach, describe, expect, it, jest } from "bun:test";
import {
   createDataAppBridge,
   type DataAppBridge,
   type DataAppBridgeOptions,
} from "./dataAppBridge";
import fixture from "./dataAppBus.fixture.json";

type Json = Record<string, any>; // eslint-disable-line @typescript-eslint/no-explicit-any
type Posted = { msg: Json; targetOrigin: string };

const APP = fixture.appOrigin;
const SRC = `${APP}/environments/e/packages/p/index.html`;

const advance = (ms: number) =>
   (
      jest as unknown as { advanceTimersByTime(ms: number): void }
   ).advanceTimersByTime(ms);
const flush = async () => {
   for (let i = 0; i < 8; i++) await Promise.resolve();
};
const plain = (v: unknown): Json => JSON.parse(JSON.stringify(v));

// A fake contentWindow with a postMessage spy, and hand-built MessageEvents: the DOM's own
// postMessage would check origins and stamp source and origin itself.
function mount(opts: DataAppBridgeOptions = { src: SRC }) {
   const posts: Posted[] = [];
   const child = {
      postMessage(msg: unknown, targetOrigin: string) {
         posts.push({ msg: plain(msg), targetOrigin });
      },
   };
   const iframe = document.createElement("iframe");
   Object.defineProperty(iframe, "contentWindow", { value: child });
   const bridge = createDataAppBridge(iframe, opts);
   const fromChild = (
      data: unknown,
      origin: string = opts.isolate ? "null" : APP,
      source: unknown = child,
   ) =>
      window.dispatchEvent(
         new MessageEvent("message", {
            data,
            origin,
            source: source as Window,
         }),
      );
   const msg = (type: string, extra: Json = {}) => ({
      protocol: "publisher",
      v: 1,
      type,
      nonce: bridge.nonce,
      ...extra,
   });
   const load = () => iframe.dispatchEvent(new Event("load"));
   const helloReply = (payload: Json = { handles: [], runtime: "1" }) =>
      fromChild(msg("hello", { id: "hello-1", reply: true, payload }));
   const ofType = (type: string) => posts.filter((p) => p.msg.type === type);
   return {
      bridge,
      iframe,
      child,
      posts,
      fromChild,
      msg,
      load,
      helloReply,
      ofType,
   };
}

// "$NONCE" and "$ID" in the shared fixture stand for the embed's nonce and the request id.
function fill(value: unknown, subs: Record<string, string>): unknown {
   if (typeof value === "string") return subs[value] ?? value;
   if (Array.isArray(value)) return value.map((v) => fill(v, subs));
   if (value && typeof value === "object") {
      return Object.fromEntries(
         Object.entries(value).map(([k, v]) => [k, fill(v, subs)]),
      );
   }
   return value;
}

let live: DataAppBridge[] = [];
const track = <T extends { bridge: DataAppBridge }>(m: T): T => {
   live.push(m.bridge);
   return m;
};

beforeEach(() => {
   jest.useFakeTimers();
});
afterEach(() => {
   live.forEach((b) => b.destroy());
   live = [];
   jest.useRealTimers();
});

describe("createDataAppBridge", () => {
   it("needs an origin unless isolated", () => {
      const iframe = document.createElement("iframe");
      expect(() => createDataAppBridge(iframe)).toThrow(TypeError);
      expect(() =>
         createDataAppBridge(iframe, { isolate: true }).destroy(),
      ).not.toThrow();
   });

   it("takes the origin from src or from origin", async () => {
      const a = track(mount({ src: SRC }));
      const b = track(mount({ origin: `${APP}/ignored/path` }));
      for (const m of [a, b]) {
         m.load();
         m.helloReply();
         await expect(m.bridge.ready).resolves.toEqual({
            handles: [],
            runtime: "1",
         });
         expect(m.posts[0].targetOrigin).toBe(APP);
      }
   });
});

describe("srcWithNonce", () => {
   it("puts a random nonce in the fragment, merging with an existing fragment", () => {
      const a = track(mount());
      const b = track(mount());
      expect(a.bridge.nonce).toMatch(/^[0-9a-f]{32}$/);
      expect(a.bridge.nonce).not.toBe(b.bridge.nonce);
      expect(a.bridge.srcWithNonce(SRC)).toBe(
         `${SRC}#publisher-nonce=${a.bridge.nonce}`,
      );
      expect(a.bridge.srcWithNonce(`${SRC}#a=1&publisher-nonce=old&b=2`)).toBe(
         `${SRC}#a=1&b=2&publisher-nonce=${a.bridge.nonce}`,
      );
      expect(a.bridge.srcWithNonce(`${SRC}?x=1#sec`)).toBe(
         `${SRC}?x=1#sec&publisher-nonce=${a.bridge.nonce}`,
      );
   });

   it("uses the nonce it is given", () => {
      const m = track(mount({ src: SRC, nonce: "fixed" }));
      expect(m.bridge.nonce).toBe("fixed");
   });
});

describe("handshake", () => {
   it("says hello on load, retries every 500 ms up to five times, and stops once the page replies", () => {
      const m = track(mount());
      expect(m.posts).toEqual([]);
      m.load();
      expect(m.posts).toEqual([
         {
            targetOrigin: APP,
            msg: { protocol: "publisher", v: 1, type: "hello", id: "hello-1" },
         },
      ]);
      advance(500);
      expect(m.ofType("hello").map((p) => p.msg.id)).toEqual([
         "hello-1",
         "hello-2",
      ]);
      m.helloReply();
      advance(10000);
      expect(m.ofType("hello")).toHaveLength(2);
   });

   it("gives up after five hellos", () => {
      const m = track(mount());
      m.load();
      advance(10000);
      expect(m.ofType("hello")).toHaveLength(5);
   });

   it("resolves ready with the reply's payload and records what the page handles", async () => {
      const m = track(mount());
      m.load();
      expect(m.bridge.handles()).toEqual([]);
      m.helloReply({ handles: ["givens.set", "chat.prefill"], runtime: "1" });
      await expect(m.bridge.ready).resolves.toEqual({
         handles: ["givens.set", "chat.prefill"],
         runtime: "1",
      });
      expect(m.bridge.handles()).toEqual(["givens.set", "chat.prefill"]);
   });

   it("updates handles from a later ready event", () => {
      const m = track(mount());
      m.load();
      m.helloReply();
      m.fromChild(
         m.msg("ready", { payload: { handles: ["a"], runtime: "1" } }),
      );
      expect(m.bridge.handles()).toEqual(["a"]);
      const copy = m.bridge.handles();
      copy.push("mutated");
      expect(m.bridge.handles()).toEqual(["a"]);
   });

   it("answers a page's unsolicited ready with a hello", () => {
      const m = track(mount());
      m.fromChild(m.msg("ready", { payload: { handles: [], runtime: "1" } }));
      expect(m.ofType("hello")).toHaveLength(1);
   });

   it("re-handshakes when the frame reloads", async () => {
      let tokens = 0;
      const m = track(mount({ src: SRC, getToken: () => `t${++tokens}` }));
      m.load();
      m.helloReply();
      await flush();
      expect(tokens).toBe(1);
      m.posts.length = 0;
      m.load();
      expect(m.posts.map((p) => p.msg.type)).toEqual(["hello"]);
      m.helloReply();
      await flush();
      expect(tokens).toBe(2);
   });
});

describe("what the host accepts, from the shared fixture", () => {
   for (const isolate of [false, true]) {
      for (const c of fixture.events) {
         it(`${isolate ? "isolated: " : ""}${c.name}`, () => {
            const m = track(mount({ src: SRC, isolate }));
            const seen: unknown[] = [];
            m.bridge.on("probe.event", (p) => seen.push(p));
            const other = { postMessage() {} };
            m.fromChild(
               fill(c.message, { $NONCE: m.bridge.nonce }),
               c.origin,
               c.source === "iframe" ? m.child : other,
            );
            expect(seen).toHaveLength(
               (isolate ? c.acceptIsolated : c.accept) ? 1 : 0,
            );
         });
      }
   }

   for (const c of fixture.handshake) {
      it(`handshake: ${c.name}`, async () => {
         const m = track(mount());
         const probe = m.bridge.send("probe", {});
         probe.catch(() => {});
         m.fromChild(fill(c.message, { $NONCE: m.bridge.nonce }));
         await flush();
         expect(m.ofType("probe").length > 0).toBe(c.probePosted);
         expect(m.ofType("hello").length > 0).toBe(c.helloPosted);
      });
   }

   for (const c of fixture.replies) {
      it(`reply: ${c.name}`, async () => {
         const m = track(mount());
         m.load();
         m.helloReply();
         m.posts.length = 0;
         const result = m.bridge
            .send("probe.request", { a: 1 })
            .then((value) => ({ value }))
            .catch((error: Json) => ({ error }));
         const id = m.ofType("probe.request")[0].msg.id;
         m.fromChild(fill(c.message, { $NONCE: m.bridge.nonce, $ID: id }));
         if ("timesOut" in c.outcome) advance(5000);
         const settled = (await result) as Json;
         if ("resolves" in c.outcome) {
            expect(settled.value).toEqual(c.outcome.resolves);
         } else if ("rejectsWithCode" in c.outcome) {
            expect(settled.error.code).toBe(c.outcome.rejectsWithCode);
         } else {
            expect(settled.error.message).toContain("no reply to");
         }
      });
   }
});

describe("what the host sends", () => {
   it("never includes the nonce, in either mode and for every kind of post", async () => {
      for (const isolate of [false, true]) {
         const m = track(mount({ src: SRC, isolate, getToken: () => "tok" }));
         m.load();
         m.helloReply();
         m.bridge.on("chat.ask", () => 1);
         const sent = m.bridge.send("ping");
         m.fromChild(m.msg("auth.token.request"));
         m.fromChild(m.msg("chat.ask", { id: "p1" }));
         m.fromChild(m.msg("nobody.ask", { id: "p2" }));
         await flush();
         const types = new Set(m.posts.map((p) => p.msg.type));
         for (const t of [
            "hello",
            "auth.token",
            "ping",
            "chat.ask",
            "nobody.ask",
         ]) {
            expect(types.has(t)).toBe(true);
         }
         for (const p of m.posts) expect("nonce" in p.msg).toBe(false);
         m.bridge.destroy();
         await sent.catch(() => {});
      }
   });

   it("posts to the page's origin, or to * when isolated", async () => {
      const plainFrame = track(mount());
      plainFrame.load();
      expect(plainFrame.posts[0].targetOrigin).toBe(APP);

      const isolated = track(mount({ isolate: true, getToken: () => "t" }));
      isolated.load();
      isolated.helloReply();
      await flush();
      expect(isolated.posts.length).toBeGreaterThan(1);
      for (const p of isolated.posts) expect(p.targetOrigin).toBe("*");
   });

   it("accepts only the null origin when isolated", async () => {
      let tokens = 0;
      const m = track(
         mount({ isolate: true, getToken: () => String(++tokens) }),
      );
      m.load();
      // From the app's origin: not what a sandboxed frame reports.
      m.fromChild(m.msg("hello", { id: "hello-1", reply: true }), APP);
      await flush();
      expect(tokens).toBe(0);
      m.fromChild(m.msg("hello", { id: "hello-1", reply: true }), "null");
      await flush();
      expect(tokens).toBe(1);
      expect(m.posts.at(-1)!.msg.type).toBe("auth.token");
   });
});

describe("send", () => {
   it("waits for ready, resolves with the reply, and rejects an error reply with its code", async () => {
      const m = track(mount());
      m.load();
      m.posts.length = 0;
      const ok = m.bridge.send("givens.set", { givens: { a: 1 } });
      const bad = m.bridge.send("nobody", {});
      expect(m.posts).toEqual([]); // queued until the page has answered hello
      m.helloReply();
      await flush();
      const sent = m.posts.filter((p) => p.msg.type !== "auth.token");
      expect(sent.map((p) => p.msg.type)).toEqual(["givens.set", "nobody"]);
      expect(sent[0].msg.payload).toEqual({ givens: { a: 1 } });
      m.fromChild(
         m.msg("givens.set", {
            id: sent[0].msg.id,
            reply: true,
            payload: { applied: true },
         }),
      );
      m.fromChild(
         m.msg("nobody", {
            id: sent[1].msg.id,
            reply: true,
            error: { code: "unhandled", message: "No handler for 'nobody'" },
         }),
      );
      expect(await ok).toEqual({ applied: true });
      const err = (await bad.catch((e: Json) => e)) as Json;
      expect(err.code).toBe("unhandled");
   });

   it("rejects after 5 s and ignores a late reply", async () => {
      const m = track(mount());
      m.load();
      m.helloReply();
      m.posts.length = 0;
      const settled = m.bridge.send("ping", {}).catch((e: Json) => e);
      const id = m.posts[0].msg.id;
      advance(5000);
      expect(((await settled) as Json).message).toContain("no reply to 'ping'");
      m.fromChild(m.msg("ping", { id, reply: true, payload: 1 })); // no throw
   });

   it("rejects when the page never becomes ready", async () => {
      const m = track(mount());
      const settled = m.bridge.send("ping").catch((e: Json) => e);
      advance(5000);
      expect(((await settled) as Json).message).toContain("no reply to 'ping'");
      expect(m.posts).toEqual([]);
   });
});

describe("on", () => {
   it("receives the page's events and answers its requests", async () => {
      const m = track(mount());
      const seen: unknown[] = [];
      const off = m.bridge.on("query.ran", (p) => seen.push(plain(p)));
      m.bridge.on("chat.ask", () => ({ ok: 1 }));
      m.bridge.on("chat.fail", () => {
         throw new Error("boom");
      });
      m.fromChild(m.msg("query.ran", { payload: { model: "m", rowCount: 2 } }));
      expect(seen).toEqual([{ model: "m", rowCount: 2 }]);
      off();
      m.fromChild(m.msg("query.ran", { payload: { model: "m2" } }));
      expect(seen).toHaveLength(1);

      m.fromChild(m.msg("chat.ask", { id: "p1" }));
      m.fromChild(m.msg("chat.fail", { id: "p2" }));
      m.fromChild(m.msg("other.ask", { id: "p3" }));
      m.fromChild(m.msg("other.event"));
      await flush();
      const byId = Object.fromEntries(m.posts.map((p) => [p.msg.id, p.msg]));
      expect(byId.p1).toEqual({
         protocol: "publisher",
         v: 1,
         type: "chat.ask",
         id: "p1",
         reply: true,
         payload: { ok: 1 },
      });
      expect(byId.p2.error).toEqual({ code: "handler_error", message: "boom" });
      expect(byId.p3.error.code).toBe("unhandled");
      expect(m.posts).toHaveLength(3);
   });
});

describe("the token", () => {
   it("is sent when the page is ready, and not without getToken", async () => {
      const calls: number[] = [];
      const m = track(
         mount({
            src: SRC,
            getToken: async () => {
               calls.push(1);
               return "tok-a";
            },
         }),
      );
      m.load();
      m.posts.length = 0;
      m.helloReply();
      await flush();
      expect(m.posts).toEqual([
         {
            targetOrigin: APP,
            msg: {
               protocol: "publisher",
               v: 1,
               type: "auth.token",
               payload: { token: "tok-a" },
            },
         },
      ]);
      expect(calls).toHaveLength(1);

      const bare = track(mount());
      bare.load();
      bare.posts.length = 0;
      bare.helloReply();
      await flush();
      expect(bare.posts).toEqual([]);
   });

   it("answers auth.token.request with a fresh token, tolerating a failing getToken", async () => {
      let n = 0;
      let fail = false;
      const m = track(
         mount({
            src: SRC,
            getToken: () => {
               if (fail) throw new Error("no token");
               return `tok-${++n}`;
            },
         }),
      );
      m.load();
      m.helloReply();
      await flush();
      m.posts.length = 0;
      m.fromChild(m.msg("auth.token.request"));
      await flush();
      expect(m.posts.map((p) => p.msg.payload)).toEqual([{ token: "tok-2" }]);
      fail = true;
      m.fromChild(m.msg("auth.token.request"));
      await flush();
      expect(m.posts).toHaveLength(1);
   });

   it("is withheld from a request that does not carry the nonce", async () => {
      let tokens = 0;
      const m = track(mount({ src: SRC, getToken: () => String(++tokens) }));
      m.load();
      m.helloReply();
      await flush();
      expect(tokens).toBe(1);
      m.posts.length = 0;
      m.fromChild({ ...m.msg("auth.token.request"), nonce: undefined });
      m.fromChild(m.msg("auth.token.request", { nonce: "wrong" }));
      await flush();
      expect(tokens).toBe(1);
      expect(m.posts).toEqual([]);
   });

   it("is withheld from a page that has not proved the nonce after a reload", async () => {
      let tokens = 0;
      const m = track(mount({ src: SRC, getToken: () => String(++tokens) }));
      m.load();
      m.helloReply();
      await flush();
      expect(tokens).toBe(1);
      // The frame navigated to a document that never had the fragment.
      m.load();
      m.posts.length = 0;
      const stranger = (type: string, extra: Json = {}) => ({
         ...m.msg(type, extra),
         nonce: undefined,
      });
      m.fromChild(stranger("hello", { id: "hello-1", reply: true }));
      m.fromChild(
         m.msg("hello", { id: "hello-1", reply: true, nonce: "wrong" }),
      );
      m.fromChild(stranger("ready"));
      m.fromChild(stranger("auth.token.request"));
      const queued = m.bridge.send("secret", {});
      queued.catch(() => {});
      await flush();
      expect(tokens).toBe(1);
      expect(m.posts.filter((p) => p.msg.type !== "hello")).toEqual([]);
      // The real page, with the fragment, still proves itself.
      m.load();
      m.helloReply();
      await flush();
      expect(tokens).toBe(2);
   });
});

describe("destroy", () => {
   it("rejects pending sends and ready, stops listening and timing", async () => {
      let tokens = 0;
      const m = mount({ src: SRC, getToken: () => String(++tokens) });
      m.load();
      const pending = m.bridge.send("ping").catch((e: Json) => e);
      const ready = m.bridge.ready.catch((e: Json) => e);
      m.bridge.destroy();
      expect(((await pending) as Json).message).toContain("destroyed");
      expect(((await ready) as Json).message).toContain("destroyed");
      m.helloReply();
      m.load();
      advance(10000);
      await flush();
      expect(tokens).toBe(0);
      expect(m.ofType("hello")).toHaveLength(1);
      await expect(m.bridge.send("again")).rejects.toThrow("destroyed");
      m.bridge.destroy(); // idempotent
   });

   it("leaves a settled ready alone", async () => {
      const m = mount();
      m.load();
      m.helloReply();
      m.bridge.destroy();
      await expect(m.bridge.ready).resolves.toBeDefined();
   });
});
