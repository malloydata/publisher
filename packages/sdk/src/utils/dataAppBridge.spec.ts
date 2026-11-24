// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { afterEach, beforeEach, describe, expect, it, jest } from "bun:test";
import {
   createDataAppBridge,
   createDataAppNonce,
   type DataAppBridge,
   type DataAppBridgeOptions,
   withDataAppNonce,
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
function mount(opts: DataAppBridgeOptions = { src: SRC }, iframeSrc?: string) {
   const posts: Posted[] = [];
   const child = {
      postMessage(msg: unknown, targetOrigin: string) {
         posts.push({ msg: plain(msg), targetOrigin });
      },
   };
   const iframe = document.createElement("iframe");
   Object.defineProperty(iframe, "contentWindow", { value: child });
   if (iframeSrc !== undefined) iframe.setAttribute("src", iframeSrc);
   const bridge = createDataAppBridge(iframe, opts);
   const fromChild = (
      data: unknown,
      origin: string = APP,
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
      fromChild(msg("hello", { id: "hello", reply: true, payload }));
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
   it("needs an origin, and one a post can name", () => {
      const iframe = document.createElement("iframe");
      expect(() => createDataAppBridge(iframe)).toThrow(TypeError);
      expect(() =>
         createDataAppBridge(iframe, { src: "data:text/html,hi" }),
      ).toThrow("http(s) origin");
      // there is no isolate option: it neither relaxes the origin nor makes a post go to "*"
      expect(() =>
         createDataAppBridge(iframe, { isolate: true } as DataAppBridgeOptions),
      ).toThrow(TypeError);
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

describe("the nonce helpers", () => {
   it("createDataAppNonce is random hex", () => {
      expect(createDataAppNonce()).toMatch(/^[0-9a-f]{32}$/);
      expect(createDataAppNonce()).not.toBe(createDataAppNonce());
   });

   it("withDataAppNonce keeps every other fragment byte and replaces an old nonce", () => {
      expect(withDataAppNonce(SRC, "n1")).toBe(`${SRC}#publisher-nonce=n1`);
      expect(withDataAppNonce(`${SRC}#/route`, "n1")).toBe(
         `${SRC}#/route&publisher-nonce=n1`,
      );
      expect(
         withDataAppNonce(`${SRC}?x=1#a=1&publisher-nonce=old&b=2`, "n1"),
      ).toBe(`${SRC}?x=1#a=1&b=2&publisher-nonce=n1`);
      expect(withDataAppNonce(`${SRC}#publisher-nonce=old`, "n1")).toBe(
         `${SRC}#publisher-nonce=n1`,
      );
   });

   it("a bridge made with a nonce and its src agree, so a recreated bridge still matches the page", async () => {
      const nonce = createDataAppNonce();
      const src = withDataAppNonce(SRC, nonce);
      const first = mount({ src, nonce }, src);
      first.bridge.destroy();
      const second = track(mount({ src, nonce }, src));
      expect(second.bridge.srcWithNonce(SRC)).toBe(src);
      second.helloReply();
      await expect(second.bridge.ready).resolves.toBeDefined();
   });
});

describe("handshake", () => {
   it("says hello once on each load, and never retries", () => {
      const m = track(mount());
      expect(m.posts).toEqual([]);
      m.load();
      expect(m.posts).toEqual([
         {
            targetOrigin: APP,
            msg: { protocol: "publisher", v: 1, type: "hello", id: "hello" },
         },
      ]);
      advance(10000);
      expect(m.ofType("hello")).toHaveLength(1);
      m.load();
      expect(m.ofType("hello")).toHaveLength(2);
   });

   it("greets a frame that had already loaded when the bridge was created", () => {
      const m = track(mount({ src: SRC }, `${SRC}#publisher-nonce=n`));
      expect(m.ofType("hello")).toHaveLength(1);
      expect(m.posts[0].targetOrigin).toBe(APP);
      m.helloReply();
      advance(10000);
      expect(m.ofType("hello")).toHaveLength(1);
   });

   it("does not greet a frame with no src, or one still on about:blank", () => {
      expect(track(mount({ src: SRC })).posts).toEqual([]);
      expect(track(mount({ src: SRC }, "")).posts).toEqual([]);
      expect(track(mount({ src: SRC }, "about:blank")).posts).toEqual([]);
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
   for (const c of fixture.events) {
      it(c.name, () => {
         const m = track(mount({ src: SRC }));
         const seen: unknown[] = [];
         m.bridge.on("probe.event", (p) => seen.push(p));
         const other = { postMessage() {} };
         m.fromChild(
            fill(c.message, { $NONCE: m.bridge.nonce }),
            c.origin,
            c.source === "iframe" ? m.child : other,
         );
         expect(seen).toHaveLength(c.accept ? 1 : 0);
      });
   }

   for (const c of fixture.handshake) {
      it(`handshake: ${c.name}`, async () => {
         const m = track(mount());
         const probe = m.bridge
            .send("probe", {})
            .then((value) => ({ value }))
            .catch((error: Json) => ({ error }));
         if ("loadFirst" in c && c.loadFirst) m.load();
         m.fromChild(fill(c.message, { $NONCE: m.bridge.nonce }));
         await flush();
         expect(m.ofType("probe").length > 0).toBe(c.probePosted);
         expect(m.ofType("hello").length > 0).toBe(c.helloPosted);
         if ("loadFirst" in c && c.loadFirst && c.probePosted) {
            const id = m.ofType("probe")[0].msg.id;
            m.fromChild(m.msg("probe", { id, reply: true, payload: "ok" }));
            expect(await probe).toEqual({ value: "ok" });
         }
      });
   }

   for (const c of fixture.givensReplay) {
      it(`givens replay: ${c.name}`, async () => {
         const m = track(mount());
         m.load();
         m.helloReply();
         await flush();
         for (const step of c.before) {
            if ("hostSends" in step) {
               m.bridge
                  .send("givens.set", { givens: step.hostSends })
                  .catch(() => {});
            } else {
               m.fromChild(
                  m.msg("givens.changed", {
                     payload: { givens: step.pageReports },
                  }),
               );
            }
         }
         await flush();
         m.posts.length = 0;
         m.load();
         m.helloReply({ handles: c.rebindHandles, runtime: "1" });
         await flush();
         const replays = () => m.ofType("givens.set").map((p) => p.msg.payload);
         expect(replays()).toEqual(c.replayed ? [{ givens: c.replayed }] : []);
         for (const r of c.readies ?? []) {
            m.posts.length = 0;
            m.fromChild(
               m.msg("ready", {
                  payload: { handles: r.handles, runtime: "1" },
               }),
            );
            await flush();
            expect(replays()).toEqual(
               r.replayed ? [{ givens: r.replayed }] : [],
            );
         }
      });
   }

   it("replays nothing on the first bind, which sends only what was queued", async () => {
      const m = track(mount());
      m.load();
      m.bridge.send("givens.set", { givens: { a: 1 } }).catch(() => {});
      m.helloReply();
      await flush();
      expect(m.ofType("givens.set")).toHaveLength(1);
   });

   for (const c of fixture.tokenRequests) {
      it(`token request: ${c.name}`, async () => {
         const getToken =
            c.getToken === "throws"
               ? () => {
                    throw new Error("no token");
                 }
               : c.getToken
                 ? () => c.getToken as string
                 : undefined;
         const m = track(
            mount(getToken ? { src: SRC, getToken } : { src: SRC }),
         );
         m.load();
         m.helloReply();
         await flush();
         m.posts.length = 0;
         m.fromChild(fill(c.message, { $NONCE: m.bridge.nonce }));
         await flush();
         if (c.reply === null) {
            expect(m.posts).toEqual([]);
            return;
         }
         expect(m.posts).toHaveLength(1);
         const posted = m.posts[0].msg;
         expect(posted.type).toBe("auth.token.request");
         expect(posted.id).toBe((c.message as Json).id);
         expect(posted.reply).toBe(true);
         if ("payload" in c.reply) {
            expect(posted.payload).toEqual(c.reply.payload);
         } else {
            expect(posted.error.code).toBe(c.reply.error.code);
         }
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
   it("never includes the nonce, and posts to the page's exact origin, for every kind of post", async () => {
      const m = track(mount({ src: SRC, getToken: () => "tok" }));
      m.load();
      m.helloReply();
      m.bridge.on("chat.ask", () => 1);
      const sent = m.bridge.send("ping");
      m.fromChild(m.msg("auth.token.request", { id: "p0" }));
      m.fromChild(m.msg("chat.ask", { id: "p1" }));
      m.fromChild(m.msg("nobody.ask", { id: "p2" }));
      await flush();
      const types = new Set(m.posts.map((p) => p.msg.type));
      for (const t of [
         "hello",
         "auth.token",
         "auth.token.request",
         "ping",
         "chat.ask",
         "nobody.ask",
      ]) {
         expect(types.has(t)).toBe(true);
      }
      for (const p of m.posts) {
         expect("nonce" in p.msg).toBe(false);
         expect(p.targetOrigin).toBe(APP);
      }
      m.bridge.destroy();
      await sent.catch(() => {});
   });

   it("rejects a message from a sandboxed (null-origin) document", async () => {
      let tokens = 0;
      const m = track(mount({ src: SRC, getToken: () => String(++tokens) }));
      m.load();
      m.fromChild(m.msg("hello", { id: "hello", reply: true }), "null");
      await flush();
      expect(tokens).toBe(0);
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

   it("issued before the frame's first load, still posts once the page replies", async () => {
      const m = track(mount());
      const sent = m.bridge.send("ping", { a: 1 });
      m.load();
      m.helloReply();
      await flush();
      const ping = m.ofType("ping");
      expect(ping).toHaveLength(1);
      m.fromChild(
         m.msg("ping", { id: ping[0].msg.id, reply: true, payload: "pong" }),
      );
      expect(await sent).toBe("pong");
   });

   it("queued across a reload, posts to the document that answers", async () => {
      const m = track(mount());
      m.load();
      m.helloReply();
      await flush();
      m.load(); // navigated: not ready again until the new document answers
      m.posts.length = 0;
      const sent = m.bridge.send("ping");
      expect(m.ofType("ping")).toEqual([]);
      m.helloReply();
      await flush();
      const ping = m.ofType("ping");
      expect(ping).toHaveLength(1);
      m.fromChild(
         m.msg("ping", { id: ping[0].msg.id, reply: true, payload: 1 }),
      );
      expect(await sent).toBe(1);
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

   it("answers the request auth.token.request with a fresh token, and with handler_error when getToken fails", async () => {
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
      m.fromChild(m.msg("auth.token.request", { id: "p1" }));
      await flush();
      expect(m.posts).toEqual([
         {
            targetOrigin: APP,
            msg: {
               protocol: "publisher",
               v: 1,
               type: "auth.token.request",
               id: "p1",
               reply: true,
               payload: { token: "tok-2" },
            },
         },
      ]);
      fail = true;
      m.posts.length = 0;
      m.fromChild(m.msg("auth.token.request", { id: "p2" }));
      await flush();
      expect(m.posts).toHaveLength(1);
      expect(m.posts[0].msg.error.code).toBe("handler_error");
   });

   it("answers a token request unhandled when there is no getToken, so the page fails at once", async () => {
      const m = track(mount());
      m.load();
      m.helloReply();
      m.posts.length = 0;
      m.fromChild(m.msg("auth.token.request", { id: "p1" }));
      await flush();
      expect(m.posts).toHaveLength(1);
      expect(m.posts[0].msg.reply).toBe(true);
      expect(m.posts[0].msg.error.code).toBe("unhandled");
   });

   it("is withheld from a request that does not carry the nonce", async () => {
      let tokens = 0;
      const m = track(mount({ src: SRC, getToken: () => String(++tokens) }));
      m.load();
      m.helloReply();
      await flush();
      expect(tokens).toBe(1);
      m.posts.length = 0;
      m.fromChild({
         ...m.msg("auth.token.request", { id: "p1" }),
         nonce: undefined,
      });
      m.fromChild(m.msg("auth.token.request", { id: "p2", nonce: "wrong" }));
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
      m.fromChild(stranger("hello", { id: "hello", reply: true }));
      m.fromChild(m.msg("hello", { id: "hello", reply: true, nonce: "wrong" }));
      m.fromChild(stranger("ready"));
      m.fromChild(stranger("auth.token.request", { id: "p1" }));
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

describe("never posting across a reload", () => {
   const deferred = <T>() => {
      let resolve!: (v: T) => void;
      const promise = new Promise<T>((r) => (resolve = r));
      return { promise, resolve };
   };

   it("posts the token on the normal path", async () => {
      const d = deferred<string>();
      const m = track(mount({ src: SRC, getToken: () => d.promise }));
      m.load();
      m.helloReply();
      d.resolve("tok");
      await flush();
      expect(m.ofType("auth.token").map((p) => p.msg.payload)).toEqual([
         { token: "tok" },
      ]);
   });

   it("drops a token that resolves after the frame reloaded", async () => {
      const d = deferred<string>();
      const m = track(mount({ src: SRC, getToken: () => d.promise }));
      m.load();
      m.helloReply();
      m.load();
      d.resolve("tok");
      await flush();
      expect(m.ofType("auth.token")).toHaveLength(0);
   });

   it("drops a token that resolves after destroy", async () => {
      const d = deferred<string>();
      const m = track(mount({ src: SRC, getToken: () => d.promise }));
      m.load();
      m.helloReply();
      m.bridge.destroy();
      d.resolve("tok");
      await flush();
      expect(m.ofType("auth.token")).toHaveLength(0);
   });

   it("drops a handler reply that resolves after a reload, or after destroy", async () => {
      const d = deferred<unknown>();
      const m = track(mount());
      m.bridge.on("ask", () => d.promise);
      m.load();
      m.helloReply();
      m.fromChild(m.msg("ask", { id: "r1" }));
      m.load();
      d.resolve(1);
      await flush();
      expect(m.ofType("ask")).toHaveLength(0);

      const e = deferred<unknown>();
      const n = track(mount());
      n.bridge.on("ask", () => e.promise);
      n.load();
      n.helloReply();
      n.fromChild(n.msg("ask", { id: "r2" }));
      n.bridge.destroy();
      e.resolve(1);
      await flush();
      expect(n.ofType("ask")).toHaveLength(0);
   });

   it("drops a handler error reply that settles after a reload", async () => {
      let reject!: (e: Error) => void;
      const p = new Promise<unknown>((_, r) => (reject = r));
      const m = track(mount());
      m.bridge.on("ask", () => p);
      m.load();
      m.helloReply();
      m.fromChild(m.msg("ask", { id: "r1" }));
      m.load();
      reject(new Error("boom"));
      await flush();
      expect(m.ofType("ask")).toHaveLength(0);
   });

   it("still answers a handler in the same load", async () => {
      const m = track(mount());
      m.bridge.on("ask", async () => 7);
      m.load();
      m.helloReply();
      m.fromChild(m.msg("ask", { id: "r1" }));
      await flush();
      expect(m.ofType("ask").map((p) => p.msg.payload)).toEqual([7]);
   });
});
