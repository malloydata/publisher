// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { describe, expect, it, spyOn } from "bun:test";
import fs from "fs";
import { Window } from "happy-dom";
import path from "path";

/**
 * publisher.js is hand-written browser JS, so it is evaluated in a happy-dom
 * window. happy-dom's own postMessage throws on a mismatched targetOrigin and
 * stamps origin/source itself, so neither is used: the window's `parent` is a
 * spy, and inbound messages are hand-built MessageEvents. Browser-only
 * behavior (a silent targetOrigin drop, the "null" origin of a sandboxed
 * frame) is covered by the manual headless run, not here.
 */

const SOURCE = fs.readFileSync(path.join(__dirname, "publisher.js"), "utf8");

const PAGE = "https://pub.example.com";
const HOST = "https://host.example.com";
const NONCE = "0123456789abcdef0123456789abcdef";
const PAGE_URL = `${PAGE}/environments/env1/packages/pkg1/index.html#publisher-nonce=${NONCE}`;
const CONTEXT = {
   v: 1,
   environment: "env1",
   package: "pkg1",
   versionId: "1.0.3",
   apiBase: "/api/v0",
   hostOrigins: ["'self'", HOST, "https://*.apps.example.com:8443"],
};
const QUERY_URL = `${PAGE}/api/v0/environments/env1/packages/pkg1/models/m.malloy/query`;

type Json = any; // eslint-disable-line @typescript-eslint/no-explicit-any
interface Posted {
   msg: Json;
   targetOrigin: string;
}
interface FetchCall {
   url: string;
   init: Json;
}
type FetchReply = {
   status?: number;
   body?: Json;
   raw?: string;
};

// A virtual clock behind the window's setTimeout, so waits are advanced, not slept.
function makeClock() {
   let now = 0;
   let nextId = 1;
   const timers = new Map<number, { at: number; fn: () => void }>();
   return {
      setTimeout(fn: () => void, ms = 0) {
         const id = nextId++;
         timers.set(id, { at: now + ms, fn });
         return id;
      },
      clearTimeout(id: number) {
         timers.delete(id);
      },
      async advance(ms: number) {
         const target = now + ms;
         for (;;) {
            const due = [...timers.entries()]
               .filter(([, t]) => t.at <= target)
               .sort((a, b) => a[1].at - b[1].at)[0];
            if (!due) break;
            timers.delete(due[0]);
            now = due[1].at;
            due[1].fn();
            await flush();
         }
         now = target;
      },
      pending: () => timers.size,
      // happy-dom schedules a hashchange for history.replaceState; a browser fires none.
      runDue() {
         for (const [id, t] of [...timers]) {
            if (t.at <= now) {
               timers.delete(id);
               t.fn();
            }
         }
      },
   };
}

const flush = () => new Promise<void>((r) => setTimeout(r, 0));
const plain = (v: unknown): Json => JSON.parse(JSON.stringify(v));

function jsonResponse(reply: FetchReply) {
   const status = reply.status ?? 200;
   return {
      ok: status >= 200 && status < 300,
      status,
      statusText: status === 200 ? "OK" : "Error",
      json: async () => {
         if (reply.raw !== undefined) return JSON.parse(reply.raw);
         return reply.body;
      },
   };
}

const rowsResult = (n: number) => ({
   status: 200,
   body: {
      result: JSON.stringify(Array.from({ length: n }, (_, i) => ({ i }))),
   },
});

interface Boot {
   win: Json;
   clock: ReturnType<typeof makeClock>;
   posted: Posted[];
   resizes: Posted[];
   fetched: FetchCall[];
   replies: FetchReply[];
   Publisher: Json;
   parent: Json;
   readyPosts(): Posted[];
   send(data: unknown, origin?: string, source?: unknown): void;
   hello(origin?: string, id?: string): void;
   postedOfType(type: string): Posted[];
}

function boot(
   opts: {
      url?: string;
      context?: Json | string | null;
      iframe?: boolean;
      fetchReplies?: FetchReply[];
      eventSource?: boolean;
   } = {},
): Boot {
   const win: Json = new Window({
      url: opts.url ?? PAGE_URL,
      settings: { disableIframePageLoading: true },
   });
   const clock = makeClock();
   win.setTimeout = clock.setTimeout;
   win.clearTimeout = clock.clearTimeout;

   // `posted` is bus traffic; the legacy envelope-less resize message is kept apart in `resizes`.
   const posted: Posted[] = [];
   const resizes: Posted[] = [];
   const parent = {
      postMessage(msg: unknown, targetOrigin: string) {
         const entry = { msg: plain(msg), targetOrigin };
         (entry.msg.protocol === "publisher" ? posted : resizes).push(entry);
      },
   };
   if (opts.iframe !== false) {
      Object.defineProperty(win, "parent", { value: parent });
      Object.defineProperty(win, "top", { value: parent });
   }

   const context = opts.context === undefined ? CONTEXT : opts.context;
   if (context !== null) {
      const el = win.document.createElement("script");
      el.setAttribute("type", "application/json");
      el.setAttribute("id", "publisher-context");
      el.textContent =
         typeof context === "string" ? context : JSON.stringify(context);
      win.document.head.appendChild(el);
   }

   const fetched: FetchCall[] = [];
   const replies = [...(opts.fetchReplies ?? [])];
   win.fetch = async (url: string, init: Json) => {
      fetched.push({ url, init });
      return jsonResponse(replies.shift() ?? rowsResult(1));
   };
   if (opts.eventSource) {
      win.EventSource = class {
         static created: string[] = [];
         static listeners: Record<string, () => void> = {};
         constructor(url: string) {
            (win.EventSource as Json).created.push(url);
         }
         addEventListener(name: string, fn: () => void) {
            (win.EventSource as Json).listeners[name] = fn;
         }
      };
   }

   win.eval(SOURCE);
   clock.runDue();

   const send = (data: unknown, origin = HOST, source: unknown = parent) => {
      win.dispatchEvent(
         new win.MessageEvent("message", { data, origin, source }),
      );
   };
   const envelope = (type: string, extra: Json = {}) => ({
      protocol: "publisher",
      v: 1,
      type,
      ...extra,
   });
   return {
      win,
      clock,
      posted,
      resizes,
      fetched,
      replies,
      Publisher: win.Publisher,
      parent,
      send,
      hello: (origin = HOST, id = "h1") =>
         send(envelope("hello", { id }), origin),
      readyPosts: () => posted.filter((p) => p.msg.type === "ready"),
      postedOfType: (type: string) => posted.filter((p) => p.msg.type === type),
   };
}

const envelope = (type: string, extra: Json = {}) => ({
   protocol: "publisher",
   v: 1,
   type,
   ...extra,
});

function boundPage(opts: Parameters<typeof boot>[0] = {}) {
   const page = boot(opts);
   page.hello();
   page.posted.length = 0;
   return page;
}

describe("page context", () => {
   it("takes version and apiBase from the injected context, and names from the URL", async () => {
      const page = boot({
         url: `${PAGE}/environments/urlenv/packages/urlpkg/index.html`,
         context: { ...CONTEXT, apiBase: "/gateway/api/v0/" },
         fetchReplies: [rowsResult(1)],
      });
      expect(plain(page.Publisher.context)).toEqual({
         environment: "urlenv",
         package: "urlpkg",
      });
      await page.Publisher.query("m.malloy", "run: x");
      expect(page.fetched).toHaveLength(1); // no pinnedVersion probe
      expect(page.fetched[0].url).toBe(
         `${PAGE}/gateway/api/v0/environments/urlenv/packages/urlpkg/models/m.malloy/query`,
      );
      expect(JSON.parse(page.fetched[0].init.body).versionId).toBe("1.0.3");
   });

   it("lets the URL's names win on every path shape the runtime knows", () => {
      for (const path of [
         "/environments/urlenv/packages/urlpkg/index.html",
         "/environments/urlenv/packages/urlpkg/versions/1.0.3/public/index.html",
         "/apps/urlenv/urlpkg/index.html",
      ]) {
         const page = boot({ url: `${PAGE}${path}` });
         expect(plain(page.Publisher.context)).toEqual({
            environment: "urlenv",
            package: "urlpkg",
         });
      }
   });

   it("uses the context's names only when the URL matches no known shape", async () => {
      const page = boot({ url: `${PAGE}/index.html` });
      expect(plain(page.Publisher.context)).toEqual({
         environment: "env1",
         package: "pkg1",
      });
      await page.Publisher.query("m.malloy", "run: x");
      expect(page.fetched[0].url).toBe(QUERY_URL);
   });

   it("pins the version named in a versioned path when no context was injected", async () => {
      const page = boot({
         context: null,
         url: `${PAGE}/environments/env1/packages/pkg1/versions/1.0.3/public/index.html`,
         fetchReplies: [rowsResult(1)],
      });
      await page.Publisher.query("m.malloy", "run: x");
      expect(page.fetched).toHaveLength(1); // no ?versionId= probe
      expect(JSON.parse(page.fetched[0].init.body).versionId).toBe("1.0.3");
      // another package is not pinned to this page's version
      await page.Publisher.query("m.malloy", "run: x", { package: "other" });
      expect(JSON.parse(page.fetched[1].init.body).versionId).toBeUndefined();
   });

   it("takes neither latest nor live in a versioned path for a version", async () => {
      for (const reserved of ["latest", "live"]) {
         const page = boot({
            eventSource: true,
            context: null,
            url: `${PAGE}/environments/env1/packages/pkg1/versions/${reserved}/public/index.html`,
         });
         await page.Publisher.query("m.malloy", "run: x");
         expect(
            JSON.parse(page.fetched[0].init.body).versionId,
         ).toBeUndefined();
         expect(page.win.EventSource.created).toHaveLength(1);
      }
      const named = boot({
         context: null,
         url: `${PAGE}/environments/env1/packages/pkg1/versions/latest-1/public/index.html`,
      });
      await named.Publisher.query("m.malloy", "run: x");
      expect(JSON.parse(named.fetched[0].init.body).versionId).toBe("latest-1");
   });

   it("prefers the context's version over the path's", async () => {
      const page = boot({
         context: { ...CONTEXT, versionId: "1.0.9" },
         url: `${PAGE}/environments/env1/packages/pkg1/versions/1.0.3/public/index.html`,
      });
      await page.Publisher.query("m.malloy", "run: x");
      expect(JSON.parse(page.fetched[0].init.body).versionId).toBe("1.0.9");
   });

   it("never takes an apiBase that is not a same-origin path", async () => {
      const page = boot({
         context: { ...CONTEXT, apiBase: "https://evil.example.com/api" },
      });
      await page.Publisher.query("m.malloy", "run: x");
      expect(page.fetched[0].url).toBe(QUERY_URL);
   });

   it("falls back to the URL and the ?versionId= probe when there is no context", async () => {
      const page = boot({
         url: `${PAGE}/environments/demo%20env/packages/my%20pkg/index.html?versionId=2.0.0`,
         context: null,
         fetchReplies: [{ body: { versionId: "2.0.0" } }, rowsResult(1)],
      });
      expect(plain(page.Publisher.context)).toEqual({
         environment: "demo env",
         package: "my pkg",
      });
      await page.Publisher.query("m.malloy", "run: x");
      expect(page.fetched).toHaveLength(2);
      expect(page.fetched[0].url).toBe(
         `${PAGE}/api/v0/environments/demo%20env/packages/my%20pkg?versionId=2.0.0`,
      );
      expect(JSON.parse(page.fetched[1].init.body).versionId).toBe("2.0.0");
   });

   it("falls back when the context is malformed or lacks names", () => {
      for (const context of [
         "{not json",
         "[]",
         JSON.stringify({ v: 1, apiBase: "/api/v0" }),
      ]) {
         const page = boot({ context });
         expect(plain(page.Publisher.context)).toEqual({
            environment: "env1",
            package: "pkg1",
         });
      }
   });

   it("aims a query at another package without the page's version", async () => {
      const page = boot();
      await page.Publisher.query("m.malloy", "run: x", { package: "other" });
      expect(JSON.parse(page.fetched[0].init.body).versionId).toBeUndefined();
   });

   it("does not live-reload a versioned page, but does an unversioned one", () => {
      const versioned = boot({ eventSource: true });
      expect(versioned.win.EventSource.created).toEqual([]);

      const unversioned = boot({
         eventSource: true,
         context: { ...CONTEXT, versionId: null },
      });
      expect(unversioned.win.EventSource.created).toHaveLength(1);

      const noContext = boot({ eventSource: true, context: null });
      expect(noContext.win.EventSource.created).toHaveLength(1);

      const versionedPath = boot({
         eventSource: true,
         context: null,
         url: `${PAGE}/environments/env1/packages/pkg1/versions/1.0.3/public/index.html`,
      });
      expect(versionedPath.win.EventSource.created).toEqual([]);
      expect(plain(versionedPath.Publisher.context)).toEqual({
         environment: "env1",
         package: "pkg1",
      });
   });
});

describe("host origin patterns", () => {
   const cases: [string[], string, boolean][] = [
      [["'self'"], PAGE, true],
      [["'self'"], "https://other.example.com", false],
      [[HOST], HOST, true],
      [[HOST], "https://host.example.com:444", false],
      [[HOST], "http://host.example.com", false],
      [
         ["https://*.apps.example.com:8443"],
         "https://a.apps.example.com:8443",
         true,
      ],
      [
         ["https://*.apps.example.com:8443"],
         "https://a.b.apps.example.com:8443",
         true,
      ],
      [
         ["https://*.apps.example.com:8443"],
         "https://apps.example.com:8443",
         false,
      ],
      [
         ["https://*.apps.example.com:8443"],
         "https://evilapps.example.com:8443",
         false,
      ],
      [
         ["https://*.apps.example.com:8443"],
         "https://a.apps.example.com",
         false,
      ],
      [
         ["https://*.apps.example.com:8443"],
         "http://a.apps.example.com:8443",
         false,
      ],
      [
         ["https://*.apps.example.com:8443"],
         "https://a.apps.example.com.evil.com:8443",
         false,
      ],
      [["https://*.apps.example.com"], "https://a.apps.example.com", true],
      [
         ["https://*.apps.example.com"],
         "https://a.apps.example.com:8443",
         false,
      ],
      [["https://*.apps.example.com"], "null", false],
      [["'self'", HOST], "null", false],
   ];
   for (const [patterns, origin, accepted] of cases) {
      it(`${accepted ? "accepts" : "rejects"} ${origin} against ${patterns.join(" ")}`, () => {
         const page = boot({ context: { ...CONTEXT, hostOrigins: patterns } });
         page.posted.length = 0;
         page.hello(origin);
         expect(page.posted).toHaveLength(accepted ? 1 : 0);
         if (accepted) {
            expect(page.posted[0].targetOrigin).toBe(origin);
         }
      });
   }
});

describe("handshake", () => {
   it("posts an unsolicited ready only to exact origins, never to a wildcard or *", () => {
      const page = boot();
      expect(page.posted.map((p) => p.targetOrigin)).toEqual([PAGE, HOST]);
      for (const p of page.posted) {
         expect(p.msg).toEqual({
            protocol: "publisher",
            v: 1,
            type: "ready",
            nonce: NONCE,
            payload: { handles: [], runtime: "1" },
         });
      }
   });

   it("posts nothing when only wildcards are allowed, outside an iframe, or with no origins", () => {
      expect(
         boot({
            context: {
               ...CONTEXT,
               hostOrigins: ["https://*.apps.example.com"],
            },
         }).posted,
      ).toEqual([]);
      const top = boot({ iframe: false });
      expect(top.posted).toEqual([]);
      top.hello(); // no listener: the bus is off
      expect(top.posted).toEqual([]);
      expect(boot({ context: { ...CONTEXT, hostOrigins: [] } }).posted).toEqual(
         [],
      );
      expect(
         boot({ context: null }).posted.filter((p) => p.msg.type === "ready"),
      ).toEqual([]);
   });

   it("binds on hello and replies to that exact origin", () => {
      const page = boot({
         context: {
            ...CONTEXT,
            hostOrigins: ["https://*.apps.example.com:8443"],
         },
      });
      const origin = "https://t1.apps.example.com:8443";
      page.hello(origin, "h9");
      expect(page.posted).toEqual([
         {
            targetOrigin: origin,
            msg: {
               protocol: "publisher",
               v: 1,
               type: "hello",
               id: "h9",
               reply: true,
               nonce: NONCE,
               payload: { handles: [], runtime: "1" },
            },
         },
      ]);
   });

   it("emits to the bound origin only, and nowhere before binding", () => {
      const page = boot();
      page.posted.length = 0;
      expect(page.Publisher.emit("x.y", { a: 1 })).toBe(false);
      expect(page.posted).toEqual([]);
      page.hello(HOST);
      page.posted.length = 0;
      expect(page.Publisher.emit("x.y", { a: 1 })).toBe(true);
      expect(page.posted).toHaveLength(1);
      expect(page.posted[0].targetOrigin).toBe(HOST);
      expect(page.posted[0].msg.payload).toEqual({ a: 1 });
      // another allowed origin cannot take over the binding
      page.posted.length = 0;
      page.hello(PAGE, "h2");
      expect(page.posted).toEqual([]);
   });

   it("answers a hello event (no id) with a ready event", () => {
      const page = boot();
      page.posted.length = 0;
      page.send(envelope("hello"));
      expect(page.posted).toHaveLength(1);
      expect(page.posted[0].msg.type).toBe("ready");
      expect(page.posted[0].msg.id).toBeUndefined();
   });

   it("rejects a wrong source, origin or malformed envelope", () => {
      const rejected: [string, () => void][] = [
         [
            "source",
            () => {
               const page = boot();
               page.posted.length = 0;
               page.send(envelope("hello", { id: "h1" }), HOST, {
                  postMessage() {},
               });
               expect(page.posted).toEqual([]);
               expect(page.Publisher.emit("x")).toBe(false);
            },
         ],
         [
            "origin",
            () => {
               const page = boot();
               page.posted.length = 0;
               page.hello("https://evil.example.com");
               expect(page.posted).toEqual([]);
               expect(page.Publisher.emit("x")).toBe(false);
            },
         ],
      ];
      for (const [, run] of rejected) run();

      const malformed = [
         null,
         "hello",
         42,
         { ...envelope("hello", { id: "h1" }), protocol: "other" },
         { ...envelope("hello", { id: "h1" }), v: 2 },
         { ...envelope("hello", { id: "h1" }), type: 7 },
         { ...envelope("hello", { id: "h1" }), type: "" },
         { ...envelope("hello"), id: {} },
         { ...envelope("hello", { id: "h1" }), reply: true },
      ];
      for (const data of malformed) {
         const page = boot();
         page.posted.length = 0;
         page.send(data);
         expect(page.posted).toEqual([]);
         expect(page.Publisher.emit("x")).toBe(false);
      }
   });

   it("takes host messages without a nonce and echoes its own on every post", () => {
      const page = boot();
      page.posted.length = 0;
      page.hello();
      page.Publisher.emit("x", 1);
      expect(page.posted).toHaveLength(2);
      for (const p of page.posted) expect(p.msg.nonce).toBe(NONCE);
   });

   it("posts no nonce when the page URL has none", () => {
      const page = boot({
         url: `${PAGE}/environments/env1/packages/pkg1/index.html`,
      });
      expect(page.posted[0].msg.nonce).toBeUndefined();
      page.posted.length = 0;
      page.send({ protocol: "publisher", v: 1, type: "hello", id: "h1" });
      expect(page.posted).toHaveLength(1);
      expect(page.posted[0].msg.nonce).toBeUndefined();
   });

   describe("the nonce in the URL", () => {
      const at = (hash: string) =>
         `${PAGE}/environments/env1/packages/pkg1/index.html${hash}`;

      for (const [before, after] of [
         ["#publisher-nonce=abc", ""],
         ["#/route&publisher-nonce=abc", "#/route"],
         ["#/route?tab=2&publisher-nonce=abc", "#/route?tab=2"],
         ["#sec&publisher-nonce=abc", "#sec"],
         ["#publisher-nonce=abc&sec", "#sec"],
         ["#a=1&publisher-nonce=abc&b=2", "#a=1&b=2"],
      ]) {
         it(`removes it from ${before}, keeping the rest exactly, and still echoes it`, () => {
            const page = boot({ url: at(before) });
            expect(page.win.location.hash).toBe(after);
            expect(page.win.location.href).toBe(at(after));
            expect(page.readyPosts()[0].msg.nonce).toBe("abc");
            page.hello();
            expect(page.postedOfType("hello").at(-1)!.msg.nonce).toBe("abc");
         });
      }

      it("leaves a fragment without a nonce alone", () => {
         const page = boot({ url: at("#sec") });
         expect(page.win.location.hash).toBe("#sec");
         expect(page.readyPosts()[0].msg.nonce).toBeUndefined();
      });

      function reloadable(hash: string) {
         const page = boot({
            url: at(hash),
            eventSource: true,
            context: { ...CONTEXT, versionId: null },
         });
         let reloads = 0;
         let hashAtReload = "";
         Object.defineProperty(page.win.location, "reload", {
            value: () => {
               reloads++;
               hashAtReload = page.win.location.hash;
            },
         });
         const changed = async () => {
            page.win.EventSource.listeners.changed();
            await page.clock.advance(100);
         };
         return {
            page,
            changed,
            reloads: () => reloads,
            hashAtReload: () => hashAtReload,
         };
      }

      for (const [before, atReload] of [
         ["#publisher-nonce=abc", "#publisher-nonce=abc"],
         ["#/route&publisher-nonce=abc", "#/route&publisher-nonce=abc"],
         ["#sec&publisher-nonce=abc", "#sec&publisher-nonce=abc"],
      ]) {
         it(`puts it back before the runtime reloads the page (${before})`, async () => {
            const r = reloadable(before);
            expect(r.page.win.location.hash).not.toContain("publisher-nonce");
            await r.changed();
            expect(r.reloads()).toBe(1);
            expect(r.hashAtReload()).toBe(atReload);
         });
      }

      it("restores it after whatever fragment the page has moved to", async () => {
         const r = reloadable("#publisher-nonce=abc");
         r.page.win.location.hash = "#/other";
         await r.changed();
         expect(r.hashAtReload()).toBe("#/other&publisher-nonce=abc");
      });

      it("reloads a page with no nonce without touching its fragment", async () => {
         const r = reloadable("#sec");
         await r.changed();
         expect(r.hashAtReload()).toBe("#sec");
      });
   });

   it("ignores non-hello messages until bound", () => {
      const page = boot();
      let calls = 0;
      page.Publisher.on("ui.ping", () => {
         calls++;
         return 1;
      });
      page.posted.length = 0;
      page.send(envelope("ui.ping", { id: "r1" }));
      expect(calls).toBe(0);
      expect(page.posted).toEqual([]);
   });
});

describe("requests and handlers", () => {
   it("replies with a handler's value, sync or async, and passes meta", async () => {
      const page = boundPage();
      const metas: Json[] = [];
      page.Publisher.on("ui.add", (p: Json, meta: Json) => {
         metas.push(plain(meta));
         return { sum: p.a + p.b };
      });
      page.Publisher.on("ui.later", async (p: Json) => {
         await flush();
         return p.v;
      });
      await flush();
      page.posted.length = 0;
      page.send(envelope("ui.add", { id: "r1", payload: { a: 1, b: 2 } }));
      page.send(envelope("ui.later", { id: "r2", payload: { v: "x" } }));
      await flush();
      await flush();
      const byId = Object.fromEntries(
         page.posted.filter((p) => p.msg.reply).map((p) => [p.msg.id, p]),
      );
      expect(byId.r1.msg).toEqual({
         protocol: "publisher",
         v: 1,
         type: "ui.add",
         id: "r1",
         reply: true,
         nonce: NONCE,
         payload: { sum: 3 },
      });
      expect(byId.r1.targetOrigin).toBe(HOST);
      expect(byId.r2.msg.payload).toBe("x");
      expect(metas).toEqual([{ type: "ui.add", origin: HOST, id: "r1" }]);
   });

   it("answers an unknown request with unhandled and ignores an unknown event", async () => {
      const page = boundPage();
      page.send(envelope("nobody.home", { id: "r1" }));
      page.send(envelope("nobody.home"));
      await flush();
      expect(page.posted).toHaveLength(1);
      expect(page.posted[0].msg.reply).toBe(true);
      expect(page.posted[0].msg.error.code).toBe("unhandled");
      expect(page.posted[0].msg.payload).toBeUndefined();
   });

   it("answers a throwing or rejecting handler with handler_error", async () => {
      const page = boundPage();
      page.Publisher.on("ui.throw", () => {
         throw new Error("boom");
      });
      page.Publisher.on("ui.reject", () => Promise.reject(new Error("nope")));
      page.Publisher.on("ui.eventthrow", () => {
         throw new Error("silent");
      });
      await flush();
      page.posted.length = 0;
      page.send(envelope("ui.throw", { id: "r1" }));
      page.send(envelope("ui.reject", { id: "r2" }));
      page.send(envelope("ui.eventthrow"));
      await flush();
      expect(page.posted).toHaveLength(2);
      expect(page.posted[0].msg.error).toEqual({
         code: "handler_error",
         message: "boom",
      });
      expect(page.posted[1].msg.error.message).toBe("nope");
   });

   it("stops calling a handler after it is unsubscribed", async () => {
      const page = boundPage();
      const off = page.Publisher.on("ui.ping", () => "pong");
      await flush();
      off();
      await flush();
      page.posted.length = 0;
      page.send(envelope("ui.ping", { id: "r1" }));
      await flush();
      expect(page.posted[0].msg.error.code).toBe("unhandled");
   });

   it("rejects bad arguments to on()", () => {
      const page = boot();
      expect(() => page.Publisher.on(1, () => {})).toThrow();
      expect(() => page.Publisher.on("x", null)).toThrow();
   });

   it("re-sends ready once, with the new handles, when handlers change after binding", async () => {
      const page = boundPage();
      const offA = page.Publisher.on("a.one", () => {});
      page.Publisher.on("b.two", () => {});
      page.Publisher.on("b.two", () => {});
      await flush();
      expect(page.readyPosts()).toHaveLength(1);
      expect(page.readyPosts()[0].targetOrigin).toBe(HOST);
      expect(page.readyPosts()[0].msg.payload.handles).toEqual([
         "a.one",
         "b.two",
      ]);
      offA();
      await flush();
      expect(page.readyPosts()).toHaveLength(2);
      expect(page.readyPosts()[1].msg.payload.handles).toEqual(["b.two"]);
   });

   it("does not re-send ready for handlers added before binding", async () => {
      const page = boot();
      page.posted.length = 0;
      page.Publisher.on("a.one", () => {});
      await flush();
      expect(page.posted).toEqual([]);
      page.hello();
      expect(page.posted[0].msg.payload.handles).toEqual(["a.one"]);
   });
});

describe("givens store", () => {
   it("returns copies and merges into the store", () => {
      const page = boot();
      expect(plain(page.Publisher.givens)).toEqual({});
      page.Publisher.setGivens({ region: "EMEA", tags: ["a"] });
      page.Publisher.setGivens({ year: 2026 });
      const copy = page.Publisher.givens;
      expect(plain(copy)).toEqual({ region: "EMEA", tags: ["a"], year: 2026 });
      copy.region = "changed";
      copy.tags.push("b");
      expect(plain(page.Publisher.givens)).toEqual({
         region: "EMEA",
         tags: ["a"],
         year: 2026,
      });
      expect(() => page.Publisher.setGivens(null)).toThrow();
      expect(() => page.Publisher.setGivens([1])).toThrow();
   });

   it("removes a key set to null, through setGivens and givens.set alike", async () => {
      const page = boundPage();
      page.Publisher.setGivens({ a: 1, b: 2, c: 3 });
      page.Publisher.setGivens({ a: null, d: undefined });
      expect(plain(page.Publisher.givens)).toEqual({ b: 2, c: 3 });
      page.Publisher.onGivensChange(() => {});
      page.send(
         envelope("givens.set", { id: "r1", payload: { givens: { b: null } } }),
      );
      await flush();
      expect(plain(page.Publisher.givens)).toEqual({ c: 3 });
      const reply = page.posted.filter((p) => p.msg.reply).at(-1)!;
      expect(reply.msg.payload).toEqual({ applied: true, givens: { c: 3 } });
      expect(page.postedOfType("givens.changed").at(-1)!.msg.payload).toEqual({
         givens: { c: 3 },
      });
      await page.Publisher.query("m.malloy", "run: x");
      expect(JSON.parse(page.fetched[0].init.body).givens).toEqual({ c: 3 });
   });

   it("notifies listeners with a copy, tolerates a throwing one, and unsubscribes", () => {
      const page = boot();
      const seen: Json[] = [];
      page.Publisher.onGivensChange(() => {
         throw new Error("bad listener");
      });
      const off = page.Publisher.onGivensChange((g: Json) =>
         seen.push(plain(g)),
      );
      page.Publisher.setGivens({ a: 1 });
      off();
      page.Publisher.setGivens({ b: 2 });
      expect(seen).toEqual([{ a: 1 }]);
   });

   it("emits givens.changed when bound", () => {
      const page = boundPage();
      page.Publisher.setGivens({ a: 1 });
      const changed = page.postedOfType("givens.changed");
      expect(changed).toHaveLength(1);
      expect(changed[0].msg.payload).toEqual({ givens: { a: 1 } });
      expect(changed[0].targetOrigin).toBe(HOST);
   });

   it("advertises givens.set only after onGivensChange or an own handler", async () => {
      const page = boundPage();
      page.hello(HOST, "h2");
      expect(page.posted.at(-1)!.msg.payload.handles).toEqual([]);

      const off = page.Publisher.onGivensChange(() => {});
      await flush();
      expect(page.readyPosts().at(-1)!.msg.payload.handles).toEqual([
         "givens.set",
      ]);

      off();
      await flush();
      expect(page.readyPosts().at(-1)!.msg.payload.handles).toEqual([]);

      page.Publisher.on("givens.set", () => ({ mine: true }));
      await flush();
      expect(page.readyPosts().at(-1)!.msg.payload.handles).toEqual([
         "givens.set",
      ]);
   });

   it("applies the default givens.set handler and replies applied", async () => {
      const page = boundPage();
      const seen: Json[] = [];
      page.Publisher.onGivensChange((g: Json) => seen.push(plain(g)));
      await flush();
      page.posted.length = 0;
      page.send(
         envelope("givens.set", {
            id: "r1",
            payload: { givens: { region: "EMEA" } },
         }),
      );
      await flush();
      const reply = page.posted.find((p) => p.msg.reply)!;
      expect(reply.msg.payload).toEqual({
         applied: true,
         givens: { region: "EMEA" },
      });
      expect(plain(page.Publisher.givens)).toEqual({ region: "EMEA" });
      expect(seen).toEqual([{ region: "EMEA" }]);
      expect(page.postedOfType("givens.changed")).toHaveLength(1);
   });

   it("applies givens.set sent as an event, and rejects a bad payload as handler_error", async () => {
      const page = boundPage();
      page.Publisher.onGivensChange(() => {});
      page.send(envelope("givens.set", { payload: { givens: { a: 1 } } }));
      await flush();
      expect(plain(page.Publisher.givens)).toEqual({ a: 1 });
      page.posted.length = 0;
      page.send(envelope("givens.set", { id: "r1", payload: { nope: 1 } }));
      await flush();
      expect(page.posted.find((p) => p.msg.reply)!.msg.error.code).toBe(
         "handler_error",
      );
      expect(plain(page.Publisher.givens)).toEqual({ a: 1 });
   });

   it("answers givens.set unhandled, and keeps the store, until the page has called onGivensChange", async () => {
      const page = boundPage();
      page.send(
         envelope("givens.set", { id: "r1", payload: { givens: { a: 1 } } }),
      );
      page.send(envelope("givens.set", { payload: { givens: { a: 2 } } }));
      await flush();
      expect(page.posted.find((p) => p.msg.reply)!.msg.error.code).toBe(
         "unhandled",
      );
      expect(plain(page.Publisher.givens)).toEqual({});

      const off = page.Publisher.onGivensChange(() => {});
      off();
      page.posted.length = 0;
      page.send(
         envelope("givens.set", { id: "r2", payload: { givens: { a: 3 } } }),
      );
      await flush();
      expect(page.posted.find((p) => p.msg.reply)!.msg.error.code).toBe(
         "unhandled",
      );
   });

   it("does nothing when the givens it is handed are already held", () => {
      const page = boundPage();
      const seen: Json[] = [];
      page.Publisher.setGivens({ a: 1, nested: { x: [1, { y: 2 }] } });
      page.Publisher.onGivensChange((g: Json) => seen.push(plain(g)));
      page.posted.length = 0;

      page.Publisher.setGivens({ a: 1 });
      page.Publisher.setGivens({ nested: { x: [1, { y: 2 }] } });
      page.Publisher.setGivens({ absent: null });
      page.Publisher.setGivens({});
      expect(seen).toEqual([]);
      expect(page.postedOfType("givens.changed")).toEqual([]);

      page.Publisher.setGivens({ nested: { x: [1, { y: 3 }] } });
      expect(seen).toHaveLength(1);
      expect(page.postedOfType("givens.changed")).toHaveLength(1);
      page.Publisher.setGivens({ a: null });
      expect(seen).toHaveLength(2);
   });

   it("lets a page's own givens.set handler replace the default", async () => {
      const page = boundPage();
      page.Publisher.on("givens.set", (p: Json) => ({ got: p.givens }));
      await flush();
      page.posted.length = 0;
      page.send(
         envelope("givens.set", { id: "r1", payload: { givens: { a: 1 } } }),
      );
      await flush();
      expect(page.posted.find((p) => p.msg.reply)!.msg.payload).toEqual({
         got: { a: 1 },
      });
      expect(plain(page.Publisher.givens)).toEqual({});
   });
});

describe("query wire format", () => {
   it("leaves an untouched page's request body exactly as before", async () => {
      const page = boot({
         context: null,
         url: `${PAGE}/environments/env1/packages/pkg1/index.html`,
      });
      await page.Publisher.query("m.malloy", "run: x");
      await page.Publisher.queryFull("m.malloy", undefined, {
         sourceName: "s",
         queryName: "q",
      });
      expect(page.fetched[0].init.body).toBe(
         '{"compactJson":true,"query":"run: x"}',
      );
      expect(page.fetched[1].init.body).toBe(
         '{"compactJson":false,"sourceName":"s","queryName":"q"}',
      );
      expect(Object.keys(JSON.parse(page.fetched[0].init.body))).not.toContain(
         "givens",
      );
   });

   it("keeps the body of a context-pinned untouched page free of givens", async () => {
      const page = boot();
      await page.Publisher.query("m.malloy", "run: x");
      expect(page.fetched[0].init.body).toBe(
         '{"compactJson":true,"query":"run: x","versionId":"1.0.3"}',
      );
   });

   it("merges store givens under opts.givens", async () => {
      const page = boot();
      await page.Publisher.query("m.malloy", "run: x", { givens: { only: 1 } });
      expect(JSON.parse(page.fetched[0].init.body).givens).toEqual({ only: 1 });

      page.Publisher.setGivens({ a: 1, b: 1 });
      await page.Publisher.query("m.malloy", "run: x");
      expect(JSON.parse(page.fetched[1].init.body).givens).toEqual({
         a: 1,
         b: 1,
      });
      await page.Publisher.query("m.malloy", "run: x", {
         givens: { b: 2, c: 3 },
      });
      expect(JSON.parse(page.fetched[2].init.body).givens).toEqual({
         a: 1,
         b: 2,
         c: 3,
      });
   });
});

describe("held givens", () => {
   it("ride only a query on the page's own package, and opts.givens always do", async () => {
      const page = boot();
      page.Publisher.setGivens({ region: "EMEA" });
      await page.Publisher.query("m.malloy", "run: x", { package: "other" });
      expect(page.fetched[0].init.body).not.toContain("givens");
      await page.Publisher.query("m.malloy", "run: x", {
         package: "other",
         givens: { year: 1 },
      });
      expect(JSON.parse(page.fetched[1].init.body).givens).toEqual({ year: 1 });
      await page.Publisher.query("m.malloy", "run: x", {
         environment: "elsewhere",
      });
      expect(page.fetched[2].init.body).not.toContain("givens");
      await page.Publisher.query("m.malloy", "run: x");
      expect(JSON.parse(page.fetched[3].init.body).givens).toEqual({
         region: "EMEA",
      });
   });
});

describe("held givens on a page with no package", () => {
   it("warns once that they are not sent, and still sends opts.givens", async () => {
      const page = boot({ context: null, url: `${PAGE}/standalone.html` });
      const warn = spyOn(page.win.console, "warn").mockImplementation(() => {});
      page.Publisher.setGivens({ region: "EMEA" });
      page.Publisher.setGivens({ region: "APAC" });
      const warnings = warn.mock.calls.map((c) => String(c[0]));
      warn.mockRestore();
      expect(warnings).toHaveLength(1);
      expect(warnings[0]).toContain("opts.givens");
      await page.Publisher.query("m.malloy", "run: x", {
         environment: "e",
         package: "p",
      });
      expect(page.fetched[0].init.body).not.toContain("givens");
      await page.Publisher.query("m.malloy", "run: x", {
         environment: "e",
         package: "p",
         givens: { region: "APAC" },
      });
      expect(JSON.parse(page.fetched[1].init.body).givens).toEqual({
         region: "APAC",
      });
   });

   it("does not warn when the page names its package, or when the store does not change", () => {
      const named = boot();
      const unchanged = boot({ context: null, url: `${PAGE}/standalone.html` });
      const warn = spyOn(named.win.console, "warn").mockImplementation(
         () => {},
      );
      const quiet = spyOn(unchanged.win.console, "warn").mockImplementation(
         () => {},
      );
      named.Publisher.setGivens({ a: 1 });
      unchanged.Publisher.setGivens({});
      expect(warn).not.toHaveBeenCalled();
      expect(quiet).not.toHaveBeenCalled();
   });
});

describe("query.ran", () => {
   it("reports what ran once bound", async () => {
      const page = boundPage();
      page.Publisher.setGivens({ region: "EMEA" });
      page.posted.length = 0;
      page.replies.push(rowsResult(3));
      const longQuery = "q".repeat(3000);
      await page.Publisher.query("m.malloy", longQuery, {
         sourceName: "orders",
         queryName: "by_region",
      });
      const ran = page.postedOfType("query.ran");
      expect(ran).toHaveLength(1);
      expect(ran[0].targetOrigin).toBe(HOST);
      const { payload } = ran[0].msg;
      expect(payload.query).toBe("q".repeat(2000));
      expect(typeof payload.ms).toBe("number");
      expect(payload.ms).toBeGreaterThanOrEqual(0);
      expect({ ...payload, query: undefined, ms: undefined }).toEqual({
         model: "m.malloy",
         sourceName: "orders",
         queryName: "by_region",
         givens: { region: "EMEA" },
         versionId: "1.0.3",
         rowCount: 3,
      });
   });

   it("omits absent fields, counts a full result's rows, and uses null for unknown shapes", async () => {
      const page = boundPage({ context: { ...CONTEXT, versionId: null } });
      page.replies.push(
         {
            body: {
               result: JSON.stringify({ data: { array_value: [{}, {}] } }),
            },
         },
         { body: { result: JSON.stringify({ something: "else" }) } },
      );
      await page.Publisher.queryFull("m.malloy", "run: x");
      await page.Publisher.queryFull("m.malloy", undefined, { queryName: "q" });
      const [first, second] = page
         .postedOfType("query.ran")
         .map((p) => p.msg.payload);
      expect(first.rowCount).toBe(2);
      expect(first.versionId).toBeNull();
      expect(first.givens).toEqual({});
      expect("sourceName" in first).toBe(false);
      expect("queryName" in first).toBe(false);
      expect(second.rowCount).toBeNull();
      expect("query" in second).toBe(false);
   });

   it("sends nothing when unbound or when the query fails", async () => {
      const page = boot();
      page.posted.length = 0;
      await page.Publisher.query("m.malloy", "run: x");
      expect(page.postedOfType("query.ran")).toEqual([]);

      page.hello();
      page.posted.length = 0;
      page.replies.push({ status: 500, body: { message: "x" } });
      await expect(
         page.Publisher.query("m.malloy", "run: x"),
      ).rejects.toThrow();
      expect(page.postedOfType("query.ran")).toEqual([]);
   });
});

describe("auth over the bus", () => {
   const unauthorized = { status: 401, body: { message: "no token" } };

   it("asks for a token on a 401, waits, and retries once with it", async () => {
      const page = boundPage();
      page.replies.push(unauthorized, rowsResult(1));
      const pending = page.Publisher.query("m.malloy", "run: x");
      await flush();
      expect(page.fetched).toHaveLength(1);
      expect(page.postedOfType("auth.token.request")).toHaveLength(1);
      expect(page.postedOfType("auth.token.request")[0].msg.id).toBe("p1");
      page.send(envelope("auth.token", { payload: { token: "tok-1" } }));
      expect(await pending).toEqual([{ i: 0 }]);
      expect(page.fetched).toHaveLength(2);
      expect(page.fetched[1].init.headers.Authorization).toBe("Bearer tok-1");
      expect(page.fetched[0].init.headers.Authorization).toBeUndefined();
      expect(page.clock.pending()).toBe(0); // the wait timer was cleared
   });

   const tokenReply = (page: Boot, id: string, extra: Json) =>
      page.send(envelope("auth.token.request", { id, reply: true, ...extra }));

   it("takes the token from the host's reply to auth.token.request", async () => {
      const page = boundPage();
      page.replies.push(unauthorized, rowsResult(1));
      const pending = page.Publisher.query("m.malloy", "run: x");
      await flush();
      tokenReply(page, "p1", { payload: { token: "tok-r" } });
      expect(await pending).toEqual([{ i: 0 }]);
      expect(page.fetched[1].init.headers.Authorization).toBe("Bearer tok-r");
      expect(page.clock.pending()).toBe(0);
   });

   it("fails at once, with the 401, when the host answers the request with an error", async () => {
      const page = boundPage();
      page.replies.push(unauthorized);
      const settled = page.Publisher.query("m.malloy", "run: x").catch(
         (e: Json) => e,
      );
      await flush();
      tokenReply(page, "p1", {
         error: { code: "unhandled", message: "No handler" },
      });
      const err = await settled;
      expect(err.status).toBe(401);
      expect(page.fetched).toHaveLength(1);
      expect(page.clock.pending()).toBe(0); // no 10 s wait left behind
      // a later 401 asks again, with a new request id
      page.replies.push(unauthorized);
      page.Publisher.query("m.malloy", "run: x").catch(() => {});
      await flush();
      expect(
         page.postedOfType("auth.token.request").map((p) => p.msg.id),
      ).toEqual(["p1", "p2"]);
   });

   it("fails at once on a reply with no usable token, and ignores a reply for another id or after the wait", async () => {
      const page = boundPage();
      page.replies.push(unauthorized);
      const settled = page.Publisher.query("m.malloy", "run: x").catch(
         (e: Json) => e,
      );
      await flush();
      tokenReply(page, "other", { payload: { token: "nope" } }); // not ours
      expect(page.clock.pending()).toBe(1);
      tokenReply(page, "p1", { payload: { token: 5 } });
      expect((await settled).status).toBe(401);
      tokenReply(page, "p1", { payload: { token: "late" } }); // the wait is over
      page.replies.push(rowsResult(1));
      await page.Publisher.query("m.malloy", "run: x");
      expect(page.fetched.at(-1)!.init.headers.Authorization).toBeUndefined();
   });

   it("ignores a reply from the wrong origin", async () => {
      const page = boundPage();
      page.replies.push(unauthorized);
      const settled = page.Publisher.query("m.malloy", "run: x").catch(
         (e: Json) => e,
      );
      await flush();
      page.send(
         envelope("auth.token.request", {
            id: "p1",
            reply: true,
            payload: { token: "evil" },
         }),
         "https://evil.example.com",
      );
      expect(page.clock.pending()).toBe(1);
      await page.clock.advance(10000);
      expect((await settled).status).toBe(401);
      expect(page.fetched).toHaveLength(1);
   });

   it("shares one token request across concurrent 401s", async () => {
      const page = boundPage();
      page.replies.push(
         unauthorized,
         unauthorized,
         rowsResult(1),
         rowsResult(2),
      );
      const a = page.Publisher.query("m.malloy", "run: a");
      const b = page.Publisher.query("m.malloy", "run: b");
      await flush();
      expect(page.postedOfType("auth.token.request")).toHaveLength(1);
      page.send(envelope("auth.token", { payload: { token: "tok-1" } }));
      await Promise.all([a, b]);
      expect(page.fetched).toHaveLength(4);
      expect(page.postedOfType("auth.token.request")).toHaveLength(1);
   });

   it("never retries a retry", async () => {
      const page = boundPage();
      page.replies.push(unauthorized, unauthorized);
      const pending = page.Publisher.query("m.malloy", "run: x");
      const settled = pending.catch((e: Json) => e);
      await flush();
      page.send(envelope("auth.token", { payload: { token: "tok-1" } }));
      const err = await settled;
      expect(err.status).toBe(401);
      expect(page.fetched).toHaveLength(2);
      expect(page.postedOfType("auth.token.request")).toHaveLength(1);
   });

   it("gives up with the original 401 after 10 s", async () => {
      const page = boundPage();
      page.replies.push(unauthorized);
      const settled = page.Publisher.query("m.malloy", "run: x").catch(
         (e: Json) => e,
      );
      await flush();
      await page.clock.advance(9999);
      expect(page.fetched).toHaveLength(1);
      await page.clock.advance(1);
      const err = await settled;
      expect(err.status).toBe(401);
      expect(err.message).toBe("Publisher.query: no token");
      expect(page.fetched).toHaveLength(1);
      // a later 401 asks again
      page.replies.push(unauthorized);
      page.Publisher.query("m.malloy", "run: x").catch(() => {});
      await flush();
      expect(page.postedOfType("auth.token.request")).toHaveLength(2);
   });

   it("throws at once on a page with no nonce, whatever the bus", async () => {
      for (const bound of [false, true]) {
         const page = boot({
            url: `${PAGE}/environments/env1/packages/pkg1/index.html`,
         });
         if (bound) page.hello();
         page.posted.length = 0;
         page.replies.push(unauthorized);
         const err = await page.Publisher.query("m.malloy", "run: x").catch(
            (e: Json) => e,
         );
         expect(err.status).toBe(401);
         expect(page.fetched).toHaveLength(1);
         expect(page.posted).toEqual([]);
         expect(page.clock.pending()).toBe(0);
      }
   });

   it("throws at once on a page whose context names no host (no bus to bind)", async () => {
      const page = boot({ context: { ...CONTEXT, hostOrigins: [] } });
      page.replies.push(unauthorized);
      const err = await page.Publisher.query("m.malloy", "run: x").catch(
         (e: Json) => e,
      );
      expect(err.status).toBe(401);
      expect(page.posted).toEqual([]);
      expect(page.clock.pending()).toBe(0);
   });

   it("waits for a page with a nonce to bind, then asks for a token and retries", async () => {
      const page = boot();
      page.posted.length = 0;
      page.replies.push(unauthorized, rowsResult(1));
      const pending = page.Publisher.query("m.malloy", "run: x");
      await flush();
      expect(page.postedOfType("auth.token.request")).toEqual([]);
      page.hello();
      await flush();
      expect(page.postedOfType("auth.token.request")).toHaveLength(1);
      page.send(envelope("auth.token", { payload: { token: "tok-b" } }));
      expect(await pending).toEqual([{ i: 0 }]);
      expect(page.fetched[1].init.headers.Authorization).toBe("Bearer tok-b");
      expect(page.clock.pending()).toBe(0);
   });

   it("gives up with the 401, sending nothing, when no host binds within 5 s", async () => {
      const page = boot();
      page.posted.length = 0;
      page.replies.push(unauthorized);
      const settled = page.Publisher.query("m.malloy", "run: x").catch(
         (e: Json) => e,
      );
      await flush();
      await page.clock.advance(4999);
      expect(page.fetched).toHaveLength(1);
      let early = false;
      settled.then(() => (early = true));
      await flush();
      expect(early).toBe(false);
      await page.clock.advance(1);
      expect((await settled).status).toBe(401);
      expect(page.posted).toEqual([]);
      expect(page.clock.pending()).toBe(0);
      // a host that binds later is not held against the next query
      page.hello();
      page.replies.push(unauthorized);
      page.Publisher.query("m.malloy", "run: x").catch(() => {});
      await flush();
      expect(page.postedOfType("auth.token.request")).toHaveLength(1);
   });

   it("sets the bearer token from auth.token, replies to a request, and ignores a bad payload", async () => {
      const page = boundPage();
      page.send(
         envelope("auth.token", { id: "r1", payload: { token: "tok-2" } }),
      );
      page.send(envelope("auth.token", { id: "r2", payload: { token: 5 } }));
      await flush();
      const replies = page.posted.filter((p) => p.msg.reply);
      expect(replies[0].msg.payload).toEqual({ applied: true });
      expect(replies[1].msg.error.code).toBe("handler_error");
      await page.Publisher.query("m.malloy", "run: x");
      expect(page.fetched[0].init.headers.Authorization).toBe("Bearer tok-2");
   });

   it("does not advertise auth.token in handles", () => {
      const page = boot();
      expect(page.readyPosts()[0].msg.payload.handles).toEqual([]);
   });
});

describe("version.unavailable", () => {
   for (const reason of ["VERSION_NOT_FOUND", "VERSION_ARCHIVED"]) {
      it(`emits once when the page's own version answers ${reason}`, async () => {
         const page = boundPage();
         const status = reason === "VERSION_ARCHIVED" ? 410 : 404;
         page.replies.push(
            { status, body: { message: "gone", reason } },
            { status, body: { message: "gone", reason } },
         );
         const err = await page.Publisher.query("m.malloy", "run: x").catch(
            (e: Json) => e,
         );
         expect(err.status).toBe(status);
         expect(err.response.reason).toBe(reason);
         await expect(
            page.Publisher.query("m.malloy", "run: x"),
         ).rejects.toThrow();
         const sent = page.postedOfType("version.unavailable");
         expect(sent).toHaveLength(1);
         expect(sent[0].msg.payload).toEqual({ versionId: "1.0.3", reason });
      });
   }

   it("ignores failures without a version reason and other packages' versions", async () => {
      const page = boundPage();
      page.replies.push(
         { status: 404, body: { message: "no model" } },
         { status: 400, body: { message: "x", reason: "VERSION_ID_INVALID" } },
         { status: 404, body: { message: "x", reason: "VERSION_NOT_FOUND" } },
      );
      await page.Publisher.query("m.malloy", "run: x").catch(() => {});
      await page.Publisher.query("m.malloy", "run: x").catch(() => {});
      await page.Publisher.query("m.malloy", "run: x", {
         package: "other",
      }).catch(() => {});
      expect(page.postedOfType("version.unavailable")).toEqual([]);
   });

   it("keys on the reason, not the status", async () => {
      const page = boundPage();
      page.replies.push({
         status: 500,
         body: { message: "x", reason: "VERSION_NOT_FOUND" },
      });
      await page.Publisher.query("m.malloy", "run: x").catch(() => {});
      expect(page.postedOfType("version.unavailable")).toHaveLength(1);
   });

   it("does not consume the once while unbound", async () => {
      const page = boot();
      const gone = {
         status: 404,
         body: { message: "gone", reason: "VERSION_NOT_FOUND" },
      };
      page.replies.push(gone, gone);
      await page.Publisher.query("m.malloy", "run: x").catch(() => {});
      expect(page.postedOfType("version.unavailable")).toEqual([]);
      page.hello();
      await page.Publisher.query("m.malloy", "run: x").catch(() => {});
      expect(page.postedOfType("version.unavailable")).toHaveLength(1);
   });

   it("throws as before, and sends nothing, on a page with no context (bus off)", async () => {
      const page = boot({
         context: null,
         url: `${PAGE}/environments/env1/packages/pkg1/index.html?versionId=2.0.0`,
      });
      page.replies.push(
         { body: { versionId: "2.0.0" } },
         {
            status: 404,
            body: { message: "gone", reason: "VERSION_NOT_FOUND" },
         },
      );
      const err = await page.Publisher.query("m.malloy", "run: x").catch(
         (e: Json) => e,
      );
      expect(err.status).toBe(404);
      expect(page.posted).toEqual([]);
   });
});

describe("legacy resize", () => {
   function withInk(page: Boot) {
      const div = page.win.document.createElement("div");
      div.getBoundingClientRect = () => ({ bottom: 120 });
      page.win.document.body.appendChild(div);
      page.win.dispatchEvent(new page.win.Event("load"));
   }

   it("keeps the legacy message and targets * until the host has said hello", () => {
      const page = boot();
      const resize = page.resizes;
      expect(resize).toHaveLength(1);
      expect(resize[0].targetOrigin).toBe("*");
      expect(Object.keys(resize[0].msg).sort()).toEqual(["height", "type"]);
   });

   it("targets the bound origin once bound", () => {
      const page = boundPage();
      page.resizes.length = 0;
      withInk(page);
      const resize = page.resizes;
      expect(resize).toHaveLength(1);
      expect(resize[0].msg).toEqual({ type: "publisher:resize", height: 120 });
      expect(resize[0].targetOrigin).toBe(HOST);
   });

   it("still posts to * from a page with no bus (no hostOrigins)", () => {
      const page = boot({ context: { ...CONTEXT, hostOrigins: [] } });
      const resize = page.resizes;
      expect(resize).toHaveLength(1);
      expect(resize[0].targetOrigin).toBe("*");
   });
});

describe("Publisher.embed (host side)", () => {
   const SRC = "https://app.example.com/environments/e/packages/p/index.html";
   const APP = "https://app.example.com";

   function bootHost() {
      const page = boot({
         iframe: false,
         context: null,
         url: "https://host.example.com/page",
      });
      const slot = page.win.document.createElement("div");
      slot.setAttribute("id", "slot");
      page.win.document.body.appendChild(slot);
      return page;
   }

   function mount(page: Boot, options: Json = {}) {
      const childPosts: Posted[] = [];
      const child = {
         postMessage(msg: unknown, targetOrigin: string) {
            childPosts.push({ msg: plain(msg), targetOrigin });
         },
      };
      const handle = page.Publisher.embed("#slot", { src: SRC, ...options });
      Object.defineProperty(handle.iframe, "contentWindow", { value: child });
      const nonce = /publisher-nonce=([0-9a-f]+)$/.exec(
         handle.iframe.getAttribute("src"),
      )![1];
      const fromChild = (data: Json, origin = APP, source: unknown = child) =>
         page.win.dispatchEvent(
            new page.win.MessageEvent("message", { data, origin, source }),
         );
      const msg = (type: string, extra: Json = {}) => ({
         protocol: "publisher",
         v: 1,
         type,
         nonce,
         ...extra,
      });
      const load = () =>
         handle.iframe.dispatchEvent(new page.win.Event("load"));
      return { handle, child, childPosts, nonce, fromChild, msg, load };
   }

   const helloReply = (m: ReturnType<typeof mount>, extra: Json = {}) =>
      m.fromChild(
         m.msg("hello", {
            id: "hello",
            reply: true,
            payload: { handles: [], runtime: "1" },
            ...extra,
         }),
      );

   it("puts a random nonce in the fragment, merging with an existing fragment", () => {
      const page = bootHost();
      const a = mount(page);
      expect(a.handle.iframe.getAttribute("src")).toBe(
         `${SRC}#publisher-nonce=${a.nonce}`,
      );
      expect(a.nonce).toMatch(/^[0-9a-f]{32}$/);
      const b = mount(page, { src: `${SRC}#a=1&publisher-nonce=old&b=2` });
      expect(b.handle.iframe.getAttribute("src")).toBe(
         `${SRC}#a=1&b=2&publisher-nonce=${b.nonce}`,
      );
      const c = mount(page, { src: `${SRC}?x=1#sec` });
      expect(c.handle.iframe.getAttribute("src")).toBe(
         `${SRC}?x=1#sec&publisher-nonce=${c.nonce}`,
      );
      expect(new Set([a.nonce, b.nonce, c.nonce]).size).toBe(3);
   });

   it("keeps the deprecated token option as a query parameter, ahead of the fragment", () => {
      const page = bootHost();
      const m = mount(page, { token: "t k", src: `${SRC}#frag` });
      expect(m.handle.iframe.getAttribute("src")).toBe(
         `${SRC}?embed_token=t%20k#frag&publisher-nonce=${m.nonce}`,
      );
   });

   it("sandboxes with same-origin and has no isolate option", () => {
      const page = bootHost();
      for (const options of [{}, { isolate: true }]) {
         expect(
            mount(page, options).handle.iframe.getAttribute("sandbox"),
         ).toBe("allow-scripts allow-same-origin allow-forms");
      }
   });

   it("refuses a src without an http(s) origin, so no post could need *", () => {
      const page = bootHost();
      expect(() =>
         page.Publisher.embed("#slot", { src: "data:text/html,hi" }),
      ).toThrow("http(s) origin");
   });

   it("says hello once on each load, and never retries", async () => {
      const page = bootHost();
      const m = mount(page);
      expect(m.childPosts).toEqual([]);
      m.load();
      expect(m.childPosts).toEqual([
         {
            targetOrigin: APP,
            msg: { protocol: "publisher", v: 1, type: "hello", id: "hello" },
         },
      ]);
      await page.clock.advance(10000);
      expect(m.childPosts).toHaveLength(1);
      m.load();
      expect(m.childPosts).toHaveLength(2);
   });

   it("answers a page's unsolicited ready with a hello", () => {
      const page = bootHost();
      const m = mount(page);
      m.fromChild(m.msg("ready", { payload: { handles: [], runtime: "1" } }));
      expect(m.childPosts.map((p) => p.msg.type)).toEqual(["hello"]);
   });

   it("sends auth.token after ready when getToken is given, and not otherwise", async () => {
      const page = bootHost();
      const calls: number[] = [];
      const m = mount(page, {
         getToken: async () => {
            calls.push(1);
            return "tok-a";
         },
      });
      m.load();
      m.childPosts.length = 0;
      helloReply(m);
      await flush();
      await flush();
      expect(m.childPosts).toEqual([
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

      const plainEmbed = mount(page);
      plainEmbed.load();
      plainEmbed.childPosts.length = 0;
      helloReply(plainEmbed);
      await flush();
      expect(plainEmbed.childPosts).toEqual([]);
   });

   it("answers the request auth.token.request with a fresh token, and with handler_error when getToken fails", async () => {
      const page = bootHost();
      let n = 0;
      let fail = false;
      const m = mount(page, {
         getToken: () => {
            if (fail) throw new Error("no token");
            return `tok-${++n}`;
         },
      });
      m.load();
      helloReply(m);
      await flush();
      await flush();
      m.childPosts.length = 0;
      m.fromChild(m.msg("auth.token.request", { id: "p1" }));
      await flush();
      await flush();
      expect(m.childPosts).toEqual([
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
      m.childPosts.length = 0;
      m.fromChild(m.msg("auth.token.request", { id: "p2" }));
      await flush();
      await flush();
      expect(m.childPosts).toHaveLength(1);
      expect(m.childPosts[0].msg.error.code).toBe("handler_error");
   });

   it("answers a token request unhandled when there is no getToken, so the page fails at once", async () => {
      const m = mount(bootHost());
      m.load();
      helloReply(m);
      m.childPosts.length = 0;
      m.fromChild(m.msg("auth.token.request", { id: "p1" }));
      await flush();
      expect(m.childPosts).toHaveLength(1);
      expect(m.childPosts[0].msg.reply).toBe(true);
      expect(m.childPosts[0].msg.error.code).toBe("unhandled");
   });

   describe("never posting across a reload", () => {
      const deferred = <T>() => {
         let resolve!: (v: T) => void;
         let reject!: (e: Error) => void;
         const promise = new Promise<T>((res, rej) => {
            resolve = res;
            reject = rej;
         });
         return { promise, resolve, reject };
      };
      const ofType = (m: ReturnType<typeof mount>, type: string) =>
         m.childPosts.filter((p) => p.msg.type === type);

      it("posts the token on the normal path", async () => {
         const d = deferred<string>();
         const m = mount(bootHost(), { getToken: () => d.promise });
         m.load();
         helloReply(m);
         d.resolve("tok");
         await flush();
         await flush();
         expect(ofType(m, "auth.token")).toHaveLength(1);
      });

      it("drops a token that resolves after the frame reloaded", async () => {
         const d = deferred<string>();
         const m = mount(bootHost(), { getToken: () => d.promise });
         m.load();
         helloReply(m);
         m.load();
         d.resolve("tok");
         await flush();
         await flush();
         expect(ofType(m, "auth.token")).toHaveLength(0);
      });

      it("drops a token that resolves after destroy", async () => {
         const d = deferred<string>();
         const m = mount(bootHost(), { getToken: () => d.promise });
         m.load();
         helloReply(m);
         m.handle.destroy();
         d.resolve("tok");
         await flush();
         await flush();
         expect(ofType(m, "auth.token")).toHaveLength(0);
      });

      it("drops a handler reply, or error reply, that settles after a reload", async () => {
         const d = deferred<unknown>();
         const m = mount(bootHost());
         m.handle.on("ask", () => d.promise);
         m.load();
         helloReply(m);
         m.fromChild(m.msg("ask", { id: "r1" }));
         m.load();
         d.resolve(1);
         await flush();
         await flush();
         expect(ofType(m, "ask")).toHaveLength(0);

         const e = deferred<unknown>();
         const n = mount(bootHost());
         n.handle.on("ask", () => e.promise);
         n.load();
         helloReply(n);
         n.fromChild(n.msg("ask", { id: "r2" }));
         n.load();
         e.reject(new Error("boom"));
         await flush();
         await flush();
         expect(ofType(n, "ask")).toHaveLength(0);
      });

      it("drops a handler reply that settles after destroy, and answers in the same load", async () => {
         const d = deferred<unknown>();
         const m = mount(bootHost());
         m.handle.on("ask", () => d.promise);
         m.load();
         helloReply(m);
         m.fromChild(m.msg("ask", { id: "r1" }));
         m.handle.destroy();
         d.resolve(1);
         await flush();
         await flush();
         expect(ofType(m, "ask")).toHaveLength(0);

         const n = mount(bootHost());
         n.handle.on("ask", async () => 7);
         n.load();
         helloReply(n);
         n.fromChild(n.msg("ask", { id: "r2" }));
         await flush();
         await flush();
         expect(ofType(n, "ask").map((p) => p.msg.payload)).toEqual([7]);
      });
   });

   it("rejects messages from the wrong source, origin or nonce", async () => {
      const page = bootHost();
      let tokens = 0;
      const m = mount(page, { getToken: () => String(++tokens) });
      m.load();
      m.fromChild(m.msg("hello", { id: "hello", reply: true }), APP, {
         postMessage() {},
      });
      m.fromChild(
         m.msg("hello", { id: "hello", reply: true }),
         "https://evil.example.com",
      );
      m.fromChild(m.msg("hello", { id: "hello", reply: true }), "null");
      m.fromChild(m.msg("hello", { id: "hello", reply: true, nonce: "wrong" }));
      m.fromChild({
         ...m.msg("hello", { id: "hello", reply: true }),
         nonce: undefined,
      });
      m.fromChild({
         ...m.msg("hello", { id: "hello", reply: true }),
         protocol: "x",
      });
      await flush();
      expect(tokens).toBe(0);
      m.fromChild(
         m.msg("auth.token.request", { id: "p1" }),
         "https://evil.example.com",
      );
      m.fromChild({
         ...m.msg("auth.token.request", { id: "p2" }),
         nonce: "wrong",
      });
      await flush();
      expect(tokens).toBe(0);
   });

   it("send() waits for ready, resolves with the reply, and rejects an error reply with its code", async () => {
      const page = bootHost();
      const m = mount(page);
      m.load();
      m.childPosts.length = 0;
      const ok = m.handle.send("givens.set", { givens: { a: 1 } });
      const bad = m.handle.send("nobody", {});
      expect(m.childPosts).toEqual([]); // queued until the page has answered hello
      helloReply(m);
      await flush();
      const sent = m.childPosts.filter((p) => p.msg.type !== "auth.token");
      expect(sent.map((p) => p.msg.type)).toEqual(["givens.set", "nobody"]);
      expect(sent[0].msg.payload).toEqual({ givens: { a: 1 } });
      expect(sent[0].msg.nonce).toBeUndefined();
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
      const err = await bad.catch((e: Json) => e);
      expect(err.code).toBe("unhandled");
   });

   it("send() issued before the frame's first load still posts once the page replies", async () => {
      const page = bootHost();
      const m = mount(page);
      const sent = m.handle.send("ping", { a: 1 });
      m.load();
      helloReply(m);
      await flush();
      const ping = m.childPosts.filter((p) => p.msg.type === "ping");
      expect(ping).toHaveLength(1);
      m.fromChild(
         m.msg("ping", { id: ping[0].msg.id, reply: true, payload: "pong" }),
      );
      expect(await sent).toBe("pong");
   });

   it("a send queued across a reload posts to the document that answers", async () => {
      const page = bootHost();
      const m = mount(page);
      m.load();
      helloReply(m);
      await flush();
      m.load(); // navigated: not ready again until the new document answers
      m.childPosts.length = 0;
      const sent = m.handle.send("ping");
      expect(m.childPosts.map((p) => p.msg.type)).toEqual([]);
      helloReply(m);
      await flush();
      const ping = m.childPosts.filter((p) => p.msg.type === "ping");
      expect(ping).toHaveLength(1);
      m.fromChild(
         m.msg("ping", { id: ping[0].msg.id, reply: true, payload: 1 }),
      );
      expect(await sent).toBe(1);
   });

   it("send() rejects after 5 s and ignores a late reply", async () => {
      const page = bootHost();
      const m = mount(page);
      m.load();
      helloReply(m);
      await flush();
      m.childPosts.length = 0;
      const settled = m.handle.send("ping", {}).catch((e: Json) => e);
      const id = m.childPosts[0].msg.id;
      await page.clock.advance(4999);
      m.fromChild(m.msg("ping", { id: "nope", reply: true, payload: 1 })); // unknown id: ignored
      await page.clock.advance(1);
      const err = await settled;
      expect(err.message).toContain("no reply to 'ping'");
      m.fromChild(m.msg("ping", { id, reply: true, payload: 1 })); // late: ignored, no throw
   });

   it("send() rejects when never ready", async () => {
      const page = bootHost();
      const m = mount(page);
      const settled = m.handle.send("ping").catch((e: Json) => e);
      await page.clock.advance(5000);
      expect((await settled).message).toContain("no reply to 'ping'");
      expect(m.childPosts).toEqual([]);
   });

   it("on() receives the page's events and answers its requests", async () => {
      const page = bootHost();
      const m = mount(page);
      const seen: Json[] = [];
      const off = m.handle.on("query.ran", (p: Json) => seen.push(plain(p)));
      m.handle.on("chat.ask", () => ({ ok: 1 }));
      m.fromChild(m.msg("query.ran", { payload: { model: "m", rowCount: 2 } }));
      expect(seen).toEqual([{ model: "m", rowCount: 2 }]);
      off();
      m.fromChild(m.msg("query.ran", { payload: { model: "m2" } }));
      expect(seen).toHaveLength(1);

      m.childPosts.length = 0;
      m.fromChild(m.msg("chat.ask", { id: "p1" }));
      m.fromChild(m.msg("other.ask", { id: "p2" }));
      m.fromChild(m.msg("other.event"));
      await flush();
      const answers = Object.fromEntries(
         m.childPosts.map((p) => [p.msg.id, p.msg.payload ?? p.msg.error.code]),
      );
      expect(answers).toEqual({ p1: { ok: 1 }, p2: "unhandled" });
      expect(m.childPosts).toHaveLength(2);
   });

   it("still applies the legacy resize message from the iframe only", () => {
      const page = bootHost();
      const m = mount(page);
      m.fromChild({ type: "publisher:resize", height: 321 });
      expect(m.handle.iframe.style.height).toBe("321px");
      m.fromChild({ type: "publisher:resize", height: 999 }, APP, {
         postMessage() {},
      });
      expect(m.handle.iframe.style.height).toBe("321px");
   });

   it("never sends the nonce, and targets the page's exact origin, for every kind of post", async () => {
      const page = bootHost();
      const m = mount(page, { getToken: () => "tok" });
      m.load();
      helloReply(m);
      m.handle.on("chat.ask", () => 1);
      const sent = m.handle.send("ping");
      m.fromChild(m.msg("auth.token.request", { id: "p0" }));
      m.fromChild(m.msg("chat.ask", { id: "p1" }));
      m.fromChild(m.msg("nobody.ask", { id: "p2" }));
      await flush();
      await flush();
      const types = new Set(m.childPosts.map((p) => p.msg.type));
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
      for (const p of m.childPosts) {
         expect("nonce" in p.msg).toBe(false);
         expect(p.targetOrigin).toBe(APP);
      }
      m.handle.destroy();
      await sent.catch(() => {});
   });

   it("rejects a page that has not proved the nonce after a reload, and sends it no token", async () => {
      const page = bootHost();
      let tokens = 0;
      const m = mount(page, { getToken: () => String(++tokens) });
      m.load();
      helloReply(m);
      await flush();
      await flush();
      expect(tokens).toBe(1);
      // The frame navigated to a document that never had the fragment.
      m.load();
      m.childPosts.length = 0;
      const strangers = [
         {
            ...m.msg("hello", { id: "hello", reply: true }),
            nonce: undefined,
         },
         { ...m.msg("hello", { id: "hello", reply: true }), nonce: "wrong" },
         { ...m.msg("ready"), nonce: undefined },
         { ...m.msg("auth.token.request"), nonce: undefined },
         { ...m.msg("auth.token.request"), nonce: "wrong" },
      ];
      for (const data of strangers) m.fromChild(data);
      const queued = m.handle.send("secret", {});
      await flush();
      await flush();
      expect(tokens).toBe(1);
      expect(m.childPosts.filter((p) => p.msg.type !== "hello")).toEqual([]);
      void queued.catch(() => {});
      await page.clock.advance(5000);
      // The real page, with the fragment, still proves itself.
      m.load();
      helloReply(m);
      await flush();
      await flush();
      expect(tokens).toBe(2);
   });

   it("re-handshakes when the frame reloads", async () => {
      const page = bootHost();
      let tokens = 0;
      const m = mount(page, { getToken: () => `t${++tokens}` });
      m.load();
      helloReply(m);
      await flush();
      await flush();
      expect(tokens).toBe(1);
      m.childPosts.length = 0;
      m.load();
      expect(m.childPosts[0].msg.type).toBe("hello");
      helloReply(m);
      await flush();
      await flush();
      expect(tokens).toBe(2);
   });

   it("destroy() removes the frame, stops listening, and rejects pending sends", async () => {
      const page = bootHost();
      let tokens = 0;
      const m = mount(page, { getToken: () => String(++tokens) });
      m.load();
      const pending = m.handle.send("ping").catch((e: Json) => e);
      m.handle.destroy();
      expect((await pending).message).toContain("destroyed");
      expect(page.win.document.querySelector("#slot iframe")).toBeNull();
      helloReply(m);
      await flush();
      expect(tokens).toBe(0);
      expect(page.clock.pending()).toBe(0);
      await expect(m.handle.send("again")).rejects.toThrow("destroyed");
   });

   it("destroy() takes its load listener off the frame", () => {
      const page = bootHost();
      const m = mount(page);
      const removed: string[] = [];
      const remove = m.handle.iframe.removeEventListener.bind(m.handle.iframe);
      m.handle.iframe.removeEventListener = (type: string, fn: Json) => {
         removed.push(type);
         remove(type, fn);
      };
      m.handle.destroy();
      expect(removed).toContain("load");
      m.load();
      expect(m.childPosts).toEqual([]);
   });

   it("requires opts.src and a matching element", () => {
      const page = bootHost();
      expect(() => page.Publisher.embed("#missing", { src: SRC })).toThrow();
      expect(() => page.Publisher.embed("#slot", {})).toThrow();
   });

   describe("shared fixture with the SDK's createDataAppBridge", () => {
      const fixture = JSON.parse(
         fs.readFileSync(
            path.join(
               __dirname,
               "../../../sdk/src/utils/dataAppBus.fixture.json",
            ),
            "utf8",
         ),
      );
      // "$NONCE" and "$ID" in the fixture stand for the embed's nonce and the request id.
      const fill = (v: unknown, subs: Record<string, string>): unknown => {
         if (typeof v === "string") return subs[v] ?? v;
         if (Array.isArray(v)) return v.map((x) => fill(x, subs));
         if (v && typeof v === "object") {
            return Object.fromEntries(
               Object.entries(v).map(([k, x]) => [k, fill(x, subs)]),
            );
         }
         return v;
      };

      it("is about the origin these tests mount at", () => {
         expect(fixture.appOrigin).toBe(APP);
      });

      for (const c of fixture.events) {
         it(c.name, () => {
            const m = mount(bootHost());
            const seen: unknown[] = [];
            m.handle.on("probe.event", (p: unknown) => seen.push(p));
            m.fromChild(
               fill(c.message, { $NONCE: m.nonce }),
               c.origin,
               c.source === "iframe" ? m.child : { postMessage() {} },
            );
            expect(seen).toHaveLength(c.accept ? 1 : 0);
         });
      }

      for (const c of fixture.handshake) {
         it(`handshake: ${c.name}`, async () => {
            const m = mount(bootHost());
            const probe = m.handle
               .send("probe", {})
               .then((value: unknown) => ({ value }))
               .catch((error: Json) => ({ error }));
            if (c.loadFirst) m.load();
            m.fromChild(fill(c.message, { $NONCE: m.nonce }));
            await flush();
            const types = m.childPosts.map((p) => p.msg.type);
            expect(types.includes("probe")).toBe(c.probePosted);
            expect(types.includes("hello")).toBe(c.helloPosted);
            if (c.loadFirst && c.probePosted) {
               const id = m.childPosts.find((p) => p.msg.type === "probe")!.msg
                  .id;
               m.fromChild(m.msg("probe", { id, reply: true, payload: "ok" }));
               expect(await probe).toEqual({ value: "ok" });
            }
         });
      }

      for (const c of fixture.givensReplay) {
         it(`givens replay: ${c.name}`, async () => {
            const m = mount(bootHost());
            m.load();
            helloReply(m);
            await flush();
            for (const step of c.before) {
               if ("hostSends" in step) {
                  m.handle
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
            m.childPosts.length = 0;
            m.load();
            helloReply(m, {
               payload: { handles: c.rebindHandles, runtime: "1" },
            });
            await flush();
            const replays = () =>
               m.childPosts
                  .filter((p) => p.msg.type === "givens.set")
                  .map((p) => p.msg.payload);
            expect(replays()).toEqual(
               c.replayed ? [{ givens: c.replayed }] : [],
            );
            for (const r of c.readies ?? []) {
               m.childPosts.length = 0;
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
         const m = mount(bootHost());
         m.load();
         m.handle.send("givens.set", { givens: { a: 1 } }).catch(() => {});
         helloReply(m);
         await flush();
         expect(
            m.childPosts.filter((p) => p.msg.type === "givens.set"),
         ).toHaveLength(1);
      });

      for (const c of fixture.tokenRequests) {
         it(`token request: ${c.name}`, async () => {
            const getToken =
               c.getToken === "throws"
                  ? () => {
                       throw new Error("no token");
                    }
                  : c.getToken
                    ? () => c.getToken
                    : undefined;
            const m = mount(bootHost(), getToken ? { getToken } : {});
            m.load();
            helloReply(m);
            await flush();
            await flush();
            m.childPosts.length = 0;
            m.fromChild(fill(c.message, { $NONCE: m.nonce }));
            await flush();
            await flush();
            if (c.reply === null) {
               expect(m.childPosts).toEqual([]);
               return;
            }
            expect(m.childPosts).toHaveLength(1);
            const posted = m.childPosts[0].msg;
            expect(posted.type).toBe("auth.token.request");
            expect(posted.id).toBe(c.message.id);
            expect(posted.reply).toBe(true);
            if (c.reply.payload) {
               expect(posted.payload).toEqual(c.reply.payload);
            } else {
               expect(posted.error.code).toBe(c.reply.error.code);
            }
         });
      }

      for (const c of fixture.replies) {
         it(`reply: ${c.name}`, async () => {
            const page = bootHost();
            const m = mount(page);
            m.load();
            helloReply(m);
            await flush();
            m.childPosts.length = 0;
            const result = m.handle
               .send("probe.request", { a: 1 })
               .then((value: unknown) => ({ value }))
               .catch((error: Json) => ({ error }));
            const id = m.childPosts[0].msg.id;
            m.fromChild(fill(c.message, { $NONCE: m.nonce, $ID: id }));
            if ("timesOut" in c.outcome) await page.clock.advance(5000);
            const settled: Json = await result;
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
});
