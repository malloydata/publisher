// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

// Host side of the data-app event bus, for React hosts that own their iframe.
//
// A deliberate duplicate of `Publisher.embed` in
// packages/server/src/runtime/publisher.js, which cannot import TypeScript. Both specs
// read utils/dataAppBus.fixture.json, which is what keeps the two on one protocol.

/** Payload of each known event type. */
export interface PublisherPayloads {
   hello: ReadyPayload;
   ready: ReadyPayload;
   "givens.set": PublisherGivensPayload;
   "givens.changed": PublisherGivensPayload;
   "auth.token": { token: string };
   "auth.token.request": Record<string, never>;
   "version.unavailable": {
      versionId: string;
      reason: "VERSION_NOT_FOUND" | "VERSION_ARCHIVED";
   };
   "query.ran": PublisherQueryRan;
   "context.update": Record<string, unknown>;
   "chat.prefill": { text: string };
}

/** An envelope's `type`: the known events, or any string a page and its host agree on. */
export type PublisherEventType = keyof PublisherPayloads | (string & {});

/** What a page's `ready` event and its reply to `hello` carry. */
export interface ReadyPayload {
   /** Event types the page has handlers for. */
   handles: string[];
   /** The runtime's protocol revision. */
   runtime: string;
}

export interface PublisherGivensPayload {
   givens: Record<string, unknown>;
}

export interface PublisherQueryRan {
   model: string;
   sourceName?: string;
   queryName?: string;
   /** Query text, truncated by the runtime. */
   query?: string;
   givens: Record<string, unknown>;
   versionId: string | null;
   rowCount: number | null;
   ms: number;
}

export type PublisherPayload<T extends PublisherEventType> =
   T extends keyof PublisherPayloads ? PublisherPayloads[T] : unknown;

/**
 * A request or event, in either direction. An `id` makes it a request. Only the
 * page sends `nonce`: the host never does, so a document that navigated into the
 * frame cannot learn it.
 */
export interface PublisherEnvelope<
   T extends PublisherEventType = PublisherEventType,
> {
   protocol: "publisher";
   v: 1;
   type: T;
   id?: string | number;
   nonce?: string;
   payload?: PublisherPayload<T>;
}

export type PublisherErrorCode = "unhandled" | "handler_error" | (string & {});

/** The answer to a request: the same `type` and `id`, with a payload or an error. */
export interface PublisherReply<
   T extends PublisherEventType = PublisherEventType,
> extends PublisherEnvelope<T> {
   id: string | number;
   reply: true;
   error?: { code: PublisherErrorCode; message: string };
}

export interface PublisherMessageMeta {
   type: string;
   id?: string | number;
}

/** A handler may return a value (or a promise) to answer a request; events ignore it. */
export type PublisherHandler<
   T extends PublisherEventType = PublisherEventType,
> = (payload: PublisherPayload<T>, meta: PublisherMessageMeta) => unknown;

export interface DataAppBridgeOptions {
   /** The page's URL. Its origin is the only one the bridge accepts and posts to. */
   src?: string;
   /** The page's origin, when `src` is not at hand. */
   origin?: string;
   /**
    * The bearer token for the page: pushed once the page proves the nonce, and returned in
    * the reply to the page's `auth.token.request`. Without it that request is answered
    * `unhandled`, so the page's 401 fails at once instead of waiting.
    */
   getToken?: () => string | Promise<string>;
   /**
    * Defaults to a fresh random value. Pass the nonce the iframe's `src` already carries
    * when the bridge is created again for the same iframe (a React effect that re-runs):
    * the page reads its nonce once, so a new random one would never match it.
    */
   nonce?: string;
}

export interface DataAppBridge {
   nonce: string;
   /** `withDataAppNonce(src, nonce)`: `src` with this bridge's nonce in its fragment. */
   srcWithNonce(src: string): string;
   /** A request to the page. Waits for the page to be ready; rejects after 5 s. */
   send<T extends PublisherEventType>(
      type: T,
      payload?: PublisherPayload<T>,
   ): Promise<unknown>;
   /** Listens for the page's events and requests; returns the unsubscribe. */
   on<T extends PublisherEventType>(
      type: T,
      fn: PublisherHandler<T>,
   ): () => void;
   /** Resolves when the page first answers `hello`; rejects if the bridge is destroyed first. */
   ready: Promise<ReadyPayload>;
   /** Event types the page last said it handles. */
   handles(): string[];
   destroy(): void;
}

/** The rejection of `send` when the page answers with an error, or never answers. */
export class PublisherBridgeError extends Error {
   code?: PublisherErrorCode;
}

const HELLO_TRIES = 5;
const HELLO_INTERVAL_MS = 500;
const SEND_TIMEOUT_MS = 5000;

/** A fresh random nonce. Make it before the iframe's `src`, and hand the same one to `createDataAppBridge`. */
export function createDataAppNonce(): string {
   const bytes = new Uint8Array(16);
   crypto.getRandomValues(bytes);
   return Array.from(bytes, (b) => b.toString(16).padStart(2, "0")).join("");
}

/**
 * `src` with the nonce in its fragment, merged with any fragment it already has (an old
 * nonce is replaced). The iframe must load this URL: the page reads the nonce from it.
 */
export function withDataAppNonce(src: string, nonce: string): string {
   const hash = src.indexOf("#");
   const base = hash === -1 ? src : src.slice(0, hash);
   const fragment =
      hash === -1
         ? ""
         : src
              .slice(hash + 1)
              .replace(/(^|&)publisher-nonce=[^&]*/g, "")
              .replace(/^&/, "");
   return `${base}#${fragment ? `${fragment}&` : ""}publisher-nonce=${nonce}`;
}

function isEnvelope(m: unknown): m is PublisherEnvelope & {
   reply?: unknown;
   error?: { code?: string; message?: string };
} {
   if (typeof m !== "object" || m === null) return false;
   const e = m as Record<string, unknown>;
   return (
      e.protocol === "publisher" &&
      e.v === 1 &&
      typeof e.type === "string" &&
      e.type !== "" &&
      (e.id === undefined ||
         typeof e.id === "string" ||
         typeof e.id === "number")
   );
}

function readyPayloadOf(p: unknown): ReadyPayload | undefined {
   if (typeof p !== "object" || p === null) return undefined;
   const { handles, runtime } = p as Record<string, unknown>;
   return {
      handles: Array.isArray(handles)
         ? handles.filter((h): h is string => typeof h === "string")
         : [],
      runtime: typeof runtime === "string" ? runtime : "",
   };
}

export function createDataAppBridge(
   iframe: HTMLIFrameElement,
   opts: DataAppBridgeOptions = {},
): DataAppBridge {
   const win = iframe.ownerDocument.defaultView ?? window;
   const source = opts.origin ?? opts.src;
   if (source === undefined) {
      throw new TypeError(
         "createDataAppBridge: opts.src or opts.origin is required",
      );
   }
   const pageOrigin = new URL(source, win.location.href).origin;
   if (pageOrigin === "null") {
      throw new TypeError(
         "createDataAppBridge: the page needs an http(s) origin",
      );
   }
   const nonce = opts.nonce ?? createDataAppNonce();

   const pending = new Map<
      string,
      {
         resolve: (v: unknown) => void;
         reject: (e: Error) => void;
         timer: ReturnType<typeof setTimeout>;
      }
   >();
   const handlers = new Map<string, PublisherHandler[]>();
   let readyWaiters: (() => void)[] = [];
   let pageHandles: string[] = [];
   let lastReady: ReadyPayload = { handles: [], runtime: "" };
   let isReady = false;
   let readySettled = false;
   let gone = false;
   // Bumped on every load and on destroy: a token or a reply to a page request is dropped if the frame moved on while it was pending.
   let generation = 0;
   let helloTries = 0;
   let helloTimer: ReturnType<typeof setTimeout> | undefined;
   let seq = 0;
   let resolveReady: (p: ReadyPayload) => void = () => {};
   let rejectReady: (e: Error) => void = () => {};
   const ready = new Promise<ReadyPayload>((resolve, reject) => {
      resolveReady = resolve;
      rejectReady = reject;
   });
   // A host that never awaits `ready` must not see an unhandled rejection on destroy.
   ready.catch(() => {});

   // The nonce is never posted: only the page may echo it, so the host's messages teach a stranger nothing.
   function post(
      type: string,
      payload?: unknown,
      extra?: {
         id?: string | number;
         reply?: boolean;
         error?: { code: string; message: string };
      },
   ): boolean {
      const msg: Record<string, unknown> = {
         protocol: "publisher",
         v: 1,
         type,
      };
      if (extra?.id !== undefined) msg.id = extra.id;
      if (extra?.reply) msg.reply = true;
      if (extra?.error) msg.error = extra.error;
      if (payload !== undefined) msg.payload = payload;
      try {
         iframe.contentWindow?.postMessage(msg, pageOrigin);
         return true;
      } catch {
         return false;
      }
   }

   // The page may load after the first hello, or never answer (an old runtime).
   function sayHello() {
      clearTimeout(helloTimer);
      if (isReady || gone || helloTries >= HELLO_TRIES) return;
      helloTries++;
      post("hello", undefined, { id: `hello-${helloTries}` });
      helloTimer = setTimeout(sayHello, HELLO_INTERVAL_MS);
   }

   function sendToken() {
      if (!opts.getToken) return;
      const gen = generation;
      Promise.resolve()
         .then(opts.getToken)
         .then(
            (token) => {
               if (token && gen === generation && !gone)
                  post("auth.token", { token: String(token) });
            },
            () => {
               /* the page's own 401 handling surfaces a missing token */
            },
         );
   }

   // The reply to the page's auth.token.request; a getToken that fails answers with handler_error.
   async function answerTokenRequest() {
      const token = await opts.getToken?.();
      if (!token) throw new Error("no token");
      return { token: String(token) };
   }

   function noteReady(payload: unknown) {
      const p = readyPayloadOf(payload);
      if (!p) return;
      pageHandles = p.handles;
      lastReady = p;
   }

   function onReady() {
      if (isReady) return;
      isReady = true;
      clearTimeout(helloTimer);
      if (!readySettled) {
         readySettled = true;
         resolveReady(lastReady);
      }
      sendToken();
      readyWaiters.splice(0).forEach((fn) => fn());
   }

   function onMessage(e: MessageEvent) {
      const frame = iframe.contentWindow;
      if (gone || !frame || e.source !== frame || e.origin !== pageOrigin) {
         return;
      }
      const m = e.data;
      // The page echoes the nonce from its own fragment; a document that navigated in has none.
      if (!isEnvelope(m) || m.nonce !== nonce) return;
      const isRequest = m.id !== undefined && !m.reply;
      if (m.reply) {
         if (m.type === "hello") {
            noteReady(m.payload);
            onReady();
            return;
         }
         const key = String(m.id);
         const waiting = m.id === undefined ? undefined : pending.get(key);
         if (!waiting) return;
         pending.delete(key);
         clearTimeout(waiting.timer);
         if (m.error) {
            const err = new PublisherBridgeError(
               `Publisher bridge: ${m.error.message ?? m.type}`,
            );
            err.code = m.error.code;
            waiting.reject(err);
         } else {
            waiting.resolve(m.payload);
         }
         return;
      }
      if (m.type === "ready") {
         noteReady(m.payload);
         if (!isReady) {
            // The page is up and offered itself before our hello landed; answer now.
            helloTries = 0;
            sayHello();
         }
      }
      const list = (handlers.get(m.type) ?? []).slice();
      if (m.type === "auth.token.request" && isRequest && opts.getToken) {
         list.unshift(answerTokenRequest);
      }
      if (list.length === 0) {
         if (isRequest) {
            post(m.type, undefined, {
               id: m.id,
               reply: true,
               error: {
                  code: "unhandled",
                  message: `No handler for '${m.type}'`,
               },
            });
         }
         return;
      }
      const gen = generation;
      const meta: PublisherMessageMeta = { type: m.type, id: m.id };
      const results = list.map((fn) => {
         try {
            return Promise.resolve(fn(m.payload as never, meta));
         } catch (err) {
            return Promise.reject(err);
         }
      });
      if (!isRequest) {
         results.forEach((r) => r.catch(() => {}));
         return;
      }
      Promise.all(results).then(
         (values) => {
            if (gen !== generation || gone) return;
            const answer = values.find((v) => v !== undefined);
            post(m.type, answer === undefined ? null : answer, {
               id: m.id,
               reply: true,
            });
         },
         (err) => {
            if (gen !== generation || gone) return;
            post(m.type, undefined, {
               id: m.id,
               reply: true,
               error: {
                  code: "handler_error",
                  message: String(err?.message ?? err),
               },
            });
         },
      );
   }

   // A (re)load is a fresh page that has not proved the nonce yet.
   function onLoad() {
      generation++;
      isReady = false;
      helloTries = 0;
      sayHello();
   }

   win.addEventListener("message", onMessage);
   iframe.addEventListener("load", onLoad);
   // Created after the frame loaded (an effect that re-runs): no load event is coming, so greet it now.
   const loaded = iframe.getAttribute("src");
   if (loaded && loaded !== "about:blank") sayHello();

   return {
      nonce,
      srcWithNonce: (src) => withDataAppNonce(src, nonce),
      send(type, payload) {
         return new Promise((resolve, reject) => {
            if (gone) {
               reject(new PublisherBridgeError("Publisher bridge: destroyed"));
               return;
            }
            const id = `h${++seq}`;
            const timer = setTimeout(() => {
               pending.delete(id);
               reject(
                  new PublisherBridgeError(
                     `Publisher bridge: no reply to '${type}' in 5 s`,
                  ),
               );
            }, SEND_TIMEOUT_MS);
            pending.set(id, { resolve, reject, timer });
            // Not generation-guarded: a queued send posts once the current document has proved itself.
            const go = () => {
               if (pending.has(id)) post(type, payload, { id });
            };
            if (isReady) go();
            else readyWaiters.push(go);
         });
      },
      on(type, fn) {
         const list = handlers.get(type) ?? [];
         list.push(fn as PublisherHandler);
         handlers.set(type, list);
         return () => {
            const i = list.indexOf(fn as PublisherHandler);
            if (i !== -1) list.splice(i, 1);
         };
      },
      ready,
      handles: () => pageHandles.slice(),
      destroy() {
         if (gone) return;
         gone = true;
         generation++;
         clearTimeout(helloTimer);
         win.removeEventListener("message", onMessage);
         iframe.removeEventListener("load", onLoad);
         readyWaiters = [];
         pending.forEach((p) => {
            clearTimeout(p.timer);
            p.reject(new PublisherBridgeError("Publisher bridge: destroyed"));
         });
         pending.clear();
         if (!readySettled) {
            readySettled = true;
            rejectReady(
               new PublisherBridgeError("Publisher bridge: destroyed"),
            );
         }
      },
   };
}
