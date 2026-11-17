// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

// Publisher runtime helper for in-package HTML dashboards.
// Served by the Publisher server at /sdk/publisher.js. Hand-authored vanilla
// JS — no bundler. Loaded via <script src="/sdk/publisher.js">.
//
// Exposes window.Publisher with:
//   - Publisher.query(model, malloy, opts?)     → Promise<rows[]>
//   - Publisher.queryFull(model, malloy, opts?) → Promise<MalloyResult>  (envelope for <malloy-render>)
//       opts: { environment?, package?, sourceName?, queryName?, filterParams?,
//               bypassFilters?, givens?, versionId? }. versionId names a published
//               version; a page opened with `?versionId=` queries its own
//               package at that version by default. givens is a name→value map bound as
//               Malloy given: runtime parameters for this query (safe parameterization,
//               not string interpolation) — see the malloy-html-data-apps skill (reference/runtime.md).
//   - Publisher.embed(selector, { src, height?, token?, getToken? })
//       → { iframe, destroy(), send(type, payload) → Promise, on(type, fn) → unsubscribe }.
//       getToken() (a string or a Promise) feeds the page its bearer token over the bus:
//       first on ready, again whenever the page asks. `token` appends ?embed_token= to
//       the URL instead and is deprecated. Every embed adds a random #publisher-nonce=<n> to src; the host accepts
//       bus messages only from that window, at src's origin, echoing that nonce. The host
//       never sends the nonce: a document that navigated into the frame cannot learn it.
//   - Publisher.context  ({ environment, package } from the injected page context, else inferred from URL)
//   - Publisher.setToken(token)  (override Bearer token; default uses cookies)
//   - Publisher.on(type, fn) → unsubscribe  (host → page messages; fn(payload, meta) may
//       return a value or promise, which answers a request)
//   - Publisher.emit(type, payload)  (page → host event; dropped until the host has said hello)
//   - Publisher.givens (a copy), setGivens(g), onGivensChange(fn) → unsubscribe
//       A page-held name→value map, merged under opts.givens of every query once non-empty.
//       A null value removes its key.
//
// Server-injected context: Publisher serves every data-app HTML page with
// <script type="application/json" id="publisher-context"> carrying
// { v, environment, package, versionId|null, apiBase, hostOrigins }. Names parsed from
// the page's own URL win (a gateway may rename paths, so the context's names are the
// ones behind it); the context's names are used only when the URL has none. Its
// versionId replaces the ?versionId= probe: it pins every query to the page's own
// package, and a versioned page never live-reloads. A page served from
// /environments/<e>/packages/<p>/versions/<v>/public/ pins <v> even with no context.
//
// Host bus (only in an iframe whose context has hostOrigins, patterns from the server's
// frame-ancestors setting). Envelope { protocol: "publisher", v: 1, type, id?, nonce?,
// payload }; a reply adds reply: true and carries payload or error: { code, message }.
// The host sends `hello`; the runtime binds to that message's origin, replies with
// { handles, runtime }, and from then on posts only to that origin. Inbound messages are
// accepted from window.parent at an allowed origin only. Every page → host envelope
// echoes the nonce from #publisher-nonce= when the URL has one (read once, then removed
// from the URL; restored before the runtime's own reload); host → page ones carry none.
// Page → host events: ready, query.ran, givens.changed, version.unavailable. Host → page:
// auth.token (built in), givens.set (built in; advertised once the page calls
// onGivensChange), and whatever the page registers with on(). A request nobody handles
// gets error "unhandled".
// On a 401 with the bus bound, the runtime sends the request auth.token.request, which the
// host answers with { token } (or "unhandled" when it has no getToken, which fails the
// query at once), waits up to 10 s for that or an auth.token event, and retries once.
//
// When loaded inside an iframe served from /environments/<env>/packages/<pkg>/...,
// the runtime auto-subscribes to a Server-Sent Events live-reload stream
// (GET .../events) and reloads the page on file changes. Served from
// /apps/<env>/<pkg>/... (a hosted embed surface) it infers the same context
// but does not subscribe: that page belongs to a customer's site. On both it
// posts size updates to the parent window so Publisher.embed() in the host
// can resize the iframe.
//
// The "publisher:resize" postMessage protocol below is the SAME contract the
// SPA host consumes. Its canonical definition lives in
// packages/sdk/src/utils/dataAppEmbed.ts (PUBLISHER_RESIZE_MESSAGE_TYPE /
// PublisherResizeMessage). This file is build-step-free vanilla JS and can't
// import it, so keep the message type/shape here in sync with that module.

(function () {
   "use strict";

   // --- Context inference -------------------------------------------------
   // URL shapes: /environments/<env>/packages/<pkg>/<file> (the in-app viewer)
   //             /apps/<env>/<pkg>/<file>                   (a hosted embed surface)
   //
   // location.pathname is URL-encoded, so we MUST decode the captured
   // segments here. Without this step, a name with a space (e.g.
   // "demo env") would arrive as "demo%20env" — and the encodeURIComponent
   // we apply when building API URLs (below) would produce "demo%2520env",
   // which Publisher then 404s on.
   var inAppMatch = location.pathname.match(
      /^\/environments\/([^/]+)\/packages\/([^/]+)\//,
   );
   var pathMatch =
      inAppMatch || location.pathname.match(/^\/apps\/([^/]+)\/([^/]+)\//);
   function safeDecode(s) {
      try {
         return decodeURIComponent(s);
      } catch (_e) {
         return s;
      }
   }
   function isString(v) {
      return typeof v === "string";
   }
   // The server-injected context; null when the page predates it or it is malformed.
   function readPageContext() {
      try {
         if (typeof document === "undefined") return null;
         var el = document.getElementById("publisher-context");
         var parsed = el ? JSON.parse(el.textContent) : null;
         return parsed && typeof parsed === "object" ? parsed : null;
      } catch (_e) {
         return null;
      }
   }
   var pageContext = readPageContext();
   // The URL's names win: behind a gateway that renames paths they are the ones the API knows.
   var ctx = pathMatch
      ? {
           environment: safeDecode(pathMatch[1]),
           package: safeDecode(pathMatch[2]),
        }
      : pageContext &&
          isString(pageContext.environment) &&
          isString(pageContext.package) &&
          pageContext.environment &&
          pageContext.package
        ? {
             environment: pageContext.environment,
             package: pageContext.package,
          }
        : {};
   // A version in the path never changes, so it pins queries and needs no live reload, context or not.
   var pathVersion = location.pathname.match(
      /^\/environments\/[^/]+\/packages\/[^/]+\/versions\/([^/]+)\/public\//,
   );
   var fixedVersion =
      pageContext && isString(pageContext.versionId) && pageContext.versionId
         ? pageContext.versionId
         : pathVersion
           ? safeDecode(pathVersion[1])
           : undefined;

   // Only a same-origin path is honored: the API base is never taken from another origin.
   var configuredApiBase = pageContext && pageContext.apiBase;
   var apiBase =
      location.origin +
      (isString(configuredApiBase) && /^\/(?!\/)/.test(configuredApiBase)
         ? configuredApiBase.replace(/\/+$/, "")
         : "/api/v0");
   var bearerToken = null;

   var inIframe = (function () {
      try {
         return window.self !== window.top;
      } catch (_e) {
         return true; // cross-origin parent — assume embedded
      }
   })();

   function authHeaders() {
      return bearerToken ? { Authorization: "Bearer " + bearerToken } : {};
   }

   // --- Published-version pinning -----------------------------------------
   // A page opened at a published version (`?versionId=` in its URL) queries
   // that version, so it keeps answering from the models it shipped with after
   // the package's `latest` moves on. Only when that version is one the
   // package has: a proxy may forward a page's query string unchanged, with a
   // `versionId` of its own, and such a page queries `latest`, as the server
   // serves its files. One read of the package at the page's version tells.
   function pageVersionParam() {
      // Read by hand, not with URLSearchParams, so the runtime also loads where
      // that is not defined. `+` in a query string is a space.
      var match = (location.search || "").match(/[?&]versionId=([^&#]*)/);
      return match && match[1]
         ? safeDecode(match[1].replace(/\+/g, " "))
         : undefined;
   }
   var pageVersion = pageVersionParam();
   var pagePin = null;
   function pinnedVersion() {
      if (fixedVersion) return Promise.resolve(fixedVersion);
      if (!pageVersion || !ctx.environment || !ctx.package) {
         return Promise.resolve(undefined);
      }
      if (!pagePin) {
         pagePin = fetch(
            apiBase +
               "/environments/" +
               encodeURIComponent(ctx.environment) +
               "/packages/" +
               encodeURIComponent(ctx.package) +
               "?versionId=" +
               encodeURIComponent(pageVersion),
            { credentials: "include", headers: authHeaders() },
         )
            .then(function (res) {
               return res.ok ? res.json() : null;
            })
            .then(function (pkg) {
               return pkg && pkg.versionId === pageVersion
                  ? pageVersion
                  : undefined;
            })
            .catch(function () {
               return undefined;
            });
      }
      return pagePin;
   }

   // --- Host bus (page side) ----------------------------------------------
   var RUNTIME_VERSION = "1";
   var QUERY_TEXT_LIMIT = 2000;
   var TOKEN_WAIT_MS = 10000;

   var hostOrigins =
      pageContext && Array.isArray(pageContext.hostOrigins)
         ? pageContext.hostOrigins.filter(isString)
         : [];
   // `fragment` (no leading #) without the nonce parameter, every other byte untouched.
   function withoutNonce(fragment) {
      return fragment
         .replace(/(^|&)publisher-nonce=[^&]*/g, "")
         .replace(/^&/, "");
   }
   function setFragment(fragment) {
      try {
         history.replaceState(
            history.state,
            "",
            location.pathname +
               location.search +
               (fragment ? "#" + fragment : ""),
         );
      } catch (_e) {
         /* a frame that cannot rewrite its URL keeps the nonce in it */
      }
   }
   // Read once at load; the host puts it in the fragment so it never reaches a log or Referer,
   // and the page drops it from the URL so its own scripts and router never see it.
   var pageNonce = (function () {
      var m = /[#&]publisher-nonce=([^&]*)/.exec(location.hash || "");
      if (!m || !m[1]) return null;
      setFragment(withoutNonce(location.hash.slice(1)));
      return safeDecode(m[1]);
   })();
   // A reload re-reads the URL, so the nonce goes back into it first.
   function reloadWithNonce() {
      if (pageNonce) {
         var rest = location.hash.slice(1);
         setFragment(
            (rest ? rest + "&" : "") +
               "publisher-nonce=" +
               encodeURIComponent(pageNonce),
         );
      }
      location.reload();
   }
   var busEnabled =
      inIframe &&
      hostOrigins.length > 0 &&
      typeof window.addEventListener === "function";
   var boundOrigin = null; // set by the first valid `hello`; every post targets it
   var handlers = {};
   var givensStore = {};
   var givensListeners = [];
   var readyScheduled = false;
   var tokenWait = null;
   var tokenSettle = null;
   var tokenRequestId = null;
   var requestSeq = 0;
   var versionUnavailableSent = false;

   // 'self' is this page's origin; `https://*.suffix[:port]` matches the scheme, the port and
   // any host ending in ".suffix" (a label boundary); anything else is an exact origin.
   function originMatches(origin, pattern) {
      if (pattern === "'self'") {
         return location.origin !== "null" && origin === location.origin;
      }
      var wild = /^(https?):\/\/\*\.([^/:]+)(?::(\d+))?$/.exec(pattern);
      if (!wild) return origin === pattern;
      var o = /^(https?):\/\/([^/:]+)(?::(\d+))?$/.exec(origin);
      var tail = "." + wild[2];
      return (
         !!o &&
         o[1] === wild[1] &&
         o[3] === wild[3] &&
         o[2].length > tail.length &&
         o[2].slice(-tail.length) === tail
      );
   }
   function originAllowed(origin) {
      if (!isString(origin)) return false;
      for (var i = 0; i < hostOrigins.length; i++) {
         if (originMatches(origin, hostOrigins[i])) return true;
      }
      return false;
   }
   // Patterns that name one origin: the only places a page may speak before it is bound.
   function exactOrigins() {
      var out = [];
      for (var i = 0; i < hostOrigins.length; i++) {
         var p = hostOrigins[i];
         var origin = p === "'self'" ? location.origin : p;
         if (origin === "null" || origin.indexOf("*") !== -1) continue;
         if (out.indexOf(origin) === -1) out.push(origin);
      }
      return out;
   }

   function isEnvelope(m) {
      return (
         !!m &&
         typeof m === "object" &&
         m.protocol === "publisher" &&
         m.v === 1 &&
         isString(m.type) &&
         m.type !== "" &&
         (m.id === undefined || isString(m.id) || typeof m.id === "number")
      );
   }

   // extra: { id?, reply?, error? }. Never posts to "*"; false when nothing could be sent.
   function post(type, payload, extra, target) {
      var origin = target || boundOrigin;
      if (!busEnabled || !origin) return false;
      var msg = { protocol: "publisher", v: 1, type: type };
      if (extra) {
         if (extra.id !== undefined) msg.id = extra.id;
         if (extra.reply) msg.reply = true;
         if (extra.error) msg.error = extra.error;
      }
      if (pageNonce) msg.nonce = pageNonce;
      if (payload !== undefined) msg.payload = payload;
      try {
         window.parent.postMessage(msg, origin);
         return true;
      } catch (_e) {
         return false;
      }
   }

   function handleTypes() {
      var types = Object.keys(handlers).filter(function (t) {
         return handlers[t].length > 0;
      });
      if (givensListeners.length > 0 && types.indexOf("givens.set") === -1) {
         types.push("givens.set");
      }
      return types;
   }
   function readyPayload() {
      return { handles: handleTypes(), runtime: RUNTIME_VERSION };
   }
   // One `ready` per task however many handlers a page adds in a row.
   function scheduleReady() {
      if (!boundOrigin || readyScheduled) return;
      readyScheduled = true;
      Promise.resolve().then(function () {
         readyScheduled = false;
         post("ready", readyPayload());
      });
   }

   function on(type, fn) {
      if (!isString(type) || typeof fn !== "function") {
         throw new TypeError(
            "Publisher.on(type, fn): type must be a string and fn a function",
         );
      }
      (handlers[type] = handlers[type] || []).push(fn);
      scheduleReady();
      return function () {
         var list = handlers[type];
         var i = list ? list.indexOf(fn) : -1;
         if (i === -1) return;
         list.splice(i, 1);
         scheduleReady();
      };
   }
   function emit(type, payload) {
      return isString(type) && post(type, payload);
   }

   // --- Givens store ------------------------------------------------------
   function cloneGivens(g) {
      return JSON.parse(JSON.stringify(g));
   }
   function setGivens(g) {
      if (!g || typeof g !== "object" || Array.isArray(g)) {
         throw new TypeError("Publisher.setGivens(g): g must be an object");
      }
      var next = cloneGivens(g);
      Object.keys(next).forEach(function (k) {
         if (next[k] === null) delete givensStore[k];
         else givensStore[k] = next[k];
      });
      givensListeners.slice().forEach(function (fn) {
         try {
            fn(cloneGivens(givensStore));
         } catch (_e) {
            /* one listener must not stop the rest */
         }
      });
      emit("givens.changed", { givens: cloneGivens(givensStore) });
   }
   function onGivensChange(fn) {
      if (typeof fn !== "function") {
         throw new TypeError(
            "Publisher.onGivensChange(fn): fn must be a function",
         );
      }
      givensListeners.push(fn);
      scheduleReady();
      return function () {
         var i = givensListeners.indexOf(fn);
         if (i === -1) return;
         givensListeners.splice(i, 1);
         scheduleReady();
      };
   }
   function applyGivensSet(payload) {
      setGivens(payload && payload.givens);
      return { applied: true, givens: cloneGivens(givensStore) };
   }

   // --- Bearer token over the bus ----------------------------------------
   function applyAuthToken(payload) {
      var token = payload && payload.token;
      if (!isString(token) || !token) {
         throw new TypeError("auth.token needs payload.token (a string)");
      }
      bearerToken = token;
      if (tokenSettle) tokenSettle(true);
      return { applied: true };
   }
   // The host's answer to our auth.token.request: { token }, or an error ("unhandled" when it has no token to give).
   function onHostReply(m) {
      if (m.type !== "auth.token.request" || m.id !== tokenRequestId) return;
      if (m.error) {
         if (tokenSettle) tokenSettle(false);
         return;
      }
      try {
         applyAuthToken(m.payload);
      } catch (_e) {
         if (tokenSettle) tokenSettle(false);
      }
   }
   // Resolves true when a token arrives (as the reply or an auth.token event), false on an error
   // reply or after TOKEN_WAIT_MS. Concurrent callers share one wait.
   function awaitToken() {
      if (!tokenWait) {
         tokenWait = new Promise(function (resolve) {
            var timer = setTimeout(function () {
               settle(false);
            }, TOKEN_WAIT_MS);
            function settle(ok) {
               clearTimeout(timer);
               tokenWait = null;
               tokenSettle = null;
               tokenRequestId = null;
               resolve(ok);
            }
            tokenSettle = settle;
         });
         tokenRequestId = "p" + ++requestSeq;
         post("auth.token.request", undefined, { id: tokenRequestId });
      }
      return tokenWait;
   }

   function handlersFor(type) {
      var list = (handlers[type] || []).slice();
      if (type === "givens.set" && list.length === 0) list.push(applyGivensSet);
      if (type === "auth.token") list.unshift(applyAuthToken);
      return list;
   }

   function onHostMessage(e) {
      if (e.source !== window.parent || !originAllowed(e.origin)) return;
      if (boundOrigin && e.origin !== boundOrigin) return;
      var m = e.data;
      if (!isEnvelope(m)) return;
      if (m.reply) {
         if (boundOrigin) onHostReply(m);
         return;
      }
      var isRequest = m.id !== undefined;
      if (m.type === "hello") {
         boundOrigin = e.origin;
         if (isRequest) {
            post("hello", readyPayload(), { id: m.id, reply: true });
         } else {
            post("ready", readyPayload());
         }
         return;
      }
      if (!boundOrigin) return;
      var list = handlersFor(m.type);
      if (list.length === 0) {
         if (isRequest) {
            post(m.type, undefined, {
               id: m.id,
               reply: true,
               error: {
                  code: "unhandled",
                  message: "No handler for '" + m.type + "'",
               },
            });
         }
         return;
      }
      var meta = { type: m.type, origin: e.origin, id: m.id };
      var results = list.map(function (fn) {
         try {
            return Promise.resolve(fn(m.payload, meta));
         } catch (err) {
            return Promise.reject(err);
         }
      });
      if (!isRequest) {
         results.forEach(function (r) {
            r.catch(function () {});
         });
         return;
      }
      Promise.all(results).then(
         function (values) {
            var answer = values.find(function (v) {
               return v !== undefined;
            });
            post(m.type, answer === undefined ? null : answer, {
               id: m.id,
               reply: true,
            });
         },
         function (err) {
            post(m.type, undefined, {
               id: m.id,
               reply: true,
               error: {
                  code: "handler_error",
                  message: String((err && err.message) || err),
               },
            });
         },
      );
   }

   function setUpHostBus() {
      if (!busEnabled) return;
      window.addEventListener("message", onHostMessage);
      // A mismatched targetOrigin drops silently, so offering ready to each exact origin is safe.
      exactOrigins().forEach(function (origin) {
         post("ready", readyPayload(), null, origin);
      });
   }

   // --- Query helpers -----------------------------------------------------
   function resolveTarget(opts) {
      var env = (opts && opts.environment) || ctx.environment;
      var pkg = (opts && opts.package) || ctx.package;
      if (!env || !pkg) {
         throw new Error(
            "Publisher: no environment/package; either serve the page from " +
               "/environments/<env>/packages/<pkg>/... or /apps/<env>/<pkg>/..., " +
               "or pass { environment, package } in opts.",
         );
      }
      return { env: env, pkg: pkg };
   }

   async function rawQuery(modelPath, malloyQuery, opts, compactJson) {
      opts = opts || {};
      var target = resolveTarget(opts);
      var url =
         apiBase +
         "/environments/" +
         encodeURIComponent(target.env) +
         "/packages/" +
         encodeURIComponent(target.pkg) +
         "/models/" +
         // Encode the whole model path as ONE path segment (slashes -> %2F). A model
         // in a subfolder ("model/x.malloy") must not become two URL segments: some
         // servers only match a single-segment {path} and would 405 a multi-segment
         // path. This matches the typed SDK (encodeURIComponent) and the server's
         // encoded-slash handling.
         encodeURIComponent(modelPath) +
         "/query";
      var body = { compactJson: compactJson };
      if (malloyQuery) body.query = malloyQuery;
      if (opts.sourceName) body.sourceName = opts.sourceName;
      if (opts.queryName) body.queryName = opts.queryName;
      if (opts.filterParams) body.filterParams = opts.filterParams;
      if (opts.bypassFilters) body.bypassFilters = true;
      // Store givens ride along only once the page has set some, so an untouched page's body is unchanged.
      if (opts.givens || Object.keys(givensStore).length > 0) {
         body.givens = Object.assign({}, givensStore, opts.givens);
      }
      // The page's own version applies only to its own package; a query aimed
      // elsewhere gets that package's latest unless it names one.
      var isOwnPackage =
         target.env === ctx.environment && target.pkg === ctx.package;
      var versionId =
         opts.versionId || (isOwnPackage ? await pinnedVersion() : undefined);
      if (versionId) body.versionId = versionId;

      var started = Date.now();
      var out = await postQuery(url, body);
      // Only a bound bus can be handed a token; a cookie-only page fails at once. The retry is never retried.
      if (out.res.status === 401 && boundOrigin && (await awaitToken())) {
         out = await postQuery(url, body);
      }
      var res = out.res;
      var json = out.json;
      if (out.malformed) {
         throw new Error(
            "Publisher: server returned non-JSON response (" + res.status + ")",
         );
      }
      if (!res.ok) {
         var msg = (json && json.message) || res.statusText || "Query failed";
         var err = new Error("Publisher.query: " + msg);
         err.response = json;
         err.status = res.status;
         if (
            isOwnPackage &&
            versionId &&
            versionId === (fixedVersion || pageVersion) &&
            json &&
            (json.reason === "VERSION_NOT_FOUND" ||
               json.reason === "VERSION_ARCHIVED") &&
            !versionUnavailableSent
         ) {
            versionUnavailableSent = emit("version.unavailable", {
               versionId: versionId,
               reason: json.reason,
            });
         }
         throw err;
      }
      // The server's QueryResult always has `result` as a JSON-encoded string.
      // Parse it before handing it back so callers see real JS values.
      var result = JSON.parse(json.result);
      if (boundOrigin) {
         var ran = {
            model: modelPath,
            givens: body.givens || {},
            versionId: versionId || null,
            rowCount: rowCountOf(result),
            ms: Date.now() - started,
         };
         if (opts.sourceName) ran.sourceName = opts.sourceName;
         if (opts.queryName) ran.queryName = opts.queryName;
         if (isString(malloyQuery)) {
            ran.query = malloyQuery.slice(0, QUERY_TEXT_LIMIT);
         }
         emit("query.ran", ran);
      }
      return result;
   }

   async function postQuery(url, body) {
      var res = await fetch(url, {
         method: "POST",
         credentials: "include",
         headers: Object.assign(
            { "content-type": "application/json" },
            authHeaders(),
         ),
         body: JSON.stringify(body),
      });
      try {
         return { res: res, json: await res.json() };
      } catch (_e) {
         return { res: res, malformed: true };
      }
   }

   // Rows of a compact result, or of a full Malloy result envelope; null when the shape is unknown.
   function rowCountOf(result) {
      if (Array.isArray(result)) return result.length;
      var rows = result && result.data && result.data.array_value;
      return Array.isArray(rows) ? rows.length : null;
   }

   function query(modelPath, malloyQuery, opts) {
      return rawQuery(modelPath, malloyQuery, opts, true);
   }
   function queryFull(modelPath, malloyQuery, opts) {
      return rawQuery(modelPath, malloyQuery, opts, false);
   }

   // --- Embed helper (host page) -----------------------------------------
   var HELLO_TRIES = 5;
   var HELLO_INTERVAL_MS = 500;
   var SEND_TIMEOUT_MS = 5000;

   function randomNonce() {
      var bytes = new Uint8Array(16);
      crypto.getRandomValues(bytes);
      var out = "";
      for (var i = 0; i < bytes.length; i++) {
         out += (bytes[i] < 16 ? "0" : "") + bytes[i].toString(16);
      }
      return out;
   }

   // `src` with the embed token (deprecated; see getToken) in its query and the nonce in its
   // fragment, merged with any fragment it already has.
   function embedUrl(src, token, nonce) {
      var hash = src.indexOf("#");
      var base = hash === -1 ? src : src.slice(0, hash);
      var fragment = hash === -1 ? "" : withoutNonce(src.slice(hash + 1));
      if (token) {
         base +=
            (base.indexOf("?") === -1 ? "?" : "&") +
            "embed_token=" +
            encodeURIComponent(token);
      }
      return (
         base +
         "#" +
         (fragment ? fragment + "&" : "") +
         "publisher-nonce=" +
         nonce
      );
   }

   function embed(selector, options) {
      options = options || {};
      var host =
         typeof selector === "string"
            ? document.querySelector(selector)
            : selector;
      if (!host) {
         throw new Error("Publisher.embed: selector did not match an element");
      }
      if (!options.src) {
         throw new Error("Publisher.embed: opts.src is required");
      }
      var nonce = randomNonce();
      var pageOrigin = new URL(options.src, location.href).origin;
      if (pageOrigin === "null") {
         throw new Error("Publisher.embed: opts.src needs an http(s) origin");
      }
      var iframe = document.createElement("iframe");
      iframe.src = embedUrl(options.src, options.token, nonce);
      iframe.style.border = "0";
      iframe.style.width = "100%";
      iframe.style.display = "block";
      if (options.height) {
         iframe.style.height =
            typeof options.height === "number"
               ? options.height + "px"
               : options.height;
      } else {
         iframe.style.height = "0px"; // will be sized via postMessage
      }
      if (options.allow) iframe.allow = options.allow;
      iframe.setAttribute(
         "sandbox",
         "allow-scripts allow-same-origin allow-forms",
      );

      var pending = {};
      var hostHandlers = {};
      var ready = false;
      var readyWaiters = [];
      var helloTries = 0;
      var helloTimer = null;
      var seq = 0;
      var gone = false;
      // Bumped on every load and on destroy: a token or a reply to a page request is dropped if the frame moved on while it was pending.
      var generation = 0;

      function hostPost(type, payload, extra) {
         var msg = { protocol: "publisher", v: 1, type: type };
         if (extra) {
            if (extra.id !== undefined) msg.id = extra.id;
            if (extra.reply) msg.reply = true;
            if (extra.error) msg.error = extra.error;
         }
         if (payload !== undefined) msg.payload = payload;
         try {
            iframe.contentWindow.postMessage(msg, pageOrigin);
            return true;
         } catch (_e) {
            return false;
         }
      }

      // The page may load after the first hello, or never answer (an old runtime).
      function sayHello() {
         clearTimeout(helloTimer);
         if (ready || gone || helloTries >= HELLO_TRIES) return;
         helloTries++;
         hostPost("hello", undefined, { id: "hello-" + helloTries });
         helloTimer = setTimeout(sayHello, HELLO_INTERVAL_MS);
      }

      function sendToken() {
         if (!options.getToken) return;
         var gen = generation;
         Promise.resolve()
            .then(function () {
               return options.getToken();
            })
            .then(
               function (token) {
                  if (token && gen === generation && !gone)
                     hostPost("auth.token", { token: String(token) });
               },
               function () {
                  /* the page's own 401 handling surfaces a missing token */
               },
            );
      }

      // The reply to the page's auth.token.request; a getToken that fails answers with handler_error.
      function answerTokenRequest() {
         return Promise.resolve()
            .then(function () {
               return options.getToken();
            })
            .then(function (token) {
               if (!token) throw new Error("no token");
               return { token: String(token) };
            });
      }

      function onReady() {
         if (ready) return;
         ready = true;
         clearTimeout(helloTimer);
         sendToken();
         readyWaiters.splice(0).forEach(function (fn) {
            fn();
         });
      }

      function onBusMessage(e) {
         if (e.source !== iframe.contentWindow || e.origin !== pageOrigin)
            return;
         var m = e.data;
         if (!isEnvelope(m) || m.nonce !== nonce) return;
         var isRequest = m.id !== undefined && !m.reply;
         if (m.reply) {
            if (m.type === "hello") return onReady();
            var waiting = pending[m.id];
            if (!waiting) return;
            delete pending[m.id];
            clearTimeout(waiting.timer);
            if (m.error) {
               var err = new Error(
                  "Publisher.embed: " +
                     ((m.error && m.error.message) || m.type),
               );
               err.code = m.error && m.error.code;
               waiting.reject(err);
            } else {
               waiting.resolve(m.payload);
            }
            return;
         }
         if (m.type === "ready" && !ready) {
            // The page is up and offered itself before our hello landed; answer now.
            helloTries = 0;
            sayHello();
         }
         var list = (hostHandlers[m.type] || []).slice();
         if (m.type === "auth.token.request" && isRequest && options.getToken) {
            list.unshift(answerTokenRequest);
         }
         if (list.length === 0) {
            if (isRequest) {
               hostPost(m.type, undefined, {
                  id: m.id,
                  reply: true,
                  error: {
                     code: "unhandled",
                     message: "No handler for '" + m.type + "'",
                  },
               });
            }
            return;
         }
         var gen = generation;
         var meta = { type: m.type, id: m.id };
         var results = list.map(function (fn) {
            try {
               return Promise.resolve(fn(m.payload, meta));
            } catch (err2) {
               return Promise.reject(err2);
            }
         });
         if (!isRequest) {
            results.forEach(function (r) {
               r.catch(function () {});
            });
            return;
         }
         Promise.all(results).then(
            function (values) {
               if (gen !== generation || gone) return;
               var answer = values.find(function (v) {
                  return v !== undefined;
               });
               hostPost(m.type, answer === undefined ? null : answer, {
                  id: m.id,
                  reply: true,
               });
            },
            function (err3) {
               if (gen !== generation || gone) return;
               hostPost(m.type, undefined, {
                  id: m.id,
                  reply: true,
                  error: {
                     code: "handler_error",
                     message: String((err3 && err3.message) || err3),
                  },
               });
            },
         );
      }

      // Resize listener (legacy envelope-less message) and the bus
      function onMessage(e) {
         if (e.data && e.data.type === "publisher:resize") {
            if (e.source !== iframe.contentWindow) return;
            if (typeof e.data.height === "number") {
               iframe.style.height = Math.max(0, e.data.height) + "px";
            }
            return;
         }
         onBusMessage(e);
      }
      window.addEventListener("message", onMessage);
      // A (re)load is a fresh page that has not bound yet.
      iframe.addEventListener("load", function () {
         generation++;
         ready = false;
         helloTries = 0;
         sayHello();
      });

      function cleanup() {
         gone = true;
         generation++;
         clearTimeout(helloTimer);
         window.removeEventListener("message", onMessage);
         observer.disconnect();
         Object.keys(pending).forEach(function (id) {
            clearTimeout(pending[id].timer);
            pending[id].reject(new Error("Publisher.embed: destroyed"));
         });
         pending = {};
      }
      // Best-effort cleanup if the host removes the iframe
      var observer = new MutationObserver(function () {
         if (!host.contains(iframe)) cleanup();
      });
      observer.observe(host, { childList: true, subtree: false });

      host.appendChild(iframe);
      return {
         iframe: iframe,
         destroy: function () {
            cleanup();
            if (iframe.parentNode) iframe.parentNode.removeChild(iframe);
         },
         // A request to the page; sent once the page has answered hello, rejected after 5 s.
         send: function (type, payload) {
            return new Promise(function (resolve, reject) {
               if (gone) {
                  reject(new Error("Publisher.embed: destroyed"));
                  return;
               }
               var id = "h" + ++seq;
               var timer = setTimeout(function () {
                  delete pending[id];
                  reject(
                     new Error(
                        "Publisher.embed: no reply to '" + type + "' in 5 s",
                     ),
                  );
               }, SEND_TIMEOUT_MS);
               pending[id] = { resolve: resolve, reject: reject, timer: timer };
               // Not generation-guarded: a queued send posts once the current document has proved itself.
               function go() {
                  if (pending[id]) hostPost(type, payload, { id: id });
               }
               if (ready) go();
               else readyWaiters.push(go);
            });
         },
         on: function (type, fn) {
            (hostHandlers[type] = hostHandlers[type] || []).push(fn);
            return function () {
               var list = hostHandlers[type];
               var i = list ? list.indexOf(fn) : -1;
               if (i !== -1) list.splice(i, 1);
            };
         },
      };
   }

   // --- When this runtime is itself inside an iframe ---------------------
   // Post size updates upstream + listen for live-reload SSE events.
   function setUpEmbeddedSelfBehaviors() {
      if (inIframe) {
         var lastHeight = -1;
         function measureContentHeight() {
            // We want the "ink height" — where the last piece of visible
            // content ends. NOT document.body.scrollHeight: any rule like
            // `body { min-height: 100vh }` (extremely common in dashboards
            // that look nice standalone) inflates scrollHeight to match
            // whatever the iframe's current viewport is, creating a
            // feedback loop where the iframe ratchets up but never shrinks.
            //
            // Sum the lowest bottom edge across body's children, in
            // document coordinates. This ignores body padding, min-height,
            // and CSS that just fills the viewport.
            var body = document.body;
            if (!body) return document.documentElement.scrollHeight;
            var maxBottom = 0;
            var kids = body.children;
            for (var i = 0; i < kids.length; i++) {
               var rect = kids[i].getBoundingClientRect();
               if (rect.bottom > maxBottom) maxBottom = rect.bottom;
            }
            if (maxBottom <= 0) {
               // Fallback for empty body / hidden children
               return document.documentElement.scrollHeight;
            }
            var scrollTop =
               window.scrollY ||
               document.documentElement.scrollTop ||
               document.body.scrollTop ||
               0;
            // Add body bottom padding (rect.bottom is content-box bottom,
            // body padding isn't part of any child's rect).
            var bodyStyle = window.getComputedStyle(body);
            var pad = parseFloat(bodyStyle.paddingBottom) || 0;
            return Math.ceil(maxBottom + scrollTop + pad);
         }
         // Smallest SHRINK worth reporting. measureContentHeight already ceils,
         // so nothing sub-pixel reaches here; what this absorbs is a 1-2px
         // rounding flip-flop between two layout passes, which the host cannot
         // act on without the resize itself becoming a layout change inside this
         // frame — the feedback loop.
         //
         // Applied to shrinks ONLY, deliberately. A frame a few pixels too tall
         // shows dead space; a few pixels too short CLIPS, and the frame has no
         // internal scrollbar. The asymmetry also stops the two sides' bands
         // compounding: the host runs its own epsilon against ITS height, which
         // its MIN clamp guarantees can differ from lastHeight here, so two
         // symmetric 8px bands leave a dead zone wider than either. Growing at
         // 1px precision keeps the host's band the only one that applies.
         var RESIZE_EPSILON = 8;
         var resizePending = false;

         function emitSize() {
            var h = measureContentHeight();
            if (h < lastHeight && lastHeight - h < RESIZE_EPSILON) return;
            if (h === lastHeight) return;
            lastHeight = h;
            try {
               // Bound: the host proved itself with a hello. Legacy hosts never say hello, and resize is only a height.
               window.parent.postMessage(
                  { type: "publisher:resize", height: h },
                  boundOrigin || "*",
               );
            } catch (_e) {
               /* ignore */
            }
         }

         // Coalesce a burst into one message per frame.
         //
         // ResizeObserver on documentElement fires on EVERY layout change, so a
         // dashboard whose tiles resolve one by one used to post a height per
         // tile. The host resized on each, which is what made an embedded app
         // visibly jitter while it loaded. Same `pending`-flag idiom as the SSE
         // reload debounce below; rAF rather than a timeout because the next
         // paint is exactly when a coalesced layout is worth measuring.
         //
         // Trailing edge, not leading: the last measurement in a burst is the
         // settled one, and reporting the first would send a stale height.
         function postSize() {
            if (resizePending) return;
            resizePending = true;
            var schedule =
               typeof requestAnimationFrame === "function"
                  ? requestAnimationFrame
                  : function (fn) {
                       setTimeout(fn, 16);
                    };
            schedule(function () {
               resizePending = false;
               emitSize();
            });
         }
         // Initial + observe content changes
         // The first report goes out synchronously: the host shows a
         // MIN_EMBED_HEIGHT placeholder until one arrives, so deferring it by a
         // frame would leave a collapsed frame for no benefit. Only the bursts
         // that follow need coalescing.
         if (document.readyState === "loading") {
            document.addEventListener("DOMContentLoaded", emitSize);
         } else {
            emitSize();
         }
         window.addEventListener("load", emitSize);
         if (typeof ResizeObserver !== "undefined") {
            var ro = new ResizeObserver(postSize);
            // Observe documentElement so we catch any layout change
            ro.observe(document.documentElement);
         } else {
            // Fallback: poll once a second
            setInterval(postSize, 1000);
         }
      }
   }

   // --- SSE live reload --------------------------------------------------
   // In-app viewer only. A hosted embed surface (/apps/...) is a customer's page: a reload
   // on publish would throw away an end user's state, the events route is not one an embed
   // session may call (so onerror's auto-reconnect would loop), and the credentialed
   // cross-origin request is exactly what that surface keeps to a minimum. The resize
   // postMessage below still runs there; it is what Publisher.embed on the host relies on.
   function setUpLiveReload() {
      if (!inAppMatch) return;
      if (!ctx.environment || !ctx.package) return;
      if (fixedVersion) return; // a version never changes
      if (typeof EventSource === "undefined") return;
      var url =
         apiBase +
         "/environments/" +
         encodeURIComponent(ctx.environment) +
         "/packages/" +
         encodeURIComponent(ctx.package) +
         "/events";
      try {
         var es = new EventSource(url, { withCredentials: true });
         var pending = false;
         es.addEventListener("changed", function () {
            if (pending) return;
            pending = true;
            // Tiny debounce to coalesce a flurry of saves
            setTimeout(reloadWithNonce, 100);
         });
         es.onerror = function () {
            // Browser will auto-reconnect; nothing to do.
         };
      } catch (_e) {
         // SSE may be blocked (e.g. corp proxy) — non-fatal.
      }
   }

   // --- Public API --------------------------------------------------------
   window.Publisher = {
      query: query,
      queryFull: queryFull,
      embed: embed,
      context: ctx,
      setToken: function (token) {
         bearerToken = token || null;
      },
      on: on,
      emit: emit,
      get givens() {
         return cloneGivens(givensStore);
      },
      setGivens: setGivens,
      onGivensChange: onGivensChange,
   };

   // Auto-init the in-iframe behaviors, the host bus and the live-reload subscription.
   // All are no-ops if not applicable.
   setUpHostBus();
   setUpEmbeddedSelfBehaviors();
   setUpLiveReload();
})();
