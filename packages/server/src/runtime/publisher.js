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
//   - Publisher.embed(selector, { src, height?, token? })
//   - Publisher.context  ({ environment, package } inferred from URL)
//   - Publisher.setToken(token)  (override Bearer token; default uses cookies)
//
// When loaded inside an iframe served from /environments/<env>/packages/<pkg>/...,
// the runtime auto-subscribes to a Server-Sent Events live-reload stream
// (GET .../events) and reloads the page on file changes. Served from
// /apps/<env>/<pkg>/... (a hosted embed surface) it infers the same context
// but does not subscribe: that page belongs to a customer's site. On both it
// posts size updates to the parent window so Publisher.embed() in the host
// can resize the iframe.
//
// Inside the in-app viewer's iframe, once the framing page has asked for them
// with a "publisher:subscribe" message, it also posts a "publisher:query"
// message after each query: what the page asked (model, query, filters,
// givens) and what came back (row count and the first rows, or the error). A
// host that frames the app, such as a chat beside it, can then say what the
// reader is looking at. Reports go only to the origin that subscribed, never
// to "*"; the last few from before the subscribe are kept and sent when it
// arrives. Never on a hosted embed surface (/apps/...): that page belongs to a
// customer's site, and its results are not the framing page's to read.
//
// The "publisher:resize" postMessage protocol below is the SAME contract the
// SPA host consumes. Its canonical definition lives in
// packages/sdk/src/utils/dataAppEmbed.ts (PUBLISHER_RESIZE_MESSAGE_TYPE /
// PublisherResizeMessage), as do "publisher:subscribe"'s and
// "publisher:query"'s (PUBLISHER_SUBSCRIBE_MESSAGE_TYPE /
// PublisherSubscribeMessage, PUBLISHER_QUERY_MESSAGE_TYPE /
// PublisherQueryMessage). This file is build-step-free vanilla JS and can't
// import it, so keep the message types/shapes here in sync with that module.

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
   var ctx = pathMatch
      ? {
           environment: safeDecode(pathMatch[1]),
           package: safeDecode(pathMatch[2]),
        }
      : {};

   var apiBase = location.origin + "/api/v0";
   var bearerToken = null;

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

   // --- Query reports to the framing page ----------------------------------
   // How many result rows a report carries, and the most characters it may take
   // once serialized: enough for a host to quote the numbers on screen, not a
   // copy of the result.
   var QUERY_REPORT_MAX_ROWS = 20;
   var QUERY_REPORT_MAX_CHARS = 16000;
   // How many reports from before the framing page subscribes are kept for it:
   // the latest few, enough to say what is on screen when it starts listening.
   var QUERY_REPORT_BACKLOG = 10;
   var reportsQueries = (function () {
      if (!inAppMatch) return false;
      try {
         return window.self !== window.top;
      } catch (_e) {
         return true; // cross-origin parent: framed
      }
   })();
   // The origin of the framing page that subscribed; until then, reports wait.
   var reportOrigin = null;
   var reportBacklog = [];
   // Reports go out as queries finish. Each carries the order its query
   // started in, so a host can keep the latest one asked rather than the last
   // one answered.
   var querySeq = 0;

   if (reportsQueries) {
      window.addEventListener("message", function (event) {
         if (event.source !== window.parent) return;
         if (!event.data || event.data.type !== "publisher:subscribe") return;
         // An opaque origin ("null") is not one postMessage can address.
         if (!event.origin || event.origin === "null") return;
         reportOrigin = event.origin;
         var backlog = reportBacklog;
         reportBacklog = [];
         for (var i = 0; i < backlog.length; i++) sendQueryReport(backlog[i]);
      });
   }

   function sendQueryReport(report) {
      try {
         window.parent.postMessage(report, reportOrigin);
      } catch (_e) {
         /* a report that can't be cloned is not worth failing the query */
      }
   }

   // A result's row count, and its first rows as plain objects. query's rows are
   // the result itself; queryFull's envelope is a Malloy result, its rows record
   // cells under data.array_value named by schema.fields, so only the rows a
   // report carries are converted.
   function resultRows(outcome) {
      var result = outcome.result;
      if (outcome.compactJson) {
         if (!Array.isArray(result)) return undefined;
         return {
            count: result.length,
            first: function (n) {
               return result.slice(0, n);
            },
         };
      }
      var cells = result && result.data && result.data.array_value;
      if (!Array.isArray(cells)) return undefined;
      var fields = (result.schema && result.schema.fields) || [];
      return {
         count: cells.length,
         first: function (n) {
            return cells.slice(0, n).map(function (cell) {
               return recordOf(cell, fields);
            });
         },
      };
   }

   function recordOf(cell, fields) {
      var row = {};
      var values = (cell && cell.record_value) || [];
      for (var i = 0; i < fields.length && i < values.length; i++) {
         row[fields[i].name] = cellValue(values[i], fields[i].type);
      }
      return row;
   }

   var SCALAR_CELL_VALUES = {
      string_cell: "string_value",
      number_cell: "number_value",
      boolean_cell: "boolean_value",
      date_cell: "date_value",
      timestamp_cell: "timestamp_value",
      json_cell: "json_value",
      sql_native_cell: "sql_native_value",
   };

   function cellValue(cell, type) {
      if (!cell) return null;
      if (cell.kind === "record_cell") {
         return recordOf(cell, (type && type.fields) || []);
      }
      if (cell.kind === "array_cell") {
         var element = type && type.element_type;
         return (cell.array_value || []).map(function (item) {
            return cellValue(item, element);
         });
      }
      var key = SCALAR_CELL_VALUES[cell.kind];
      return key && key in cell ? cell[key] : null;
   }

   function reportSize(report) {
      return JSON.stringify(report).length;
   }

   // Fit a report into QUERY_REPORT_MAX_CHARS, serialized. Rows go first, from
   // the end, since a host reads the first ones; then the page's own text is
   // cut, and the report marked truncated. False when it still does not fit.
   function fitQueryReport(report) {
      if (report.rows) {
         // Drop rows one at a time from the end, measuring each row once: the
         // report with n rows is the report with none, plus those rows, plus a
         // comma between each two.
         var rows = report.rows;
         report.rows = [];
         var size = reportSize(report);
         var kept = 0;
         while (kept < rows.length) {
            var next =
               size + JSON.stringify(rows[kept]).length + (kept > 0 ? 1 : 0);
            if (next > QUERY_REPORT_MAX_CHARS) break;
            size = next;
            kept++;
         }
         report.rows = rows.slice(0, kept);
      }
      if (reportSize(report) <= QUERY_REPORT_MAX_CHARS) return true;
      report.truncated = true;
      cutText(report, "query");
      cutText(report, "error");
      if (reportSize(report) > QUERY_REPORT_MAX_CHARS) {
         delete report.givens;
         delete report.filterParams;
      }
      return reportSize(report) <= QUERY_REPORT_MAX_CHARS;
   }

   // Shorten report[key] until the report fits, ending it with an ellipsis.
   function cutText(report, key) {
      var text = report[key];
      var over = reportSize(report) - QUERY_REPORT_MAX_CHARS;
      while (typeof text === "string" && text.length > 0 && over > 0) {
         text = text.slice(0, Math.max(0, text.length - over - 1));
         report[key] = text + "\u2026";
         over = reportSize(report) - QUERY_REPORT_MAX_CHARS;
      }
   }

   function postQueryReport(
      target,
      modelPath,
      malloyQuery,
      opts,
      seq,
      outcome,
   ) {
      if (!reportsQueries) return;
      var report = {
         type: "publisher:query",
         seq: seq,
         environment: target.env,
         package: target.pkg,
         model: modelPath,
      };
      if (malloyQuery) report.query = malloyQuery;
      if (opts.sourceName) report.sourceName = opts.sourceName;
      if (opts.queryName) report.queryName = opts.queryName;
      if (opts.filterParams) report.filterParams = opts.filterParams;
      if (opts.givens) report.givens = opts.givens;
      try {
         var all = outcome.error ? undefined : resultRows(outcome);
         if (outcome.error) {
            report.error = String(outcome.error);
         } else if (all) {
            report.rowCount = all.count;
            report.rows = all.first(QUERY_REPORT_MAX_ROWS);
         }
         if (!fitQueryReport(report)) return;
      } catch (_e) {
         return; // a report that can't be built is not worth failing the query
      }
      if (reportOrigin) {
         sendQueryReport(report);
      } else {
         // A copy: the page may change the givens or filters it passed before
         // the report goes out.
         reportBacklog.push(JSON.parse(JSON.stringify(report)));
         if (reportBacklog.length > QUERY_REPORT_BACKLOG) reportBacklog.shift();
      }
   }

   async function rawQuery(modelPath, malloyQuery, opts, compactJson) {
      opts = opts || {};
      var target = resolveTarget(opts);
      var seq = ++querySeq;
      try {
         var result = await runQuery(
            target,
            modelPath,
            malloyQuery,
            opts,
            compactJson,
         );
      } catch (err) {
         postQueryReport(target, modelPath, malloyQuery, opts, seq, {
            error: (err && err.message) || err,
         });
         throw err;
      }
      postQueryReport(target, modelPath, malloyQuery, opts, seq, {
         result: result,
         compactJson: compactJson,
      });
      return result;
   }

   async function runQuery(target, modelPath, malloyQuery, opts, compactJson) {
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
      if (opts.givens) body.givens = opts.givens;
      // The page's own version applies only to its own package; a query aimed
      // elsewhere gets that package's latest unless it names one.
      var versionId =
         opts.versionId ||
         (target.env === ctx.environment && target.pkg === ctx.package
            ? await pinnedVersion()
            : undefined);
      if (versionId) body.versionId = versionId;

      var headers = Object.assign(
         { "content-type": "application/json" },
         authHeaders(),
      );
      var res = await fetch(url, {
         method: "POST",
         credentials: "include",
         headers: headers,
         body: JSON.stringify(body),
      });
      var json;
      try {
         json = await res.json();
      } catch (_e) {
         throw new Error(
            "Publisher: server returned non-JSON response (" + res.status + ")",
         );
      }
      if (!res.ok) {
         var msg = (json && json.message) || res.statusText || "Query failed";
         var err = new Error("Publisher.query: " + msg);
         err.response = json;
         err.status = res.status;
         throw err;
      }
      // The server's QueryResult always has `result` as a JSON-encoded string.
      // Parse it before handing it back so callers see real JS values.
      return JSON.parse(json.result);
   }

   function query(modelPath, malloyQuery, opts) {
      return rawQuery(modelPath, malloyQuery, opts, true);
   }
   function queryFull(modelPath, malloyQuery, opts) {
      return rawQuery(modelPath, malloyQuery, opts, false);
   }

   // --- Embed helper (host page) -----------------------------------------
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
      var iframe = document.createElement("iframe");
      iframe.src = options.token
         ? options.src +
           (options.src.indexOf("?") === -1 ? "?" : "&") +
           "embed_token=" +
           encodeURIComponent(options.token)
         : options.src;
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

      // Resize listener
      function onMessage(e) {
         if (!e.data || e.data.type !== "publisher:resize") return;
         if (e.source !== iframe.contentWindow) return;
         if (typeof e.data.height === "number") {
            iframe.style.height = Math.max(0, e.data.height) + "px";
         }
      }
      window.addEventListener("message", onMessage);
      // Best-effort cleanup if the host removes the iframe
      var observer = new MutationObserver(function () {
         if (!host.contains(iframe)) {
            window.removeEventListener("message", onMessage);
            observer.disconnect();
         }
      });
      observer.observe(host, { childList: true, subtree: false });

      host.appendChild(iframe);
      return {
         iframe: iframe,
         destroy: function () {
            window.removeEventListener("message", onMessage);
            observer.disconnect();
            if (iframe.parentNode) iframe.parentNode.removeChild(iframe);
         },
      };
   }

   // --- When this runtime is itself inside an iframe ---------------------
   // Post size updates upstream + listen for live-reload SSE events.
   function setUpEmbeddedSelfBehaviors() {
      var inIframe = (function () {
         try {
            return window.self !== window.top;
         } catch (_e) {
            return true; // cross-origin parent — assume embedded
         }
      })();

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
               window.parent.postMessage(
                  { type: "publisher:resize", height: h },
                  "*",
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
            setTimeout(function () {
               location.reload();
            }, 100);
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
   };

   // Auto-init the in-iframe behaviors and live-reload subscription.
   // Both are no-ops if not applicable.
   setUpEmbeddedSelfBehaviors();
   setUpLiveReload();
})();
