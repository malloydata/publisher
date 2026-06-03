// Publisher runtime helper for in-package HTML dashboards.
// Served by the Publisher server at /sdk/publisher.js. Hand-authored vanilla
// JS — no bundler. Loaded via <script src="/sdk/publisher.js">.
//
// Exposes window.Publisher with:
//   - Publisher.query(model, malloy, opts?)     → Promise<rows[]>
//   - Publisher.queryFull(model, malloy, opts?) → Promise<MalloyResult>  (envelope for <malloy-render>)
//   - Publisher.embed(selector, { src, height?, token? })
//   - Publisher.context  ({ environment, package } inferred from URL)
//   - Publisher.setToken(token)  (override Bearer token; default uses cookies)
//
// When loaded inside an iframe served from /environments/<env>/packages/<pkg>/...,
// the runtime posts size updates to the parent window so Publisher.embed() in
// the host can resize the iframe.
//
// The "publisher:resize" postMessage protocol below is the SAME contract the
// SPA host consumes. Its canonical definition lives in
// packages/sdk/src/utils/pageEmbed.ts (PUBLISHER_RESIZE_MESSAGE_TYPE /
// PublisherResizeMessage). This file is build-step-free vanilla JS and can't
// import it, so keep the message type/shape here in sync with that module.

(function () {
   "use strict";

   // --- Context inference -------------------------------------------------
   // URL shape: /environments/<env>/packages/<pkg>/<file>
   //
   // location.pathname is URL-encoded, so we MUST decode the captured
   // segments here. Without this step, a name with a space (e.g.
   // "demo env") would arrive as "demo%20env" — and the encodeURIComponent
   // we apply when building API URLs (below) would produce "demo%2520env",
   // which Publisher then 404s on.
   var pathMatch = location.pathname.match(
      /^\/environments\/([^/]+)\/packages\/([^/]+)\//,
   );
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

   // --- Query helpers -----------------------------------------------------
   function resolveTarget(opts) {
      var env = (opts && opts.environment) || ctx.environment;
      var pkg = (opts && opts.package) || ctx.package;
      if (!env || !pkg) {
         throw new Error(
            "Publisher: no environment/package; either serve the page from " +
               "/environments/<env>/packages/<pkg>/... or pass { environment, package } in opts.",
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
         modelPath.split("/").map(encodeURIComponent).join("/") +
         "/query";
      var body = { compactJson: compactJson };
      if (malloyQuery) body.query = malloyQuery;
      if (opts.sourceName) body.sourceName = opts.sourceName;
      if (opts.queryName) body.queryName = opts.queryName;
      if (opts.filterParams) body.filterParams = opts.filterParams;
      if (opts.bypassFilters) body.bypassFilters = true;

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
   // Post size updates upstream so the host can resize this iframe.
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
         function postSize() {
            var h = measureContentHeight();
            if (h !== lastHeight) {
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
         }
         // Initial + observe content changes
         if (document.readyState === "loading") {
            document.addEventListener("DOMContentLoaded", postSize);
         } else {
            postSize();
         }
         window.addEventListener("load", postSize);
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

   // Auto-init the in-iframe resize behavior. No-op if not in an iframe.
   setUpEmbeddedSelfBehaviors();
})();
