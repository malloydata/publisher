// Pre-load the instrumentation module; the instrumentation module must be loaded before the other imports.
import type { GivenValue } from "@malloydata/malloy";
import { StreamableHTTPServerTransport } from "@modelcontextprotocol/sdk/server/streamableHttp.js";
import bodyParser from "body-parser";
import cors from "cors";
import express from "express";
import * as http from "http";
import { createProxyMiddleware } from "http-proxy-middleware";
import { AddressInfo } from "net";
import * as path from "path";
import { ParsedQs } from "qs";
import { fileURLToPath } from "url";
import { CompileController } from "./controller/compile.controller";
import { ConnectionController } from "./controller/connection.controller";
import { DatabaseController } from "./controller/database.controller";
import { ModelController } from "./controller/model.controller";
import { PackageController } from "./controller/package.controller";
import { QueryController } from "./controller/query.controller";
import { WatchModeController } from "./controller/watch-mode.controller";
import {
   BadRequestError,
   internalErrorToHttpError,
   NotImplementedError,
} from "./errors";
import {
   drainingGuard,
   registerHealthEndpoints,
   registerSignalHandlers,
} from "./health";
import "./instrumentation";
import {
   getPrometheusMetricsHandler,
   httpMetricsMiddleware,
} from "./instrumentation";
import { logger, loggerMiddleware } from "./logger";

import { getMemoryGovernorConfig } from "./config";
import { setFilterDeprecationHeaders } from "./filter_deprecation";
import { checkHeapConfiguration } from "./heap_check";
import { queryConcurrency } from "./query_concurrency";
import { MaterializationController } from "./controller/materialization.controller";
import { initializeMcpServer } from "./mcp/server";
import { registerLegacyRoutes } from "./server-old";
import { EnvironmentStore } from "./service/environment_store";
import { MaterializationService } from "./service/materialization_service";
import { normalizeQueryArray } from "./query_param_utils";
import { PackageMemoryGovernor } from "./service/package_memory_governor";
import { assertSafePackageName, safeJoinUnderRoot } from "./path_safety";

export { normalizeQueryArray } from "./query_param_utils";

// Parse command line arguments
function parseArgs() {
   const args = process.argv.slice(2);
   let sawServerRoot = false;
   let sawConfig = false;
   for (let i = 0; i < args.length; i++) {
      const arg = args[i];
      if (arg === "--port" && args[i + 1]) {
         process.env.PUBLISHER_PORT = args[i + 1];
         i++;
      } else if (arg === "--host" && args[i + 1]) {
         process.env.PUBLISHER_HOST = args[i + 1];
         i++;
      } else if (arg === "--server_root" && args[i + 1]) {
         sawServerRoot = true;
         process.env.SERVER_ROOT = args[i + 1];
         i++;
      } else if (arg === "--config" && args[i + 1]) {
         sawConfig = true;
         process.env.PUBLISHER_CONFIG_PATH = args[i + 1];
         i++;
      } else if (arg === "--mcp_port" && args[i + 1]) {
         process.env.MCP_PORT = args[i + 1];
         i++;
      } else if (arg === "--shutdown_drain_duration_seconds" && args[i + 1]) {
         process.env.SHUTDOWN_DRAIN_DURATION_SECONDS = args[i + 1];
         i++;
      } else if (
         arg === "--shutdown_graceful_close_timeout_seconds" &&
         args[i + 1]
      ) {
         process.env.SHUTDOWN_GRACEFUL_CLOSE_TIMEOUT_SECONDS = args[i + 1];
         i++;
      } else if (arg === "--init") {
         process.env.INITIALIZE_STORAGE = "true";
      } else if (arg === "--watch-env" && args[i + 1]) {
         // Append (don't overwrite) so multiple --watch-env flags compose
         // and so an explicit env var pre-set still wins.
         const existing = process.env.PUBLISHER_WATCH || "";
         process.env.PUBLISHER_WATCH = existing
            ? `${existing},${args[i + 1]}`
            : args[i + 1];
         i++;
      } else if (arg === "--help" || arg === "-h") {
         console.log("Malloy Publisher Server");
         console.log("");
         console.log("Usage: malloy-publisher [options]");
         console.log("");
         console.log("Options:");
         console.log(
            "  --port <number>        Port to run the server on (default: 4000)",
         );
         console.log(
            "  --host <string>        Host to bind the server to (default: localhost)",
         );
         console.log(
            "  --server_root <path>   Root directory to serve files from (default: .)",
         );
         console.log(
            "  --config <path>        Path to publisher.config.json (default: <server_root>/publisher.config.json; falls back to bundled DuckDB-only sample config if missing)",
         );
         console.log(
            "  --mcp_port <number>    Port for MCP server (default: 4040)",
         );
         console.log(
            "  --shutdown_drain_duration_seconds <number>  Time in seconds to keep service in draining state before closing servers (default: 0)",
         );
         console.log(
            "  --shutdown_graceful_close_timeout_seconds <number>  Time in seconds to wait after closing servers before exit (default: 0)",
         );
         console.log(
            "  --init                 Initialize the storage (default: false)",
         );
         console.log(
            "  --watch-env <name>     Enable dev-mode watch for the named environment.",
         );
         console.log(
            "                         Mounts local-dir packages in-place (symlink, not",
         );
         console.log(
            "                         copy) so source-edit live reload works. A comma-",
         );
         console.log(
            "                         separated PUBLISHER_WATCH mounts all listed envs in",
         );
         console.log(
            "                         place, but only the first one auto-reloads.",
         );
         console.log("  --help, -h             Show this help message");
         process.exit(0);
      }
   }
   // Zero-config invocation (`npx @malloy-publisher/server`) opts in to
   // the bundled DuckDB-only sample config so the Quick Start works
   // without any flags. Any explicit --server_root or --config disables
   // this — the user told us where to look. Skip in NODE_ENV=test as a
   // belt-and-suspenders so any spec that ends up evaluating this
   // module doesn't accidentally pin the EnvironmentStore to the
   // bundled malloy-samples config; query-param helpers have been
   // moved to `./query_param_utils` precisely so unit specs no longer
   // need to import this module at all.
   if (!sawServerRoot && !sawConfig && process.env.NODE_ENV !== "test") {
      process.env.PUBLISHER_USE_BUNDLED_DEFAULT = "true";
   }
}

// Parse CLI arguments before setting up constants
parseArgs();

const PUBLISHER_PORT = Number(process.env.PUBLISHER_PORT || 4000);
const PUBLISHER_HOST = process.env.PUBLISHER_HOST || "0.0.0.0";
const MCP_PORT = Number(process.env.MCP_PORT || 4040);
const MCP_ENDPOINT = "/mcp";
const SHUTDOWN_DRAIN_DURATION_SECONDS = Number(
   process.env.SHUTDOWN_DRAIN_DURATION_SECONDS || 0,
);
const SHUTDOWN_GRACEFUL_CLOSE_TIMEOUT_SECONDS = Number(
   process.env.SHUTDOWN_GRACEFUL_CLOSE_TIMEOUT_SECONDS || 0,
);
// Find the app directory relative to this bundled server file.
// Works under both ESM (import.meta.url) and when invoked via NPX.
const __filename_esm = fileURLToPath(import.meta.url);
const ROOT = path.join(path.dirname(__filename_esm), "app");
const SERVER_ROOT = path.resolve(process.cwd(), process.env.SERVER_ROOT || ".");
const API_PREFIX = "/api/v0";
const isDevelopment = process.env["NODE_ENV"] === "development";

export const app = express();
app.use(loggerMiddleware);
app.use(httpMetricsMiddleware);
// Probe the V8 heap ceiling once at startup and warn if it's below
// the recommended floor. The row/byte caps from Steps 1–3 still
// bound per-request memory; this is a "your --max-old-space-size
// looks low for the default caps" advisory so operators don't
// chase OOMKills before checking the obvious config.
checkHeapConfiguration();
const environmentStore = new EnvironmentStore(SERVER_ROOT);
const watchModeController = new WatchModeController(environmentStore);
const connectionController = new ConnectionController(environmentStore);
const modelController = new ModelController(environmentStore);
// PackageMemoryGovernor is opt-in via PUBLISHER_MAX_MEMORY_BYTES.
// When set, it polls process RSS and flips an `isBackpressured` flag
// that Environment.getPackage / addPackage consult before allocating
// any new package — the server responds with HTTP 503 instead of
// OOM-killing the pod.
const memoryGovernorConfig = getMemoryGovernorConfig();
const memoryGovernor = memoryGovernorConfig
   ? new PackageMemoryGovernor(memoryGovernorConfig)
   : null;
memoryGovernor?.start();
environmentStore.setMemoryGovernor(memoryGovernor);
const packageController = new PackageController(environmentStore);
const databaseController = new DatabaseController(environmentStore);
const queryController = new QueryController(environmentStore);
const compileController = new CompileController(environmentStore);
const materializationService = new MaterializationService(environmentStore);
const materializationController = new MaterializationController(
   materializationService,
);

export const mcpApp = express();

// Register health endpoints on mcpApp (for E2E tests)
registerHealthEndpoints(mcpApp);

mcpApp.use(MCP_ENDPOINT, express.json());
mcpApp.use(MCP_ENDPOINT, cors());

mcpApp.all(MCP_ENDPOINT, async (req, res) => {
   logger.info(`[MCP Debug] Handling ${req.method} (Stateless)`);

   try {
      if (req.method === "POST") {
         const transport = new StreamableHTTPServerTransport({
            sessionIdGenerator: undefined,
         });

         transport.onclose = () => {
            logger.info(
               `[MCP Transport Info] Stateless transport closed for a request.`,
            );
         };
         transport.onerror = (err: Error) => {
            logger.error(`[MCP Transport Error] Stateless transport error:`, {
               error: err,
            });
         };

         const requestMcpServer = initializeMcpServer(environmentStore);
         await requestMcpServer.connect(transport);

         res.on("close", () => {
            logger.info(
               "[MCP Transport Info] Response closed, cleaning up stateless transport.",
            );
            transport.close().catch((err) => {
               logger.error(
                  "[MCP Transport Error] Error closing stateless transport on response close:",
                  { error: err },
               );
            });
         });

         await transport.handleRequest(req, res, req.body);
      } else if (req.method === "GET" || req.method === "DELETE") {
         logger.warn(
            `[MCP Transport Warn] Method Not Allowed in Stateless Mode: ${req.method}`,
         );
         res.setHeader("Allow", "POST");
         res.status(405).json({
            jsonrpc: "2.0",
            error: {
               code: -32601,
               message: "Method Not Allowed in Stateless Mode",
            },
            id: null,
         });
         return;
      } else {
         logger.warn(`[MCP Transport Warn] Method Not Allowed: ${req.method}`);
         res.setHeader("Allow", "POST");
         res.status(405).json({
            jsonrpc: "2.0",
            error: { code: -32601, message: "Method Not Allowed" },
            id: null,
         });
         return;
      }
   } catch (error) {
      logger.error(
         `[MCP Transport Error] Unhandled error in ${req.method} handler (Stateless):`,
         { error },
      );
      if (!res.headersSent) {
         res.status(500).json({
            jsonrpc: "2.0",
            error: { code: -32603, message: "Internal server error" },
            id:
               typeof req.body === "object" &&
               req.body !== null &&
               "id" in req.body
                  ? req.body.id
                  : null,
         });
      }
   }
});

// ---------------------------------------------------------------------------
// In-package HTML data apps
// ---------------------------------------------------------------------------
// These routes must come before the SPA catch-all and (in dev) the Vite proxy
// so that:
//   - `/sdk/publisher.js`     → Publisher runtime helper
//   - `/environments/<env>/packages/<pkg>/<file.ext>` → static file from
//                                                       inside the package dir
//   - `/api/v0/.../events`    → live-reload SSE (registered in API routes
//                                below; this comment is the cross-reference)

// Serve the runtime helper that in-package HTML pages load via
// <script src="/sdk/publisher.js">. Path resolved once at module load.
const PUBLISHER_RUNTIME_PATH = path.join(
   path.dirname(__filename_esm),
   "runtime",
   "publisher.js",
);
app.get("/sdk/publisher.js", (_req, res) => {
   res.type("application/javascript");
   // Short cache so live edits during local dev show up quickly. In
   // production this file is content-stable per release.
   res.setHeader("cache-control", "public, max-age=60");
   res.setHeader("X-Content-Type-Options", "nosniff");
   res.sendFile(PUBLISHER_RUNTIME_PATH, (err) => {
      if (err) {
         logger.error("Failed to send publisher.js runtime", { error: err });
         if (!res.headersSent) res.status(500).end();
      }
   });
});

// Serve files from inside a package directory at
//   /environments/<env>/packages/<pkg>/<relative-path>
//
// This route fully owns its prefix — it does NOT fall through to the SPA on
// missing files, because doing so would mask 404s (and in dev mode the SPA
// catch-all errors out before it can reply). Behavior:
//   - `/environments/<env>/packages/<pkg>`      → 302 to `…/<pkg>/`
//   - `/environments/<env>/packages/<pkg>/`     → serve `<pkgRoot>/public/index.html`
//   - `/environments/<env>/packages/<pkg>/foo/` → serve `<pkgRoot>/public/foo/index.html`
//   - `/environments/<env>/packages/<pkg>/<file>` → serve `<pkgRoot>/public/<file>`, or 404
// Only the package's `public/` directory is web-served. Models, data files, and
// the publisher.json manifest live outside it and are never reachable here, so
// nothing can be downloaded around the per-model #(authorize) and query
// controls. The data stays reachable through the permission-checked query path.

async function serveFromPackage(
   req: express.Request,
   res: express.Response,
): Promise<void> {
   const subPathRaw = (req.params as Record<string, string>)["0"] ?? "";
   try {
      const environment = await environmentStore.getEnvironment(
         req.params.environmentName,
         false,
      );
      const pkg = await environment.getPackage(req.params.packageName, false);
      // Only the package's public/ directory is web-served. Models, data, and
      // the publisher.json manifest live outside it and are never reachable
      // through this route. This single directory boundary is the whole
      // access-control story for static files.
      const publicRoot = path.join(pkg.getPackagePath(), "public");

      // Directory-style fallback: empty path or trailing slash → look for
      // index.html within that directory.
      let subPath = subPathRaw;
      if (subPath === "" || subPath.endsWith("/")) {
         subPath = subPath + "index.html";
      }

      // Resolve the requested file under public/ and reject anything that
      // escapes it (`..`, encoded traversal) before touching the disk.
      // safeJoinUnderRoot is the shared lexical-containment primitive (it throws
      // BadRequestError on escape, surfaced as 400 by the outer catch); the
      // realpath check below additionally catches symlinks inside public/ that
      // point outward (403).
      const fullPath = safeJoinUnderRoot(publicRoot, subPath);

      // Containment check via realpath against the resolved public/ root.
      // Catches symlinks inside public/ that point out (e.g. a malicious
      // package shipping `public/leak -> /etc/passwd`), and tolerates the
      // package root itself being a symlink (how watch-mode in-place mount
      // works): realpath resolves it transparently and legitimate accesses
      // inside public/ stay within realPublicRoot. Missing public/ dir or
      // missing file: realpath throws ENOENT and we 404 cleanly instead of
      // leaking via Express's default error handler.
      const fsp = await import("fs/promises");
      let realPublicRoot: string;
      let realFullPath: string;
      try {
         realPublicRoot = await fsp.realpath(publicRoot);
         realFullPath = await fsp.realpath(fullPath);
      } catch {
         if (!res.headersSent) {
            // Generic 404 with no reflected request input (avoids reflecting
            // user-controlled path/package name into the response body).
            res.status(404).end();
         }
         return;
      }
      const rel = path.relative(realPublicRoot, realFullPath);
      if (rel.startsWith("..") || path.isAbsolute(rel)) {
         res.status(403).end();
         return;
      }

      // Framing policy only applies to HTML documents — setting it on CSS/JS/
      // image assets is meaningless and needlessly strips their default
      // SAMEORIGIN protection. Embeddability defaults to "*" so same-tenant
      // embeds work out of the box, and is overridable via PUBLISHER_FRAME_ANCESTORS.
      const ext = path.extname(realFullPath).toLowerCase();
      if (ext === ".html" || ext === ".htm") {
         const frameAncestors = process.env.PUBLISHER_FRAME_ANCESTORS || "*";
         res.setHeader(
            "Content-Security-Policy",
            `frame-ancestors ${frameAncestors}`,
         );
         res.removeHeader("X-Frame-Options");
      }
      // Never let a served asset be MIME-sniffed into a different content type.
      res.setHeader("X-Content-Type-Options", "nosniff");
      res.sendFile(realFullPath, (err) => {
         if (err) {
            // Own the 404 instead of letting Express fall through to a
            // catch-all that may error.
            if (!res.headersSent) {
               // Generic 404, no reflected request input (see above).
               res.status(404).end();
            }
         }
      });
   } catch (e) {
      // Map service errors to their real status — a bad package name is a 400,
      // memory back-pressure is a 503 — rather than flattening everything to
      // 404. A genuine missing file is already handled by the realpath/sendFile
      // 404 paths above; this catch only sees service-layer failures.
      if (!res.headersSent) {
         const { json, status } = internalErrorToHttpError(e as Error);
         res.status(status).json(json);
      }
   }
}

// `/environments/<env>/packages/<pkg>` (no trailing slash, no path) redirect so
// relative URLs in the served HTML resolve as expected. Express's default loose
// matching also catches the trailing-slash form here, so only redirect URLs that
// don't already end with `/`.
//
// Build the target from the validated route params and the parsed query, not
// from the raw request URL, so it is always this same canonical, same-origin
// path with a trailing slash. That removes any open-redirect / header-injection
// surface from user-controlled input, with the slash placed before any query
// string (e.g. ?embed_token=...).
app.get(
   "/environments/:environmentName/packages/:packageName",
   (req, res, next) => {
      if (req.path.endsWith("/")) return next();
      const canonical =
         `/environments/${encodeURIComponent(req.params.environmentName)}` +
         `/packages/${encodeURIComponent(req.params.packageName)}/`;
      const query = new URLSearchParams();
      for (const [key, value] of Object.entries(req.query)) {
         if (Array.isArray(value)) {
            for (const v of value) query.append(key, String(v));
         } else if (value !== undefined) {
            query.append(key, String(value));
         }
      }
      const qs = query.toString();
      res.redirect(308, qs ? `${canonical}?${qs}` : canonical);
   },
);

app.get(
   "/environments/:environmentName/packages/:packageName/*",
   serveFromPackage,
);

// List the static HTML pages bundled inside a package. Used by the SPA's
// package-detail view to surface a clickable list, and by anyone who wants
// to discover pages programmatically without scraping the directory.
//
// Returns a `Page[]` (see api-doc.yaml) — each item carries the relative
// `path`, the `packageName`, the page `title` (from its <title> tag), and a
// `resource` URL. `resource` is the root-relative static-serve URL (NOT under
// `${API_PREFIX}`) because pages are static assets served off the server root,
// unlike API resources such as `Package.resource`.
// Recursive depth is capped to keep this cheap for huge package directories.
const PAGES_DEPTH_CAP = 3;
type PageItem = {
   resource: string;
   packageName: string;
   path: string;
   title: string;
};
async function listPackagePages(
   environmentName: string,
   packageName: string,
   publicRoot: string,
): Promise<PageItem[]> {
   const fs = await import("fs/promises");
   const out: PageItem[] = [];

   // Resolve the public/ root once and reject any entry whose realpath escapes
   // it. Same containment defense as serveFromPackage: catches symlinks inside
   // public/ pointing outside (e.g. `public/leak -> ../report.malloy`) before we
   // open and read the target's first 4KB for title extraction. A package with
   // no public/ dir fails realpath here and yields an empty list.
   let realPublicRoot: string;
   try {
      realPublicRoot = await fs.realpath(publicRoot);
   } catch {
      return out;
   }

   async function walk(dir: string, depth: number) {
      if (depth > PAGES_DEPTH_CAP) return;
      let entries: import("fs").Dirent[];
      try {
         entries = await fs.readdir(dir, { withFileTypes: true });
      } catch {
         return;
      }
      for (const entry of entries) {
         if (entry.name.startsWith(".") || entry.name === "node_modules")
            continue;
         const full = path.join(dir, entry.name);
         let realFull: string;
         try {
            realFull = await fs.realpath(full);
         } catch {
            continue;
         }
         const contained = path.relative(realPublicRoot, realFull);
         if (contained.startsWith("..") || path.isAbsolute(contained)) continue;
         if (entry.isDirectory()) {
            await walk(full, depth + 1);
         } else if (
            entry.isFile() &&
            (entry.name.endsWith(".html") || entry.name.endsWith(".htm"))
         ) {
            const rel = path.relative(publicRoot, full).replace(/\\/g, "/");
            // Cheap title extraction: read first 4KB and grep for <title>.
            let title = rel;
            try {
               const fh = await fs.open(full, "r");
               try {
                  const buf = Buffer.alloc(4096);
                  const { bytesRead } = await fh.read(buf, 0, 4096, 0);
                  const head = buf.slice(0, bytesRead).toString("utf8");
                  const m = head.match(/<title[^>]*>([^<]+)<\/title>/i);
                  if (m) title = m[1].trim();
               } finally {
                  await fh.close();
               }
            } catch {
               // ignore; fall back to relative path as title
            }
            out.push({
               resource: `/environments/${environmentName}/packages/${packageName}/${rel}`,
               packageName,
               path: rel,
               title,
            });
         }
      }
   }

   await walk(publicRoot, 0);
   out.sort((a, b) => {
      // Surface index.html first, then alphabetical.
      if (a.path === "index.html") return -1;
      if (b.path === "index.html") return 1;
      return a.path.localeCompare(b.path);
   });
   return out;
}

// NOTE: route registration for /pages moved below the CORS middleware so
// cross-origin SDK consumers (e.g. a customer's React app pointing at
// `<ServerProvider baseURL="https://publisher.example.com/api/v0">`) get
// the proper CORS headers. See the registration after `app.use(cors(...))`.

// Only serve static files in production mode
// Otherwise we proxy to the React dev server
if (!isDevelopment) {
   app.use("/", express.static(ROOT));
   app.use("/api-doc.html", express.static(path.join(ROOT, "api-doc.html")));
} else {
   // In development mode, proxy requests to React dev server
   // Handle API routes first
   app.use(`${API_PREFIX}`, loggerMiddleware);

   // Proxy everything else to Vite
   app.use(
      createProxyMiddleware({
         target: "http://localhost:5173",
         changeOrigin: true,
         ws: true,
         pathFilter: (path) =>
            !path.startsWith("/api/") &&
            !path.startsWith("/metrics") &&
            !path.startsWith("/health"),
      }),
   );
}

const setVersionIdError = (res: express.Response) => {
   const { json, status } = internalErrorToHttpError(
      new NotImplementedError("Version IDs not implemented."),
   );
   res.status(status).json(json);
};

app.use(
   cors({
      origin: "http://localhost:5173",
      credentials: true,
   }),
);

// Set body-parser JSON limit to 1Mb (default: 100kb)
app.use(bodyParser.json({ limit: "1mb" }));

// Register health check endpoints on main app:
// - Required for production/Kubernetes monitoring (main server on PUBLISHER_PORT)
registerHealthEndpoints(app);

// Register Prometheus metrics endpoint
try {
   const metricsHandler = getPrometheusMetricsHandler();
   app.get("/metrics", metricsHandler);
   logger.info("Prometheus metrics endpoint registered at /metrics");
} catch (error) {
   logger.warn("Failed to register Prometheus metrics endpoint", { error });
}

// Register draining guard middleware - must be after health endpoints but before other routes
app.use(drainingGuard);

// /pages — registered here (post-CORS, post-body-parser, post-draining) so
// cross-origin SDK consumers and authenticated requests both work.
app.get(
   `${API_PREFIX}/environments/:environmentName/packages/:packageName/pages`,
   async (req, res) => {
      try {
         const environment = await environmentStore.getEnvironment(
            req.params.environmentName,
            false,
         );
         const pkg = await environment.getPackage(
            req.params.packageName,
            false,
         );
         const pages = await listPackagePages(
            req.params.environmentName,
            req.params.packageName,
            path.join(pkg.getPackagePath(), "public"),
         );
         res.json(pages);
      } catch (error) {
         logger.error("Failed to list package pages", { error });
         const { json, status } = internalErrorToHttpError(error as Error);
         res.status(status).json(json);
      }
   },
);

app.get(`${API_PREFIX}/status`, async (_req, res) => {
   try {
      const status = await environmentStore.getStatus();
      res.status(200).json(status);
   } catch (error) {
      logger.error("Error getting status", { error });
      const { json, status } = internalErrorToHttpError(error as Error);
      res.status(status).json(json);
   }
});

app.get(`${API_PREFIX}/watch-mode/status`, watchModeController.getWatchStatus);
app.post(`${API_PREFIX}/watch-mode/start`, watchModeController.startWatching);
app.post(`${API_PREFIX}/watch-mode/stop`, watchModeController.stopWatchMode);

// Live-reload Server-Sent Events stream for in-package HTML dashboards.
//
// This endpoint does NOT start watch mode on its own — that's an explicit
// opt-in (`--watch-env <name>` CLI flag, or `POST /api/v0/watch-mode/start`).
// Instead it reports whether watch mode is currently active for the requested
// env via a `mode` event and, if so, fans out file-change events to the
// browser. This avoids two earlier bugs:
//   - Auto-starting from the request handler made arbitrary fetches reach
//     in to mutate global watch-mode state (`event traversal — see below).
//   - The runtime previously had no way to know "watch mode isn't running,
//     don't expect reloads"; with the `mode` event it can choose to surface
//     a small dev indicator (today: silent).
//
// Inputs are validated before any state lookup. Names that don't pass the
// canonical `assertSafePackageName` allowlist get 400 — preventing requests
// like `/api/v0/environments/%2e%2e/packages/x/events` from reaching the
// EnvironmentStore at all. We reuse the shared sanitizer rather than a local
// regex so the rules stay in one place (see path_safety.ts).
// Cap concurrent live-reload SSE connections so the endpoint can't be used to
// exhaust server sockets/memory with unbounded long-lived streams. Generous,
// since legitimate use is one stream per open dashboard tab.
const MAX_SSE_CONNECTIONS = 1000;
let sseConnectionCount = 0;
app.get(
   `${API_PREFIX}/environments/:environmentName/packages/:packageName/events`,
   async (req, res) => {
      const env = req.params.environmentName;
      const pkg = req.params.packageName;
      try {
         assertSafePackageName(env);
         assertSafePackageName(pkg);
         const environment = await environmentStore.getEnvironment(env, false);
         await environment.getPackage(pkg, false); // 404 if missing
      } catch (error) {
         const { json, status } = internalErrorToHttpError(error as Error);
         res.status(status).json(json);
         return;
      }

      if (sseConnectionCount >= MAX_SSE_CONNECTIONS) {
         res.status(503).json({
            code: 503,
            message: "Too many live-reload connections; try again shortly.",
         });
         return;
      }
      sseConnectionCount++;

      res.set({
         "content-type": "text/event-stream",
         "cache-control": "no-cache",
         connection: "keep-alive",
         // Disable proxy/CDN buffering so events flush immediately.
         "x-accel-buffering": "no",
      });
      res.flushHeaders();

      const watching = watchModeController.isWatching(env);
      res.write("event: hello\ndata: connected\n\n");
      res.write(`event: mode\ndata: ${watching ? "enabled" : "disabled"}\n\n`);

      const key = `${env}/${pkg}`;
      const send = () => {
         res.write("event: changed\ndata: changed\n\n");
      };
      watchModeController.events.on(key, send);
      // Keep the connection alive through idle proxies (heartbeat every 25s).
      const heartbeat = setInterval(() => {
         res.write(": heartbeat\n\n");
      }, 25000);
      const cleanup = () => {
         clearInterval(heartbeat);
         watchModeController.events.off(key, send);
         sseConnectionCount--;
      };
      // "close" covers both clean and abrupt disconnects on Node >= 20.
      req.on("close", cleanup);
   },
);

app.get(`${API_PREFIX}/environments`, async (_req, res) => {
   try {
      res.status(200).json(await environmentStore.listEnvironments());
   } catch (error) {
      logger.error(error);
      const { json, status } = internalErrorToHttpError(error as Error);
      res.status(status).json(json);
   }
});

app.post(`${API_PREFIX}/environments`, async (req, res) => {
   try {
      logger.info("Adding environment", { body: req.body });
      const environment = await environmentStore.addEnvironment(req.body);
      res.status(200).json(await environment.serialize());
   } catch (error) {
      logger.error(error);
      const { json, status } = internalErrorToHttpError(error as Error);
      res.status(status).json(json);
   }
});

app.get(`${API_PREFIX}/environments/:environmentName`, async (req, res) => {
   try {
      const environment = await environmentStore.getEnvironment(
         req.params.environmentName,
         req.query.reload === "true",
      );
      res.status(200).json(await environment.serialize());
   } catch (error) {
      logger.error(error);
      const { json, status } = internalErrorToHttpError(error as Error);
      res.status(status).json(json);
   }
});

app.patch(`${API_PREFIX}/environments/:environmentName`, async (req, res) => {
   try {
      const environment = await environmentStore.updateEnvironment(req.body);
      res.status(200).json(await environment.serialize());
   } catch (error) {
      logger.error(error);
      const { json, status } = internalErrorToHttpError(error as Error);
      res.status(status).json(json);
   }
});

app.delete(`${API_PREFIX}/environments/:environmentName`, async (req, res) => {
   try {
      const environment = await environmentStore.deleteEnvironment(
         req.params.environmentName,
      );
      res.status(200).json(await environment?.serialize());
   } catch (error) {
      logger.error(error);
      const { json, status } = internalErrorToHttpError(error as Error);
      res.status(status).json(json);
   }
});

app.get(
   `${API_PREFIX}/environments/:environmentName/connections`,
   async (req, res) => {
      try {
         res.status(200).json(
            await connectionController.listConnections(
               req.params.environmentName,
            ),
         );
      } catch (error) {
         logger.error(error);
         const { json, status } = internalErrorToHttpError(error as Error);
         res.status(status).json(json);
      }
   },
);

app.get(
   `${API_PREFIX}/environments/:environmentName/connections/:connectionName`,
   async (req, res) => {
      try {
         res.status(200).json(
            await connectionController.getConnection(
               req.params.environmentName,
               req.params.connectionName,
            ),
         );
      } catch (error) {
         logger.error(error);
         const { json, status } = internalErrorToHttpError(error as Error);
         res.status(status).json(json);
      }
   },
);

app.post(
   `${API_PREFIX}/environments/:environmentName/connections/:connectionName`,
   async (req, res) => {
      try {
         const result = await connectionController.addConnection(
            req.params.environmentName,
            req.params.connectionName,
            req.body,
         );
         res.status(201).json(result);
      } catch (error) {
         logger.error("Error creating connection", { error });
         const { json, status } = internalErrorToHttpError(error as Error);
         res.status(status).json(json);
      }
   },
);

app.patch(
   `${API_PREFIX}/environments/:environmentName/connections/:connectionName`,
   async (req, res) => {
      try {
         const result = await connectionController.updateConnection(
            req.params.environmentName,
            req.params.connectionName,
            req.body,
         );
         res.status(200).json(result);
      } catch (error) {
         logger.error("Error updating connection", { error });
         const { json, status } = internalErrorToHttpError(error as Error);
         res.status(status).json(json);
      }
   },
);

app.delete(
   `${API_PREFIX}/environments/:environmentName/connections/:connectionName`,
   async (req, res) => {
      try {
         const result = await connectionController.deleteConnection(
            req.params.environmentName,
            req.params.connectionName,
         );
         res.status(200).json(result);
      } catch (error) {
         logger.error("Error deleting connection", { error });
         const { json, status } = internalErrorToHttpError(error as Error);
         res.status(status).json(json);
      }
   },
);

app.post(`${API_PREFIX}/connections/test`, async (req, res) => {
   try {
      const connectionStatus =
         await connectionController.testConnectionConfiguration(req.body);
      res.status(200).json(connectionStatus);
   } catch (error) {
      logger.error(error);
      const { json, status } = internalErrorToHttpError(error as Error);
      res.status(status).json(json);
   }
});

app.get(
   `${API_PREFIX}/environments/:environmentName/connections/:connectionName/schemas`,
   async (req, res) => {
      try {
         res.status(200).json(
            await connectionController.listSchemas(
               req.params.environmentName,
               req.params.connectionName,
            ),
         );
      } catch (error) {
         logger.error(error);
         const { json, status } = internalErrorToHttpError(error as Error);
         res.status(status).json(json);
      }
   },
);

app.get(
   `${API_PREFIX}/environments/:environmentName/connections/:connectionName/schemas/:schemaName/tables`,
   async (req, res) => {
      logger.info("req.params", { params: req.params });
      try {
         const results = await connectionController.listTables(
            req.params.environmentName,
            req.params.connectionName,
            req.params.schemaName,
            normalizeQueryArray(req.query.tableNames),
         );
         res.status(200).json(results);
      } catch (error) {
         logger.error(error);
         const { json, status } = internalErrorToHttpError(error as Error);
         res.status(status).json(json);
      }
   },
);

app.get(
   `${API_PREFIX}/environments/:environmentName/connections/:connectionName/schemas/:schemaName/tables/:tablePath`,
   async (req, res) => {
      logger.info("req.params", { params: req.params });
      try {
         const results = await connectionController.getTable(
            req.params.environmentName,
            req.params.connectionName,
            req.params.schemaName,
            req.params.tablePath,
         );
         res.status(200).json(results);
      } catch (error) {
         logger.error(error);
         const { json, status } = internalErrorToHttpError(error as Error);
         res.status(status).json(json);
      }
   },
);

// ── Per-package connection data routes ─────────────────────────────
// `duckdb` is per-package; non-`duckdb` names fall through to the
// project's connection registry via the package's MalloyConfig wrapper.
app.get(
   `${API_PREFIX}/environments/:environmentName/packages/:packageName/connections/:connectionName/schemas`,
   async (req, res) => {
      try {
         res.status(200).json(
            await connectionController.listSchemas(
               req.params.environmentName,
               req.params.connectionName,
               req.params.packageName,
            ),
         );
      } catch (error) {
         logger.error(error);
         const { json, status } = internalErrorToHttpError(error as Error);
         res.status(status).json(json);
      }
   },
);

app.get(
   `${API_PREFIX}/environments/:environmentName/packages/:packageName/connections/:connectionName/schemas/:schemaName/tables`,
   async (req, res) => {
      try {
         res.status(200).json(
            await connectionController.listTables(
               req.params.environmentName,
               req.params.connectionName,
               req.params.schemaName,
               normalizeQueryArray(req.query.tableNames),
               req.params.packageName,
            ),
         );
      } catch (error) {
         logger.error(error);
         const { json, status } = internalErrorToHttpError(error as Error);
         res.status(status).json(json);
      }
   },
);

app.get(
   `${API_PREFIX}/environments/:environmentName/packages/:packageName/connections/:connectionName/schemas/:schemaName/tables/:tablePath`,
   async (req, res) => {
      try {
         res.status(200).json(
            await connectionController.getTable(
               req.params.environmentName,
               req.params.connectionName,
               req.params.schemaName,
               req.params.tablePath,
               req.params.packageName,
            ),
         );
      } catch (error) {
         logger.error(error);
         const { json, status } = internalErrorToHttpError(error as Error);
         res.status(status).json(json);
      }
   },
);

/**
 * @deprecated Use /environments/:environmentName/connections/:connectionName/sqlSource POST method instead
 */
app.get(
   `${API_PREFIX}/environments/:environmentName/connections/:connectionName/sqlSource`,
   async (req, res) => {
      try {
         res.status(200).json(
            await connectionController.getConnectionSqlSource(
               req.params.environmentName,
               req.params.connectionName,
               req.query.sqlStatement as string,
            ),
         );
      } catch (error) {
         logger.error(error);
         const { json, status } = internalErrorToHttpError(error as Error);
         res.status(status).json(json);
      }
   },
);

app.post(
   `${API_PREFIX}/environments/:environmentName/connections/:connectionName/sqlSource`,
   async (req, res) => {
      try {
         res.status(200).json(
            await connectionController.getConnectionSqlSource(
               req.params.environmentName,
               req.params.connectionName,
               req.body.sqlStatement as string,
            ),
         );
      } catch (error) {
         logger.error(error);
         const { json, status } = internalErrorToHttpError(error as Error);
         res.status(status).json(json);
      }
   },
);

// Per-package versions
app.get(
   `${API_PREFIX}/environments/:environmentName/packages/:packageName/connections/:connectionName/sqlSource`,
   async (req, res) => {
      try {
         res.status(200).json(
            await connectionController.getConnectionSqlSource(
               req.params.environmentName,
               req.params.connectionName,
               req.query.sqlStatement as string,
               req.params.packageName,
            ),
         );
      } catch (error) {
         logger.error(error);
         const { json, status } = internalErrorToHttpError(error as Error);
         res.status(status).json(json);
      }
   },
);

app.post(
   `${API_PREFIX}/environments/:environmentName/packages/:packageName/connections/:connectionName/sqlSource`,
   async (req, res) => {
      try {
         res.status(200).json(
            await connectionController.getConnectionSqlSource(
               req.params.environmentName,
               req.params.connectionName,
               req.body.sqlStatement as string,
               req.params.packageName,
            ),
         );
      } catch (error) {
         logger.error(error);
         const { json, status } = internalErrorToHttpError(error as Error);
         res.status(status).json(json);
      }
   },
);

// NOTE: The deprecated `GET …/connections/:connectionName/queryData`
// and `GET …/packages/:packageName/connections/:connectionName/queryData`
// routes were removed in the operational-guards changeset.
// They had been marked `@deprecated` for several releases; clients
// must now use the POST `…/sqlQuery` endpoints below, which take the
// SQL in the request body so the row/byte caps and query-timeout
// signals introduced in the OOM-mitigation work apply uniformly.
// The legacy `GET /projects/…/queryData` twins under `server-old.ts`
// remain in place for now.
app.post(
   `${API_PREFIX}/environments/:environmentName/connections/:connectionName/sqlQuery`,
   queryConcurrency(),
   async (req, res) => {
      try {
         let options: string | ParsedQs | (string | ParsedQs)[] | undefined;

         // Support both body and query parameters for options for backwards compatibility
         // TODO: To be removed in the future
         if (req.body?.options) {
            options = req.body.options;
         } else {
            options = req.query.options;
         }
         res.status(200).json(
            await connectionController.getConnectionQueryData(
               req.params.environmentName,
               req.params.connectionName,
               req.body.sqlStatement as string,
               options as string,
            ),
         );
      } catch (error) {
         logger.error(error);
         const { json, status } = internalErrorToHttpError(error as Error);
         res.status(status).json(json);
      }
   },
);

app.post(
   `${API_PREFIX}/environments/:environmentName/packages/:packageName/connections/:connectionName/sqlQuery`,
   queryConcurrency(),
   async (req, res) => {
      try {
         let options: string | ParsedQs | (string | ParsedQs)[] | undefined;
         if (req.body?.options) {
            options = req.body.options;
         } else {
            options = req.query.options;
         }
         res.status(200).json(
            await connectionController.getConnectionQueryData(
               req.params.environmentName,
               req.params.connectionName,
               req.body.sqlStatement as string,
               options as string,
               req.params.packageName,
            ),
         );
      } catch (error) {
         logger.error(error);
         const { json, status } = internalErrorToHttpError(error as Error);
         res.status(status).json(json);
      }
   },
);

/**
 * @deprecated Use environments/:environmentName/connections/:connectionName/sqlTemporaryTable POST method instead
 */
app.get(
   `${API_PREFIX}/environments/:environmentName/connections/:connectionName/temporaryTable`,
   queryConcurrency(),
   async (req, res) => {
      try {
         res.status(200).json(
            await connectionController.getConnectionTemporaryTable(
               req.params.environmentName,
               req.params.connectionName,
               req.query.sqlStatement as string,
            ),
         );
      } catch (error) {
         logger.error(error);
         const { json, status } = internalErrorToHttpError(error as Error);
         res.status(status).json(json);
      }
   },
);

/**
 * @deprecated Use /environments/:environmentName/packages/:packageName/connections/:connectionName/sqlTemporaryTable
 */
app.get(
   `${API_PREFIX}/environments/:environmentName/packages/:packageName/connections/:connectionName/temporaryTable`,
   queryConcurrency(),
   async (req, res) => {
      try {
         res.status(200).json(
            await connectionController.getConnectionTemporaryTable(
               req.params.environmentName,
               req.params.connectionName,
               req.query.sqlStatement as string,
               req.params.packageName,
            ),
         );
      } catch (error) {
         logger.error(error);
         const { json, status } = internalErrorToHttpError(error as Error);
         res.status(status).json(json);
      }
   },
);

app.post(
   `${API_PREFIX}/environments/:environmentName/connections/:connectionName/sqlTemporaryTable`,
   queryConcurrency(),
   async (req, res) => {
      try {
         res.status(200).json(
            await connectionController.getConnectionTemporaryTable(
               req.params.environmentName,
               req.params.connectionName,
               req.body.sqlStatement as string,
            ),
         );
      } catch (error) {
         logger.error(error);
         const { json, status } = internalErrorToHttpError(error as Error);
         res.status(status).json(json);
      }
   },
);

app.post(
   `${API_PREFIX}/environments/:environmentName/packages/:packageName/connections/:connectionName/sqlTemporaryTable`,
   queryConcurrency(),
   async (req, res) => {
      try {
         res.status(200).json(
            await connectionController.getConnectionTemporaryTable(
               req.params.environmentName,
               req.params.connectionName,
               req.body.sqlStatement as string,
               req.params.packageName,
            ),
         );
      } catch (error) {
         logger.error(error);
         const { json, status } = internalErrorToHttpError(error as Error);
         res.status(status).json(json);
      }
   },
);

app.get(
   `${API_PREFIX}/environments/:environmentName/packages`,
   async (req, res) => {
      if (req.query.versionId) {
         setVersionIdError(res);
         return;
      }

      try {
         res.status(200).json(
            await packageController.listPackages(req.params.environmentName),
         );
      } catch (error) {
         logger.error(error);
         const { json, status } = internalErrorToHttpError(error as Error);
         res.status(status).json(json);
      }
   },
);

app.post(
   `${API_PREFIX}/environments/:environmentName/packages`,
   async (req, res) => {
      try {
         const _package = await packageController.addPackage(
            req.params.environmentName,
            req.body,
         );
         res.status(200).json(_package?.getPackageMetadata());
      } catch (error) {
         logger.error(error);
         const { json, status } = internalErrorToHttpError(error as Error);
         res.status(status).json(json);
      }
   },
);

app.get(
   `${API_PREFIX}/environments/:environmentName/packages/:packageName`,
   async (req, res) => {
      if (req.query.versionId) {
         setVersionIdError(res);
         return;
      }

      try {
         res.status(200).json(
            await packageController.getPackage(
               req.params.environmentName,
               req.params.packageName,
               req.query.reload === "true",
            ),
         );
      } catch (error) {
         logger.error(error);
         const { json, status } = internalErrorToHttpError(error as Error);
         res.status(status).json(json);
      }
   },
);

app.patch(
   `${API_PREFIX}/environments/:environmentName/packages/:packageName`,
   async (req, res) => {
      try {
         res.status(200).json(
            await packageController.updatePackage(
               req.params.environmentName,
               req.params.packageName,
               req.body,
            ),
         );
      } catch (error) {
         logger.error(error);
         const { json, status } = internalErrorToHttpError(error as Error);
         res.status(status).json(json);
      }
   },
);

app.delete(
   `${API_PREFIX}/environments/:environmentName/packages/:packageName`,
   async (req, res) => {
      try {
         res.status(200).json(
            await packageController.deletePackage(
               req.params.environmentName,
               req.params.packageName,
            ),
         );
      } catch (error) {
         logger.error(error);
         const { json, status } = internalErrorToHttpError(error as Error);
         res.status(status).json(json);
      }
   },
);

app.get(
   `${API_PREFIX}/environments/:environmentName/packages/:packageName/models`,
   async (req, res) => {
      if (req.query.versionId) {
         setVersionIdError(res);
         return;
      }

      try {
         res.status(200).json(
            await modelController.listModels(
               req.params.environmentName,
               req.params.packageName,
            ),
         );
      } catch (error) {
         logger.error(error);
         const { json, status } = internalErrorToHttpError(error as Error);
         res.status(status).json(json);
      }
   },
);

app.get(
   `${API_PREFIX}/environments/:environmentName/packages/:packageName/models/*?`,
   async (req, res) => {
      if (req.query.versionId) {
         setVersionIdError(res);
         return;
      }

      try {
         // Express stores wildcard matches in params['0']
         const modelPath = (req.params as Record<string, string>)["0"];
         res.status(200).json(
            await modelController.getModel(
               req.params.environmentName,
               req.params.packageName,
               modelPath,
            ),
         );
      } catch (error) {
         logger.error(error);
         const { json, status } = internalErrorToHttpError(error as Error);
         res.status(status).json(json);
      }
   },
);

app.get(
   `${API_PREFIX}/environments/:environmentName/packages/:packageName/notebooks`,
   async (req, res) => {
      if (req.query.versionId) {
         setVersionIdError(res);
         return;
      }

      try {
         res.status(200).json(
            await modelController.listNotebooks(
               req.params.environmentName,
               req.params.packageName,
            ),
         );
      } catch (error) {
         logger.error(error);
         const { json, status } = internalErrorToHttpError(error as Error);
         res.status(status).json(json);
      }
   },
);

// Execute notebook cell route must come BEFORE the general get notebook route
// to avoid the wildcard matching incorrectly
app.get(
   `${API_PREFIX}/environments/:environmentName/packages/:packageName/notebooks/*/cells/:cellIndex`,
   queryConcurrency(),
   async (req, res) => {
      if (req.query.versionId) {
         setVersionIdError(res);
         return;
      }

      try {
         const cellIndex = parseInt(req.params.cellIndex, 10);
         if (isNaN(cellIndex)) {
            res.status(400).json({
               error: "Invalid cell index",
            });
            return;
         }

         // Express stores wildcard matches in params['0']
         const notebookPath = (req.params as Record<string, string>)["0"];

         // Parse optional filter_params (JSON query string) and bypass_filters
         let filterParams: Record<string, string | string[]> | undefined;
         if (typeof req.query.filter_params === "string") {
            try {
               filterParams = JSON.parse(req.query.filter_params);
            } catch {
               res.status(400).json({
                  error: "Invalid filter_params: must be valid JSON",
               });
               return;
            }
         }
         const bypassFilters =
            req.query.bypass_filters === "true" ? true : undefined;

         let givens: Record<string, GivenValue> | undefined;
         if (typeof req.query.givens === "string") {
            try {
               givens = JSON.parse(req.query.givens);
            } catch {
               res.status(400).json({
                  error: "Invalid givens: must be valid JSON",
               });
               return;
            }
         }

         const result = await modelController.executeNotebookCell(
            req.params.environmentName,
            req.params.packageName,
            notebookPath,
            cellIndex,
            filterParams,
            bypassFilters,
            givens,
         );
         setFilterDeprecationHeaders(res, {
            filterParams,
            bypassFilters,
         });
         res.status(200).json(result);
      } catch (error) {
         logger.error(error);
         const { json, status } = internalErrorToHttpError(error as Error);
         res.status(status).json(json);
      }
   },
);

app.get(
   `${API_PREFIX}/environments/:environmentName/packages/:packageName/notebooks/*?`,
   async (req, res) => {
      if (req.query.versionId) {
         setVersionIdError(res);
         return;
      }

      try {
         // Express stores wildcard matches in params['0']
         const notebookPath = (req.params as Record<string, string>)["0"];
         res.status(200).json(
            await modelController.getNotebook(
               req.params.environmentName,
               req.params.packageName,
               notebookPath,
            ),
         );
      } catch (error) {
         logger.error(error);
         const { json, status } = internalErrorToHttpError(error as Error);
         res.status(status).json(json);
      }
   },
);

app.post(
   `${API_PREFIX}/environments/:environmentName/packages/:packageName/models/*?/query`,
   queryConcurrency(),
   async (req, res) => {
      if (req.body.versionId) {
         setVersionIdError(res);
         return;
      }

      try {
         // Express stores wildcard matches in params['0']
         const modelPath = (req.params as Record<string, string>)["0"];
         const result = await queryController.getQuery(
            req.params.environmentName,
            req.params.packageName,
            modelPath,
            req.body.sourceName as string,
            req.body.queryName as string,
            req.body.query as string,
            req.body.compactJson === true,
            (req.body.filterParams ?? req.body.sourceFilters) as
               | Record<string, string | string[]>
               | undefined,
            req.body.bypassFilters === true ? true : undefined,
            req.body.givens as Record<string, GivenValue> | undefined,
         );
         setFilterDeprecationHeaders(res, {
            filterParams: req.body.filterParams ?? req.body.sourceFilters,
            bypassFilters: req.body.bypassFilters === true ? true : undefined,
         });
         res.status(200).json(result);
      } catch (error) {
         logger.error(error);
         const { json, status } = internalErrorToHttpError(error as Error);
         res.status(status).json(json);
      }
   },
);

app.get(
   `${API_PREFIX}/environments/:environmentName/packages/:packageName/databases`,
   async (req, res) => {
      if (req.query.versionId) {
         setVersionIdError(res);
         return;
      }

      try {
         res.status(200).json(
            await databaseController.listDatabases(
               req.params.environmentName,
               req.params.packageName,
            ),
         );
      } catch (error) {
         logger.error(error);
         const { json, status } = internalErrorToHttpError(error as Error);
         res.status(status).json(json);
      }
   },
);

app.post(
   `${API_PREFIX}/environments/:environmentName/packages/:packageName/models/:modelName/compile`,
   async (req, res) => {
      try {
         const result = await compileController.compile(
            req.params.environmentName,
            req.params.packageName,
            req.params.modelName,
            req.body.source,
            req.body.includeSql === true,
            req.body.givens as Record<string, GivenValue> | undefined,
         );
         res.status(200).json(result);
      } catch (error) {
         logger.error("Compilation error", { error });
         const { json, status } = internalErrorToHttpError(error as Error);
         res.status(status).json(json);
      }
   },
);

// ==================== MATERIALIZATION ROUTES ====================

app.post(
   `${API_PREFIX}/environments/:environmentName/packages/:packageName/materializations`,
   async (req, res) => {
      try {
         const build = await materializationController.createMaterialization(
            req.params.environmentName,
            req.params.packageName,
            req.body || {},
         );
         res.status(201).json(build);
      } catch (error) {
         const { json, status } = internalErrorToHttpError(error as Error);
         res.status(status).json(json);
      }
   },
);

app.get(
   `${API_PREFIX}/environments/:environmentName/packages/:packageName/materializations`,
   async (req, res) => {
      try {
         const limit = req.query.limit
            ? parseInt(req.query.limit as string, 10)
            : undefined;
         const offset = req.query.offset
            ? parseInt(req.query.offset as string, 10)
            : undefined;
         const builds = await materializationController.listMaterializations(
            req.params.environmentName,
            req.params.packageName,
            { limit, offset },
         );
         res.status(200).json(builds);
      } catch (error) {
         const { json, status } = internalErrorToHttpError(error as Error);
         res.status(status).json(json);
      }
   },
);

app.get(
   `${API_PREFIX}/environments/:environmentName/packages/:packageName/materializations/:materializationId`,
   async (req, res) => {
      try {
         const build = await materializationController.getMaterialization(
            req.params.environmentName,
            req.params.packageName,
            req.params.materializationId,
         );
         res.status(200).json(build);
      } catch (error) {
         const { json, status } = internalErrorToHttpError(error as Error);
         res.status(status).json(json);
      }
   },
);

app.post(
   `${API_PREFIX}/environments/:environmentName/packages/:packageName/materializations/:materializationId`,
   async (req, res) => {
      try {
         const action = req.query.action;
         if (action === "build") {
            const build = await materializationController.buildMaterialization(
               req.params.environmentName,
               req.params.packageName,
               req.params.materializationId,
               req.body || {},
            );
            res.status(202).json(build);
         } else if (action === "stop") {
            const build = await materializationController.stopMaterialization(
               req.params.environmentName,
               req.params.packageName,
               req.params.materializationId,
            );
            res.status(200).json(build);
         } else {
            throw new BadRequestError(
               `Unsupported action '${String(action ?? "")}'. Expected 'build' or 'stop'.`,
            );
         }
      } catch (error) {
         const { json, status } = internalErrorToHttpError(error as Error);
         res.status(status).json(json);
      }
   },
);

app.delete(
   `${API_PREFIX}/environments/:environmentName/packages/:packageName/materializations/:materializationId`,
   async (req, res) => {
      try {
         await materializationController.deleteMaterialization(
            req.params.environmentName,
            req.params.packageName,
            req.params.materializationId,
            { dropTables: req.query.dropTables === "true" },
         );
         res.status(204).send();
      } catch (error) {
         const { json, status } = internalErrorToHttpError(error as Error);
         res.status(status).json(json);
      }
   },
);

// Register legacy `/projects/...` routes for backwards compatibility with
// clients that haven't migrated to `/environments/...` yet. Must be added
// before the SPA catch-all below.
registerLegacyRoutes(app, {
   environmentStore,
   connectionController,
   modelController,
   packageController,
   databaseController,
   queryController,
   compileController,
   materializationController,
});

// Modify the catch-all route to only serve index.html in production
if (!isDevelopment) {
   const SPA_INDEX = path.resolve(ROOT, "index.html");
   app.get("*", (req, res) => {
      res.sendFile(SPA_INDEX, (err) => {
         if (!err) return;
         // The SPA bundle isn't built. This happens when running directly
         // from source (`bun run src/server.ts`) without first running
         // `bun run build:app`. Return a friendly placeholder rather than
         // a 500, and surface package URLs the user might be looking for.
         if (res.headersSent) return;
         res.status(404)
            .type("text/html")
            .send(
               `<!doctype html><meta charset="utf-8">
<title>Publisher</title>
<style>body{font:14px/1.4 -apple-system,system-ui,sans-serif;margin:40px;max-width:720px;color:#222}</style>
<h1>Publisher is running, but the SPA bundle isn't built.</h1>
<p>You requested <code>${req.path.replace(/[<>&]/g, (c) => ({ "<": "&lt;", ">": "&gt;", "&": "&amp;" })[c] ?? c)}</code>.
The Publisher API is available at <a href="/api/v0/environments">/api/v0/environments</a>.</p>
<p>To get the Publisher web UI, run <code>cd packages/app &amp;&amp; bunx vite build</code>
or start the server with <code>NODE_ENV=development</code> after launching Vite on <code>:5173</code>.</p>
<p>For in-package HTML data apps, browse to <code>/environments/&lt;env&gt;/packages/&lt;pkg&gt;/&lt;file&gt;</code> directly.</p>`,
            );
      });
   });
}

app.use(
   (
      err: Error,
      _req: express.Request,
      res: express.Response,
      _next: express.NextFunction,
   ) => {
      logger.error("Unhandled error:", err);
      const { json, status } = internalErrorToHttpError(err);
      res.status(status).json(json);
   },
);

// Eagerly construct the package-load worker pool so we fail fast at
// boot if PACKAGE_LOAD_WORKERS is misconfigured (e.g. set to 0, the
// removed in-process fallback). Surfacing the bad config here is much
// friendlier than surfacing it on the first package load, which could
// be hours after start.
{
   const { getPackageLoadPool } = await import(
      "./package_load/package_load_pool"
   );
   getPackageLoadPool();
}

const mainServer = http.createServer({ maxHeaderSize: 262144 }, app);

mainServer.timeout = 600000;
mainServer.keepAliveTimeout = 600000;
mainServer.headersTimeout = 600000;

mainServer.listen(PUBLISHER_PORT, PUBLISHER_HOST, async () => {
   const address = mainServer.address() as AddressInfo;
   logger.info(
      `Publisher server listening at http://${address.address}:${address.port}`,
   );
   if (isDevelopment) {
      logger.info(
         "Running in development mode - proxying to React dev server at http://localhost:5173",
      );
   }
   // If `--watch-env <name>` (or PUBLISHER_WATCH=name1,name2) was passed,
   // wait for env initialization to settle, then start watch mode for each
   // named env. Packages in those envs are already mounted in-place via the
   // EnvironmentStore in-place path (see `loadEnvironmentIntoDisk`), so the
   // chokidar watcher will see edits to your source repo and fan them out
   // to any connected SSE clients.
   const watchEnvList = (process.env.PUBLISHER_WATCH || "")
      .split(",")
      .map((s) => s.trim())
      .filter((s) => s.length > 0);
   if (watchEnvList.length > 0) {
      // The watcher tracks exactly one env at a time (`WatchModeController`
      // holds a single chokidar instance). Every env in PUBLISHER_WATCH is
      // still mounted in place (live source) by the EnvironmentStore, but only
      // the first is watched, so the others do not auto-reload.
      if (watchEnvList.length > 1) {
         logger.warn(
            `Multiple watch environments requested (${watchEnvList.join(
               ", ",
            )}); watch mode auto-reloads one at a time. Watching "${
               watchEnvList[0]
            }". The others are mounted in place (their source is live) but will not auto-reload. Pass a single --watch-env (or one PUBLISHER_WATCH value) to silence this.`,
         );
      }
      const envName = watchEnvList[0];
      try {
         await environmentStore.finishedInitialization;
         await watchModeController.ensureWatching(envName);
         logger.info(
            `Watch mode active for environment "${envName}" (in-place mount, source-edit live reload).`,
         );
      } catch (error) {
         logger.error(
            `Failed to start watch mode for environment "${envName}"`,
            { error },
         );
      }
   }
});
const mcpServer = mcpApp.listen(MCP_PORT, PUBLISHER_HOST, () => {
   logger.info(`MCP server listening at http://${PUBLISHER_HOST}:${MCP_PORT}`);
});

mcpServer.timeout = 600000;
mcpServer.keepAliveTimeout = 600000;
mcpServer.headersTimeout = 600000;

registerSignalHandlers(
   mainServer,
   mcpServer,
   SHUTDOWN_DRAIN_DURATION_SECONDS,
   SHUTDOWN_GRACEFUL_CLOSE_TIMEOUT_SECONDS,
);
