// Shared contract for serving and embedding in-package HTML pages.
//
// Both the Publisher SPA host (PageViewer.tsx, Package.tsx) and the
// build-step-free browser runtime (packages/server/src/runtime/publisher.js)
// speak this protocol. publisher.js cannot import from here — it ships as
// standalone vanilla JS — so it carries a cross-reference comment pointing at
// this file as the single source of truth. Keep the two in sync.

/**
 * postMessage `type` an embedded page emits to its host frame as its content
 * height changes, so the host can resize the iframe to avoid nested scrollbars.
 * Payload shape is {@link PublisherResizeMessage}.
 */
export const PUBLISHER_RESIZE_MESSAGE_TYPE = "publisher:resize";

/** Resize message posted by an embedded page's publisher.js runtime. */
export interface PublisherResizeMessage {
   type: typeof PUBLISHER_RESIZE_MESSAGE_TYPE;
   /** Content height in CSS pixels. */
   height: number;
}

/** Type guard for a {@link PublisherResizeMessage} arriving via postMessage. */
export function isPublisherResizeMessage(
   data: unknown,
): data is PublisherResizeMessage {
   return (
      typeof data === "object" &&
      data !== null &&
      (data as { type?: unknown }).type === PUBLISHER_RESIZE_MESSAGE_TYPE &&
      typeof (data as { height?: unknown }).height === "number"
   );
}

/**
 * Derive the Publisher data origin (where static package files are served)
 * from the configured API base URL by stripping the trailing `/api/v0`.
 *
 * The static-file routes live off the server root, not under the API prefix,
 * and the data origin can differ from the SPA origin in multi-host
 * deployments — so a page's standalone URL is `serverBaseUrl(server)` joined
 * with the page's root-relative `resource`.
 */
export function serverBaseUrl(server: string): string {
   return server.replace(/\/api\/v0\/?$/, "");
}

/**
 * Absolute URL of a file inside a package, as Publisher serves it:
 * `<data origin>/environments/<env>/packages/<pkg>/<path>`.
 *
 * The one home for this string. Both the page viewer's iframe and the
 * "this is not a model" page point at it, and hand-building it in each place is
 * how the two drift. `path` is already relative to the package's `public/`
 * directory and is inserted as given, since it may contain slashes.
 */
export function packageFileUrl({
   server,
   environmentName,
   packageName,
   path,
}: {
   server: string;
   environmentName: string;
   packageName: string;
   path: string;
}): string {
   return (
      `${serverBaseUrl(server)}/environments/${encodeURIComponent(
         environmentName,
      )}/packages/${encodeURIComponent(packageName)}/` + path
   );
}
