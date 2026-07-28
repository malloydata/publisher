// Shared contract for serving and embedding in-package HTML data apps.
//
// Both the Publisher SPA host (DataAppViewer.tsx, Package.tsx) and the
// build-step-free browser runtime (packages/server/src/runtime/publisher.js)
// speak this protocol. publisher.js cannot import from here — it ships as
// standalone vanilla JS — so it carries a cross-reference comment pointing at
// this file as the single source of truth. Keep the two in sync.

/**
 * postMessage `type` an embedded data app emits to its host frame as its
 * content height changes, so the host can resize the iframe to avoid nested
 * scrollbars. Payload shape is {@link PublisherResizeMessage}.
 */
export const PUBLISHER_RESIZE_MESSAGE_TYPE = "publisher:resize";

/** Resize message posted by an embedded data app's publisher.js runtime. */
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
 * deployments — so a data app's standalone URL is `serverBaseUrl(server)`
 * joined with its root-relative `resource`.
 */
export function serverBaseUrl(server: string): string {
   return server.replace(/\/api\/v0\/?$/, "");
}
