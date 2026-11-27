// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

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
 * postMessage `type` a host sends an embedded data app with the reader's light or dark mode: when the frame loads,
 * whenever the reader switches, and whenever the app asks with {@link PUBLISHER_COLOR_SCHEME_REQUEST_MESSAGE_TYPE}.
 * Payload shape is {@link PublisherColorSchemeMessage}. A "system" choice is resolved to light or dark by the host.
 */
export const PUBLISHER_COLOR_SCHEME_MESSAGE_TYPE = "publisher:color-scheme";

/**
 * postMessage `type` a data app's runtime sends its host on start, asking for the reader's mode, in case the host
 * spoke before the page was listening. It carries nothing else.
 */
export const PUBLISHER_COLOR_SCHEME_REQUEST_MESSAGE_TYPE =
   "publisher:color-scheme-request";

/**
 * postMessage `type` a data app sends its host once it has drawn the mode it was told. A host may hold a frame
 * light until its page answers, since a page written only for a light background would be unreadable on a dark one.
 * Payload shape is {@link PublisherColorSchemeMessage}.
 */
export const PUBLISHER_COLOR_SCHEME_APPLIED_MESSAGE_TYPE =
   "publisher:color-scheme-applied";

export type PublisherColorScheme = "light" | "dark";

/** The reader's mode, from the host, or the mode a page answers it drew. */
export interface PublisherColorSchemeMessage {
   type:
      | typeof PUBLISHER_COLOR_SCHEME_MESSAGE_TYPE
      | typeof PUBLISHER_COLOR_SCHEME_APPLIED_MESSAGE_TYPE;
   mode: PublisherColorScheme;
}

const isMode = (m: unknown): m is PublisherColorScheme =>
   m === "light" || m === "dark";

/** Type guard for a page's {@link PUBLISHER_COLOR_SCHEME_REQUEST_MESSAGE_TYPE}. */
export function isPublisherColorSchemeRequest(
   data: unknown,
): data is { type: typeof PUBLISHER_COLOR_SCHEME_REQUEST_MESSAGE_TYPE } {
   return (
      typeof data === "object" &&
      data !== null &&
      (data as { type?: unknown }).type ===
         PUBLISHER_COLOR_SCHEME_REQUEST_MESSAGE_TYPE
   );
}

/** Type guard for a page's {@link PUBLISHER_COLOR_SCHEME_APPLIED_MESSAGE_TYPE} answer. */
export function isPublisherColorSchemeApplied(
   data: unknown,
): data is PublisherColorSchemeMessage {
   return (
      typeof data === "object" &&
      data !== null &&
      (data as { type?: unknown }).type ===
         PUBLISHER_COLOR_SCHEME_APPLIED_MESSAGE_TYPE &&
      isMode((data as { mode?: unknown }).mode)
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

/**
 * Absolute URL of a file inside a package, as Publisher serves it:
 * `<data origin>/environments/<env>/packages/<pkg>/<path>`.
 *
 * The one home for this string. Both the data app viewer's iframe and the
 * "this is not a model" page point at it, and hand-building it in each place is
 * how the two drift.
 *
 * `path` is relative to the package's `public/` directory and is inserted as
 * given, since it may contain slashes. A leading `public/` is dropped, because
 * the served path does not include it and a caller passing a path a reader typed
 * would otherwise produce `public/public/<file>`, which 404s. The server's own
 * redirect drops it for the same reason.
 *
 * `versionId` pins a published version: the static route reads it from the
 * URL, and a page's runtime reads it there to pin its own queries. Absent, the
 * server serves `latest`.
 */
export function packageFileUrl({
   server,
   environmentName,
   packageName,
   path,
   versionId,
}: {
   server: string;
   environmentName: string;
   packageName: string;
   path: string;
   versionId?: string;
}): string {
   const relative = path.startsWith("public/")
      ? path.slice("public/".length)
      : path;
   const url =
      `${serverBaseUrl(server)}/environments/${encodeURIComponent(
         environmentName,
      )}/packages/${encodeURIComponent(packageName)}/` + relative;
   return versionId === undefined
      ? url
      : `${url}${url.includes("?") ? "&" : "?"}versionId=${encodeURIComponent(versionId)}`;
}
