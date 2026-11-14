// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * The Malloy renderer, loaded once and only when a result is on its way.
 *
 * `@malloydata/render` is about 3.4 MB (1 MB gzipped). It used to be warmed by
 * a statement at the top of `RenderedResult`, which ran whenever that module
 * was evaluated, and Rollup places shared code beside that module, so every
 * page of a host downloaded the renderer at idle whether or not it ever drew a
 * result. Warming from here, when a result surface mounts, keeps what the
 * module-level warm-up was for (the first chart does not wait on the import,
 * which is what widened the clear-then-repaint gap into a visible flicker)
 * without charging pages that draw nothing.
 */
let rendererModule: Promise<typeof import("@malloydata/render")> | undefined;

export function loadMalloyRenderer(): Promise<
   typeof import("@malloydata/render")
> {
   if (!rendererModule) {
      rendererModule = import("@malloydata/render");
      // A failed load (a dropped connection, say) is not cached, so the next
      // result to render tries again rather than inheriting the rejection.
      rendererModule.catch(() => {
         rendererModule = undefined;
      });
   }
   return rendererModule;
}

/**
 * Start the renderer download without waiting on it. Call it from a surface
 * that is about to show a result: a result panel whose query is in flight, so
 * the download runs alongside the query rather than after it.
 */
export function warmMalloyRenderer(): void {
   if (typeof window === "undefined") return;
   loadMalloyRenderer().catch(() => {
      // Reported where the renderer is actually used, in `createRenderer`.
   });
}
