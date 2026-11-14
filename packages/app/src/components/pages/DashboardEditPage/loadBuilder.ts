// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * The builder's entry, loaded once. It carries the Malloy parser (440 KB
 * gzipped), which most Console users never need, so it stays out of every
 * page's chunk until someone heads for Edit.
 *
 * One promise for both callers: the edit page's `React.lazy`, and the Edit
 * button, which starts the download when the pointer reaches it or it takes
 * focus, so the code is usually in hand by the time the builder mounts and
 * switching to edit shows no "Opening the builder" spinner.
 */
let builder:
   | Promise<typeof import("@malloy-publisher/sdk/builder")>
   | undefined;

export function loadBuilder() {
   builder ??= import("@malloy-publisher/sdk/builder");
   return builder;
}
