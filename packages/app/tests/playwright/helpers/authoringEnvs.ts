// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * The environments a spec may write into. Every other package a server serves
 * is a published version, which is immutable, so the builders' writes and a
 * reload in place need a watched environment, and a server mounts in place
 * only the environments it was started with (`PUBLISHER_WATCH`).
 * playwright.config.ts starts its server with these; a server you start
 * yourself needs the same: `PUBLISHER_WATCH=pw-authoring-1,pw-authoring-2,…`.
 *
 * The suite runs on one worker, one test at a time, so a spec holds a name
 * only while it runs (see registerPackageEnv) and a few are enough.
 */
export const AUTHORING_ENVS = [
   "pw-authoring-1",
   "pw-authoring-2",
   "pw-authoring-3",
   "pw-authoring-4",
];
