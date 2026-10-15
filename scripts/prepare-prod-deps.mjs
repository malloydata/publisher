#!/usr/bin/env bun
// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * Prepares the workspace package.json files in the Docker image's `prod-deps`
 * stage, before its `bun install --production`. Run only there.
 *
 * That stage installs node_modules from the dependency manifests alone, so its
 * build cache, and the image layer it produces, stays the same until a
 * dependency changes. Two things in the manifests would defeat that or break
 * the install, and this script handles both:
 *
 * 1. The `version` of each workspace package is bumped by every release. Nothing
 *    installs by it (app takes the sdk as `workspace:*`), so it is pinned to one
 *    value here. Otherwise every release would change the stage's inputs and
 *    rerun the install.
 *
 * 2. bun links a workspace's `bin` into node_modules/.bin only when the bin's
 *    target file exists, and this stage has no dist/. Each bin target gets an
 *    empty placeholder so the install creates the link. Only node_modules leaves
 *    the stage, so the placeholders never ship; in the image the links resolve
 *    to the real dist/ files copied from the builder.
 */

const WORKSPACES = ["server", "app", "sdk"];
const PINNED_VERSION = "0.0.0";

for (const workspace of WORKSPACES) {
   const manifestPath = `packages/${workspace}/package.json`;
   const manifest = await Bun.file(manifestPath).json();

   manifest.version = PINNED_VERSION;
   await Bun.write(manifestPath, JSON.stringify(manifest, null, 2) + "\n");

   // `bin` is either a single path or a map of command name to path.
   const binTargets =
      typeof manifest.bin === "string"
         ? [manifest.bin]
         : Object.values(manifest.bin ?? {});
   for (const target of binTargets) {
      await Bun.write(`packages/${workspace}/${target}`, "");
   }
}
