#!/usr/bin/env bun
// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * Prepares the workspace manifests the Docker image installs its production
 * node_modules from. It runs in the Dockerfile's `prod-manifests` stage, and
 * `prod-deps` copies the result in, so the install's build cache is keyed on
 * these prepared files rather than on the manifests as committed.
 *
 * It rewrites package.json files and truncates bin targets, so it refuses to
 * run unless PREPARE_PROD_DEPS=docker, which only that stage sets.
 *
 * For every packages/<name>/package.json present:
 *
 * 1. The `version` is pinned to 0.0.0, with scripts/set-version.mjs. Every
 *    release bumps it and nothing installs by it (app takes the sdk as
 *    `workspace:*`), so pinning keeps a release from changing the install's
 *    cache key.
 *
 * 2. Each `bin` target gets an empty placeholder. bun links a workspace's bin
 *    into node_modules/.bin only when the target file exists, and the stage has
 *    no dist/. Only node_modules leaves prod-deps, so the placeholders never
 *    ship; in the image the links resolve to the real dist/ files copied from
 *    the builder.
 */

import path from "node:path";
import { Glob } from "bun";

const PINNED_VERSION = "0.0.0";

if (process.env.PREPARE_PROD_DEPS !== "docker") {
  console.error(
    "prepare-prod-deps: refusing to run outside the Dockerfile's prod-manifests stage (PREPARE_PROD_DEPS=docker)",
  );
  process.exit(1);
}

const manifests = [...new Glob("packages/*/package.json").scanSync(".")].sort();
if (manifests.length === 0) {
  console.error("prepare-prod-deps: no packages/*/package.json to prepare");
  process.exit(1);
}

const pin = Bun.spawnSync(
  ["bun", "scripts/set-version.mjs", PINNED_VERSION, ...manifests],
  { stdout: "inherit", stderr: "inherit" },
);
if (pin.exitCode !== 0) process.exit(pin.exitCode ?? 1);

for (const manifestPath of manifests) {
  const manifest = await Bun.file(manifestPath).json();
  // `bin` is either a single path or a map of command name to path.
  const binTargets =
    typeof manifest.bin === "string"
      ? [manifest.bin]
      : Object.values(manifest.bin ?? {});
  for (const target of binTargets) {
    await Bun.write(path.join(path.dirname(manifestPath), target), "");
  }
}
