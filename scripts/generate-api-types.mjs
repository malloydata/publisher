#!/usr/bin/env node
// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * Run openapi-generator-cli so that a failure is a failure.
 *
 *   bun ../../scripts/generate-api-types.mjs -i ../../api-doc.yaml -g typescript-axios -o src/client/
 *
 * The arguments are the generator's own (`generate` is prepended), with the
 * configuration file at the repository root (`openapitools.json`) passed
 * explicitly so every package resolves the same pinned generator version and
 * stores its jar in the same place, whatever directory the script runs from.
 *
 * Why a wrapper: the CLI downloads the generator jar from Maven Central the
 * first time a version is used, and when that download does not complete it
 * logs "Download failed" and EXITS 0 with nothing written. `bun generate-api-
 * types && bunx vite build` then carried on into vite, which failed on the
 * missing client with an error that named neither the download nor the
 * generator (malloydata/publisher#1428). So:
 *
 *   - the output directory must hold files written by this run, or the run
 *     fails here, with the generator's own output in the log;
 *   - a failed attempt is retried, after removing the jar the CLI may have
 *     left half-written (its presence is all the CLI checks before trusting
 *     it), so a transient network error does not cost the build;
 *   - the version is pinned in openapitools.json, so no "latest 7.x" lookup is
 *     made against Maven's search API either.
 */

import { spawnSync } from "node:child_process";
import {
  existsSync,
  mkdirSync,
  readdirSync,
  readFileSync,
  rmSync,
  statSync,
} from "node:fs";
import os from "node:os";
import path from "node:path";
import { fileURLToPath } from "node:url";

const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");
const CONFIG = path.join(ROOT, "openapitools.json");
const ATTEMPTS = 3;
const BACKOFF_MS = [0, 5_000, 20_000];

const args = process.argv.slice(2);
const outputDir = outputDirOf(args);
if (!outputDir) {
  console.error("generate-api-types: no -o/--output argument to check");
  process.exit(2);
}

const config = JSON.parse(readFileSync(CONFIG, "utf8"))["generator-cli"] ?? {};
const version = config.version;
if (!version) {
  console.error(`generate-api-types: ${CONFIG} pins no generator-cli.version`);
  process.exit(2);
}
// The CLI resolves `storageDir` against its own cwd after expanding `~`; this
// mirrors that for the one thing the wrapper does to it, deleting a jar.
const storageDir = config.storageDir
  ? path.resolve(process.cwd(), config.storageDir.replace("~", os.homedir()))
  : path.join(
      ROOT,
      "node_modules/@openapitools/openapi-generator-cli/versions",
    );
const jar = path.join(storageDir, `${version}.jar`);

for (let attempt = 1; attempt <= ATTEMPTS; attempt++) {
  if (attempt > 1) {
    const wait = BACKOFF_MS[attempt - 1];
    console.error(
      `generate-api-types: attempt ${attempt - 1} of ${ATTEMPTS} failed; ` +
        `retrying in ${wait / 1000}s` +
        (existsSync(jar) ? ` after removing ${jar}` : ""),
    );
    rmSync(jar, { force: true });
    sleep(wait);
  }
  const started = Date.now();
  mkdirSync(outputDir, { recursive: true });
  const run = spawnSync(
    "bunx",
    ["openapi-generator-cli", "--openapitools", CONFIG, "generate", ...args],
    { stdio: "inherit", shell: process.platform === "win32" },
  );
  if (run.error) {
    console.error(
      `generate-api-types: could not start bunx: ${run.error.message}`,
    );
    continue;
  }
  if (run.status !== 0) {
    console.error(
      `generate-api-types: openapi-generator-cli exited with ${run.status}`,
    );
    continue;
  }
  const written = filesWrittenSince(outputDir, started);
  if (written === 0) {
    console.error(
      `generate-api-types: openapi-generator-cli exited 0 but wrote nothing under ` +
        `${outputDir} (its jar download from Maven Central probably did not complete)`,
    );
    continue;
  }
  process.exit(0);
}
console.error(
  `generate-api-types: giving up after ${ATTEMPTS} attempts; the OpenAPI client ` +
    `was not generated`,
);
process.exit(1);

function outputDirOf(argv) {
  for (let i = 0; i < argv.length; i++) {
    if (argv[i] === "-o" || argv[i] === "--output")
      return path.resolve(argv[i + 1]);
    const m = /^(?:-o|--output)=(.+)$/.exec(argv[i]);
    if (m) return path.resolve(m[1]);
  }
  return undefined;
}

function filesWrittenSince(dir, since) {
  let n = 0;
  for (const entry of readdirSync(dir, { withFileTypes: true })) {
    const p = path.join(dir, entry.name);
    if (entry.isDirectory()) n += filesWrittenSince(p, since);
    // One second of slack: some filesystems round mtime down.
    else if (statSync(p).mtimeMs >= since - 1000) n++;
  }
  return n;
}

function sleep(ms) {
  if (ms > 0) Atomics.wait(new Int32Array(new SharedArrayBuffer(4)), 0, 0, ms);
}
