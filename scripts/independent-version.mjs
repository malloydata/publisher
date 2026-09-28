#!/usr/bin/env node
// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { realpathSync } from "node:fs";
import { fileURLToPath, pathToFileURL } from "node:url";

// Pure decision logic for release-time versioning of the independently-versioned
// npm packages (skills, create-malloy-package). `scripts/publish-packages.sh`
// gathers the facts — what npm's `latest` is, what commit it was published
// from, whether that commit is reachable, whether anything changed since it —
// and hands them to the functions here, which decide publish/skip/abort with
// no network access and no git calls of their own. Kept pure and separate from
// the shell so the decision itself can be unit-tested without a registry or a
// checkout, the same split `set-version.mjs` makes for the version-writing side.

// Plain major.minor.patch only, deliberately stricter than set-version.mjs's
// VERSION regex: a "next patch" is arithmetic on the third component, and a
// prerelease or build suffix has no obvious next patch. `npm view
// dist-tags.latest` should never answer with one of those for a package this
// pipeline publishes, so refusing it here is a real guard, not decoration.
const PLAIN_VERSION = /^\d+\.\d+\.\d+$/;

// A gitHead as npm records it: the full commit sha `npm publish` stamped into
// the published manifest. Anything else — empty, short, non-hex — means the
// registry's record cannot be checked against this checkout at all.
const GIT_HEAD = /^[0-9a-f]{40}$/;

/** Collapse embedded newlines so a reason string built from untrusted registry
 * or git text can never start a NEW line with "::", which the Actions runner
 * would read as a workflow command. Quoting alone does not stop that: a
 * literal newline still ends the current line. */
function sanitizeForLine(value) {
  return String(value).replace(/[\r\n]+/g, " ↵ ");
}

/** "x.y.(z+1)" for a plain major.minor.patch version. Throws on anything else,
 * including a prerelease or build suffix — there is no well-defined "next
 * patch" for either, and nothing that reaches here should be publishing one. */
export function nextPatch(latest) {
  const match = PLAIN_VERSION.exec(latest);
  if (!match) {
    throw new Error(
      `refusing to compute a next patch for "${sanitizeForLine(latest)}": expected plain major.minor.patch`,
    );
  }
  const [, major, minor, patch] = /^(\d+)\.(\d+)\.(\d+)$/.exec(latest);
  return `${major}.${minor}.${Number(patch) + 1}`;
}

function abort(reason) {
  return { action: "abort", reason };
}

/**
 * Decide whether to publish `@malloy-publisher/skills` this release.
 *
 * - `npmOk`: whether the registry answered dist-tags.latest and its gitHead at
 *   all. false aborts — a registry that did not answer is not "unchanged".
 * - `latest`: npm's `dist-tags.latest` version string.
 * - `gitHead`: the gitHead npm recorded for that published version.
 * - `objectPresent`: whether `gitHead` is a commit reachable from this
 *   checkout (`git cat-file -e`). A shallow checkout or a rewritten history
 *   can make it not, and there is then nothing to diff against.
 * - `changed`: "changed" | "unchanged" | "error", the three outcomes of
 *   `git diff --quiet <gitHead> HEAD -- <watched paths>`.
 */
export function decideSkills({ npmOk, latest, gitHead, objectPresent, changed }) {
  if (!npmOk) {
    return abort(
      "npm did not answer for @malloy-publisher/skills (latest or its gitHead), so publish/skip cannot be decided",
    );
  }
  if (!PLAIN_VERSION.test(latest)) {
    return abort(
      `npm latest "${sanitizeForLine(latest)}" for @malloy-publisher/skills is not major.minor.patch`,
    );
  }
  if (!gitHead || !GIT_HEAD.test(gitHead)) {
    return abort(
      `npm's gitHead for @malloy-publisher/skills@${latest} is "${sanitizeForLine(gitHead)}", not 40 hex characters, so there is nothing to diff against`,
    );
  }
  if (!objectPresent) {
    return abort(
      `commit ${gitHead} (the gitHead npm recorded for @malloy-publisher/skills@${latest}) is not present in this checkout ` +
        `(a version published by hand from a workstation, outside this checkout's history, can do this). ` +
        `Recovery: hand-dispatch skills-npm.yml on main to publish a CI-built version, which records a gitHead this checkout can diff against next time.`,
    );
  }
  if (changed === "error") {
    return abort(
      `git diff against ${gitHead} errored, so whether @malloy-publisher/skills content changed is unknown`,
    );
  }
  if (changed !== "changed" && changed !== "unchanged") {
    return abort(
      `unrecognised diff result "${sanitizeForLine(changed)}" for @malloy-publisher/skills`,
    );
  }
  if (changed === "unchanged") {
    return {
      action: "skip",
      reason: `no published content changed since npm latest's gitHead ${gitHead}`,
    };
  }
  return {
    action: "publish",
    version: nextPatch(latest),
    reason: `published content changed since npm latest's gitHead ${gitHead}`,
  };
}

/**
 * Decide whether to publish `@malloy-publisher/create-malloy-package` this
 * release. Unlike skills, this one bakes the server's npm `latest` into the
 * workspaces it scaffolds, so it publishes on every non-prerelease release
 * UNLESS it is a re-run where its published `publisherServer` field already
 * equals this release's version — meaning an earlier attempt of this same
 * release already got it out.
 *
 * The pin matching alone is not enough to skip on, though: a hand dispatch or
 * an earlier attempt of this release can have already published a scaffolder
 * pinned to it, and then scaffolder content can land on `main` before this
 * run (or a retry of it). Skipping unconditionally on a pin match would ship
 * that new content nowhere. So a pin match only skips when the scaffolder's
 * own published content is ALSO unchanged since npm latest's `gitHead` — the
 * same content diff skills runs, just with a different verdict on every other
 * outcome: an extra scaffolder patch is harmless (it publishes every release
 * anyway), so a diff that reads "changed", or a `gitHead` this checkout
 * cannot diff against at all (empty, non-hex, or a commit not reachable
 * here), means PUBLISH, not abort.
 *
 * - `publisherServer`: npm's `publisherServer` field on the manifest for
 *   `latest` (empty string/undefined when the field is not set at all).
 * - `release`: the version this release is stamping (`NEW_VERSION`).
 * - `gitHead`: the gitHead npm recorded for `latest`.
 * - `objectPresent`: whether `gitHead` is a commit reachable from this
 *   checkout (`git cat-file -e`).
 * - `changed`: "changed" | "unchanged" | "error", the diff of the
 *   scaffolder's watched paths against `gitHead`.
 */
export function decideScaffolder({
  npmOk,
  latest,
  publisherServer,
  release,
  gitHead,
  objectPresent,
  changed,
}) {
  if (!npmOk) {
    return abort(
      "npm did not answer for @malloy-publisher/create-malloy-package's latest, so publish/skip cannot be decided",
    );
  }
  if (!PLAIN_VERSION.test(latest)) {
    return abort(
      `npm latest "${sanitizeForLine(latest)}" for @malloy-publisher/create-malloy-package is not major.minor.patch`,
    );
  }
  if (publisherServer && publisherServer === release) {
    const gitHeadUsable = gitHead && GIT_HEAD.test(gitHead) && objectPresent;
    if (gitHeadUsable && changed === "unchanged") {
      return {
        action: "skip",
        reason: `latest's publisherServer already equals ${release} (a re-run of this release), and no published content changed since npm latest's gitHead ${gitHead}`,
      };
    }
    let why;
    if (!gitHeadUsable) {
      why = `its gitHead "${sanitizeForLine(gitHead)}" cannot be diffed against (not 40 hex characters, or not present in this checkout)`;
    } else if (changed === "changed") {
      why = `published content changed since npm latest's gitHead ${gitHead}`;
    } else {
      why = `the diff against gitHead ${gitHead} errored, so whether content changed is unknown`;
    }
    return {
      action: "publish",
      version: nextPatch(latest),
      reason: `latest's publisherServer already equals ${release} (a re-run), but ${why}; publishing an extra patch is harmless`,
    };
  }
  return {
    action: "publish",
    version: nextPatch(latest),
    reason: publisherServer
      ? `latest's publisherServer is ${publisherServer}, not this release's ${release}`
      : "latest has no publisherServer field yet",
  };
}

/**
 * A computed version already published is an ERROR, never a skip: it means
 * this pipeline's own `nextPatch` math landed on something npm already has,
 * which should be impossible given `latest` was just read from the same
 * registry, and publishing over it is not an option npm allows anyway.
 * `status` mirrors `registry_has` in publish-packages.sh: "published",
 * "free", or "unknown" (the registry did not give a usable answer).
 */
export function checkFree({ status, name, version }) {
  switch (status) {
    case "free":
      return null;
    case "published":
      return abort(
        `${name}@${version} is already on npm; refusing to publish over it (the version this release computed should have been free). ` +
          `This can happen if a CDN edge was still serving a stale \`latest\` when this was checked. Recovery: re-run this job after a few minutes.`,
      );
    default:
      return abort(
        `could not confirm ${name}@${version} is free on npm (registry did not answer)`,
      );
  }
}

// ---- CLI ----------------------------------------------------------------
//
// One machine-readable line on stdout, one of:
//   publish <version>
//   skip
//   abort <reason>
// `abort` also exits with ABORT_EXIT_CODE, distinct from the usage-error exit
// code below, so a caller under `set -e` can branch on the outcome with
// `out="$(...)" && rc=0 || rc=$?` the way the rest of publish-packages.sh does.
const ABORT_EXIT_CODE = 3;
const USAGE_EXIT_CODE = 1;

function usageFail(message) {
  console.error(`independent-version: ${message}`);
  process.exit(USAGE_EXIT_CODE);
}

function boolEnv(name) {
  const raw = process.env[name];
  return raw === "1" || raw === "true";
}

function reportDecision(decision) {
  if (decision.action === "abort") {
    console.log(`abort ${decision.reason}`);
    process.exit(ABORT_EXIT_CODE);
  }
  if (decision.action === "publish") {
    console.log(`publish ${decision.version}`);
    console.error(decision.reason);
    process.exit(0);
  }
  console.log("skip");
  console.error(decision.reason);
  process.exit(0);
}

function main(argv) {
  const [command, ...rest] = argv;

  switch (command) {
    case "next": {
      const [latest] = rest;
      if (!latest) usageFail("usage: independent-version.mjs next <latest>");
      let version;
      try {
        version = nextPatch(latest);
      } catch (error) {
        usageFail(error.message);
      }
      console.log(version);
      return;
    }

    case "decide-skills": {
      const decision = decideSkills({
        npmOk: boolEnv("NPM_OK"),
        latest: process.env.LATEST ?? "",
        gitHead: process.env.GIT_HEAD ?? "",
        objectPresent: boolEnv("OBJECT_PRESENT"),
        changed: process.env.CHANGED ?? "",
      });
      reportDecision(decision);
      return;
    }

    case "decide-scaffolder": {
      const decision = decideScaffolder({
        npmOk: boolEnv("NPM_OK"),
        latest: process.env.LATEST ?? "",
        publisherServer: process.env.PUBLISHER_SERVER ?? "",
        release: process.env.RELEASE ?? "",
        gitHead: process.env.GIT_HEAD ?? "",
        objectPresent: boolEnv("OBJECT_PRESENT"),
        changed: process.env.CHANGED ?? "",
      });
      reportDecision(decision);
      return;
    }

    case "check-free": {
      const status = process.env.STATUS ?? "";
      const name = process.env.NAME ?? "";
      const version = process.env.VERSION ?? "";
      const result = checkFree({ status, name, version });
      if (result) {
        console.log(`abort ${result.reason}`);
        process.exit(ABORT_EXIT_CODE);
      }
      console.log("ok");
      return;
    }

    default:
      usageFail(
        `unknown command "${command ?? ""}"; expected next, decide-skills, decide-scaffolder, or check-free`,
      );
  }
}

// Only run the CLI when executed directly, so the spec can import the pure
// functions above with no side effects. Resolved through realpath on both
// sides: a caller invoking this file through a symlink (as the release
// workflow can, depending on checkout layout) would otherwise compare a
// symlink path against import.meta.url's real one and never match, silently
// turning every dispatch into a no-op CLI.
if (
  pathToFileURL(realpathSync(process.argv[1])).href ===
  pathToFileURL(realpathSync(fileURLToPath(import.meta.url))).href
) {
  main(process.argv.slice(2));
}
