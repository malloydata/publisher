#!/usr/bin/env node
// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

// Set the top-level "version" field of one or more package.json files.
//
//   node scripts/set-version.mjs <version> <file>...
//
// Prints the number of files whose bytes actually changed, so a caller can tell
// "wrote the version" from "the version was already that" without diffing. Both
// are success; only a file it could not set is a failure.
//
// This exists because `release.yml` needs it in two jobs now — `prepare`, which
// stamps the release branch, and `gh-release`, which resets `main` on the
// post-release stamp branch — and a shell function cannot be shared between
// jobs, let alone tested. It was lifted verbatim from `prepare`, and the reason
// it is hand-rolled comes with it:
//
//   We can't use `npm version --workspaces` here: this repo uses Bun's
//   `workspace:*` dependency protocol, which npm rejects with
//   EUNSUPPORTEDPROTOCOL. A direct, format-preserving replace avoids npm
//   entirely.
//
// Format-preserving is not a nicety either. `JSON.parse` then `JSON.stringify`
// would reformat every one of these files on every release — reordering nothing
// but rewriting indentation and dropping the trailing newline — and turn a
// one-line version bump into a whole-file diff nobody can review.
//
// Two things it does that the shell function did not, both of which are silent
// failures it hit on the way here:
//
//  - It asserts the replace matched. The original regex was applied and the
//    result written unconditionally, so a package.json whose "version" field it
//    could not find was rewritten byte-identical, `set_version` exited 0, and
//    the release published the OLD version under the new tag.
//  - It validates the version. `prepare` strips a leading `v` and whitespace
//    from the dispatch input and otherwise trusts it, so a stray quote landed
//    inside the JSON string and produced a package.json that no longer parsed.

import { readFileSync, writeFileSync } from "node:fs";

// Deliberately looser than release.yml's `^(\d+)\.(\d+)\.(\d+)$`, which guards
// arithmetic and so must refuse a prerelease. Nothing is computed from the
// version here, and prereleases are a supported release shape (`npm-sdk.yml`
// routes a hyphenated version to the `next` dist-tag), so `0.2.0-rc.1` has to
// pass. What this rejects is anything that could not be a version at all — most
// importantly a `"` or a newline, which would corrupt the JSON rather than
// merely being wrong.
const VERSION = /^\d+\.\d+\.\d+(?:[-+][0-9A-Za-z.+-]+)?$/;

// The first `"version": "..."` in the file. For a package.json that is the
// top-level field, because npm's own convention puts name and version first and
// every file here follows it. Anchoring harder than this would mean parsing, and
// parsing is what loses the formatting.
const FIELD = /("version"\s*:\s*")[^"]*(")/;

function fail(message) {
  console.error(`set-version: ${message}`);
  process.exit(1);
}

const [version, ...files] = process.argv.slice(2);

if (!version || files.length === 0) {
  fail("usage: set-version.mjs <version> <file>...");
}
if (!VERSION.test(version)) {
  fail(
    `refusing to write "${version}": expected major.minor.patch with an optional -prerelease or +build`,
  );
}

let changed = 0;

for (const file of files) {
  let before;
  try {
    before = readFileSync(file, "utf8");
  } catch (error) {
    fail(`could not read ${file}: ${error.message}`);
  }

  if (!FIELD.test(before)) {
    fail(`${file} has no "version" field to set`);
  }

  const after = before.replace(FIELD, `$1${version}$2`);

  // The replace is textual, so confirm the result is still valid JSON and that
  // the field it now carries is the one asked for. This is what catches a
  // "version" that was matched somewhere other than the top level: the file
  // parses, but `.version` is not what we wrote.
  let parsed;
  try {
    parsed = JSON.parse(after);
  } catch (error) {
    fail(
      `setting the version in ${file} produced invalid JSON: ${error.message}`,
    );
  }
  if (parsed.version !== version) {
    fail(
      `${file} still declares version "${parsed.version}" after the replace, so the field that matched was not the top-level one`,
    );
  }

  if (after !== before) {
    try {
      writeFileSync(file, after);
    } catch (error) {
      fail(`could not write ${file}: ${error.message}`);
    }
    changed++;
  }
}

// The count, and nothing else, on stdout. Callers read it.
console.log(String(changed));
