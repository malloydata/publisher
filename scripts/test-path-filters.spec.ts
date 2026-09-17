// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

// Guards the path filter every `bun test` script in this repo passes.
//
// `bun test <arg>` does not take a directory. Its own --help says it runs "all
// test files with "foo" or "bar" in the file name", so a bare argument is a
// substring match against the whole path. `packages/server`'s integration
// script passed `tests`, and once anything boots a server the gitignored
// `publisher_data/` holds a copy of the storefront example, its own `tests/`
// directory included. Those files were collected into the server's figure: 40
// files where the server has 37, and 72 tests that measure an example rather
// than the server. They already run as `test:examples`, from repo source.
//
// The wrong total is the smaller half of it. The copy under `publisher_data/`
// is a snapshot, so it drifts from repo source, and a drifted copy fails
// correctly and reports the SERVER's suite red for a reason outside the server.
//
// It reaches CI, which is easy to miss because a fresh checkout has no
// `publisher_data/` and collects the right files. The integration step in
// cross-platform-tests.yml retries up to three times, and attempt 1 boots a
// server, so attempts 2 and 3 measure something attempt 1 did not.
//
// The fix is a leading `./`, which makes the argument a path instead of a
// substring. This file is the part that keeps it fixed.
//
// Two things it deliberately does not do.
//
// It does not walk the repo for `publisher_data/`. That is a runtime directory
// a fresh checkout does not have, so a walk would find nothing to collect and
// pass while saying nothing. Every tree below is built in a scratch directory
// instead, so this reaches the same verdict on a clean runner and on a machine
// that has booted a server sixty times.
//
// It does not model bun's matching rule. An earlier version of this file
// reimplemented the rule as a `collects()` predicate and asserted filters
// against that, which made the central "does not reach into publisher_data"
// check a restatement of the predicate's own definition rather than a fact
// about bun. Every anchoring assertion here shells out to the bun running the
// suite. That also covers the case a model cannot reach: a platform where `./`
// is not honoured would fall back to a substring and match anyway, so the
// anchoring would silently stop applying while CI stayed green.

import { describe, expect, it } from "bun:test";
import {
  existsSync,
  mkdirSync,
  mkdtempSync,
  readFileSync,
  readdirSync,
  rmSync,
  writeFileSync,
} from "node:fs";
import { tmpdir } from "node:os";
import path from "node:path";

const REPO_ROOT = path.resolve(import.meta.dir, "..");

// Flags whose value is a separate argv token, from `bun test --help`.
// Everything else beginning with "-" is read as a boolean.
//
// `--max-workers` is the exception, and it is here because two integration
// scripts pass it even though `bun test --help` does not list it. Measured
// rather than assumed: with a `file1.test.ts` in the tree,
// `bun test ./tests --max-workers 1` collects one file, not two — bun swallows
// the value instead of reading it as a filter. A parser that did not would
// report a path filter the run never had.
const VALUE_FLAGS = new Set([
  "--max-workers",
  "--timeout",
  "--rerun-each",
  "--retry",
  "-t",
  "--test-name-pattern",
  "--reporter",
  "--reporter-outfile",
  "--max-concurrency",
  "--path-ignore-patterns",
  "--coverage-reporter",
  "--coverage-dir",
  "--seed",
  "--shard",
  "--parallel-delay",
]);

// `--bail`, `--changed` and `--parallel` are deliberately NOT above. Their value
// is optional, so bun reads `bun test --bail tests` as bail-with-no-value plus
// the filter `tests`. Listing them would make this eat a real filter, which is
// the silent direction: with one positional it fails somewhere unrelated, with
// two the first goes unchecked.

interface Invocation {
  /** Repo-relative directory the script runs in, "." for the root package. */
  pkg: string;
  script: string;
  filters: string[];
}

/**
 * Strip leading `VAR=value` assignments, so `NODE_ENV=test bun test ./src` is
 * recognised as a `bun test`. Skipping it instead would be this file's own
 * version of the bug it guards: quietly checking less than it appears to.
 */
function withoutEnvPrefix(segment: string): string {
  return segment.trim().replace(/^(?:[A-Za-z_][A-Za-z0-9_]*=\S*\s+)+/, "");
}

/**
 * Strip a workflow step's `- run:` lead-in, so a one-line `run: bun test ./src`
 * is read as well as the `run: |` block form the repo uses today.
 */
function withoutRunPrefix(line: string): string {
  return line.trim().replace(/^-?\s*run:\s*/, "");
}

/** Is this command segment a `bun test ...`? */
function isBunTest(segment: string): boolean {
  return /^bun\s+test(\s|$)/.test(withoutEnvPrefix(segment));
}

/**
 * The segments of a shell command that a `bun test` could start.
 *
 * Splitting on `&&` alone was wrong, and wrong in the silent direction: a
 * script joined with `;` or `||` disappeared from the parsed set entirely, so
 * no assertion fired for it and the run still reported a clean sweep.
 */
function commandSegments(command: string): string[] {
  return command.split(/&&|\|\||;/);
}

/**
 * Tokens of one command, quotes respected and stopping at the first redirection
 * or pipe.
 *
 * Both halves matter for commands already idiomatic in this repo. A run piped
 * into `tee` would otherwise contribute `2>&1`, `|`, `tee` and a filename as
 * "path filters", and `-t "explicit false"` (documented in the SDK's test
 * README) would contribute a filter named `false"`.
 */
function tokenize(command: string): string[] {
  const tokens: string[] = [];
  const pattern = /"([^"]*)"|'([^']*)'|(\S+)/g;
  let match: RegExpExecArray | null;
  while ((match = pattern.exec(command)) !== null) {
    if (/^(?:\d*[<>]|\||&)/.test(match[0])) break;
    tokens.push(match[1] ?? match[2] ?? match[3]);
  }
  return tokens;
}

/** The positional path filters of one `bun test ...` command segment. */
function filtersOf(segment: string): string[] {
  const tokens = tokenize(withoutEnvPrefix(segment)).slice(2);
  const filters: string[] = [];
  for (let i = 0; i < tokens.length; i++) {
    const token = tokens[i];
    if (token.startsWith("-")) {
      if (VALUE_FLAGS.has(token)) i++;
      continue;
    }
    filters.push(token);
  }
  return filters;
}

/**
 * The root package plus every workspace member, read from the root manifest's
 * `workspaces` globs rather than assuming "packages/*". A new workspace that
 * this did not know to look in would be unchecked without saying so, which is
 * the failure this whole file is about.
 */
function packageDirs(): string[] {
  const root = JSON.parse(
    readFileSync(path.join(REPO_ROOT, "package.json"), "utf8"),
  );
  const patterns: string[] = root.workspaces ?? [];
  const dirs = ["."];
  for (const pattern of patterns) {
    const [prefix, star] = pattern.split("/*");
    const candidates =
      star === undefined ? [prefix] : readdirSync(path.join(REPO_ROOT, prefix));
    for (const entry of candidates) {
      const dir = star === undefined ? prefix : `${prefix}/${entry}`;
      if (existsSync(path.join(REPO_ROOT, dir, "package.json"))) {
        dirs.push(dir);
      }
    }
  }
  return dirs;
}

/** Every `bun test` invocation in the workspace, one entry per command. */
function invocations(): Invocation[] {
  const found: Invocation[] = [];
  for (const pkg of packageDirs()) {
    const manifest = JSON.parse(
      readFileSync(path.join(REPO_ROOT, pkg, "package.json"), "utf8"),
    );
    for (const [script, command] of Object.entries(
      (manifest.scripts ?? {}) as Record<string, string>,
    )) {
      for (const segment of commandSegments(command)) {
        if (!isBunTest(segment)) continue;
        found.push({ pkg, script, filters: filtersOf(segment) });
      }
    }
  }
  return found;
}

/**
 * `bun test` also gets invoked straight out of workflow steps, which no
 * package.json knows about. Those runs carry real credentials, so an extra file
 * collected there is a worse surprise than most. A line scan rather than a YAML
 * parse: the steps are `run:` blocks, so the commands are plain shell lines by
 * the time they matter.
 */
function workflowInvocations(): Invocation[] {
  const dir = path.join(REPO_ROOT, ".github", "workflows");
  if (!existsSync(dir)) return [];
  const found: Invocation[] = [];
  for (const file of readdirSync(dir)) {
    if (!/\.ya?ml$/.test(file)) continue;
    const lines = readFileSync(path.join(dir, file), "utf8").split("\n");
    lines.forEach((line, index) => {
      for (const segment of commandSegments(withoutRunPrefix(line))) {
        if (!isBunTest(segment)) continue;
        found.push({
          pkg: `.github/workflows/${file}`,
          script: `line ${index + 1}`,
          filters: filtersOf(segment),
        });
      }
    });
  }
  return found;
}

const SCRIPT_INVOCATIONS = invocations();
const WORKFLOW_INVOCATIONS = workflowInvocations();
const ALL = [...SCRIPT_INVOCATIONS, ...WORKFLOW_INVOCATIONS];

/**
 * The file a filter is meant to collect, relative to the run's cwd.
 *
 * Some filters name a file rather than a directory. Appending a child segment
 * to a file path names something that cannot exist, so the positive leg would
 * pass without meaning anything.
 */
function ownPathFor(filter: string): string {
  const target = filter.replace(/^\.\//, "").replace(/\/+$/, "");
  return /\.(test|spec)\.[cm]?[jt]sx?$/.test(target)
    ? target
    : `${target}/example.test.ts`;
}

const TEST_FILE =
  'import{test,expect}from"bun:test";test("t",()=>{expect(1).toBe(1)});\n';

/**
 * How many files the bun running this suite collects for `filter`, in a scratch
 * tree holding the filter's own target and a `publisher_data/` copy of it.
 *
 * This is the shape the bug actually took: the copy sits under a gitignored
 * runtime directory, so its path contains the filter as a substring while not
 * being under it.
 */
function filesCollected(filter: string, own: string): number {
  const dir = mkdtempSync(path.join(tmpdir(), "bun-test-filter-"));
  try {
    for (const rel of [own, path.posix.join("publisher_data", own)]) {
      const full = path.join(dir, rel);
      mkdirSync(path.dirname(full), { recursive: true });
      writeFileSync(full, TEST_FILE);
    }
    const run = Bun.spawnSync({
      cmd: [process.execPath, "test", filter],
      cwd: dir,
      stdout: "pipe",
      stderr: "pipe",
    });
    const out = run.stdout.toString() + run.stderr.toString();
    const match = out.match(/across (\d+) files?/);
    if (!match) {
      throw new Error(`could not read a file count from bun:\n${out}`);
    }
    return Number(match[1]);
  } finally {
    rmSync(dir, { recursive: true, force: true });
  }
}

describe("reading a bun test command", () => {
  // `--timeout 200000` puts its value in its own argv slot, so a parser that
  // only skipped tokens beginning with "-" would read 200000 as a path filter.
  it("skips the value of a space-separated flag", () => {
    expect(
      filtersOf("bun test --timeout 200000 ./tests --max-workers=1"),
    ).toEqual(["./tests"]);
  });

  it("skips a space-separated value for a flag bun does not document", () => {
    // Both forms appear the same to this parser; only the space form can eat a
    // token, and bun does not hand that token to the filter list either.
    expect(filtersOf("bun test ./tests --max-workers=1")).toEqual(["./tests"]);
    expect(filtersOf("bun test ./tests --max-workers 1")).toEqual(["./tests"]);
  });

  it("reads a command with an env prefix", () => {
    expect(withoutEnvPrefix("NODE_ENV=test bun test ./src")).toBe(
      "bun test ./src",
    );
    expect(filtersOf("NODE_ENV=test bun test ./src")).toEqual(["./src"]);
  });

  it("leaves a command with no env prefix alone", () => {
    expect(withoutEnvPrefix(" bun test ./src ")).toBe("bun test ./src");
  });

  it("splits on every shell separator, not just &&", () => {
    // A `;`- or `||`-joined script used to vanish from the parsed set with no
    // assertion firing, which reported a clean sweep while checking less.
    expect(commandSegments("bun run copy-skills; bun test ./src").length).toBe(
      2,
    );
    expect(commandSegments("a || bun test ./src").length).toBe(2);
    expect(commandSegments("a && bun test ./src").length).toBe(2);
  });

  it("stops at a redirection or pipe", () => {
    expect(filtersOf("bun test ./tests 2>&1 | tee out.log")).toEqual([
      "./tests",
    ]);
    expect(filtersOf("bun test ./tests > out.log")).toEqual(["./tests"]);
  });

  it("keeps a quoted flag value in one token", () => {
    expect(filtersOf('bun test -t "explicit false" ./src')).toEqual(["./src"]);
  });

  it("does not eat the filter after an optional-value flag", () => {
    // bun reads these as flag-with-no-value plus a filter.
    expect(filtersOf("bun test --bail ./tests")).toEqual(["./tests"]);
    expect(filtersOf("bun test --parallel ./tests")).toEqual(["./tests"]);
    expect(filtersOf("bun test --changed ./tests")).toEqual(["./tests"]);
  });

  it("reads a workflow step written on one line", () => {
    expect(withoutRunPrefix("  - run: bun test ./src")).toBe("bun test ./src");
    expect(withoutRunPrefix("          bun test ./src")).toBe("bun test ./src");
  });
});

// Everything below loops over what the parser found, so a parser that quietly
// found less would report a clean sweep over the remainder. These pin the roster
// itself, by name: a package whose scripts stop being read, a script that is
// renamed, or a workflow whose `run:` lines stop matching all fail here and make
// someone look. Names rather than a count, because a count only has to be
// nudged upward to go green again.
describe("the roster this file checks", () => {
  it("reads every bun test script in the workspace", () => {
    expect(
      SCRIPT_INVOCATIONS.map((i) => `${i.pkg} ${i.script}`).sort(),
    ).toEqual([
      ". test:hammer",
      ". test:scripts",
      "packages/cli test",
      "packages/cli test:integration",
      "packages/create-malloy-package test",
      "packages/create-malloy-package test:e2e",
      "packages/sdk test",
      "packages/server test:integration",
      "packages/server test:unit",
      "packages/skills test",
    ]);
  });

  it("reads every workflow that runs bun test directly", () => {
    const files = [...new Set(WORKFLOW_INVOCATIONS.map((i) => i.pkg))].sort();
    expect(files).toEqual([
      ".github/workflows/connection-integration-tests.yml",
    ]);
  });
});

// The property, checked against the bun running this suite rather than a model
// of it. Each case builds a scratch tree holding the filter's own target and a
// `publisher_data/` copy at the same relative path, then runs bun twice: once
// with the filter as written, once with the anchor stripped. The unanchored leg
// is what makes the anchored one mean something — it demonstrates, per filter,
// that the `./` is the thing doing the work and not an accident of the tree.
describe("every path filter is anchored", () => {
  const filtered = ALL.filter((i) => i.filters.length > 0);

  for (const invocation of filtered) {
    for (const filter of invocation.filters) {
      const own = ownPathFor(filter);
      const label = `${invocation.pkg} ${invocation.script}: ${filter}`;

      it(`${label} collects its own path and not the publisher_data copy`, () => {
        expect(filesCollected(filter, own)).toBe(1);
      });

      it(`${label} would collect the copy unanchored`, () => {
        // Non-vacuity for the assertion above. If this ever returns 1, the
        // tree stopped reproducing the bug and the anchored check is
        // passing for a reason that has nothing to do with the anchor.
        expect(filesCollected(filter.replace(/^\.\//, ""), own)).toBe(2);
      });
    }
  }

  it("checked at least one filter per package that has one", () => {
    // The loop above is generated from `filtered`, so an empty roster would
    // register no cases at all and the file would report green.
    expect(filtered.length).toBeGreaterThanOrEqual(10);
  });
});

// A `bun test` with no filter collects everything under its package, which is
// the same exposure by another route. Only the SDK does it, and it has no
// runtime directory for anything to be copied into. An allowlist rather than a
// count on purpose: it goes stale exactly when someone adds an unfiltered
// `bun test` somewhere new, which is when a person should look.
it("only the SDK runs bun test without a path filter", () => {
  const unfiltered = ALL.filter((i) => i.filters.length === 0).map(
    (i) => `${i.pkg} ${i.script}`,
  );
  expect(unfiltered).toEqual(["packages/sdk test"]);
});
