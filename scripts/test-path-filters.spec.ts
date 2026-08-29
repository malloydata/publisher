// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

// Guards the path filter every `bun test` script in this repo passes.
//
// `bun test <arg>` does not take a directory. Its own --help says it runs "all
// test files with "foo" or "bar" in the file name", so a bare argument is a
// substring match against the whole path. `packages/server`'s integration
// script passed `tests`, and once anything boots a server the gitignored
// `publisher_data/` holds a copy of the storefront example, its own `tests/`
// directory included. Those three files were collected into the server's
// integration figure: 40 files where the server has 37, and 72 tests that
// measure an example rather than the server. They already run as
// `test:examples`, from repo source, under a step that fails on a skip.
//
// The wrong total is the smaller half of it. The copy under `publisher_data/`
// is a snapshot, so it drifts from repo source, and a drifted copy fails
// correctly and reports the SERVER's suite red for a reason outside the server.
// That happened: a vendored file stamped 0.0.427 against 0.0.432 everywhere
// else.
//
// It reaches CI too, which is easy to miss because a fresh checkout has no
// `publisher_data/` and collects the right 37 files. The integration step in
// cross-platform-tests.yml retries up to three times, and attempt 1 boots a
// server, so attempts 2 and 3 measure something attempt 1 did not.
//
// The fix is a leading `./`, which makes the argument a path instead of a
// substring. This file is the part that keeps it fixed.
//
// Three things this deliberately does not do.
//
// It does not walk the filesystem. `publisher_data/` is a runtime directory
// that a fresh checkout does not have, so a walk would find nothing to collect
// and pass on every CI run while saying nothing at all. That is the same
// "cannot tell 'I could not run' from 'everything passed'" shape the example
// step in cross-platform-tests.yml already documents. The paths below are
// synthetic, so this reaches the same verdict on a clean runner and on a
// machine that has booted a server sixty times.
//
// It does not assert a file count. A count has to be raised by hand every time
// someone adds a test file, and an assertion people routinely edit to make
// green is not an assertion.
//
// It does not check that the argument starts with "./". That would test the
// fix. `collects()` reproduces the matching rule instead, so what is asserted
// is the thing that actually matters: which files an argument pulls in.

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

/**
 * Does `filter` collect `filePath`, under bun's rule?
 *
 * Measured against bun 1.3.13 rather than taken off the docs, because the
 * interesting case is not in them: an argument beginning "./" anchors to the
 * run's cwd and matches by path prefix, and anything else is a plain substring
 * of the path whether or not it contains a slash. `src/tests` collects
 * `nested/src/tests/b.test.ts`; `./src/tests` does not.
 *
 * `filePath` is cwd-relative and POSIX-separated, which is what bun compares.
 */
function collects(filter: string, filePath: string): boolean {
  if (filter.startsWith("./")) {
    const prefix = filter.slice(2).replace(/\/+$/, "");
    return filePath === prefix || filePath.startsWith(`${prefix}/`);
  }
  return filePath.includes(filter);
}

// Flags that consume the next argv token, from `bun test --help`. Everything
// else beginning with "-" is treated as a boolean, so an unrecognised
// value-taking flag leaves its value looking like a path filter. That fails
// loudly here rather than passing quietly, which is the direction to be wrong
// in: a value like "200000" collects nothing, so the "collects its own
// directory" assertion below catches it.
const VALUE_FLAGS = new Set([
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
 * is read as well as the `run: |` block form the repo uses today. Same reason
 * as the env prefix: a form this did not recognise would go unchecked without
 * saying so.
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
 * no assertion fired for it and the run still reported a clean sweep. That is
 * this file's own version of the bug it guards.
 */
function commandSegments(command: string): string[] {
  return command.split(/&&|\|\||;/);
}

/**
 * Tokens of one command, quotes respected and stopping at the first redirection
 * or pipe.
 *
 * Both halves matter for commands already idiomatic in this repo. A run piped
 * into `tee` (cross-platform-tests.yml does this) would otherwise contribute
 * `2>&1`, `|`, `tee` and a filename as "path filters", and `-t "explicit false"`
 * (documented in the SDK's test README) would contribute a filter named
 * `false"`. Both fail loudly rather than silently, but they fail on a legitimate
 * edit with an error naming nothing real.
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
      if (existsSync(path.join(REPO_ROOT, dir, "package.json"))) dirs.push(dir);
    }
  }
  return dirs;
}

/** Every `bun test` invocation in the workspace, one entry per command. */
function invocations(): Invocation[] {
  const dirs = packageDirs();

  const found: Invocation[] = [];
  for (const pkg of dirs) {
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

const ALL = [...invocations(), ...workflowInvocations()];
const label = (i: Invocation) => `${i.pkg} ${i.script}`;

describe("reading a bun test command", () => {
  // `--timeout 200000` puts its value in its own argv slot, so a parser that
  // only skipped tokens beginning with "-" would read 200000 as a path filter.
  it("skips the value of a space-separated flag", () => {
    expect(
      filtersOf("bun test --timeout 200000 ./tests --max-workers=1"),
    ).toEqual(["./tests"]);
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

// Everything above reasons about bun's matching rule from a model of it. This
// checks the model against the real thing, on whatever platform the run happens
// to be on.
//
// It exists for Windows specifically. The rest of this file is synthetic by
// design, and synthetic cannot tell the difference between "./x anchors here"
// and "./x is not recognised here, so it fell back to a substring and matched
// anyway". The second would mean the anchoring silently does not apply on the
// one platform where the retrying integration step populates `publisher_data/`
// between attempts, and CI would stay green while the fix did nothing. A model
// cannot answer that; running bun can.
describe("the model against real bun", () => {
  it("matches what bun collects on this platform", () => {
    const dir = mkdtempSync(path.join(tmpdir(), "bun-test-filter-"));
    try {
      for (const rel of ["tests", path.join("nested", "tests")]) {
        mkdirSync(path.join(dir, rel), { recursive: true });
        writeFileSync(
          path.join(dir, rel, "a.test.ts"),
          'import{test,expect}from"bun:test";test("t",()=>{expect(1).toBe(1)});\n',
        );
      }

      const collected = (filter: string) => {
        const run = Bun.spawnSync({
          cmd: [process.execPath, "test", filter],
          cwd: dir,
          stdout: "pipe",
          stderr: "pipe",
        });
        const out = run.stdout.toString() + run.stderr.toString();
        const files = out.match(/across (\d+) files?/);
        return files ? Number(files[1]) : 0;
      };

      // The bug, and the fix, as bun itself reports them.
      expect(collected("tests")).toBe(2);
      expect(collected("./tests")).toBe(1);

      // And the model agrees about the file that separates them.
      const nested = "nested/tests/a.test.ts";
      expect(collects("tests", nested)).toBe(true);
      expect(collects("./tests", nested)).toBe(false);
    } finally {
      rmSync(dir, { recursive: true, force: true });
    }
  });
});

describe("bun test path filters", () => {
  // Non-vacuity. Everything below is a loop over what the parser found, so a
  // parser that silently found nothing would report a clean sweep. Named
  // scripts rather than a count: a rename should stop this file and make
  // someone look, where a count only has to be nudged upward.
  it("parses the scripts it exists to check", () => {
    expect(ALL.length).toBeGreaterThan(0);
    const labels = ALL.map(label);
    expect(labels).toContain("packages/server test:integration");
    expect(labels).toContain("packages/server test:unit");
    expect(labels).toContain(". test:scripts");
    // The workflow scan is a regex over plain lines, so it is the part most
    // likely to quietly stop matching. Asserted as a floor of one rather than
    // by file and line, which move whenever a step is edited.
    expect(
      ALL.filter((i) => i.pkg.startsWith(".github/")).length,
    ).toBeGreaterThan(0);
  });

  // The regression, in the shape it actually happened in.
  it("the server's integration filter leaves the storefront copies alone", () => {
    const server = ALL.find(
      (i) => i.pkg === "packages/server" && i.script === "test:integration",
    );
    expect(server).toBeDefined();
    expect(server!.filters.length).toBe(1);

    for (const name of ["conformance", "controls", "format"]) {
      const copy = `publisher_data/examples/storefront/tests/${name}.test.mjs`;
      expect(collects(server!.filters[0], copy)).toBe(false);
    }
    expect(
      collects(server!.filters[0], "tests/integration/mcp/a.spec.ts"),
    ).toBe(true);
  });

  // The general rule, applied to every filter in the workspace. The
  // `publisher_data/` path here is constructed rather than observed: the
  // question is not whether a runtime copy of this particular directory exists
  // today, it is whether the filter would collect one if it did.
  describe.each(ALL.filter((i) => i.filters.length > 0))(
    "$pkg $script",
    (invocation: Invocation) => {
      for (const filter of invocation.filters) {
        const target = filter.replace(/^\.\//, "").replace(/\/+$/, "");
        // Two of these name a file, not a directory. Appending a child segment
        // to a file path asserts about something that cannot exist, so the
        // positive leg would pass without meaning anything.
        const own = /\.(test|spec)\.[cm]?[jt]sx?$/.test(target)
          ? target
          : `${target}/example.test.ts`;

        it(`${filter} collects its own path`, () => {
          expect(collects(filter, own)).toBe(true);
        });

        it(`${filter} does not reach into publisher_data`, () => {
          const copy = `publisher_data/examples/storefront/${own}`;
          expect(collects(filter, copy)).toBe(false);
        });
      }
    },
  );

  // A `bun test` with no filter collects everything under its package, which is
  // the same exposure by another route. Only the SDK does it, and it has no
  // runtime directory for anything to be copied into. This is an allowlist
  // rather than a count on purpose: it goes stale exactly when someone adds an
  // unfiltered `bun test` somewhere new, which is when a person should look.
  it("only the SDK runs bun test without a path filter", () => {
    const unfiltered = ALL.filter((i) => i.filters.length === 0).map(label);
    expect(unfiltered).toEqual(["packages/sdk test"]);
  });
});
