// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

// Shell-level tests for scripts/publish-packages.sh, the release.yml
// `publish-packages` job body. Runs the real script against fake `npm` and
// `gh` executables (first on PATH) that answer from a scripted table, so the
// whole decide/dispatch/poll flow is exercised without a network call or a
// real Actions run. Real `git` and `node` are used throughout: the script's
// git calls (rev-parse, diff, cat-file) run against THIS checkout, so gitHead
// values below are real commits in this repo's history, chosen so the
// skills content diff comes out "changed" or "unchanged" as each test needs.
//
// The fake `gh` answers `api .../commits/main` with this checkout's own HEAD
// (via `git rev-parse HEAD`), which always equals CHECKOUT_SHA, so the
// "main moved" fast-forward compare never triggers here — that guard has no
// registry or git dependency of its own and is exercised by reading the
// script, not by this harness.
//
// python-client's publish is paused in the script; the fake python3 and curl
// keep it skipping (already on PyPI) if that line is restored.

import { afterAll, afterEach, describe, expect, it } from "bun:test";
import {
  chmodSync,
  existsSync,
  mkdtempSync,
  readFileSync,
  rmSync,
  writeFileSync,
} from "node:fs";
import { tmpdir } from "node:os";
import path from "node:path";

const REPO_ROOT = path.join(import.meta.dir, "..");
const SCRIPT = path.join(REPO_ROOT, "scripts", "publish-packages.sh");

// A synthetic empty-tree commit, so the skills diff against HEAD is "changed"
// in any clone, shallow CI checkouts included. It is written to a temp object
// store layered over the repo's, so the repo itself gains no objects.
const GIT_OBJECTS = mkdtempSync(
  path.join(tmpdir(), "publish-packages-objects-"),
);
const GIT_ENV: Record<string, string> = {
  GIT_OBJECT_DIRECTORY: GIT_OBJECTS,
  GIT_ALTERNATE_OBJECT_DIRECTORIES: path.resolve(
    REPO_ROOT,
    Bun.spawnSync(["git", "rev-parse", "--git-path", "objects"], {
      cwd: REPO_ROOT,
    })
      .stdout.toString()
      .trim(),
  ),
};
function gitOut(args: string[], stdin?: string): string {
  const proc = Bun.spawnSync(["git", ...args], {
    cwd: REPO_ROOT,
    env: {
      ...process.env,
      ...GIT_ENV,
      GIT_AUTHOR_NAME: "test",
      GIT_AUTHOR_EMAIL: "test@example.com",
      GIT_COMMITTER_NAME: "test",
      GIT_COMMITTER_EMAIL: "test@example.com",
    },
    stdin: stdin === undefined ? undefined : Buffer.from(stdin),
  });
  if (proc.exitCode !== 0)
    throw new Error(`git ${args.join(" ")}: ${proc.stderr}`);
  return proc.stdout.toString().trim();
}
const OLD_SHA = gitOut(["commit-tree", gitOut(["mktree"], ""), "-m", "old"]);
afterAll(() => rmSync(GIT_OBJECTS, { recursive: true, force: true }));

// This checkout's own HEAD, used as a stand-in npm `gitHead` so the skills
// content diff comes out "unchanged" (diffing HEAD against HEAD is always
// empty).
const HEAD_SHA = Bun.spawnSync(["git", "rev-parse", "HEAD"], { cwd: REPO_ROOT })
  .stdout.toString()
  .trim();

const dirs: string[] = [];

afterEach(() => {
  while (dirs.length) {
    rmSync(dirs.pop()!, { recursive: true, force: true });
  }
});

type NpmAnswer = { exit?: number; stdout?: string; stderr?: string };
type NpmConfig = Record<string, NpmAnswer | NpmAnswer[]>;

// Fake `npm` that only understands `view`. Reads its scripted answers from
// FAKE_NPM_CONFIG (JSON, keyed "<spec> <field>"); an array cycles through
// answers per key across calls (via a small counter file), so a poll loop can
// see "not yet published" and then "published" without a real registry.
const FAKE_NPM = `#!/usr/bin/env node
const fs = require("node:fs");
const args = process.argv.slice(2);
const logPath = process.env.FAKE_NPM_LOG;
if (logPath) fs.appendFileSync(logPath, JSON.stringify(args) + "\\n");
if (args[0] !== "view") {
  console.error("fake npm: unsupported subcommand " + args[0]);
  process.exit(1);
}
const spec = args[1];
const field = args[2];
const key = spec + " " + field;
const config = JSON.parse(fs.readFileSync(process.env.FAKE_NPM_CONFIG, "utf8"));
const entry = config[key];
if (entry === undefined) {
  console.error("fake npm: no scripted answer for \\"" + key + "\\"");
  process.exit(1);
}
let answer;
if (Array.isArray(entry)) {
  const statePath = process.env.FAKE_NPM_STATE;
  let state = {};
  if (statePath && fs.existsSync(statePath)) {
    state = JSON.parse(fs.readFileSync(statePath, "utf8"));
  }
  const idx = state[key] ?? 0;
  answer = entry[Math.min(idx, entry.length - 1)];
  state[key] = idx + 1;
  if (statePath) fs.writeFileSync(statePath, JSON.stringify(state));
} else {
  answer = entry;
}
if (answer.stdout) process.stdout.write(answer.stdout);
if (answer.stderr) process.stderr.write(answer.stderr);
process.exit(answer.exit ?? 0);
`;

// Fake \`gh\`: only \`api repos/.../commits/main\` (answered with this
// checkout's real HEAD, via git) and \`workflow run\` (answered success) are
// needed — the "main moved" compare never fires when live_sha equals
// CHECKOUT_SHA, which it always does here.
const FAKE_GH = `#!/usr/bin/env node
const fs = require("node:fs");
const { execFileSync } = require("node:child_process");
const args = process.argv.slice(2);
const logPath = process.env.FAKE_GH_LOG;
if (logPath) fs.appendFileSync(logPath, JSON.stringify(args) + "\\n");
if (args[0] === "api") {
  const apiPath = args[1] || "";
  if (apiPath.endsWith("/commits/main")) {
    const sha = execFileSync("git", ["rev-parse", "HEAD"], {
      cwd: process.env.FAKE_GH_CWD || process.cwd(),
    })
      .toString()
      .trim();
    process.stdout.write(sha);
    process.exit(0);
  }
  console.error("fake gh: unsupported api path " + apiPath);
  process.exit(1);
}
if (args[0] === "workflow" && args[1] === "run") {
  process.exit(0);
}
console.error("fake gh: unsupported command " + args.join(" "));
process.exit(1);
`;

// Always reports HTTP 200 (published), so registry_has's pypi branch treats
// python-client as already published and publish_pkg skips it.
const FAKE_CURL = `#!/usr/bin/env node
process.stdout.write("200");
process.exit(0);
`;

const FAKE_PYTHON3 = `#!/bin/sh
case "$*" in *"tomllib.load"*) echo "malloy-publisher-sdk 0.1.0 malloy-publisher-sdk" ;; esac
exit 0
`;

function makeFakeBin(): string {
  const dir = mkdtempSync(path.join(tmpdir(), "publish-packages-bin-"));
  dirs.push(dir);
  const write = (name: string, content: string) => {
    const p = path.join(dir, name);
    writeFileSync(p, content);
    chmodSync(p, 0o755);
  };
  write("npm", FAKE_NPM);
  write("gh", FAKE_GH);
  write("curl", FAKE_CURL);
  write("python3", FAKE_PYTHON3);
  return dir;
}

interface RunResult {
  code: number | null;
  stdout: string;
  stderr: string;
  summary: string;
  npmLog: unknown[][];
  ghLog: unknown[][];
}

function runPublishScript(
  npmConfig: NpmConfig,
  envOverrides: Record<string, string> = {},
  scriptPath: string = SCRIPT,
): RunResult {
  const workDir = mkdtempSync(path.join(tmpdir(), "publish-packages-run-"));
  dirs.push(workDir);
  const binDir = makeFakeBin();
  const npmConfigPath = path.join(workDir, "npm-config.json");
  const npmStatePath = path.join(workDir, "npm-state.json");
  const npmLogPath = path.join(workDir, "npm.log");
  const ghLogPath = path.join(workDir, "gh.log");
  const summaryPath = path.join(workDir, "summary.md");
  writeFileSync(npmConfigPath, JSON.stringify(npmConfig));
  writeFileSync(summaryPath, "");

  const env: Record<string, string> = {
    ...process.env,
    ...GIT_ENV,
    PATH: `${binDir}:${process.env.PATH}`,
    FAKE_NPM_CONFIG: npmConfigPath,
    FAKE_NPM_STATE: npmStatePath,
    FAKE_NPM_LOG: npmLogPath,
    FAKE_GH_LOG: ghLogPath,
    FAKE_GH_CWD: REPO_ROOT,
    GITHUB_STEP_SUMMARY: summaryPath,
    GITHUB_REPOSITORY: "malloydata/publisher",
    GH_TOKEN: "fake-token-for-tests",
    NEW_VERSION: "0.9.0",
    POLL_SLEEP: "1",
    POLL_BUDGET_SECONDS: "8",
    POLL_MAX_REGISTRY_ERRORS: "3",
    SERVER_LATEST_BUDGET_SECONDS: "8",
    ...envOverrides,
  } as Record<string, string>;

  // cwd stays REPO_ROOT even when scriptPath is a modified copy elsewhere:
  // the script's own relative path checks (validate_content_paths, the
  // watched-path guard) resolve against the real checkout, which is the
  // point of running a copy rather than a fully synthetic fixture.
  const proc = Bun.spawnSync(["bash", scriptPath], {
    cwd: REPO_ROOT,
    env,
  });

  const readLog = (p: string): unknown[][] =>
    existsSync(p)
      ? readFileSync(p, "utf8")
          .split("\n")
          .filter(Boolean)
          .map((line) => JSON.parse(line))
      : [];

  return {
    code: proc.exitCode,
    stdout: proc.stdout.toString(),
    stderr: proc.stderr.toString(),
    summary: existsSync(summaryPath) ? readFileSync(summaryPath, "utf8") : "",
    npmLog: readLog(npmLogPath),
    ghLog: readLog(ghLogPath),
  };
}

/** Whether the gh log recorded a `workflow run <name> ...` dispatch. */
function dispatchedArgs(
  ghLog: unknown[][],
  workflowFile: string,
): string[] | null {
  for (const call of ghLog) {
    if (
      call[0] === "workflow" &&
      call[1] === "run" &&
      call[2] === workflowFile
    ) {
      return call as string[];
    }
  }
  return null;
}

// Identity wrapper kept only so every scenario below reads as "this is the
// npm config for this run" at the call site.
function baseConfig(overrides: NpmConfig): NpmConfig {
  return { ...overrides };
}

describe("publish-packages.sh", () => {
  it("prerelease NEW_VERSION: exits 0 and dispatches nothing", () => {
    const result = runPublishScript(baseConfig({}), {
      NEW_VERSION: "0.9.0-rc.1",
    });
    expect(result.code).toBe(0);
    expect(result.summary).toContain("Skipped for prerelease version");
    expect(result.ghLog.length).toBe(0);
    expect(result.npmLog.length).toBe(0);
  });

  it("skills content changed: dispatches skills-npm.yml and the scaffolder with the right inputs", () => {
    const result = runPublishScript(
      baseConfig({
        "@malloy-publisher/skills dist-tags.latest": { stdout: "0.1.28" },
        "@malloy-publisher/skills@0.1.28 gitHead": { stdout: OLD_SHA },
        "@malloy-publisher/skills@0.1.29 version": [
          { exit: 1, stdout: "npm ERR! code E404\nnpm ERR! 404 Not Found" },
          { exit: 1, stdout: "npm ERR! code E404\nnpm ERR! 404 Not Found" },
          { stdout: "0.1.29" },
        ],
        "@malloy-publisher/create-malloy-package dist-tags.latest": {
          stdout: "0.0.22",
        },
        "@malloy-publisher/create-malloy-package@0.0.22 publisherServer": {
          stdout: "",
        },
        // Unused by the decision here (the pin doesn't match, so it publishes
        // regardless), but gather_scaffolder_facts always reads it.
        "@malloy-publisher/create-malloy-package@0.0.22 gitHead": {
          stdout: HEAD_SHA,
        },
        "@malloy-publisher/create-malloy-package@0.0.23 version": [
          { exit: 1, stdout: "npm ERR! code E404\nnpm ERR! 404 Not Found" },
          { exit: 1, stdout: "npm ERR! code E404\nnpm ERR! 404 Not Found" },
          { stdout: "0.0.23" },
        ],
        "@malloy-publisher/server dist-tags.latest": { stdout: "0.9.0" },
      }),
    );

    expect(result.stderr).toBe("");
    expect(result.code).toBe(0);

    const skillsRun = dispatchedArgs(result.ghLog, "skills-npm.yml");
    expect(skillsRun).not.toBeNull();
    expect(skillsRun).toEqual(expect.arrayContaining(["-f", "version=0.1.29"]));

    const scaffolderRun = dispatchedArgs(
      result.ghLog,
      "create-malloy-package-npm.yml",
    );
    expect(scaffolderRun).not.toBeNull();
    expect(scaffolderRun).toEqual(
      expect.arrayContaining([
        "-f",
        "version=0.0.23",
        "-f",
        "server_version=0.9.0",
        "-f",
        "skills_version=0.1.29",
      ]),
    );
  }, 15000);

  it("skills content unchanged: skips skills-npm.yml, still dispatches the scaffolder with skills' npm latest", () => {
    const result = runPublishScript(
      baseConfig({
        "@malloy-publisher/skills dist-tags.latest": { stdout: "0.1.28" },
        "@malloy-publisher/skills@0.1.28 gitHead": { stdout: HEAD_SHA },
        "@malloy-publisher/create-malloy-package dist-tags.latest": {
          stdout: "0.0.22",
        },
        "@malloy-publisher/create-malloy-package@0.0.22 publisherServer": {
          stdout: "",
        },
        "@malloy-publisher/create-malloy-package@0.0.23 version": [
          { exit: 1, stdout: "npm ERR! code E404" },
          { exit: 1, stdout: "npm ERR! code E404" },
          { stdout: "0.0.23" },
        ],
        "@malloy-publisher/server dist-tags.latest": { stdout: "0.9.0" },
      }),
    );

    expect(dispatchedArgs(result.ghLog, "skills-npm.yml")).toBeNull();
    expect(result.summary).toContain("skipped: nothing to publish");

    const scaffolderRun = dispatchedArgs(
      result.ghLog,
      "create-malloy-package-npm.yml",
    );
    expect(scaffolderRun).not.toBeNull();
    expect(scaffolderRun).toEqual(
      expect.arrayContaining([
        "-f",
        "version=0.0.23",
        "-f",
        "server_version=0.9.0",
        "-f",
        "skills_version=0.1.28",
      ]),
    );
  }, 15000);

  it("scaffolder publisherServer already equals this release AND content unchanged: skips it, dispatches nothing for it", () => {
    const result = runPublishScript(
      baseConfig({
        "@malloy-publisher/skills dist-tags.latest": { stdout: "0.1.28" },
        "@malloy-publisher/skills@0.1.28 gitHead": { stdout: OLD_SHA },
        "@malloy-publisher/skills@0.1.29 version": [
          { exit: 1, stdout: "npm ERR! code E404" },
          { exit: 1, stdout: "npm ERR! code E404" },
          { stdout: "0.1.29" },
        ],
        "@malloy-publisher/create-malloy-package dist-tags.latest": {
          stdout: "0.0.22",
        },
        // Already pinned to this release's server version: a re-run.
        "@malloy-publisher/create-malloy-package@0.0.22 publisherServer": {
          stdout: "0.9.0",
        },
        // Diffing HEAD against HEAD is always empty, so this reads "unchanged".
        "@malloy-publisher/create-malloy-package@0.0.22 gitHead": {
          stdout: HEAD_SHA,
        },
      }),
    );

    expect(dispatchedArgs(result.ghLog, "skills-npm.yml")).not.toBeNull();
    expect(
      dispatchedArgs(result.ghLog, "create-malloy-package-npm.yml"),
    ).toBeNull();
    expect(result.summary).toContain(
      "@malloy-publisher/create-malloy-package` skipped: nothing to publish",
    );
    // The specific reason (decideScaffolder's stderr) rides on stdout via the
    // job log prefixer, not the step summary.
    expect(result.stdout).toContain(
      "latest's publisherServer already equals 0.9.0",
    );
  }, 15000);

  it("scaffolder publisherServer already equals this release BUT content changed since npm latest's gitHead: publishes anyway", () => {
    // A scaffolder pinning this release was already published (a hand
    // dispatch, or an earlier attempt), and scaffolder content landed on
    // main before this run. Skipping here would leave that new content
    // unshipped for the whole release.
    const result = runPublishScript(
      baseConfig({
        "@malloy-publisher/skills dist-tags.latest": { stdout: "0.1.28" },
        "@malloy-publisher/skills@0.1.28 gitHead": { stdout: HEAD_SHA },
        "@malloy-publisher/create-malloy-package dist-tags.latest": {
          stdout: "0.0.22",
        },
        "@malloy-publisher/create-malloy-package@0.0.22 publisherServer": {
          stdout: "0.9.0",
        },
        // OLD_SHA is an ancestor commit distinct from HEAD, so the diff
        // against it reads "changed".
        "@malloy-publisher/create-malloy-package@0.0.22 gitHead": {
          stdout: OLD_SHA,
        },
        "@malloy-publisher/create-malloy-package@0.0.23 version": [
          { exit: 1, stdout: "npm ERR! code E404" },
          { exit: 1, stdout: "npm ERR! code E404" },
          { stdout: "0.0.23" },
        ],
        "@malloy-publisher/server dist-tags.latest": { stdout: "0.9.0" },
      }),
    );

    const scaffolderRun = dispatchedArgs(
      result.ghLog,
      "create-malloy-package-npm.yml",
    );
    expect(scaffolderRun).not.toBeNull();
    expect(scaffolderRun).toEqual(
      expect.arrayContaining(["-f", "version=0.0.23"]),
    );
  }, 15000);

  it("gitHead empty: aborts, exits non-zero, dispatches nothing", () => {
    const result = runPublishScript(
      baseConfig({
        "@malloy-publisher/skills dist-tags.latest": { stdout: "0.1.28" },
        "@malloy-publisher/skills@0.1.28 gitHead": { stdout: "" },
      }),
    );
    expect(result.code).not.toBe(0);
    expect(result.summary).toMatch(/NOT dispatched/);
    expect(result.summary).toContain("40 hex characters");
    expect(result.ghLog.length).toBe(0);
  });

  it("gitHead not reachable in this checkout: aborts, exits non-zero, dispatches nothing", () => {
    const result = runPublishScript(
      baseConfig({
        "@malloy-publisher/skills dist-tags.latest": { stdout: "0.1.28" },
        "@malloy-publisher/skills@0.1.28 gitHead": {
          stdout: "0000000000000000000000000000000000000000",
        },
      }),
    );
    expect(result.code).not.toBe(0);
    expect(result.summary).toContain("not present in this checkout");
    expect(result.ghLog.length).toBe(0);
  });

  it("computed version already on npm: errors rather than skipping, dispatches nothing", () => {
    const result = runPublishScript(
      baseConfig({
        "@malloy-publisher/skills dist-tags.latest": { stdout: "0.1.28" },
        "@malloy-publisher/skills@0.1.28 gitHead": { stdout: OLD_SHA },
        // The computed next-patch, 0.1.29, is already on npm.
        "@malloy-publisher/skills@0.1.29 version": { stdout: "0.1.29" },
      }),
    );
    expect(result.code).not.toBe(0);
    expect(result.summary).toContain("already on npm");
    expect(result.ghLog.length).toBe(0);
  });

  describe("watched content path validation", () => {
    // Runs a copy of the real script with one content-path entry swapped for
    // a path that matches nothing in this checkout, so validate_content_paths
    // has something to reject. cwd stays REPO_ROOT (see runPublishScript), so
    // every OTHER path in SKILLS_CONTENT_PATHS/SCAFFOLDER_CONTENT_PATHS still
    // resolves against the real tree; only the injected one is bogus.
    function scriptWithBadPath(search: string, replacement: string): string {
      const original = readFileSync(SCRIPT, "utf8");
      if (!original.includes(search)) {
        throw new Error(`fixture setup: "${search}" not found in ${SCRIPT}`);
      }
      const patched = original.replace(search, replacement);
      const dir = mkdtempSync(path.join(tmpdir(), "publish-packages-script-"));
      dirs.push(dir);
      const copyPath = path.join(dir, "publish-packages.sh");
      writeFileSync(copyPath, patched);
      chmodSync(copyPath, 0o755);
      return copyPath;
    }

    it("a typo'd skills content path is rejected before any decision, not read as unchanged", () => {
      const scriptPath = scriptWithBadPath(
        "SKILLS_CONTENT_PATHS=(skills/ packages/skills/ bun.lock package.json)",
        "SKILLS_CONTENT_PATHS=(skills/ packages/skills/ bun.lock package.json nonexistent-file-xyz)",
      );
      const result = runPublishScript({}, {}, scriptPath);
      expect(result.code).not.toBe(0);
      expect(result.summary).toContain(
        "@malloy-publisher/skills's watched content path 'nonexistent-file-xyz' is malformed",
      );
      // Rejected before either package's npm/gh calls, not merely before its
      // dispatch: a pathspec that matches nothing would otherwise read the
      // diff as "unchanged" and skip silently instead of failing loudly.
      expect(result.npmLog.length).toBe(0);
      expect(result.ghLog.length).toBe(0);
    });

    it("a typo'd scaffolder content path is rejected before any decision", () => {
      const scriptPath = scriptWithBadPath(
        "SCAFFOLDER_CONTENT_PATHS=(packages/create-malloy-package/)",
        "SCAFFOLDER_CONTENT_PATHS=(packages/create-malloy-package/ nonexistent-file-xyz)",
      );
      const result = runPublishScript({}, {}, scriptPath);
      expect(result.code).not.toBe(0);
      expect(result.summary).toContain(
        "@malloy-publisher/create-malloy-package's watched content path 'nonexistent-file-xyz' is malformed",
      );
      expect(result.npmLog.length).toBe(0);
      expect(result.ghLog.length).toBe(0);
    });
  });
});
