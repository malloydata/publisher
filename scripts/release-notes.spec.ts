// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

// Tests for scripts/release-notes.mjs, the release workflow's narrative step.
//
// This script is exercised once per release, on a dispatch-only path that CI
// cannot run: `prepare` stamps the release branch and `gh-release` extracts from
// it after npm and Docker have already published. A bug in it is discovered by
// reading a wrong release page. One of the cases below is a bug that reached
// review: a bare `## [Unreleased]` heading put `## ## [Unreleased]` onto the
// public page.
//
// The script is run as a subprocess rather than imported: its argv parsing,
// exit codes and stdout contract are the interface the workflow depends on, and
// a test that called an exported function would not cover any of them.

import { afterEach, describe, expect, it } from "bun:test";
import { mkdtempSync, readFileSync, rmSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import path from "node:path";

const SCRIPT = path.join(import.meta.dir, "release-notes.mjs");

const HEADER = `# Release Notes

Preamble that mentions the word Unreleased in prose, because these sections
discuss prior releases constantly and a blanket replace would rewrite this line.

---
`;

const workspaces: string[] = [];

/** A throwaway RELEASE_NOTES.md. */
function workspace(notes: string) {
  const dir = mkdtempSync(path.join(tmpdir(), "release-notes-"));
  workspaces.push(dir);
  const file = path.join(dir, "RELEASE_NOTES.md");
  writeFileSync(file, notes);
  return { file };
}

afterEach(() => {
  while (workspaces.length) {
    rmSync(workspaces.pop()!, { recursive: true, force: true });
  }
});

function run(file: string, ...args: string[]) {
  const proc = Bun.spawnSync(["node", SCRIPT, ...args], {
    env: { ...process.env, RELEASE_NOTES_FILE: file },
  });
  return {
    code: proc.exitCode,
    stdout: proc.stdout.toString(),
    stderr: proc.stderr.toString(),
  };
}

describe("extract", () => {
  it("strips every separator this file has actually used", () => {
    // Em dash, colon and hyphen all appear in RELEASE_NOTES.md's history.
    const { file } = workspace(
      `${HEADER}
## [Unreleased] — em dash

dash body

---

## [Unreleased]: colon

colon body

---

## [Unreleased] - hyphen

hyphen body
`,
    );

    const { code, stdout } = run(file, "extract");
    expect(code).toBe(0);
    expect(stdout).toContain("## em dash");
    expect(stdout).toContain("## colon");
    expect(stdout).toContain("## hyphen");
    // The marker itself never survives onto the page, in any form.
    expect(stdout).not.toContain("[Unreleased]");
    // Three sections, joined by the horizontal rule the page separates them by.
    expect(stdout.match(/^## /gm)).toHaveLength(3);
    expect(stdout).toContain("\n\n---\n\n");
  });

  it("carries ### subheadings and fenced code through untouched", () => {
    const { file } = workspace(
      `${HEADER}
## [Unreleased] — has structure

### Breaking changes

\`\`\`bash
## not a heading, it is a shell comment
\`\`\`
`,
    );

    const { code, stdout } = run(file, "extract");
    expect(code).toBe(0);
    expect(stdout).toContain("### Breaking changes");
    expect(stdout).toContain("## not a heading, it is a shell comment");
  });

  it("rejects a bare heading rather than putting `## ` on the page", () => {
    // The bug: an earlier regex required a separator, so this heading matched
    // as a section but had nothing stripped, and `## ## [Unreleased]` went out
    // on the release body.
    const { file } = workspace(`${HEADER}
## [Unreleased]

body with no title above it
`);

    const { code, stdout, stderr } = run(file, "extract");
    expect(code).toBe(1);
    expect(stdout).toBe("");
    expect(stderr).toContain("has no title");
    expect(stdout).not.toContain("## ## [Unreleased]");
  });

  it("rejects a heading whose separator is followed by nothing", () => {
    const { file } = workspace(`${HEADER}
## [Unreleased] —

body
`);

    const { code, stderr } = run(file, "extract");
    expect(code).toBe(1);
    expect(stderr).toContain("has no title");
  });

  it("prints nothing when there is no narrative", () => {
    const { file } = workspace(`${HEADER}
## [0.0.248] — already shipped

body
`);

    const { code, stdout } = run(file, "extract");
    expect(code).toBe(0);
    expect(stdout).toBe("");
  });
});

describe("extract <version>", () => {
  // `gh-release` reads the release branch after `prepare` stamped it, so the
  // narrative it puts on the page is whatever is headed with this version.
  const NOTES = `${HEADER}
## [Unreleased] — merged after the release was cut

later body

---

## [0.0.249] — first

a

---

## [0.0.249]: second

b

---

## [0.0.248] — previous release

old body
`;

  it("prints only the sections headed with that version", () => {
    const { file } = workspace(NOTES);

    const { code, stdout } = run(file, "extract", "0.0.249");
    expect(code).toBe(0);
    expect(stdout).toBe("## first\n\na\n\n---\n\n## second\n\nb\n");
  });

  it("does not match a version that only shares a prefix", () => {
    // `.` is escaped, and the closing bracket is part of the match, so 0.0.24
    // is not read as a prefix of 0.0.249 or 0.0.248.
    const { file } = workspace(NOTES);

    const { code, stdout } = run(file, "extract", "0.0.24");
    expect(code).toBe(0);
    expect(stdout).toBe("");
  });

  it("reads back exactly what [Unreleased] would have printed after a stamp", () => {
    // The workflow's contract: `prepare` stamps, `gh-release` extracts by
    // version, and the release page is the same text the authors wrote.
    const { file } = workspace(`${HEADER}
## [Unreleased] — em dash

dash body

---

## [Unreleased]: colon

colon body

---

## [0.0.248] — already shipped

old body
`);
    const before = run(file, "extract");
    expect(before.code).toBe(0);
    expect(before.stdout).not.toBe("");

    expect(run(file, "stamp", "0.0.249").stdout).toBe("2\n");

    const after = run(file, "extract", "0.0.249");
    expect(after.code).toBe(0);
    expect(after.stdout).toBe(before.stdout);
    expect(run(file, "extract").stdout).toBe("");
  });
});

describe("stamp", () => {
  it("rewrites the [Unreleased] headings and nothing else", () => {
    const { file } = workspace(`${HEADER}
## [Unreleased] — shipping now

body that says Unreleased in prose

---

## [0.0.248] — already shipped

body
`);

    const { code, stdout } = run(file, "stamp", "0.0.249");
    expect(code).toBe(0);
    expect(stdout).toBe("1\n");

    const after = readFileSync(file, "utf8");
    expect(after).toContain("## [0.0.249] — shipping now");
    expect(after).toContain("body that says Unreleased in prose");
    expect(after).toContain("## [0.0.248] — already shipped");
    expect(after).not.toContain("[Unreleased]");
    // The preamble's prose mention is untouched.
    expect(after).toContain("mentions the word Unreleased in prose");
  });

  it("stamps only the exact [Unreleased] marker, not other bracketed headings", () => {
    const { file } = workspace(`${HEADER}
## [Unreleased] — shipping now

a

## [0.0.1-rc] — a prerelease section

b

## [Warning] — not a release

c

## [unreleased] — lowercase is not the marker

d
`);

    expect(run(file, "stamp", "0.0.249").stdout).toBe("1\n");
    const headings = readFileSync(file, "utf8").match(/^## \[.*$/gm);
    expect(headings).toEqual([
      "## [0.0.249] — shipping now",
      "## [0.0.1-rc] — a prerelease section",
      "## [Warning] — not a release",
      "## [unreleased] — lowercase is not the marker",
    ]);
    expect(run(file, "extract", "0.0.249").stdout).toBe(
      "## shipping now\n\na\n",
    );
  });

  it("is a no-op, byte for byte, when there is nothing to stamp", () => {
    const notes = `${HEADER}
## [0.0.248] — already shipped

body
`;
    const { file } = workspace(notes);

    const { code, stdout } = run(file, "stamp", "0.0.249");
    expect(code).toBe(0);
    expect(stdout).toBe("0\n");
    expect(readFileSync(file, "utf8")).toBe(notes);
  });

  it("is idempotent: a second run stamps nothing and changes nothing", () => {
    const { file } = workspace(`${HEADER}
## [Unreleased] — shipping now

body
`);

    expect(run(file, "stamp", "0.0.249").stdout).toBe("1\n");
    const once = readFileSync(file, "utf8");

    const second = run(file, "stamp", "0.0.250");
    expect(second.code).toBe(0);
    expect(second.stdout).toBe("0\n");
    expect(readFileSync(file, "utf8")).toBe(once);
  });

  it("stamps a bare heading, which extract refuses", () => {
    // Deliberately asymmetric. `extract` fails on an untitled section because
    // it has nothing to put on the page; `stamp` still records what shipped, so
    // a heading that somehow got past authoring is not left [[Unreleased]]
    // forever.
    const { file } = workspace(`${HEADER}
## [Unreleased]

body
`);

    expect(run(file, "stamp", "0.0.249").stdout).toBe("1\n");
    expect(readFileSync(file, "utf8")).toContain("## [0.0.249]");
  });
});

describe("fit", () => {
  // `gh-release` writes the page from this. GitHub refuses a body over 125,000
  // characters, after npm, Docker and PyPI have published, which is what 0.13.0's
  // 136k of narrative did.

  /** A header, a narrative of `count` sections of `size` characters each, and a generated list of `prs` entries. */
  function release(count: number, size: number, prs = 0) {
    const sectionsText = Array.from(
      { length: count },
      (_, i) => `## [0.0.249] — section ${i + 1}\n\n${"x".repeat(size)}\n`,
    ).join("\n---\n\n");
    const { file } = workspace(`${HEADER}\n${sectionsText}`);
    const dir = path.dirname(file);
    const header = path.join(dir, "header.md");
    writeFileSync(header, "## Release v0.0.249\n\nthe npm and Docker links\n");
    const generated = path.join(dir, "generated.md");
    writeFileSync(
      generated,
      "## What's Changed\n" +
        Array.from(
          { length: prs },
          (_, i) =>
            `* change ${i + 1} by @someone in https://github.com/malloydata/publisher/pull/${i + 1}`,
        ).join("\n") +
        "\n\n**Full Changelog**: https://github.com/malloydata/publisher/compare/v0.0.248...v0.0.249\n",
    );
    return { file, header, generated, out: path.join(dir, "body.md") };
  }

  function fitRun(
    r: ReturnType<typeof release>,
    ...args: string[]
  ): ReturnType<typeof run> & { body: string } {
    const result = run(
      r.file,
      "fit",
      "0.0.249",
      "--header",
      r.header,
      "--out",
      r.out,
      "--prev-tag",
      "v0.0.248",
      ...args,
    );
    let body = "";
    try {
      body = readFileSync(r.out, "utf8");
    } catch {}
    return { ...result, body };
  }

  it("keeps the full narrative and the generated list when they fit", () => {
    const r = release(2, 100, 3);
    const { code, stdout, body } = fitRun(r, "--generated", r.generated);
    expect(code).toBe(0);
    expect(stdout).toMatch(/^narrative=full generated=kept length=\d+\n$/);
    expect(body.startsWith("## Release v0.0.249")).toBe(true);
    // The same text `extract` prints, between the header and the list.
    expect(body).toContain(run(r.file, "extract", "0.0.249").stdout.trim());
    expect(body).toContain("* change 3 by @someone");
    expect(body.trimEnd().endsWith("v0.0.248...v0.0.249")).toBe(true);
  });

  it("falls back to an index linking RELEASE_NOTES.md when the narrative is too long", () => {
    // 0.13.0's shape: many sections, together past the page's limit.
    const r = release(75, 1_900, 140);
    const { code, stdout, body } = fitRun(r, "--generated", r.generated);
    expect(code).toBe(0);
    expect(stdout).toContain("narrative=index generated=kept");
    expect(body.length).toBeLessThanOrEqual(120_000);
    expect(body).toContain(
      "so the full text is in [RELEASE_NOTES.md at v0.0.249](https://github.com/malloydata/publisher/blob/v0.0.249/RELEASE_NOTES.md). The sections it ships:",
    );
    expect(body).toContain("\n- section 1\n");
    expect(body).toContain("\n- section 75\n");
    expect(body).not.toContain("xxxxxxxxxx");
    // The whole generated list is still there.
    expect(body).toContain("* change 140 by @someone");
  });

  it("truncates the generated list, keeping its compare link, when the index alone does not leave room", () => {
    const r = release(5, 1_000, 200);
    const { code, stdout, body } = fitRun(
      r,
      "--generated",
      r.generated,
      "--limit",
      "3000",
    );
    expect(code).toBe(0);
    expect(stdout).toContain("narrative=index generated=truncated");
    expect(body.length).toBeLessThanOrEqual(3000);
    expect(body).toContain("- section 5");
    expect(body).toContain("* change 1 by @someone");
    expect(body).not.toContain("* change 200 by");
    expect(body).toMatch(/_…and \d+ more, too many for this page/);
    expect(body.trimEnd().endsWith("v0.0.248...v0.0.249")).toBe(true);
  });

  it("shortens the index itself, and always fits, however small the limit", () => {
    const r = release(50, 10, 50);
    for (const limit of [400, 120, 10]) {
      const { code, stdout, body } = fitRun(
        r,
        "--generated",
        r.generated,
        "--limit",
        String(limit),
      );
      expect(code).toBe(0);
      expect(stdout).toContain("narrative=index-truncated");
      expect(body.length).toBeLessThanOrEqual(limit);
    }
  });

  it("links the compare view when the generated list could not be fetched", () => {
    const r = release(1, 10);
    const { code, stdout, body } = fitRun(r);
    expect(code).toBe(0);
    expect(stdout).toContain("narrative=full generated=link");
    expect(
      body
        .trimEnd()
        .endsWith(
          "**Full Changelog**: https://github.com/malloydata/publisher/compare/v0.0.248...v0.0.249",
        ),
    ).toBe(true);
  });

  it("leaves the narrative off with --no-narrative, for a RELEASE_NOTES.md extract could not read", () => {
    const r = release(2, 10, 1);
    const { code, stdout, body } = fitRun(
      r,
      "--generated",
      r.generated,
      "--no-narrative",
    );
    expect(code).toBe(0);
    expect(stdout).toContain("narrative=none generated=kept");
    expect(body).not.toContain("section 1");
  });

  it("uses the repo URL it is given", () => {
    const r = release(10, 100);
    const { body } = fitRun(
      r,
      "--repo-url",
      "https://github.example/acme/fork/",
      "--limit",
      "1000",
    );
    expect(body).toContain(
      "(https://github.example/acme/fork/blob/v0.0.249/RELEASE_NOTES.md)",
    );
    expect(body).toContain(
      "https://github.example/acme/fork/compare/v0.0.248...v0.0.249",
    );
  });

  it("fails, writing nothing, on a section extract would refuse", () => {
    const r = release(1, 10);
    writeFileSync(r.file, `${HEADER}\n## [0.0.249]\n\nuntitled\n`);
    const { code, stderr, body } = fitRun(r);
    expect(code).toBe(1);
    expect(stderr).toContain("has no title");
    expect(body).toBe("");
  });
});

describe("bad invocations leave the file alone", () => {
  const NOTES = `${HEADER}
## [Unreleased] — shipping now

body
`;

  const cases: Array<[string, string[]]> = [
    ["a version that is not a version", ["stamp", "nope"]],
    ["a partial version", ["stamp", "0.0"]],
    ["no version at all", ["stamp"]],
    ["an empty version", ["stamp", ""]],
    ["an unknown command", ["publish", "0.0.249"]],
    ["no command", []],
    ["extract with a version that is not a version", ["extract", "nope"]],
    ["a stray extra argument", ["stamp", "0.0.249", "0.0.250"]],
    ["fit with no --header or --out", ["fit", "0.0.249"]],
    ["fit with an unknown flag", ["fit", "0.0.249", "--bogus", "x"]],
    ["fit with no version", ["fit"]],
    [
      "fit with a limit that is not a number",
      ["fit", "0.0.249", "--header", "h", "--out", "o", "--limit", "big"],
    ],
  ];

  for (const [name, args] of cases) {
    it(`exits 1 on ${name}`, () => {
      const { file } = workspace(NOTES);
      const result = run(file, ...args);
      expect(result.code).toBe(1);
      expect(result.stderr).not.toBe("");
      expect(readFileSync(file, "utf8")).toBe(NOTES);
    });
  }
});
