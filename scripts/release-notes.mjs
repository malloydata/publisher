#!/usr/bin/env node
// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

// Read and stamp the `## [Unreleased]` sections in RELEASE_NOTES.md.
//
// The release workflow uses this for the two manual steps that used to sit
// after a release and were reliably forgotten: pasting the narrative onto the
// GitHub release page, and stamping the section with the version that shipped
// it. Five consecutive releases (0.0.243 through 0.0.247) went out with neither,
// so a deprecation and a changed metric label reached nobody.
//
//   extract [--titles <path>]          print every [Unreleased] section,
//                                      formatted for a release body
//   stamp <version> [--titles <path>]   rewrite those headings in place as
//                                      [<version>]
//
// `--titles` is what keeps the two halves talking about the same sections.
// `extract` reads the release branch's snapshot of the file; `stamp` rewrites
// whatever `main` has become several minutes later, and a section merged inside
// that window is neither on the release page nor still marked [Unreleased] —
// it can never appear on any release page again. So `extract --titles` records
// the exact heading lines it consumed, and `stamp --titles` rewrites only
// those, leaving anything newer alone. A recorded heading that is no longer in
// the file (edited or removed mid-release) is reported rather than passed over
// in silence.
//
// Both are no-ops when the file carries no [Unreleased] section, which is the
// normal case for a routine patch: the auto-generated PR list is enough, and a
// release that needs no narrative should not be made to invent one.

import { readFileSync, writeFileSync } from "node:fs";

const FILE = process.env.RELEASE_NOTES_FILE ?? "RELEASE_NOTES.md";
const HEADING = /^## \[Unreleased\]/;

// The separator between the marker and the title is optional, and this file has
// written it three ways (em dash, colon, hyphen). An earlier `[ :]+—?` REQUIRED
// one, so a bare `## [Unreleased]` matched HEADING, was collected as a section,
// and then had nothing stripped — putting `## ## [Unreleased]` onto the public
// release page. All three separators are accepted here, and so is none at all;
// a heading with no title after it is an error, not a `## ` on the page.
const TITLE = /^## \[Unreleased\]\s*[—:-]?\s*/;

function fail(message) {
  console.error(`release-notes: ${message}`);
  process.exit(1);
}

// A section runs from its own heading to the next `## [` heading, so prose,
// `###` subheadings and fenced code inside it are carried along untouched.
// Matching on `## [` rather than `## ` is what makes the `###` subheadings the
// sections are full of safe.
function sections(lines) {
  const starts = lines
    .map((l, i) => (l.startsWith("## [") ? i : -1))
    .filter((i) => i !== -1);

  return starts
    .map((start, n) => ({
      start,
      end: starts[n + 1] ?? lines.length,
    }))
    .filter(({ start }) => HEADING.test(lines[start]))
    .map(({ start, end }) => {
      const body = lines.slice(start, end);
      // Drop the blank lines and `---` rule that separate this section from the
      // next. They belong to the file's layout, not to the section.
      while (
        body.length &&
        ["", "---"].includes(body[body.length - 1].trim())
      ) {
        body.pop();
      }
      return { start, body };
    });
}

// Pull `--titles <path>` out before positional parsing, so it can sit either
// side of the command and version.
const argv = process.argv.slice(2);
const flagAt = argv.indexOf("--titles");
let titlesPath;
if (flagAt !== -1) {
  titlesPath = argv[flagAt + 1];
  if (!titlesPath) {
    fail("--titles needs a path");
  }
  argv.splice(flagAt, 2);
}

const [command, version] = argv;

if (command !== "extract" && command !== "stamp") {
  console.error(
    "usage: release-notes.mjs extract [--titles <path>] | stamp <version> [--titles <path>]",
  );
  process.exit(1);
}

const raw = readFileSync(FILE, "utf8");
const lines = raw.split("\n");
const found = sections(lines);

if (command === "extract") {
  // The version is already the release's title and its every npm link, so
  // repeating it in each heading just makes the page stutter. `## [0.0.248] —
  // measures can be pre-aggregated` becomes `## measures can be pre-aggregated`.
  const bodies = [];
  const titles = [];
  for (const { start, body } of found) {
    const heading = body[0];
    const title = heading.replace(TITLE, "").trim();
    if (!title) {
      fail(
        `${FILE}:${start + 1}: '${heading.trim()}' has no title. Write it as ` +
          "`## [Unreleased] — what changed`; an untitled section has nothing to " +
          "put on the release page.",
      );
    }
    bodies.push([`## ${title}`, ...body.slice(1)].join("\n").trim());
    titles.push(heading);
  }

  // The exact heading lines, for `stamp --titles` to match on. Written even
  // when empty: an empty list is a real answer (this release has no narrative)
  // and a stamp that then finds sections must not touch them, because they
  // arrived after this ran.
  if (titlesPath) {
    writeFileSync(titlesPath, titles.length ? `${titles.join("\n")}\n` : "");
  }

  const text = bodies.join("\n\n---\n\n");
  process.stdout.write(text ? `${text}\n` : "");
} else {
  if (!/^\d+\.\d+\.\d+/.test(version ?? "")) {
    fail(`stamp needs a version, got ${version ?? "nothing"}`);
  }

  let wanted;
  if (titlesPath) {
    let recorded;
    try {
      recorded = readFileSync(titlesPath, "utf8");
    } catch (e) {
      fail(`could not read the titles file ${titlesPath}: ${e.message}`);
    }
    wanted = new Set(recorded.split("\n").filter((l) => l.trim() !== ""));
  }

  // Rewrite only the heading lines this run identified. A blanket replace over
  // the file would also rewrite the word "Unreleased" wherever it appears in
  // prose, and these sections discuss prior releases by name constantly.
  //
  // With `--titles`, narrow that again to the headings `extract` actually put on
  // the release page. Matched headings are collected rather than removed from
  // `wanted` so that two sections sharing a heading are both stamped.
  const matched = new Set();
  let stamped = 0;
  for (const { start } of found) {
    if (wanted && !wanted.has(lines[start])) {
      continue;
    }
    matched.add(lines[start]);
    lines[start] = lines[start].replace("## [Unreleased]", `## [${version}]`);
    stamped += 1;
  }

  // A recorded heading that is not here any more: someone edited or removed the
  // section during the release window. Its narrative is on the release page, so
  // there is nothing to undo, but the section it came from is now untracked and
  // that should be visible in the log rather than inferred later.
  const missing = wanted ? [...wanted].filter((t) => !matched.has(t)) : [];
  if (missing.length) {
    console.error(
      `release-notes: ${missing.length} section(s) on the release page are no ` +
        `longer in ${FILE} and were not stamped: ` +
        missing.map((t) => JSON.stringify(t.trim())).join(", "),
    );
  }

  // No write when nothing matched, so a no-op run leaves the file untouched
  // rather than merely unchanged.
  if (stamped) {
    writeFileSync(FILE, lines.join("\n"));
  }
  process.stdout.write(`${stamped}\n`);
}
