#!/usr/bin/env node
// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

// Read and stamp the narrative sections in RELEASE_NOTES.md.
//
// The release workflow uses this for the two manual steps that used to sit
// after a release and were reliably forgotten: stamping a section with the
// version that ships it, and putting the narrative onto the GitHub release page.
// Five consecutive releases (0.0.243 through 0.0.247) went out with neither, so
// a deprecation and a changed metric label reached nobody.
//
//   stamp <version>      rewrite every `## [Unreleased]` heading in place as
//                        `## [<version>]`, and print how many it rewrote
//   extract [<version>]  print every section headed `[<version>]` (default
//                        `[Unreleased]`), formatted for a release body
//
// `prepare` stamps on the release branch, so the stamp and the version bump
// are one commit and the release ships exactly the sections that commit
// carries. `gh-release` then extracts `[<version>]` from that same commit. Both
// read one tree, so a section merged to main while the release runs is in
// neither: it is still `[Unreleased]` on main and ships in the next release.
//
// `extract` with no version is the authoring-time check build.yml runs on every
// PR, so a heading it cannot read a title from fails the PR that wrote it.
//
// A prerelease version is accepted but `prepare` never stamps one, so
// `extract <prerelease>` prints nothing; the narrative stays `[Unreleased]` on
// main for the next ordinary release.
//
// Both are no-ops when the file carries no matching section, which is the
// normal case for a routine patch: the auto-generated PR list is enough, and a
// release that needs no narrative should not be made to invent one.

import { readFileSync, writeFileSync } from "node:fs";

const FILE = process.env.RELEASE_NOTES_FILE ?? "RELEASE_NOTES.md";
const VERSION = /^\d+\.\d+\.\d+/;

function fail(message) {
  console.error(`release-notes: ${message}`);
  process.exit(1);
}

// The heading and title patterns for one marker: `Unreleased`, or a version.
//
// The separator between the marker and the title is optional, and this file has
// written it three ways (em dash, colon, hyphen). An earlier `[ :]+—?` REQUIRED
// one, so a bare `## [Unreleased]` matched the heading, was collected as a
// section, and then had nothing stripped — putting `## ## [Unreleased]` onto the
// public release page. All three separators are accepted here, and so is none at
// all; a heading with no title after it is an error, not a `## ` on the page.
function patterns(marker) {
  const escaped = marker.replace(/[.*+?^${}()|[\]\\]/g, "\\$&");
  return {
    heading: new RegExp(`^## \\[${escaped}\\]`),
    title: new RegExp(`^## \\[${escaped}\\]\\s*[—:-]?\\s*`),
  };
}

// A section runs from its own heading to the next `## [` heading, so prose,
// `###` subheadings and fenced code inside it are carried along untouched.
// Matching on `## [` rather than `## ` is what makes the `###` subheadings the
// sections are full of safe.
function sections(lines, heading) {
  const starts = lines
    .map((l, i) => (l.startsWith("## [") ? i : -1))
    .filter((i) => i !== -1);

  return starts
    .map((start, n) => ({
      start,
      end: starts[n + 1] ?? lines.length,
    }))
    .filter(({ start }) => heading.test(lines[start]))
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

const [command, version, ...extra] = process.argv.slice(2);

if ((command !== "extract" && command !== "stamp") || extra.length) {
  console.error(
    "usage: release-notes.mjs extract [<version>] | stamp <version>",
  );
  process.exit(1);
}

if (command === "stamp" || version !== undefined) {
  if (!VERSION.test(version ?? "")) {
    fail(`${command} needs a version, got ${version || "nothing"}`);
  }
}

const raw = readFileSync(FILE, "utf8");
const lines = raw.split("\n");

if (command === "extract") {
  const { heading, title: TITLE } = patterns(version ?? "Unreleased");
  // The version is already the release's title and its every npm link, so
  // repeating it in each heading just makes the page stutter. `## [0.0.248] —
  // measures can be pre-aggregated` becomes `## measures can be pre-aggregated`.
  const bodies = [];
  for (const { start, body } of sections(lines, heading)) {
    const title = body[0].replace(TITLE, "").trim();
    if (!title) {
      fail(
        `${FILE}:${start + 1}: '${body[0].trim()}' has no title. Write it as ` +
          "`## [Unreleased] — what changed`; an untitled section has nothing to " +
          "put on the release page.",
      );
    }
    bodies.push([`## ${title}`, ...body.slice(1)].join("\n").trim());
  }

  const text = bodies.join("\n\n---\n\n");
  process.stdout.write(text ? `${text}\n` : "");
} else {
  // Rewrite only the heading lines. A blanket replace over the file would also
  // rewrite the word "Unreleased" wherever it appears in prose, and these
  // sections discuss prior releases by name constantly.
  let stamped = 0;
  for (const { start } of sections(lines, patterns("Unreleased").heading)) {
    lines[start] = lines[start].replace("## [Unreleased]", `## [${version}]`);
    stamped += 1;
  }

  // No write when nothing matched, so a no-op run leaves the file untouched
  // rather than merely unchanged.
  if (stamped) {
    writeFileSync(FILE, lines.join("\n"));
  }
  process.stdout.write(`${stamped}\n`);
}
