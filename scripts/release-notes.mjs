#!/usr/bin/env node
// Read and stamp the `## [Unreleased]` sections in RELEASE_NOTES.md.
//
// The release workflow uses this for the two manual steps that used to sit
// after a release and were reliably forgotten: pasting the narrative onto the
// GitHub release page, and stamping the section with the version that shipped
// it. Five consecutive releases (0.0.243 through 0.0.247) went out with neither,
// so a deprecation and a changed metric label reached nobody.
//
//   extract           print every [Unreleased] section, formatted for a release body
//   stamp <version>   rewrite those headings in place as [<version>]
//
// Both are no-ops when the file carries no [Unreleased] section, which is the
// normal case for a routine patch: the auto-generated PR list is enough, and a
// release that needs no narrative should not be made to invent one.

import { readFileSync, writeFileSync } from "node:fs";

const FILE = process.env.RELEASE_NOTES_FILE ?? "RELEASE_NOTES.md";
const HEADING = /^## \[Unreleased\]/;

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

const raw = readFileSync(FILE, "utf8");
const lines = raw.split("\n");
const found = sections(lines);
const [command, version] = process.argv.slice(2);

if (command === "extract") {
  // The version is already the release's title and its every npm link, so
  // repeating it in each heading just makes the page stutter. `## [0.0.248] —
  // measures can be pre-aggregated` becomes `## measures can be pre-aggregated`.
  const text = found
    .map(({ body }) => {
      const title = body[0].replace(/^## \[Unreleased\][ :]+—?\s*/, "").trim();
      return [`## ${title}`, ...body.slice(1)].join("\n").trim();
    })
    .join("\n\n---\n\n");
  process.stdout.write(text ? `${text}\n` : "");
} else if (command === "stamp") {
  if (!/^\d+\.\d+\.\d+/.test(version ?? "")) {
    console.error(
      `release-notes: stamp needs a version, got ${version ?? "nothing"}`,
    );
    process.exit(1);
  }
  // Rewrite only the heading lines this run identified. A blanket replace over
  // the file would also rewrite the word "Unreleased" wherever it appears in
  // prose, and these sections discuss prior releases by name constantly.
  for (const { start } of found) {
    lines[start] = lines[start].replace("## [Unreleased]", `## [${version}]`);
  }
  writeFileSync(FILE, lines.join("\n"));
  process.stdout.write(`${found.length}\n`);
} else {
  console.error("usage: release-notes.mjs extract | stamp <version>");
  process.exit(1);
}
