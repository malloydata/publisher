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
//   fit <version> --header <file> --out <file> [--generated <file>]
//       [--repo-url <url>] [--prev-tag <tag>] [--no-narrative] [--limit <n>]
//                        write the whole release body to --out, sized to fit a
//                        GitHub release page, and print what it had to give up
//                        (see `fit` below)
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

// The section bodies `extract` prints, one per heading, each retitled without
// its marker.
function extractBodies(lines, version) {
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
    bodies.push({
      title,
      text: [`## ${title}`, ...body.slice(1)].join("\n").trim(),
    });
  }
  return bodies;
}

// GitHub refuses a release body over 125,000 characters with a 422, and it
// does so in `gh-release`, after npm, Docker and PyPI have published: 0.13.0's
// 75 sections came to 136k and its page had to be made by hand. The default
// budget leaves room under that for however GitHub counts (this counts UTF-16
// code units, so an emoji already costs two here).
const DEFAULT_LIMIT = 120_000;
const DEFAULT_REPO_URL = "https://github.com/malloydata/publisher";

// The tail of `--generate-notes` output, and the line that stands in for the
// whole list when it was not fetched or does not fit.
const FULL_CHANGELOG = /^\*\*Full Changelog\*\*/;

// Keep the generated PR list's leading lines that fit in `budget`, then say how
// many were left out and keep its Full Changelog link, which carries the rest.
// Returns null when not even the link fits.
function truncateGenerated(generated, budget, compareLine) {
  const lines = generated.trimEnd().split("\n");
  let tail = compareLine;
  const at = lines.findLastIndex((l) => FULL_CHANGELOG.test(l));
  if (at !== -1) {
    tail = lines[at];
    lines.splice(at, 1);
  }
  while (lines.length && !lines[lines.length - 1].trim()) lines.pop();
  const items = (ls) => ls.filter((l) => /^\* /.test(l)).length;
  const total = items(lines);
  const note = (left) =>
    `\n\n_…and ${left} more, too many for this page; the compare view has every change._` +
    (tail ? `\n\n${tail}` : "");

  for (let keep = lines.length; keep >= 0; keep--) {
    const head = lines.slice(0, keep).join("\n").trimEnd();
    const text = `${head}${note(total - items(lines.slice(0, keep)))}`.trim();
    if (text.length <= budget) return text;
  }
  return tail && tail.length <= budget ? tail : null;
}

// Compose the whole release body, giving up detail in this order until it fits
// `limit`: the narrative's full text (replaced by an index of its section titles
// and a link to RELEASE_NOTES.md at the tag), then the generated PR list's tail
// (truncated, keeping its compare link), then the list entirely, then the index's
// own tail. The last step is a plain cut, so the body always fits: a release
// that has already published must never fail here over its length.
function fit({ header, bodies, generated, version, repoUrl, prevTag, limit }) {
  const tag = `v${version}`;
  const compareLine = prevTag
    ? `**Full Changelog**: ${repoUrl}/compare/${prevTag}...${tag}`
    : "";
  const join = (...parts) =>
    `${parts
      .map((p) => p.trim())
      .filter(Boolean)
      .join("\n\n")}\n`;

  const full = bodies.map((b) => b.text).join("\n\n---\n\n");
  const list = generated.trim() || compareLine;
  const listKind = generated.trim() ? "kept" : compareLine ? "link" : "none";
  const narrative = bodies.length ? "full" : "none";

  let body = join(header, full, list);
  if (body.length <= limit) return { body, narrative, generated: listKind };

  // The index form: the page a person made by hand for 0.13.0.
  const intro =
    "This release's notes are longer than a GitHub release page allows, so the " +
    `full text is in [RELEASE_NOTES.md at ${tag}](${repoUrl}/blob/${tag}/RELEASE_NOTES.md). ` +
    "The sections it ships:";
  let bullets = bodies.map((b) => `- ${b.title}`);
  let index = bodies.length ? `${intro}\n\n${bullets.join("\n")}` : "";
  const kind = bodies.length ? "index" : "none";

  body = join(header, index, list);
  if (body.length <= limit)
    return { body, narrative: kind, generated: listKind };

  // What is left for the list once the header and index are placed. The
  // separators `join` adds are counted by measuring with an empty list.
  const budget = limit - join(header, index, "x").length + 1;
  if (generated.trim() && budget > 0) {
    const cut = truncateGenerated(generated, budget, compareLine);
    if (cut) {
      body = join(header, index, cut);
      if (body.length <= limit) {
        return {
          body,
          narrative: kind,
          generated: cut === compareLine ? "link" : "truncated",
        };
      }
    }
  }

  // Not even the index fits beside a compare link: shorten the index.
  while (bullets.length) {
    bullets = bullets.slice(0, -1);
    const left = bodies.length - bullets.length;
    index = `${intro}\n\n${[...bullets, `- …and ${left} more`].join("\n")}`;
    body = join(header, index, compareLine);
    if (body.length <= limit) {
      return {
        body,
        narrative: "index-truncated",
        generated: compareLine ? "link" : "none",
      };
    }
  }
  body = join(header, index, compareLine).slice(0, limit);
  return { body, narrative: "index-truncated", generated: "cut" };
}

const [command, version, ...extra] = process.argv.slice(2);

// `fit` takes flags after the version; the other two take none.
const FLAGS = new Set([
  "--header",
  "--out",
  "--generated",
  "--repo-url",
  "--prev-tag",
  "--limit",
]);
const options = {};
if (command === "fit") {
  for (let i = 0; i < extra.length; i++) {
    const flag = extra[i];
    if (flag === "--no-narrative") {
      options.noNarrative = true;
    } else if (FLAGS.has(flag) && i + 1 < extra.length) {
      options[flag.slice(2)] = extra[++i];
    } else {
      fail(`fit: unexpected argument ${flag}`);
    }
  }
  if (!options.header || !options.out) fail("fit needs --header and --out");
  if (
    options.limit !== undefined &&
    !(Number.isInteger(Number(options.limit)) && Number(options.limit) > 0)
  ) {
    fail(`fit: --limit must be a positive integer, got ${options.limit}`);
  }
}

if (
  !["extract", "stamp", "fit"].includes(command) ||
  (command !== "fit" && extra.length)
) {
  console.error(
    "usage: release-notes.mjs extract [<version>] | stamp <version> | " +
      "fit <version> --header <file> --out <file> [--generated <file>] " +
      "[--repo-url <url>] [--prev-tag <tag>] [--no-narrative] [--limit <n>]",
  );
  process.exit(1);
}

if (command !== "extract" || version !== undefined) {
  if (!VERSION.test(version ?? "")) {
    fail(`${command} needs a version, got ${version || "nothing"}`);
  }
}

const raw = readFileSync(FILE, "utf8");
const lines = raw.split("\n");

if (command === "extract") {
  const text = extractBodies(lines, version)
    .map((b) => b.text)
    .join("\n\n---\n\n");
  process.stdout.write(text ? `${text}\n` : "");
} else if (command === "fit") {
  const result = fit({
    header: readFileSync(options.header, "utf8"),
    bodies: options.noNarrative ? [] : extractBodies(lines, version),
    generated: options.generated ? readFileSync(options.generated, "utf8") : "",
    version,
    repoUrl: (options["repo-url"] ?? DEFAULT_REPO_URL).replace(/\/+$/, ""),
    prevTag: options["prev-tag"] ?? "",
    limit: Number(options.limit ?? DEFAULT_LIMIT),
  });
  writeFileSync(options.out, result.body);
  // One line the workflow reads to word its notice: how the narrative went on
  // the page (full, index, index-truncated, none) and the PR list (kept,
  // truncated, link, none, cut), then the body's length.
  process.stdout.write(
    `narrative=${result.narrative} generated=${result.generated} length=${result.body.length}\n`,
  );
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
