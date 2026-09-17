// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

// A NUL byte anywhere in a text file makes the WHOLE file binary to the tools
// people search this repo with, and they skip a binary file silently.
//
// `model.ts` carried three, as the separator in a composite Map key —
// `${label}\0${exprs.join("\0")}\0${selfContained}` — chosen for the right
// reason: the parts are concatenated, so a separator the data can contain lets
// `{label: "a b", exprs: []}` key the same as `{label: "a", exprs: ["b"]}`. NUL
// cannot appear in a label or an expression, so it collides with nothing.
//
// What it also does is classify the file. ugrep with `-I` (skip binary files),
// which is what some editor and agent integrations shell out to, returned
// nothing for every pattern in the largest file in the server package, with
// exit 1 and no message — indistinguishable from "no matches".
//
// How much damage a NUL does depends on where it lands, which is the reason to
// keep them all out rather than to judge each one. ugrep scans the whole file,
// so it was affected; git sniffs only the first 8000 bytes, so at offset ~76k
// it still produced ordinary text diffs and gave no hint anything was wrong.
//
// `\x1f` (ASCII Unit Separator) has the identical property — it cannot appear
// in the data either — and is exactly what that control character is for. This
// file is what keeps the distinction from being re-litigated by whoever next
// reaches for `\0` because it is the obvious separator, which it is.
//
// Scope: files git tracks, minus the extensions that are legitimately binary.
// Asking git rather than walking keeps `node_modules`, build output and the
// runtime `publisher_data/` out without naming any of them, and means a fixture
// someone adds is covered the moment it is committed.
import { describe, expect, it } from "bun:test";
import { execFileSync } from "child_process";
import { readFileSync } from "fs";
import path from "path";

/** Extensions whose files are binary by nature; a NUL in one says nothing. */
const BINARY_EXTENSIONS = new Set([
  ".png",
  ".jpg",
  ".jpeg",
  ".gif",
  ".ico",
  ".webp",
  ".pdf",
  ".woff",
  ".woff2",
  ".ttf",
  ".otf",
  ".eot",
  ".zip",
  ".gz",
  ".tgz",
  ".xlsx",
  ".xls",
  ".docx",
  ".parquet",
  ".db",
  ".duckdb",
  ".sqlite",
  ".dxt",
  ".wasm",
  ".mp4",
  ".mov",
  ".gif",
]);

const REPO_ROOT = path.resolve(import.meta.dir, "..");

function trackedTextFiles(): string[] {
  const out = execFileSync("git", ["ls-files", "-z"], {
    cwd: REPO_ROOT,
    encoding: "buffer",
    maxBuffer: 64 * 1024 * 1024,
  });
  return out
    .toString("utf8")
    .split("\0")
    .filter((f) => f.length > 0)
    .filter((f) => !BINARY_EXTENSIONS.has(path.extname(f).toLowerCase()));
}

describe("no NUL bytes in tracked text files", () => {
  it("keeps every searchable file searchable", () => {
    const offenders: string[] = [];
    for (const file of trackedTextFiles()) {
      let buf: Buffer;
      try {
        buf = readFileSync(path.join(REPO_ROOT, file));
      } catch {
        // A tracked path that is not readable as a file (a submodule entry,
        // a broken symlink) is not this guard's business.
        continue;
      }
      const at = buf.indexOf(0);
      if (at !== -1) {
        const line = buf.subarray(0, at).toString("utf8").split("\n").length;
        offenders.push(`${file}:${line}`);
      }
    }
    // Named, not counted: the fix is per-file and the message should say
    // which file and which line rather than that some file somewhere has one.
    expect(offenders).toEqual([]);
  });
});
