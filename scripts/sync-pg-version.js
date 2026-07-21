#!/usr/bin/env node

/**
 * Keep the root `pg` resolution equal to the version @malloydata/db-postgres
 * declares, so Publisher runs the exact Postgres driver Malloy's connector is
 * built and tested against.
 *
 * `pg` is not a direct dependency here; it comes in transitively via
 * @malloydata/db-postgres, which pins its own pg (an exact 8.7.3 as of 0.0.421).
 * Publisher never touches the pg Client/Pool API -- every Postgres connection
 * goes through Malloy's connector -- so there is no reason to force pg above the
 * connector's version. PR #786 added a root `resolutions.pg` (8.16.3) that rode
 * in incidentally with the theming family bump; this script keeps that
 * resolution in lockstep with the connector instead of leaving it a
 * hand-maintained constant, mirroring scripts/sync-duckdb-version.js.
 *
 * Policy: PLAIN-TRACK. The resolution is set to exactly what
 * @malloydata/db-postgres declares. To move Publisher's pg, bump pg upstream in
 * @malloydata/db-postgres; the next Malloy bump carries Publisher along.
 *
 * The source of truth is the version @malloydata/db-postgres declares in
 * bun.lock, NOT the top-level `pg` entry -- that is our own resolution reflected
 * back at us. This mirrors duckdb-version.js reading the nested
 * @malloydata/db-duckdb entry rather than the top-level one.
 *
 * Usage:
 *   bun scripts/sync-pg-version.js            # check; exit 1 on drift
 *   bun scripts/sync-pg-version.js --write    # rewrite resolutions.pg to match
 *   bun scripts/sync-pg-version.js --print    # print the bare target version
 */

import fs from "fs";
import path from "path";
import { fileURLToPath } from "url";

const REPO_ROOT = path.resolve(
  path.dirname(fileURLToPath(import.meta.url)),
  "..",
);

const WRITE = process.argv.includes("--write");
const PRINT = process.argv.includes("--print");

function fail(message) {
  console.error(`✗ ${message}`);
  process.exit(1);
}

function read(relPath) {
  const abs = path.join(REPO_ROOT, relPath);
  if (!fs.existsSync(abs)) {
    fail(`Expected file not found: ${relPath}`);
  }
  return fs.readFileSync(abs, "utf8");
}

/**
 * Parse a version spec to [major, minor, patch], stripping any range operator
 * (^, ~, >=) and pre-release/build suffix, then re-join to an exact
 * major.minor.patch pin. Mirrors duckdb-version.js's toReleaseVersion regex
 * approach (no semver dependency).
 */
function normalizeVersion(raw) {
  const m = String(raw).match(/(\d+)\.(\d+)\.(\d+)/);
  if (!m) {
    fail(`Could not parse a version from "${raw}".`);
  }
  return `${m[1]}.${m[2]}.${m[3]}`;
}

/**
 * The pg version @malloydata/db-postgres declares in bun.lock -- "what the
 * connector uses". Reads the nested dependency, not the top-level `pg` entry
 * (which is our resolution). bun.lock is JSONC; match the spec directly.
 */
function connectorPgSpec() {
  const lock = read("bun.lock");
  const m = lock.match(
    /"@malloydata\/db-postgres":\s*\[[^\]]*?"dependencies":\s*\{[^}]*?"pg":\s*"([^"]+)"/,
  );
  if (!m) {
    fail(
      "@malloydata/db-postgres pg dependency not found in bun.lock. Run " +
        '"bun install" first, or confirm @malloydata/db-postgres is a dependency.',
    );
  }
  return m[1];
}

/** The current root `resolutions.pg` literal, or null if unset. */
function currentResolutionPg(pkgText) {
  const m = pkgText.match(/"resolutions":\s*\{[^}]*?"pg":\s*"([^"]+)"/);
  return m ? m[1] : null;
}

function main() {
  const connector = connectorPgSpec();
  const pkgText = read("package.json");
  const current = currentResolutionPg(pkgText);

  // Plain-track: pin the resolution to exactly what the connector declares.
  const target = normalizeVersion(connector);

  if (PRINT) {
    process.stdout.write(target);
    return;
  }

  console.log(
    `pg (via @malloydata/db-postgres): connector declares ${connector}, ` +
      `target ${target}\n`,
  );

  if (current === null) {
    fail(
      "root resolutions.pg is not set. It pins pg to the connector's version " +
        "so Publisher and Malloy stay on the same driver; re-add it, or drop " +
        "this sync if pg should track the connector without a resolution.",
    );
  }

  const ok = current === target;
  console.log(`  ${ok ? "✓" : "✗"} root resolutions.pg: ${current}`);

  if (ok) {
    console.log(
      "\n✓ pg resolution already matches the connector-synced target.",
    );
    return;
  }

  if (WRITE) {
    const updated = pkgText.replace(
      /("resolutions":\s*\{[^}]*?"pg":\s*")[^"]+(")/,
      `$1${target}$2`,
    );
    if (updated === pkgText) {
      fail(
        "--write could not update resolutions.pg in package.json; its " +
          "structure changed. Update scripts/sync-pg-version.js to match.",
      );
    }
    fs.writeFileSync(path.join(REPO_ROOT, "package.json"), updated);
    console.log(`      -> rewrote resolutions.pg to ${target}`);
    console.log(
      "\npg resolution rewritten. Run `bun install` to apply it to bun.lock.",
    );
    return;
  }

  fail(
    `pg resolution drift: pinned ${current}, but @malloydata/db-postgres now ` +
      `wants ${target}. Run \`bun scripts/sync-pg-version.js --write\` (then ` +
      "`bun install`) to adopt it.",
  );
}

main();
