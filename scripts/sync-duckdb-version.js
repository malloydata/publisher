#!/usr/bin/env node

/**
 * Verify the DuckDB build knobs match the version Malloy resolves.
 *
 * The query engine is `@duckdb/node-api` (pulled in by `@malloydata/db-duckdb`,
 * and used directly by the storage layer). DuckDB resolves/downloads extensions
 * under ~/.duckdb/extensions/v<version>/<platform>/, so the DuckDB CLI installed
 * at build time must be the same version -- otherwise the engine looks in a
 * directory the CLI never wrote to and re-fetches every extension from
 * extensions.duckdb.org at query time (which fails on hosts that cannot reach
 * it, e.g. the macOS GitHub fleet).
 *
 * The version is resolved from bun.lock by scripts/duckdb-version.js (the
 * single source of truth). Build scripts derive it directly from there. The
 * two places that can't -- the Dockerfile ARG default (parsed before any
 * script runs) and the server's @duckdb/node-api dependency pin (an npm spec
 * literal) -- carry a literal this script checks and, with --write, rewrites.
 *
 * Usage:
 *   node scripts/sync-duckdb-version.js            # check; exit 1 on drift
 *   node scripts/sync-duckdb-version.js --write    # rewrite the literals to match
 *   node scripts/sync-duckdb-version.js --print    # print the bare version
 */

import fs from "fs";
import path from "path";
import {
  resolveDuckDBVersion,
  resolveDuckDBSpec,
  REPO_ROOT,
} from "./duckdb-version.js";

const WRITE = process.argv.includes("--write");
// --print emits just the resolved engine version (no report), for scripting
// and Docker build-args, e.g. `--build-arg DUCKDB_VERSION=$(node scripts/duckdb-version.js)`.
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

function resolveEngineVersion() {
  try {
    return resolveDuckDBVersion();
  } catch (err) {
    fail(err instanceof Error ? err.message : String(err));
  }
}

function resolveEngineSpec() {
  try {
    return resolveDuckDBSpec();
  } catch (err) {
    fail(err instanceof Error ? err.message : String(err));
  }
}

// Each knob: a file, a human label, a matcher that extracts the version it
// currently encodes, and (for --write) a rewriter that pins it to `target`.
//
// Two knobs carry a literal the lockfile can't supply at the point they're
// read:
//   - The Dockerfile ARG default, parsed by the Docker builder before any
//     script runs (CI passes --build-arg from the lockfile; this is the
//     fallback for a plain `docker build`). Uses the release version
//     (major.minor.patch) -- the extension-directory form.
//   - The server's @duckdb/node-api dependency pin, which must be the EXACT
//     resolved spec (with the -r.N suffix) so it dedupes to the same copy
//     @malloydata/db-duckdb resolves. A divergence here resolves two engine
//     copies.
// Every other build script resolves the version at runtime via
// scripts/duckdb-version.js, so there is nothing to sync there.
function buildKnobs(target, spec) {
  return [
    {
      file: "Dockerfile",
      label: "Dockerfile CLI install/symlink/PATH (ARG default)",
      extract: (text) => {
        const m = text.match(/ARG DUCKDB_VERSION=(\d+\.\d+\.\d+)/);
        return m ? m[1] : null;
      },
      expected: target,
      rewrite: (text) => pinDockerfile(text, target),
    },
    {
      file: "packages/server/package.json",
      label: "server @duckdb/node-api dependency pin",
      extract: (text) => {
        const m = text.match(/"@duckdb\/node-api":\s*"([^"]+)"/);
        return m ? m[1] : null;
      },
      expected: spec,
      rewrite: (text) =>
        text.replace(/("@duckdb\/node-api":\s*")[^"]+(")/, `$1${spec}$2`),
    },
  ];
}

// --- Dockerfile rewriting -------------------------------------------------

function pinDockerfile(text, target) {
  let out = text;

  // 1. Pin the CLI install + symlink in base-deps.
  if (/ARG DUCKDB_VERSION=\d+\.\d+\.\d+\nRUN DUCKDB_VERSION=/.test(out)) {
    out = out.replace(
      /ARG DUCKDB_VERSION=\d+\.\d+\.\d+/g,
      `ARG DUCKDB_VERSION=${target}`,
    );
  } else {
    out = out.replace(
      /RUN curl -L https:\/\/install\.duckdb\.org \| bash && \\\n\s*ln -s \/root\/\.duckdb\/cli\/latest\/duckdb \/usr\/local\/bin\/duckdb && \\/,
      "# DuckDB CLI version, pinned to @duckdb/node-api (the query engine) so the\n" +
        "# CLI bakes extensions into the same ~/.duckdb/extensions/v<version>/ dir\n" +
        "# the runtime reads. Managed by scripts/sync-duckdb-version.js.\n" +
        `ARG DUCKDB_VERSION=${target}\n` +
        'RUN DUCKDB_VERSION=${DUCKDB_VERSION} bash -c "curl -L https://install.duckdb.org | bash" && \\\n' +
        "    ln -s /root/.duckdb/cli/${DUCKDB_VERSION}/duckdb /usr/local/bin/duckdb && \\",
    );
  }

  // 2. Re-declare the ARG in the final stage and pin the runtime PATH.
  if (/ARG DUCKDB_VERSION=\d+\.\d+\.\d+\nENV NODE_ENV=production/.test(out)) {
    // already present; the global ARG replace above handled the value
  } else {
    out = out.replace(
      /ENV NODE_ENV=production\nENV PATH="\/root\/\.duckdb\/cli\/latest:\$PATH"/,
      `ARG DUCKDB_VERSION=${target}\n` +
        "ENV NODE_ENV=production\n" +
        'ENV PATH="/root/.duckdb/cli/${DUCKDB_VERSION}:$PATH"',
    );
  }

  return out;
}

// --- guard: the classic `duckdb` binding must not come back ---------------

// The storage layer was migrated off the legacy `duckdb` npm binding onto
// @duckdb/node-api so the repo carries a single DuckDB engine. If `duckdb`
// reappears in any package.json (a stray dependency, a re-added resolution),
// flag it -- it would reintroduce the two-engine version split this script
// exists to prevent.
function classicBindingReintroductions() {
  const files = ["package.json", "packages/server/package.json"];
  return files.filter((file) => {
    const json = JSON.parse(read(file));
    return Boolean(
      (json.dependencies && json.dependencies.duckdb) ||
        (json.devDependencies && json.devDependencies.duckdb) ||
        (json.resolutions && json.resolutions.duckdb),
    );
  });
}

// --- main -----------------------------------------------------------------

function main() {
  const target = resolveEngineVersion();

  if (PRINT) {
    // Bare version on stdout, nothing else, for `$(... --print)` substitution.
    process.stdout.write(target);
    return;
  }

  const spec = resolveEngineSpec();
  console.log(`DuckDB query engine (@duckdb/node-api): ${spec} (v${target})\n`);

  const knobs = buildKnobs(target, spec);
  let drift = false;

  for (const knob of knobs) {
    const text = read(knob.file);
    const current = knob.extract(text);
    const ok = current === knob.expected;
    if (!ok) drift = true;

    const status = ok ? "✓" : "✗";
    const shown = current === null ? "(unpinned / not set)" : current;
    console.log(`  ${status} ${knob.label}: ${shown}`);

    if (WRITE && !ok) {
      const updated = knob.rewrite(text);
      if (updated === text) {
        fail(
          `--write could not update ${knob.file}; its structure changed. ` +
            "Update scripts/sync-duckdb-version.js to match.",
        );
      }
      fs.writeFileSync(path.join(REPO_ROOT, knob.file), updated);
      console.log(`      -> rewrote ${knob.file} to ${knob.expected}`);
    }
  }

  // Guard: the legacy `duckdb` binding was removed in favor of a single engine.
  const reintroduced = classicBindingReintroductions();
  if (reintroduced.length > 0) {
    drift = true;
    console.log("");
    for (const file of reintroduced) {
      console.log(
        `  ✗ legacy 'duckdb' binding reintroduced in ${file} -- the storage ` +
          "layer uses @duckdb/node-api; remove it to keep one DuckDB engine.",
      );
    }
  }

  console.log("");
  if (WRITE) {
    if (drift) {
      console.log(
        "DuckDB knobs rewritten. Review the diff" +
          (reintroduced.length
            ? " (note: a reintroduced legacy 'duckdb' dependency must be removed by hand)."
            : "; if the server pin changed, run `bun install` to update the lockfile."),
      );
    } else {
      console.log("All DuckDB knobs already match. Nothing to write.");
    }
    return;
  }

  if (drift) {
    fail(
      "DuckDB version drift detected. Run `bun run sync-duckdb --write` to " +
        "align the CLI/build knobs with the @duckdb/node-api query engine " +
        "(and remove any reintroduced legacy 'duckdb' dependency).",
    );
  }
  console.log("✓ All DuckDB knobs match the query engine.");
}

main();
