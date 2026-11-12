// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

// Cancels workflow runs that have waited too long for a runner. Run by
// .github/workflows/cancel-stale-queued-runs.yml on a schedule.
//
// GitHub-hosted runners have no queue timeout: a queued job waits until the
// 35-day workflow run limit. A run's own `status` is not the signal either: a
// run reads `queued` while some of its jobs are already in progress. The
// signal is per job. A job blocked on `needs:` is not listed until its
// dependency finishes, so a listed job with status `queued` is waiting for a
// runner, and its `created_at` is when that wait began.
//
// Usage: GITHUB_REPOSITORY=owner/repo [STALE_AFTER_MINUTES=60] [DRY_RUN=true]
//        node scripts/cancel-stale-queued-runs.mjs

import { execFileSync } from "node:child_process";
import { appendFileSync, realpathSync } from "node:fs";
import { fileURLToPath, pathToFileURL } from "node:url";

// `waiting` is left out on purpose: it is a run held at an environment's
// approval gate, which is a person's decision, not a stuck queue.
export const ACTIVE_RUN_STATUSES = ["queued", "pending", "requested", "in_progress"];

const minutesSince = (iso, nowMs) => Math.floor((nowMs - Date.parse(iso)) / 60_000);

/**
 * Decides what to do with one active run, given its latest attempt's jobs.
 *
 * - No jobs at all, older than the threshold: force-cancel. Nothing ran, so
 *   force-cancel skips no cleanup, and a run that never got a job does not
 *   always respond to a plain cancel.
 * - A job waiting for an approval gate: skip.
 * - No job queued past the threshold: skip.
 * - Nothing started yet: cancel, whatever the event.
 * - Some jobs already ran: cancel a pull_request run. Any other event (a
 *   release or publish dispatch) is reported and left alone, since cancelling
 *   it partway could leave a release half done.
 *
 * @returns {{action: "skip"} | {action: "cancel", force: boolean, reason: string} | {action: "report", reason: string}}
 */
export function decide({ run, jobs, nowMs, staleAfterMinutes }) {
  if (jobs.length === 0) {
    const since = run.run_started_at ?? run.created_at;
    const age = minutesSince(since, nowMs);
    if (age < staleAfterMinutes) return { action: "skip" };
    return { action: "cancel", force: true, reason: `no job has started in ${age} min` };
  }
  if (jobs.some((j) => j.status === "waiting")) return { action: "skip" };

  const stale = jobs
    .filter((j) => j.status === "queued" && minutesSince(j.created_at, nowMs) >= staleAfterMinutes)
    .sort((a, b) => Date.parse(a.created_at) - Date.parse(b.created_at));
  if (stale.length === 0) return { action: "skip" };

  const oldest = stale[0];
  const reason = `job "${oldest.name}" has been queued for ${minutesSince(oldest.created_at, nowMs)} min`;
  const started = jobs.some((j) => j.status === "in_progress" || j.status === "completed");
  if (!started || run.event === "pull_request") {
    return { action: "cancel", force: false, reason };
  }
  return {
    action: "report",
    reason: `${reason}, but other jobs already ran, so this ${run.event} run is left for a person to cancel`,
  };
}

function parseLines(stdout) {
  return stdout
    .split("\n")
    .filter((line) => line.trim() !== "")
    .map((line) => JSON.parse(line));
}

function parseStaleAfter(raw) {
  const value = raw === undefined || raw === "" ? 60 : Number(raw);
  if (!Number.isInteger(value) || value < 1) {
    throw new Error(
      `STALE_AFTER_MINUTES must be a positive whole number of minutes, got ${JSON.stringify(raw)}. Fix: STALE_AFTER_MINUTES=60`,
    );
  }
  return value;
}

/**
 * One sweep over the repository's active runs.
 *
 * @param {object} opts
 * @param {(args: string[]) => string} opts.gh runs `gh` with the given
 *   arguments and returns stdout; throws an error carrying `stderr` on failure.
 * @param {Record<string, string | undefined>} opts.env
 * @param {number} opts.nowMs
 * @param {(line: string) => void} opts.log
 * @returns {{exitCode: number, rows: object[]}}
 */
export function sweep({ gh, env, nowMs, log }) {
  const repo = env.GITHUB_REPOSITORY;
  if (!repo) throw new Error("GITHUB_REPOSITORY is not set. Fix: GITHUB_REPOSITORY=owner/repo");
  const staleAfterMinutes = parseStaleAfter(env.STALE_AFTER_MINUTES);
  const dryRun = env.DRY_RUN === "true";
  const selfRunId = Number(env.GITHUB_RUN_ID ?? 0);

  // A run can change status between two list calls and appear under both.
  const runs = new Map();
  for (const status of ACTIVE_RUN_STATUSES) {
    const out = gh([
      "api",
      "--paginate",
      `repos/${repo}/actions/runs?status=${status}&per_page=100`,
      "--jq",
      ".workflow_runs[] | {id, name, event, status, created_at, run_started_at, head_branch, html_url}",
    ]);
    for (const run of parseLines(out)) runs.set(run.id, run);
  }
  log(`${runs.size} active run(s); stale after ${staleAfterMinutes} min${dryRun ? "; dry run" : ""}`);

  const rows = [];
  let failures = 0;
  for (const run of runs.values()) {
    if (run.id === selfRunId) continue;
    const jobs = parseLines(
      gh([
        "api",
        "--paginate",
        `repos/${repo}/actions/runs/${run.id}/jobs?per_page=100`,
        "--jq",
        ".jobs[] | {name, status, created_at}",
      ]),
    );
    const decision = decide({ run, jobs, nowMs, staleAfterMinutes });
    if (decision.action === "skip") continue;

    const label = `${run.name} (${run.event}, ${run.head_branch}) ${run.html_url}`;
    if (decision.action === "report") {
      log(`::warning title=Stale queued run::${label}: ${decision.reason}`);
      rows.push({ run, outcome: "left alone", reason: decision.reason });
      continue;
    }

    const endpoint = decision.force ? "force-cancel" : "cancel";
    if (dryRun) {
      log(`would ${endpoint} ${label}: ${decision.reason}`);
      rows.push({ run, outcome: `would ${endpoint}`, reason: decision.reason });
      continue;
    }
    try {
      gh(["api", "-X", "POST", `repos/${repo}/actions/runs/${run.id}/${endpoint}`]);
      log(`${endpoint} ${label}: ${decision.reason}`);
      rows.push({ run, outcome: endpoint, reason: decision.reason });
    } catch (err) {
      const stderr = String(err.stderr ?? err.message ?? err).trim();
      // 409 is GitHub declining the cancel for the run's state, not a failure
      // of this sweep: the run finished or began cancelling after it was
      // listed, or it was created and never processed ("has not been queued
      // yet"). The latter holds no runner and only GitHub can clear it, so it
      // is reported on every sweep rather than failing every sweep.
      if (stderr.includes("HTTP 409")) {
        log(`::notice title=Cancel declined::${label}: ${stderr}`);
        rows.push({ run, outcome: "declined by GitHub (409)", reason: stderr });
        continue;
      }
      failures += 1;
      log(`::error title=Cancel failed::${endpoint} ${label} failed: ${stderr}`);
      rows.push({ run, outcome: `${endpoint} failed`, reason: stderr });
    }
  }
  log(`${rows.length} stale run(s) acted on or reported, ${failures} failure(s)`);
  return { exitCode: failures > 0 ? 1 : 0, rows };
}

function writeSummary(path, rows) {
  if (!path) return;
  const lines = ["## Stale queued runs", ""];
  if (rows.length === 0) {
    lines.push("None found.");
  } else {
    lines.push("| Run | Event | Branch | Outcome | Reason |", "|---|---|---|---|---|");
    for (const { run, outcome, reason } of rows) {
      const cell = (s) => String(s).replaceAll("|", "\\|");
      lines.push(
        `| [${cell(run.name)}](${run.html_url}) | ${run.event} | ${cell(run.head_branch)} | ${outcome} | ${cell(reason)} |`,
      );
    }
  }
  appendFileSync(path, lines.join("\n") + "\n");
}

function main() {
  const gh = (args) =>
    execFileSync("gh", args, { encoding: "utf8", stdio: ["ignore", "pipe", "pipe"] });
  try {
    const { exitCode, rows } = sweep({ gh, env: process.env, nowMs: Date.now(), log: console.log });
    writeSummary(process.env.GITHUB_STEP_SUMMARY, rows);
    process.exit(exitCode);
  } catch (err) {
    console.error(`::error title=Stale run sweep failed::${String(err.stderr ?? err.message ?? err).trim()}`);
    process.exit(1);
  }
}

// Compared through realpath so a symlinked invocation still runs main().
if (
  pathToFileURL(realpathSync(process.argv[1])).href ===
  pathToFileURL(realpathSync(fileURLToPath(import.meta.url))).href
) {
  main();
}
