// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

// Tests for scripts/cancel-stale-queued-runs.mjs. `decide` is the whole
// cancel-or-not policy and is tested as a pure function. `sweep` is driven
// through an in-process fake `gh` that answers the list calls from a table and
// records every call, so the loop's own contract (dry run, self-exclusion,
// de-duplication, 409 handling, exit code) is checked without a network call.

import { describe, expect, it } from "bun:test";
import { decide, sweep } from "./cancel-stale-queued-runs.mjs";

const NOW = Date.parse("2026-11-12T12:00:00Z");
const minutesAgo = (m: number) => new Date(NOW - m * 60_000).toISOString();

type Job = { name: string; status: string; created_at: string };
const job = (name: string, status: string, queuedMinutesAgo: number): Job => ({
  name,
  status,
  created_at: minutesAgo(queuedMinutesAgo),
});
const run = (over: Record<string, unknown> = {}) => ({
  id: 1,
  name: "build",
  event: "pull_request",
  status: "queued",
  created_at: minutesAgo(120),
  run_started_at: minutesAgo(120),
  head_branch: "feature",
  html_url: "https://github.com/o/r/actions/runs/1",
  ...over,
});
const decideWith = (r: ReturnType<typeof run>, jobs: Job[]) =>
  decide({ run: r, jobs, nowMs: NOW, staleAfterMinutes: 60 });

describe("decide", () => {
  it("skips a run whose queued job has waited less than the threshold", () => {
    expect(decideWith(run(), [job("Typecheck", "queued", 59)])).toEqual({ action: "skip" });
  });

  it("cancels a run with a job queued exactly at the threshold", () => {
    expect(decideWith(run(), [job("Typecheck", "queued", 60)])).toEqual({
      action: "cancel",
      force: false,
      reason: 'job "Typecheck" has been queued for 60 min',
    });
  });

  it("times the wait from the job, not the run: a job listed late is not stale", () => {
    // The run is two hours old, but its only queued job entered the queue ten
    // minutes ago (as a job blocked on `needs:` does when its dependency ends).
    expect(
      decideWith(run({ created_at: minutesAgo(120) }), [
        job("Build", "completed", 120),
        job("Smoke test", "queued", 10),
      ]),
    ).toEqual({ action: "skip" });
  });

  it("names the longest-queued job in the reason", () => {
    const d = decideWith(run(), [job("Lint", "queued", 70), job("macOS", "queued", 95)]);
    expect(d).toEqual({ action: "cancel", force: false, reason: 'job "macOS" has been queued for 95 min' });
  });

  it("cancels a partly-run pull_request run", () => {
    const d = decideWith(run({ event: "pull_request" }), [
      job("Typecheck", "completed", 90),
      job("macOS", "queued", 90),
    ]);
    expect(d).toEqual({ action: "cancel", force: false, reason: 'job "macOS" has been queued for 90 min' });
  });

  for (const event of ["workflow_dispatch", "schedule", "workflow_run", "push"]) {
    it(`reports, and does not cancel, a partly-run ${event} run`, () => {
      const d = decideWith(run({ event }), [job("prepare", "completed", 90), job("publish", "queued", 90)]);
      expect(d).toEqual({
        action: "report",
        reason: `job "publish" has been queued for 90 min, but other jobs already ran, so this ${event} run is left for a person to cancel`,
      });
    });

    it(`cancels a ${event} run none of whose jobs has started`, () => {
      expect(decideWith(run({ event }), [job("prepare", "queued", 61)])).toEqual({
        action: "cancel",
        force: false,
        reason: 'job "prepare" has been queued for 61 min',
      });
    });
  }

  it("treats an in-progress job as started", () => {
    const d = decideWith(run({ event: "workflow_dispatch" }), [
      job("prepare", "in_progress", 90),
      job("publish", "queued", 90),
    ]);
    expect(d.action).toBe("report");
  });

  it("skips a run held at an approval gate, even with a stale queued job", () => {
    expect(
      decideWith(run({ event: "workflow_dispatch" }), [job("publish", "waiting", 300), job("scan", "queued", 300)]),
    ).toEqual({ action: "skip" });
  });

  it("force-cancels a run that has had no job for longer than the threshold", () => {
    expect(decideWith(run({ run_started_at: minutesAgo(61) }), [])).toEqual({
      action: "cancel",
      force: true,
      reason: "no job has started in 61 min",
    });
  });

  it("skips a run with no job yet when it is younger than the threshold", () => {
    expect(decideWith(run({ run_started_at: minutesAgo(30) }), [])).toEqual({ action: "skip" });
  });

  it("times a jobless re-run from its attempt, not from the original run", () => {
    expect(
      decideWith(run({ created_at: minutesAgo(3 * 24 * 60), run_started_at: minutesAgo(5) }), []),
    ).toEqual({ action: "skip" });
  });

  it("falls back to created_at when run_started_at is absent", () => {
    expect(decideWith(run({ run_started_at: null, created_at: minutesAgo(61) }), []).action).toBe("cancel");
  });
});

type Call = string[];
function fakeGh(opts: {
  runsByStatus: Record<string, ReturnType<typeof run>[]>;
  jobsByRun: Record<number, Job[]>;
  failCancel?: Record<number, string>;
}) {
  const calls: Call[] = [];
  const gh = (args: string[]) => {
    calls.push(args);
    if (args[1] === "-X") {
      const id = Number(args[3].split("/").at(-2));
      const err = opts.failCancel?.[id];
      if (err) throw Object.assign(new Error("gh failed"), { stderr: err });
      return "";
    }
    const path = args[2];
    const listMatch = path.match(/actions\/runs\?status=(\w+)/);
    if (listMatch) {
      return (opts.runsByStatus[listMatch[1]] ?? []).map((r) => JSON.stringify(r)).join("\n") + "\n";
    }
    const jobsMatch = path.match(/actions\/runs\/(\d+)\/jobs/);
    if (jobsMatch) {
      return (opts.jobsByRun[Number(jobsMatch[1])] ?? []).map((j) => JSON.stringify(j)).join("\n");
    }
    throw new Error(`unexpected gh call: ${args.join(" ")}`);
  };
  const posts = () => calls.filter((c) => c[1] === "-X").map((c) => c[3]);
  return { gh, calls, posts };
}

const ENV = { GITHUB_REPOSITORY: "o/r", GITHUB_RUN_ID: "999" };
const sweepWith = (gh: (a: string[]) => string, env: Record<string, string> = ENV) => {
  const lines: string[] = [];
  const result = sweep({ gh, env, nowMs: NOW, log: (l: string) => lines.push(l) });
  return { ...result, lines };
};

describe("sweep", () => {
  const stale = run({ id: 1 });
  const fresh = run({ id: 2 });
  const jobless = run({ id: 3, run_started_at: minutesAgo(200) });
  const jobsByRun = {
    1: [job("Typecheck", "queued", 90)],
    2: [job("Typecheck", "queued", 5)],
  };

  it("cancels only stale runs, using force-cancel for a jobless run", () => {
    const f = fakeGh({ runsByStatus: { queued: [stale, fresh, jobless] }, jobsByRun });
    const { exitCode } = sweepWith(f.gh);
    expect(exitCode).toBe(0);
    expect(f.posts()).toEqual(["repos/o/r/actions/runs/1/cancel", "repos/o/r/actions/runs/3/force-cancel"]);
  });

  it("lists every active status and never the approval-gated `waiting`", () => {
    const f = fakeGh({ runsByStatus: {}, jobsByRun });
    sweepWith(f.gh);
    const listed = f.calls.map((c) => c[2]?.match(/status=(\w+)/)?.[1]).filter(Boolean);
    expect(listed).toEqual(["queued", "pending", "requested", "in_progress"]);
  });

  it("makes no cancel call on a dry run, but reports what it would cancel", () => {
    const f = fakeGh({ runsByStatus: { queued: [stale] }, jobsByRun });
    const { rows, lines } = sweepWith(f.gh, { ...ENV, DRY_RUN: "true" });
    expect(f.posts()).toEqual([]);
    expect(rows.map((r: { outcome: string }) => r.outcome)).toEqual(["would cancel"]);
    expect(lines.some((l) => l.startsWith("would cancel build"))).toBe(true);
  });

  it("never cancels its own run", () => {
    const self = run({ id: 999 });
    const f = fakeGh({ runsByStatus: { in_progress: [self] }, jobsByRun: { 999: [job("x", "queued", 500)] } });
    sweepWith(f.gh);
    expect(f.posts()).toEqual([]);
  });

  it("acts once on a run listed under two statuses", () => {
    const f = fakeGh({ runsByStatus: { queued: [stale], in_progress: [stale] }, jobsByRun });
    sweepWith(f.gh);
    expect(f.posts()).toEqual(["repos/o/r/actions/runs/1/cancel"]);
  });

  it("records a 409 as declined, not as a failure", () => {
    const f = fakeGh({
      runsByStatus: { queued: [stale] },
      jobsByRun,
      failCancel: { 1: "gh: Cannot cancel a workflow run that has not been queued yet. (HTTP 409)" },
    });
    const { exitCode, rows } = sweepWith(f.gh);
    expect(exitCode).toBe(0);
    expect(rows[0].outcome).toBe("declined by GitHub (409)");
  });

  it("exits 1 on any other cancel error, after still processing the remaining runs", () => {
    const other = run({ id: 4 });
    const f = fakeGh({
      runsByStatus: { queued: [stale, other] },
      jobsByRun: { ...jobsByRun, 4: [job("Typecheck", "queued", 90)] },
      failCancel: { 1: "gh: Resource not accessible by integration (HTTP 403)" },
    });
    const { exitCode, lines } = sweepWith(f.gh);
    expect(exitCode).toBe(1);
    expect(f.posts()).toEqual(["repos/o/r/actions/runs/1/cancel", "repos/o/r/actions/runs/4/cancel"]);
    expect(lines.some((l) => l.startsWith("::error title=Cancel failed::cancel build") && l.includes("HTTP 403"))).toBe(
      true,
    );
  });

  it("does not cancel a partly-run dispatch, and warns about it instead", () => {
    const release = run({ id: 5, event: "workflow_dispatch", name: "Release" });
    const f = fakeGh({
      runsByStatus: { in_progress: [release] },
      jobsByRun: { 5: [job("prepare", "completed", 90), job("publish", "queued", 90)] },
    });
    const { lines } = sweepWith(f.gh);
    expect(f.posts()).toEqual([]);
    expect(lines.some((l) => l.startsWith("::warning title=Stale queued run::Release"))).toBe(true);
  });

  for (const [value, ok] of [
    ["60", true],
    ["", true],
    ["0", false],
    ["-5", false],
    ["1.5", false],
    ["sixty", false],
  ] as const) {
    it(`${ok ? "accepts" : "rejects"} STALE_AFTER_MINUTES=${JSON.stringify(value)}`, () => {
      const f = fakeGh({ runsByStatus: {}, jobsByRun: {} });
      const call = () => sweepWith(f.gh, { ...ENV, STALE_AFTER_MINUTES: value });
      if (ok) expect(call).not.toThrow();
      else expect(call).toThrow("STALE_AFTER_MINUTES must be a positive whole number of minutes");
    });
  }

  it("requires GITHUB_REPOSITORY", () => {
    const f = fakeGh({ runsByStatus: {}, jobsByRun: {} });
    expect(() => sweepWith(f.gh, {})).toThrow("GITHUB_REPOSITORY is not set");
  });
});
