// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * The main-thread regression check: builds a baseline and a candidate server,
 * runs the same storm against each on this machine, alternating (B C C B ...)
 * so drift in the machine's speed lands on both sides, and judges the
 * candidate (verdict.ts). See README.md.
 *
 *   bun tests/perf/main-thread/run.ts --baseline <spec> --candidate <spec>
 *       [--rounds 2] [--out <dir>] [--ops 160] [--concurrency 8] [--envs 4]
 *       [--port 4941] [--cpu-prof] [--limits <json>]
 *
 * A build spec is `git:<ref>` (a worktree at that commit, installed and built
 * under `<out>/builds`) or a path to a checkout, `packages/server` or a
 * `dist` directory (built with `build:server-only:bundle` when it has no
 * `dist/server.mjs`).
 *
 * Writes `<out>/summary.md` (also appended to `$GITHUB_STEP_SUMMARY` when
 * set), `<out>/verdict.json` and each run's result and server log. Exits 0 on
 * PASS, 1 on FAIL, 2 when a run was invalid or the harness failed.
 */

import { spawnSync } from "bun";
import { appendFileSync, existsSync, mkdirSync, writeFileSync } from "fs";
import os from "os";
import path from "path";
import { buildFixtures } from "./fixtures";
import type { PreflightResult, StormResult } from "./storm";
import { DEFAULT_LIMITS, judge, summarize, type Limits } from "./verdict";

const HERE = import.meta.dir;
const REPO_ROOT = path.resolve(HERE, "../../../../..");

const args = process.argv.slice(2);
function flag(name: string): string | undefined;
function flag(name: string, dflt: string): string;
function flag(name: string, dflt?: string): string | undefined {
   const i = args.indexOf(`--${name}`);
   return i >= 0 ? args[i + 1] : dflt;
}

function sh(cmd: string[], cwd: string): void {
   console.log(`$ (${path.relative(REPO_ROOT, cwd) || "."}) ${cmd.join(" ")}`);
   const r = spawnSync(cmd, { cwd, stdout: "inherit", stderr: "inherit" });
   if (r.exitCode !== 0) {
      throw new Error(`${cmd.join(" ")} exited ${r.exitCode}`);
   }
}

function git(argv: string[], cwd = REPO_ROOT): string {
   const r = spawnSync(["git", ...argv], { cwd });
   if (r.exitCode !== 0) {
      throw new Error(`git ${argv.join(" ")}: ${r.stderr.toString()}`);
   }
   return r.stdout.toString().trim();
}

/** Resolve a build spec to a `dist` directory holding `server.mjs`. */
function prepareBuild(spec: string, buildsDir: string): string {
   if (spec.startsWith("git:")) {
      const sha = git(["rev-parse", "--verify", `${spec.slice(4)}^{commit}`]);
      const dir = path.join(buildsDir, sha.slice(0, 12));
      if (!existsSync(path.join(dir, "packages/server/dist/server.mjs"))) {
         if (!existsSync(dir)) {
            git(["worktree", "add", "--detach", dir, sha]);
         }
         // Once more on failure: a registry hiccup is not a verdict.
         try {
            sh(["bun", "install", "--frozen-lockfile"], dir);
         } catch {
            sh(["bun", "install", "--frozen-lockfile"], dir);
         }
         sh(
            ["bun", "run", "build:server-only:bundle"],
            path.join(dir, "packages/server"),
         );
      }
      return path.join(dir, "packages/server/dist");
   }
   const p = path.resolve(spec);
   for (const dist of [
      p,
      path.join(p, "dist"),
      path.join(p, "packages/server/dist"),
   ]) {
      if (existsSync(path.join(dist, "server.mjs"))) return dist;
   }
   const server = existsSync(path.join(p, "packages/server"))
      ? path.join(p, "packages/server")
      : p;
   sh(["bun", "run", "build:server-only:bundle"], server);
   return path.join(server, "dist");
}

function storm(argv: string[]): void {
   sh(
      ["bun", path.join(HERE, "storm.ts"), ...argv],
      path.join(HERE, "../../.."),
   );
}

/** Where results go; module-level so a harness error can still leave a summary. */
let out = "";

async function main(): Promise<number> {
   const baselineSpec = flag("baseline");
   const candidateSpec = flag("candidate");
   if (!baselineSpec || !candidateSpec) {
      console.error(
         "usage: bun run.ts --baseline <git:ref|path> --candidate <git:ref|path> [--rounds 2] [--out dir] [--cpu-prof] [--expect-cpus 4]",
      );
      return 2;
   }
   out = path.resolve(flag("out", path.join(os.tmpdir(), `mtr-${Date.now()}`)));
   const rounds = Number(flag("rounds", "2"));
   if (!Number.isInteger(rounds) || rounds < 1) {
      throw new Error(
         `--rounds must be a whole number of at least 1, not ${flag("rounds")}`,
      );
   }
   const limits: Limits = {
      ...DEFAULT_LIMITS,
      ...(flag("limits")
         ? (JSON.parse(flag("limits")!) as Partial<Limits>)
         : {}),
   };
   const common = [
      "--ops",
      flag("ops", "160"),
      "--concurrency",
      flag("concurrency", "8"),
      "--envs",
      flag("envs", "4"),
      "--seed",
      flag("seed", "1"),
      "--port",
      flag("port", "4941"),
      "--log-dir",
      out,
      ...(args.includes("--cpu-prof") ? ["--cpu-prof"] : []),
   ];
   mkdirSync(out, { recursive: true });

   const sides = {
      baseline: prepareBuild(baselineSpec, path.join(out, "builds")),
      candidate: prepareBuild(candidateSpec, path.join(out, "builds")),
   };
   console.log(`baseline:  ${baselineSpec} -> ${sides.baseline}`);
   console.log(`candidate: ${candidateSpec} -> ${sides.candidate}`);

   const set = await buildFixtures(path.join(out, "fixtures"));
   const fixturesJson = path.join(out, "fixtures", "fixtures.json");

   // Keep the A/B to the packages both builds load.
   const loaded: Record<string, PreflightResult> = {};
   for (const [side, dist] of Object.entries(sides)) {
      const file = path.join(out, `preflight-${side}.json`);
      storm([
         "--preflight",
         "--dist",
         dist,
         "--fixtures",
         fixturesJson,
         "--out",
         file,
         "--label",
         `${side}-preflight`,
         ...common,
      ]);
      loaded[side] = (await Bun.file(file).json()) as PreflightResult;
   }
   const only = set.fixtures
      .map((f) => f.name)
      .filter((n) => Object.values(loaded).every((p) => p.loaded.includes(n)));
   const dropped = set.fixtures
      .map((f) => f.name)
      .filter((n) => !only.includes(n));
   if (dropped.length) {
      console.log(`left out (a side refuses them): ${dropped.join(", ")}`);
      for (const [side, p] of Object.entries(loaded)) {
         for (const [n, why] of Object.entries(p.refused)) {
            console.log(`  ${side} refuses ${n}: ${why}`);
         }
      }
   }
   const probe = set.fixtures.find((f) => f.rebindProbe);
   const indexed = set.fixtures.find((f) => f.kind === "indexed");
   for (const [what, f] of [
      ["re-bind probe", probe],
      ["indexed", indexed],
   ] as const) {
      if (!f || !only.includes(f.name)) {
         throw new Error(
            `the ${what} fixture does not load on both sides, so its check would go untested (see the preflight lines above)`,
         );
      }
   }

   const results: Record<"baseline" | "candidate", StormResult[]> = {
      baseline: [],
      candidate: [],
   };
   for (let r = 0; r < rounds; r++) {
      const order: Array<"baseline" | "candidate"> =
         r % 2 === 0 ? ["baseline", "candidate"] : ["candidate", "baseline"];
      for (const side of order) {
         const file = path.join(out, `${side}-${r + 1}.json`);
         storm([
            "--dist",
            sides[side],
            "--fixtures",
            fixturesJson,
            "--out",
            file,
            "--label",
            `${side}-${r + 1}`,
            "--only",
            only.join(","),
            ...common,
         ]);
         results[side].push((await Bun.file(file).json()) as StormResult);
      }
   }

   // An anchor baseline brings the ranges it was calibrated at (anchor.json).
   const anchor = (await Bun.file(path.join(HERE, "anchor.json")).json()) as {
      commit: string;
      calibrated?: {
         statusP90Ms: [number, number];
         maxBlockMs: [number, number];
      };
   };
   const baselineSha = baselineSpec.startsWith("git:")
      ? git(["rev-parse", "--verify", `${baselineSpec.slice(4)}^{commit}`])
      : undefined;
   const verdict = judge(results.baseline, results.candidate, limits, {
      cpus: os.availableParallelism(),
      expectCpus: flag("expect-cpus") ? Number(flag("expect-cpus")) : undefined,
      baselineRange:
         baselineSha === anchor.commit ? anchor.calibrated : undefined,
   });
   const summary = summarize(verdict, results.baseline, results.candidate, {
      baseline: baselineSpec,
      candidate: candidateSpec,
   });
   writeFileSync(path.join(out, "summary.md"), summary);
   writeFileSync(
      path.join(out, "verdict.json"),
      JSON.stringify({ ...verdict, limits, results, dropped }, null, 2),
   );
   if (process.env.GITHUB_STEP_SUMMARY) {
      appendFileSync(process.env.GITHUB_STEP_SUMMARY, summary);
   }
   console.log(`\n${summary}\nresults: ${out}`);
   if (verdict.outcome === "invalid" && process.env.GITHUB_ACTIONS) {
      const why = verdict.checks
         .filter((c) => c.outcome === "invalid")
         .map((c) => `${c.name}: ${c.value}`)
         .join("; ")
         .replace(/\s+/g, " ")
         .slice(0, 400);
      console.log(
         `::error title=Main-thread check INVALID (not a performance verdict)::${why}`,
      );
   }
   return verdict.outcome === "pass" ? 0 : verdict.outcome === "fail" ? 1 : 2;
}

try {
   process.exit(await main());
} catch (e) {
   // Not a performance verdict: say so where a release manager looks first,
   // and leave a summary, so a failed install or boot never reads as a FAIL.
   const message = e instanceof Error ? e.message : String(e);
   console.error(e);
   const summary =
      `## Main-thread regression: harness error (not a performance verdict)\n\n` +
      `The check could not run, so it says nothing about the candidate. ` +
      `Re-run the failed jobs; if it fails the same way, read the step log ` +
      `and the server logs in the artifact.\n\n` +
      "```\n" +
      message.slice(0, 2000) +
      "\n```\n";
   if (process.env.GITHUB_ACTIONS) {
      const oneLine = message.replace(/\s+/g, " ").slice(0, 400);
      console.log(
         `::error title=Main-thread harness error (not a performance verdict)::${oneLine} (re-run the failed jobs)`,
      );
   }
   if (out) {
      mkdirSync(out, { recursive: true });
      writeFileSync(path.join(out, "summary.md"), summary);
   }
   if (process.env.GITHUB_STEP_SUMMARY) {
      appendFileSync(process.env.GITHUB_STEP_SUMMARY, summary);
   }
   process.exit(2);
}
