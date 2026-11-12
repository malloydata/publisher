// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

// Small shared utilities for the hammer harness. Zero external deps — bun + node builtins only.

export const sleep = (ms: number): Promise<void> =>
   new Promise((r) => setTimeout(r, ms));

let quiet = false;
export const setQuiet = (q: boolean): void => {
   quiet = q;
};

const ts = (): string => new Date().toISOString().slice(11, 23);

export const log = {
   info: (msg: string): void => {
      if (!quiet) console.log(`\x1b[2m${ts()}\x1b[0m ${msg}`);
   },
   step: (msg: string): void => {
      if (!quiet) console.log(`\x1b[36m▶\x1b[0m ${msg}`);
   },
   ok: (msg: string): void => {
      if (!quiet) console.log(`\x1b[32m✓\x1b[0m ${msg}`);
   },
   warn: (msg: string): void => {
      if (!quiet) console.log(`\x1b[33m!\x1b[0m ${msg}`);
   },
   err: (msg: string): void => {
      console.log(`\x1b[31m✗\x1b[0m ${msg}`);
   },
};

/** Like `sleep`, but ends early (without throwing) once `signal` aborts. */
function sleepUnless(ms: number, signal?: AbortSignal): Promise<void> {
   if (!signal) return sleep(ms);
   return new Promise((resolve) => {
      if (signal.aborted) return resolve();
      const done = (): void => {
         clearTimeout(timer);
         signal.removeEventListener("abort", done);
         resolve();
      };
      const timer = setTimeout(done, ms);
      signal.addEventListener("abort", done);
   });
}

/**
 * Poll `fn` until it returns truthy or the deadline passes. Returns the value or
 * throws. An aborted `signal` ends the wait at once, mid-interval included; `fn`
 * should pass the same signal to anything it awaits (a fetch) so an attempt in
 * flight stops too.
 */
export async function waitFor<T>(
   what: string,
   fn: () => Promise<T | undefined | null | false>,
   opts: { timeoutMs?: number; intervalMs?: number; signal?: AbortSignal } = {},
): Promise<T> {
   const timeoutMs = opts.timeoutMs ?? 60_000;
   const intervalMs = opts.intervalMs ?? 500;
   const deadline = Date.now() + timeoutMs;
   let lastErr: unknown;
   while (Date.now() < deadline) {
      if (opts.signal?.aborted) throw new Error(`stopped waiting for ${what}`);
      try {
         const v = await fn();
         if (v) return v as T;
      } catch (e) {
         lastErr = e;
      }
      await sleepUnless(intervalMs, opts.signal);
   }
   if (opts.signal?.aborted) throw new Error(`stopped waiting for ${what}`);
   throw new Error(
      `Timed out after ${timeoutMs}ms waiting for ${what}` +
         (lastErr ? ` (last error: ${(lastErr as Error).message})` : ""),
   );
}

export interface RunResult {
   code: number;
   stdout: string;
   stderr: string;
}

/** Run a command to completion, capturing output. Never throws on nonzero exit. */
export async function run(
   cmd: string[],
   opts: { stdin?: string; cwd?: string; env?: Record<string, string> } = {},
): Promise<RunResult> {
   const proc = Bun.spawn(cmd, {
      cwd: opts.cwd,
      env: opts.env ? { ...process.env, ...opts.env } : process.env,
      stdin: opts.stdin ? new TextEncoder().encode(opts.stdin) : undefined,
      stdout: "pipe",
      stderr: "pipe",
   });
   const [stdout, stderr] = await Promise.all([
      new Response(proc.stdout).text(),
      new Response(proc.stderr).text(),
   ]);
   const code = await proc.exited;
   return { code, stdout, stderr };
}

/** Like `run`, but throws with captured output on nonzero exit. */
export async function runOrThrow(
   cmd: string[],
   opts: { stdin?: string; cwd?: string; env?: Record<string, string> } = {},
): Promise<RunResult> {
   const r = await run(cmd, opts);
   if (r.code !== 0) {
      throw new Error(
         `Command failed (${r.code}): ${cmd.join(" ")}\n${r.stdout}\n${r.stderr}`,
      );
   }
   return r;
}
