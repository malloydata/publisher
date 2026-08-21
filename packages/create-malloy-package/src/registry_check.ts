// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * Is this the current scaffolder?
 *
 * `npm create @malloy-publisher/malloy-package` resolves an unversioned name
 * through npm's npx cache, and any cached copy satisfies it, so on a machine that
 * has run the command before npm never asks the registry. The user gets whatever
 * version they first installed, forever, and nothing says so: the scaffolder
 * reports success either way. That is not hypothetical. A live run on 2026-07-28
 * scaffolded from a cached 0.0.1 while the registry held 0.0.2, which pinned the
 * one server release that could not read the spreadsheet it had just been seeded
 * from.
 *
 * The mechanism, measured on npm 11.12.1 against the live registry: `npm create`
 * rewrites a bare spec to `@*` before caching it, and the `_npx` slot then freezes
 * the range it first resolved (`^0.0.1`, which on 0.0.x means exactly 0.0.1). `@*`
 * is satisfied by that, so no request is made. Reproduced with the real slot: the
 * bare command served 0.0.1 and pinned server 0.0.231 with 29 skills, twice, while
 * `@latest` in the same cache got 0.0.2 and 30 skills. `npx` with a bare name
 * behaves the OPPOSITE way, recording the spec verbatim and re-resolving it, which
 * is why the server pin in scaffold.ts exists. Same cache, different subcommand.
 *
 * So this asks the registry what `latest` is and says something when we are
 * behind. Four rules, because a scaffolder that misbehaves offline is worse than
 * a stale one:
 *
 *   - Fail open. Every failure path resolves undefined and prints nothing. No
 *     network, DNS blocked, proxy, 500, junk body, timeout: silence.
 *   - Bounded on the promise always, and on process exit too once the hostname
 *     resolves. See the note on node:https below; this is not a detail. The one
 *     case it does not cover is a DNS server that accepts the query and never
 *     answers: `req.destroy()` releases a socket, and no socket exists yet, so
 *     the pending `getaddrinfo` holds the loop until the system resolver gives up
 *     (measured ~1s here, longer on a resolver with retries). `fetch` behaves
 *     identically there, since it uses the same lookup, so this is not a reason
 *     to go back; it is a limit worth stating rather than a claim to make.
 *   - Cancellable, so a run that fails before the output is written does not sit
 *     waiting on a check nobody will read.
 *   - Off by default where nobody is reading. CI and NO_UPDATE_NOTIFIER both
 *     skip the call, and CREATE_MALLOY_PACKAGE_NO_UPDATE_CHECK is ours for
 *     anyone else who does not want it made. See OPT_OUT_ENV; documented in
 *     both READMEs.
 *
 * This is the only network call this package makes. It is a plain unauthenticated
 * GET of a public package document, it sends nothing about the user or their
 * files (the local version is compared here, never transmitted), and its result
 * can only ever add a printed line.
 *
 * Why node:https and not fetch. Because `AbortSignal.timeout` bounds the promise
 * and not the socket. With fetch, against an address that drops packets rather
 * than refusing (a corporate blackhole, which is exactly the case fail-open
 * exists for), the await returns on time and then the process sits with nothing
 * left to do until undici's own connect timeout: measured on node 24, promise
 * settled at 1503ms, process exited at 10511ms. Nine silent seconds after the
 * scaffold has already printed, which reads as a hang. node:https lets us destroy
 * the request and release the handle: re-measured on this module, 1503ms to
 * settle and 1504ms to exit. Do not "simplify" this back to fetch, and see the
 * two-timer note on getBody before touching the timeouts.
 */
import * as http from "node:http";
import * as https from "node:https";
import { preview } from "./names";

/** Public registry endpoint for the dist-tag `latest` manifest. */
const LATEST_URL =
   "https://registry.npmjs.org/@malloy-publisher/create-malloy-package/latest";

const TIMEOUT_MS = 1500;

/** Enough for the manifest; a hostile endpoint does not get to stream forever. */
const MAX_BODY_BYTES = 1_000_000;

/**
 * Names that switch the check off. Any of them is enough.
 *
 * CREATE_MALLOY_PACKAGE_NO_UPDATE_CHECK is ours, and documented in both READMEs.
 *
 * CI is set by every mainstream runner, and a build has nobody to read the
 * advice: the check would be a network call and up to TIMEOUT_MS added to a job
 * that did not ask for it, and its own answer changes nothing there. This
 * repository's create-malloy-package-npm.yml scaffolds with the built bin, so
 * without this line CI made the call on every run.
 *
 * NO_UPDATE_NOTIFIER is the de-facto name in this ecosystem (npm's own
 * update-notifier honours it). Someone who has already turned these off globally
 * should not have to discover that we invented a second spelling.
 *
 * `false` and `0` mean off, not on. Plain truthiness would read `CI=false` as
 * "in CI", and that value is real: Netlify and create-react-app builds set it
 * deliberately. This does change one thing about the flag we already shipped,
 * `CREATE_MALLOY_PACKAGE_NO_UPDATE_CHECK=0` now leaves the check running, but
 * the READMEs document `=1`, and someone writing `=0` means "no, do check".
 */
const OPT_OUT_ENV = [
   "CREATE_MALLOY_PACKAGE_NO_UPDATE_CHECK",
   "CI",
   "NO_UPDATE_NOTIFIER",
] as const;

export function updateCheckDisabled(
   env: NodeJS.ProcessEnv = process.env,
): boolean {
   return OPT_OUT_ENV.some((name) => {
      const value = env[name]?.trim().toLowerCase();
      return (
         value !== undefined &&
         value !== "" &&
         value !== "0" &&
         value !== "false"
      );
   });
}

/**
 * Parse a plain `x.y.z`. Anything else (a prerelease, build metadata, a range,
 * "unknown") returns undefined, which makes the comparison decline rather than
 * guess. Declining is the safe direction: the cost is a missed warning, where
 * guessing wrong means telling someone their current install is out of date.
 */
function parseVersion(v: string): [number, number, number] | undefined {
   const m = /^(\d+)\.(\d+)\.(\d+)$/.exec(v.trim());
   if (!m) return undefined;
   return [Number(m[1]), Number(m[2]), Number(m[3])];
}

/** True when `a` is strictly older than `b`, and undefined when unknowable. */
export function isOlder(a: string, b: string): boolean | undefined {
   const left = parseVersion(a);
   const right = parseVersion(b);
   if (!left || !right) return undefined;
   for (let i = 0; i < 3; i++) {
      if (left[i] < right[i]) return true;
      if (left[i] > right[i]) return false;
   }
   return false;
}

/**
 * GET a URL, resolving the body or undefined. Never rejects, never hangs.
 *
 * Two timers, and both are needed. `{ timeout }` on the request is
 * `socket.setTimeout`, an INACTIVITY timer: every byte read resets it. A server
 * that completes the handshake, sends 200 and headers, then writes one byte every
 * few hundred milliseconds never trips it, so on its own it bounds nothing and
 * the promise never settles (measured: still unsettled at 8s on both bun and
 * node). A captive portal or a wedged mirror does exactly that. So there is also
 * a total deadline armed when the request is made, which is what actually makes
 * "never hangs" true.
 */
function getBody(
   url: string,
   timeoutMs: number,
   onCancel: (cancel: () => void) => void,
): Promise<string | undefined> {
   return new Promise((resolve) => {
      let settled = false;
      let deadline: ReturnType<typeof setTimeout> | undefined;
      const finish = (value: string | undefined): void => {
         if (settled) return;
         settled = true;
         if (deadline !== undefined) clearTimeout(deadline);
         resolve(value);
      };
      try {
         const mod = url.startsWith("http:") ? http : https;
         const req = mod.get(
            url,
            { timeout: timeoutMs, headers: { accept: "application/json" } },
            (res) => {
               if (res.statusCode !== 200) {
                  // Drain, or the socket is held open by an unread body.
                  res.resume();
                  finish(undefined);
                  return;
               }
               let body = "";
               let bytes = 0;
               res.setEncoding("utf8");
               res.on("data", (chunk: string) => {
                  body += chunk;
                  // Bytes, not chunk.length. With setEncoding the chunks are
                  // strings, and String#length counts UTF-16 code units, so a
                  // body of non-ASCII would be measured at up to half its real
                  // size and the cap would let through twice what it names.
                  bytes += Buffer.byteLength(chunk, "utf8");
                  if (bytes > MAX_BODY_BYTES) {
                     req.destroy();
                     // Settle here rather than leaving it to whatever destroy()
                     // emits. It does settle on its own, and quickly, but not
                     // by the same route on each runtime: measured against a
                     // server streaming 1MB every 5ms, node lands on res 'error'
                     // (ECONNRESET) at 12ms and bun on res 'end' at 12ms. Both
                     // are fine and neither is promised anywhere, so the one
                     // line makes the outcome ours instead of theirs.
                     finish(undefined);
                  }
               });
               res.on("end", () => finish(body));
               res.on("error", () => finish(undefined));
            },
         );
         // The total deadline. destroy(), not just resolve: this is what releases
         // the handle so the process can exit immediately rather than waiting out
         // the OS connect. It also backstops every other path, including a
         // destroy() mid-body that does not happen to emit 'error'.
         deadline = setTimeout(() => {
            req.destroy();
            finish(undefined);
         }, timeoutMs);
         req.on("timeout", () => {
            req.destroy();
            finish(undefined);
         });
         req.on("error", () => finish(undefined));
         onCancel(() => {
            req.destroy();
            finish(undefined);
         });
      } catch {
         finish(undefined);
      }
   });
}

/**
 * The version the registry currently serves as `latest`, or undefined if we could
 * not find out for any reason at all. Never throws, never rejects.
 *
 * Trimmed on the way out. `parseVersion` trims before matching, so an untrimmed
 * return would let `"\n\n\n1.2.3"` pass the check and then be printed verbatim,
 * scrolling the advice above it off a short terminal. This package already treats
 * externally-sourced strings that way everywhere else (`printable()` in names.ts,
 * whose comment records a lone CR being used to overwrite a printed line).
 */
export async function fetchLatestVersion(
   url: string = LATEST_URL,
   timeoutMs: number = TIMEOUT_MS,
   onCancel: (cancel: () => void) => void = () => {},
): Promise<string | undefined> {
   if (updateCheckDisabled()) return undefined;
   const body = await getBody(url, timeoutMs, onCancel);
   if (body === undefined) return undefined;
   try {
      const parsed: unknown = JSON.parse(body);
      if (typeof parsed !== "object" || parsed === null) return undefined;
      const version = (parsed as { version?: unknown }).version;
      return typeof version === "string" ? version.trim() : undefined;
   } catch {
      return undefined;
   }
}

/** A check in flight, and the means to give up on it. */
export interface PendingVersionCheck {
   result: Promise<string | undefined>;
   /** Stop waiting. Safe to call at any time, including after it has settled. */
   cancel: () => void;
}

/**
 * Start the check. The caller holds the cancel, because the request is fired
 * before the scaffold and a run that fails has no use for the answer: without
 * this, the error is printed and the process then sits until the deadline
 * expires, which is the "reads as a hang" symptom this module exists to avoid,
 * reached by the other path.
 */
export function startVersionCheck(
   url: string = LATEST_URL,
   timeoutMs: number = TIMEOUT_MS,
): PendingVersionCheck {
   let cancel = (): void => {};
   const result = fetchLatestVersion(url, timeoutMs, (fn) => {
      cancel = fn;
   });
   return { result, cancel: () => cancel() };
}

/**
 * The warning to print, or undefined when there is nothing to say: we are current,
 * we could not reach the registry, or either version is not a plain x.y.z.
 *
 * States the fact and the remedy, and deliberately does NOT assert why the old
 * copy is being run. The npx cache is the common cause but this fires on any
 * older version, including a deliberate `@0.0.1` pin to reproduce a bug, and
 * telling that user their cache did it is simply false. The remedy line is
 * correct on every path, which is the part that matters.
 */
export function staleScaffolderWarning(
   running: string,
   latest: string | undefined,
): string | undefined {
   if (!latest) return undefined;
   if (isOlder(running, latest) !== true) return undefined;
   // preview(), like every other externally-sourced string this package prints.
   // The regex is anchored and ASCII-only so control characters cannot get here,
   // but it puts no ceiling on length: a 200,000-digit major version parses
   // (Number() gives Infinity, which compares as older) and would otherwise be
   // echoed whole. Only npm or a MITM with a valid cert could send it, and both
   // already control the tarball we are about to run, so this is consistency
   // rather than defence.
   return [
      `This is create-malloy-package ${running}, and npm has ${preview(latest)}.`,
      "",
      "The workspace above was written by the older one, so the server version it",
      "pins is whatever that release pinned. If you did not choose this version on",
      "purpose, name it explicitly and scaffold again:",
      "  npm create @malloy-publisher/malloy-package@latest <name>",
      "",
      "(Set CREATE_MALLOY_PACKAGE_NO_UPDATE_CHECK=1 to skip this check entirely.)",
   ].join("\n");
}
