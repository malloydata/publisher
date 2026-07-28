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
 * So this asks the registry what `latest` is and says something when we are
 * behind. Three rules, because a scaffolder that cannot work offline is worse than
 * a stale one:
 *
 *   - Fail open. Every failure path returns undefined and prints nothing. No
 *     network, DNS blocked, proxy, 500, junk body, registry down: silence.
 *   - Time-boxed, so an unreachable registry costs a second and not a hang.
 *   - Opt out with CREATE_MALLOY_PACKAGE_NO_UPDATE_CHECK, for CI and for anyone
 *     who does not want the call made at all.
 *
 * This is the only network call this package makes. It is a plain unauthenticated
 * GET of a public package document, it sends nothing about the user or their
 * files, and its result can only ever add a printed line.
 */

/** Public registry endpoint for the dist-tag `latest` manifest. */
const LATEST_URL =
   "https://registry.npmjs.org/@malloy-publisher/create-malloy-package/latest";

const TIMEOUT_MS = 1500;

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
 * The version the registry currently serves as `latest`, or undefined if we could
 * not find out for any reason at all. Never throws, never rejects.
 */
export async function fetchLatestVersion(
   url: string = LATEST_URL,
   timeoutMs: number = TIMEOUT_MS,
): Promise<string | undefined> {
   if (process.env.CREATE_MALLOY_PACKAGE_NO_UPDATE_CHECK) return undefined;
   try {
      const res = await fetch(url, {
         signal: AbortSignal.timeout(timeoutMs),
         headers: { accept: "application/json" },
      });
      if (!res.ok) return undefined;
      const body: unknown = await res.json();
      if (typeof body !== "object" || body === null) return undefined;
      const version = (body as { version?: unknown }).version;
      return typeof version === "string" ? version : undefined;
   } catch {
      // Offline, blocked, timed out, malformed: all the same answer, which is
      // that we do not know, which prints nothing.
      return undefined;
   }
}

/**
 * The warning to print, or undefined when there is nothing to say: we are current,
 * we could not reach the registry, or either version is not a plain x.y.z.
 *
 * Named for what it returns rather than what it does, because the caller decides
 * where it goes; it is the last block of the success output so it is still on
 * screen when the user reads the next steps above it.
 */
export function staleScaffolderWarning(
   running: string,
   latest: string | undefined,
): string | undefined {
   if (!latest) return undefined;
   if (isOlder(running, latest) !== true) return undefined;
   return [
      `This is create-malloy-package ${running}, and npm has ${latest}.`,
      "",
      "`npm create` resolves an unversioned name from npm's npx cache, so it reused",
      "a copy already on this machine instead of asking the registry. The workspace",
      "above was written by the older one, and the server version it pins is the one",
      `${running} pins, not the one ${latest} would.`,
      "",
      "To scaffold with the current release, name the version:",
      "  npm create @malloy-publisher/malloy-package@latest <name>",
   ].join("\n");
}
