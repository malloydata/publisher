import * as fs from "node:fs";

/**
 * Minimum Node major version Publisher supports.
 *
 * Kept in step with `engines.node` in this package's package.json, which
 * `node_version_check.spec.ts` asserts. The two exist for different reasons:
 * `engines` is what npm reads at install time, this constant is what the
 * process enforces at boot. npm treats an `engines` mismatch as a warning
 * (`npm WARN EBADENGINE`) unless the user has set `engine-strict=true`, so
 * `engines` alone does not stop anybody.
 *
 * 20 rather than a higher floor because every `@malloydata/*` dependency
 * declares exactly `>=20`, as does the repo root package.json.
 */
export const MIN_NODE_MAJOR = 20;

/** The `engines.node` range this check enforces, for the operator-facing text. */
export const REQUIRED_NODE_RANGE = `>=${MIN_NODE_MAJOR}`;

/** Machine-readable prefix, matching PUBLISHER_READY / PUBLISHER_INIT_FAILED. */
export const UNSUPPORTED_NODE_TOKEN = "PUBLISHER_UNSUPPORTED_NODE";

export interface RuntimeVersions {
   /** `process.version`, e.g. "v18.20.8". */
   nodeVersion: string;
   /**
    * `process.versions.bun` when running under Bun, undefined under Node.
    * Bun reports a Node-compatible `process.version`, so it cannot be told
    * apart from Node by version string alone.
    */
   bunVersion?: string | undefined;
}

export interface RuntimeVerdict {
   supported: boolean;
   /** Operator-facing text. Populated only when unsupported. */
   message?: string;
}

/**
 * Decide whether this runtime can serve queries, and if not, say so in terms
 * an operator can act on.
 *
 * Two deliberate exemptions:
 *
 * - **Bun is always supported.** The Docker image runs the built bundle under
 *   Bun (`CMD ["bun", "run", ...]`), as does `start:dev`, and Bun provides the
 *   global Web Crypto API regardless of the Node version it reports through
 *   `process.version`. Checking the reported version without this exemption
 *   would refuse to boot the image.
 * - **An unparseable version is treated as supported.** We only guard a
 *   version we can actually read. Refusing to boot a working runtime because
 *   its version string had an unexpected shape would be a worse failure than
 *   the one this check exists to prevent.
 */
export function evaluateRuntime(versions: RuntimeVersions): RuntimeVerdict {
   if (versions.bunVersion) {
      return { supported: true };
   }

   const major = parseMajor(versions.nodeVersion);
   if (major === undefined || major >= MIN_NODE_MAJOR) {
      return { supported: true };
   }

   return {
      supported: false,
      message: unsupportedMessage(versions.nodeVersion),
   };
}

function parseMajor(version: string): number | undefined {
   const match = /^v?(\d+)\./.exec(version.trim());
   if (!match) {
      return undefined;
   }
   const major = Number(match[1]);
   return Number.isFinite(major) ? major : undefined;
}

/**
 * The whole point of this module is that the old failure named neither Node
 * nor a version, so the text names the requirement, what is actually running,
 * and the concrete next command.
 *
 * The mise line looks like over-explaining and is not: `mise use -g node@24 &&
 * npm start` in one chain re-runs on the OLD Node, because the shell does not
 * re-evaluate PATH mid-chain. That produced an identical second failure during
 * testing and would lead a user to discard the Node theory entirely.
 */
function unsupportedMessage(nodeVersion: string): string {
   return [
      `${UNSUPPORTED_NODE_TOKEN} required=${REQUIRED_NODE_RANGE} detected=${nodeVersion}`,
      `Malloy Publisher requires Node.js ${MIN_NODE_MAJOR} or newer, but this process is running Node.js ${nodeVersion}.`,
      `Older releases are missing APIs Publisher and the Malloy libraries depend on (before Node 19`,
      `there is no global Web Crypto API), so queries fail at run time with an error that never`,
      `mentions Node.`,
      `Upgrade Node, then run the command again: https://nodejs.org`,
      `  nvm:  nvm install ${MIN_NODE_MAJOR} && nvm use ${MIN_NODE_MAJOR}`,
      `  mise: mise use -g node@${MIN_NODE_MAJOR}   then open a new shell before running the command`,
      "",
   ].join("\n");
}

export interface AssertNodeVersionOptions extends Partial<RuntimeVersions> {
   /** Test seam. Defaults to writing to stderr. */
   write?: (text: string) => void;
   /** Test seam. Defaults to exiting the process with a non-zero code. */
   exit?: (code: number) => never;
}

/**
 * Refuse to run on an unsupported Node, loudly and before anything is served.
 *
 * Hard exit rather than a warning because the failure this replaces was a
 * silent success: the server booted healthy, reported `load_errors=0`, served
 * MCP discovery, and only died on the first real query with a 500 that named
 * neither Node nor a version. A warning would scroll past in that same
 * healthy-looking boot log, and a half-working server is precisely what made
 * the original defect expensive to diagnose.
 *
 * Writes to stderr directly rather than through `logger` so the message does not
 * depend on winston being configured. This module imports only `node:fs`, and
 * nothing from the app, so importing it first in server.ts costs nothing and
 * pulls no application code in ahead of the check.
 */
export function assertSupportedNodeVersion(
   options: AssertNodeVersionOptions = {},
): void {
   // Key presence, not `??`: the tests (and the whole Node leg of the manual
   // verification) run under Bun, where defaulting an explicitly-passed
   // `bunVersion: undefined` back to `process.versions.bun` would silently take
   // the Bun exemption and assert nothing.
   const verdict = evaluateRuntime({
      nodeVersion:
         "nodeVersion" in options && options.nodeVersion !== undefined
            ? options.nodeVersion
            : process.version,
      bunVersion:
         "bunVersion" in options ? options.bunVersion : process.versions.bun,
   });
   if (verdict.supported) {
      return;
   }

   const write = options.write ?? writeToStderr;
   const exit = options.exit ?? ((code: number) => process.exit(code) as never);

   try {
      write(verdict.message ?? "");
   } catch {
      // A failed stderr write must not turn a clear refusal into a crash.
   }
   exit(1);
}

/**
 * Synchronous by preference, because the next thing that happens is
 * `process.exit`. On POSIX, writes to a piped stderr are asynchronous and
 * `process.exit` does not flush them, so `process.stderr.write` can drop the
 * message under `npm start | tee`, in CI log capture, or in Docker. Losing it
 * would recreate the silent failure this whole check exists to replace.
 *
 * `writeSync` can throw EAGAIN on a non-blocking fd, so the stream write stays
 * as the fallback rather than the primary.
 */
function writeToStderr(text: string): void {
   try {
      fs.writeSync(2, text);
   } catch {
      process.stderr.write(text);
   }
}
