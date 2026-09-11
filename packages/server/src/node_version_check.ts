// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import * as fs from "node:fs";

/**
 * Minimum Node major version Publisher supports: a support policy, not a proven
 * technical minimum. Every `@malloydata/*` dependency declares `>=20`, as does
 * the repo root, so running below 20 means running on a configuration
 * Publisher's own dependencies disclaim and nothing here tests.
 *
 * `engines` cannot enforce that policy alone, which is why this check exists:
 * npm downgrades a mismatch to `npm WARN EBADENGINE` unless `engine-strict` is
 * set, it is an install-time check for a run-time condition (install on 20,
 * point a shell at 18 later), and neither the Docker image nor `bun run` from a
 * clone consults it at all.
 *
 * Every other declaration of the floor is pinned to this constant by the
 * "engines contract" block in `node_version_check.spec.ts`.
 */
export const MIN_NODE_MAJOR = 20;

/** The `engines.node` range this check enforces, for the operator-facing text. */
export const REQUIRED_NODE_RANGE = `>=${MIN_NODE_MAJOR}`;

/** Machine-readable prefix, matching PUBLISHER_READY / PUBLISHER_INIT_FAILED. */
export const UNSUPPORTED_NODE_TOKEN = "PUBLISHER_UNSUPPORTED_NODE";

export interface RuntimeVersions {
   /** `process.version`, e.g. "v18.20.8". */
   nodeVersion: string;
   /** `process.versions.bun` under Bun, undefined under Node. */
   bunVersion?: string | undefined;
}

export interface RuntimeVerdict {
   supported: boolean;
   /** Operator-facing text. Populated only when unsupported. */
   message?: string;
}

/**
 * Two deliberate exemptions:
 *
 * - **Bun is always supported.** The Docker image runs the bundle under Bun
 *   (`CMD ["bun", "run", ...]`), as does `start:dev`, and the Node version Bun
 *   reports says nothing about what Bun provides. Without this the image would
 *   refuse to boot.
 * - **An unparseable version is supported.** Refusing a working runtime over an
 *   odd version string would be worse than the failure this prevents.
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
 * Names the requirement, what is running, and the next command, because the
 * failure this replaces named none of the three. States the floor as policy: the
 * crash that exposed it is fixed in `service/query_metadata.ts`, so naming a
 * mechanism would describe something that no longer happens.
 *
 * The mise line is not over-explaining: `mise use -g node@24 && npm start` in
 * one chain re-runs on the old Node, because PATH is not re-evaluated mid-chain.
 */
function unsupportedMessage(nodeVersion: string): string {
   return [
      `${UNSUPPORTED_NODE_TOKEN} required=${REQUIRED_NODE_RANGE} detected=${nodeVersion}`,
      `Malloy Publisher supports Node.js ${MIN_NODE_MAJOR} and newer, but this process is running Node.js ${nodeVersion}.`,
      `Publisher and every Malloy library it depends on declare Node ${REQUIRED_NODE_RANGE}, so this`,
      `runtime is untested and has failed in ways that never mention Node.`,
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
 * Refuse to run on an unsupported Node, before any argument parsing, storage
 * init, or listener. Hard exit rather than a warning because the failure that
 * exposed this floor was a silent success: healthy boot log, `load_errors=0`,
 * MCP discovery served, dead on the first query. A warning would scroll past in
 * that same log.
 *
 * Writes to stderr directly, not through `logger`, so the message does not
 * depend on winston being configured.
 */
export function assertSupportedNodeVersion(
   options: AssertNodeVersionOptions = {},
): void {
   // Key presence, not `??`: these tests run under Bun, where defaulting an
   // explicitly-passed `bunVersion: undefined` back to `process.versions.bun`
   // would take the Bun exemption and assert nothing.
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
 * Synchronous, because `process.exit` does not flush an async write to a piped
 * stderr and losing the message under `npm start | tee`, CI log capture, or
 * Docker would recreate the silent failure. `writeSync` can throw EAGAIN on a
 * non-blocking fd, so the stream write is the fallback.
 */
function writeToStderr(text: string): void {
   try {
      fs.writeSync(2, text);
   } catch {
      process.stderr.write(text);
   }
}
