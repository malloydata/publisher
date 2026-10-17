// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import * as fs from "node:fs";

/**
 * Minimum supported Node major: a support policy (every `@malloydata/*`
 * dependency declares `>=20`), not a proven technical minimum.
 *
 * `engines` alone cannot enforce it: npm only warns without `engine-strict`,
 * it is checked at install rather than run time, and neither Docker nor
 * `bun run` consults it. Every other declaration of the floor is pinned to
 * this constant by `node_version_check.spec.ts`.
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
 * Two deliberate exemptions: Bun is always supported (the Docker image and
 * `start:dev` run under it, and the Node version it reports means nothing),
 * and so is an unparseable version, since refusing a working runtime over an
 * odd string is worse than the failure this prevents.
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
 * Names the requirement, what is running, and the next command. The mise line
 * says to open a new shell because `mise use ... && npm start` in one chain
 * still runs the old Node: PATH is not re-evaluated mid-chain.
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
 * Refuse to run on an unsupported Node. A hard exit, not a warning: on an old
 * Node the boot looks healthy and the first query fails, so a warning would
 * scroll past. Writes to stderr directly so it does not depend on winston.
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
 * stderr. `writeSync` can throw EAGAIN on a non-blocking fd, hence the
 * fallback.
 */
function writeToStderr(text: string): void {
   try {
      fs.writeSync(2, text);
   } catch {
      process.stderr.write(text);
   }
}
