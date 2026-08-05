/**
 * Minimum Node major version Publisher supports. One floor, declared in four
 * places across the repo and pinned to a single value by the "engines contract"
 * block in `packages/server/src/node_version_check.spec.ts`, which reads this
 * package's manifest too.
 */
export const MIN_NODE_MAJOR = 20;

export const REQUIRED_NODE_RANGE = `>=${MIN_NODE_MAJOR}`;

export interface RuntimeVersions {
   /** `process.version`, e.g. "v18.20.8". */
   nodeVersion: string;
   /** `process.versions.bun` when running under Bun, undefined under Node. */
   bunVersion?: string | undefined;
}

/**
 * The warning text for an unsupported Node, or undefined when the runtime is
 * fine. A warning rather than the server's hard exit: the scaffolder works on an
 * older Node and the workspace it writes is correct, so refusing would block
 * work that succeeds.
 *
 * Deliberately does not promise that `npm start` will refuse to boot, because
 * the server version this scaffolder pins may predate the server's own check.
 * "Will not work" is true either way.
 */
export function nodeVersionWarning(
   versions: RuntimeVersions,
): string | undefined {
   if (versions.bunVersion) {
      return undefined;
   }
   // Only warn about a version we could actually read.
   const match = /^v?(\d+)\./.exec(versions.nodeVersion.trim());
   if (!match || Number(match[1]) >= MIN_NODE_MAJOR) {
      return undefined;
   }
   return [
      `! Node.js ${MIN_NODE_MAJOR} or newer is required to run Publisher. This shell is running Node.js ${versions.nodeVersion}.`,
      `  The workspace is scaffolded and correct, but Publisher will not work on this Node:`,
      `  upgrade before running npm start.`,
      `    nvm:  nvm install ${MIN_NODE_MAJOR} && nvm use ${MIN_NODE_MAJOR}`,
      `    mise: mise use -g node@${MIN_NODE_MAJOR}   then open a new shell`,
   ].join("\n");
}
