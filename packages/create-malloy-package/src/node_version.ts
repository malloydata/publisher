/**
 * Minimum Node major version Publisher needs to serve queries.
 *
 * Declared here as well as in this package's own `engines.node` (which
 * `node_version.spec.ts` pins them together) because `engines` is only read by
 * npm at install time, and npm treats a mismatch as a warning rather than a
 * refusal unless the user has set `engine-strict=true`.
 *
 * This is a warning rather than a hard exit, unlike the server's equivalent
 * check. The scaffolder itself works fine on an older Node and the workspace it
 * writes is correct and complete; it is only *running* Publisher that fails. So
 * refusing to scaffold would block work that would have succeeded.
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
 * fine. Names the requirement and the detected version, because the failure it
 * is warning about names neither.
 *
 * Deliberately does not promise that `npm start` will refuse to boot: the
 * server version this scaffolder pins may predate the server's own boot check,
 * in which case the failure is still a 500 on the first query. "Will not work"
 * is true either way.
 */
export function nodeVersionWarning(
   versions: RuntimeVersions,
): string | undefined {
   if (versions.bunVersion) {
      return undefined;
   }
   const match = /^v?(\d+)\./.exec(versions.nodeVersion.trim());
   if (!match) {
      // Only warn about a version we could actually read.
      return undefined;
   }
   if (Number(match[1]) >= MIN_NODE_MAJOR) {
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
