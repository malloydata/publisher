// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * The last `/segment` of the run of non-empty path segments at the start of
 * `afterRepo`, or undefined (the capture semantics of a `(\/[^/]+)*` group).
 */
function lastGitHubPathSegment(afterRepo: string): string | undefined {
   if (!afterRepo.startsWith("/")) return undefined;
   const run: string[] = [];
   for (const seg of afterRepo.slice(1).split("/")) {
      if (seg === "") break;
      run.push(seg);
   }
   return run.length > 0 ? `/${run[run.length - 1]}` : undefined;
}

/**
 * Parse a GitHub package location into owner, repo, and (optionally) the
 * subdirectory path segment.
 *
 * String ops rather than a `(\/[^/]+)*` regex, which backtracks
 * polynomially (ReDoS) on a user-supplied location. `packagePath` is only the
 * LAST path segment after the repo, a quirk callers rely on;
 * environment_store.spec.ts fuzzes this against the reference regexes.
 */
export function parseGitHubUrl(
   githubUrl: string,
): { owner: string; repoName: string; packagePath?: string } | null {
   // HTTPS: https://github.com/owner/repo/tree/branch/subdir; unanchored.
   const httpsMatch = /github\.com\/([^/]+)\/([^/]+)/.exec(githubUrl);
   if (httpsMatch) {
      const afterRepo = githubUrl.slice(
         httpsMatch.index + httpsMatch[0].length,
      );
      return {
         owner: httpsMatch[1],
         repoName: httpsMatch[2],
         packagePath: lastGitHubPathSegment(afterRepo),
      };
   }

   // SSH: git@github.com:owner/repo(.git)?(/subdir)*, unanchored at the start
   // but $-anchored: a trailing slash or empty segment fails the parse.
   const SSH_PREFIX = "git@github.com:";
   const sshAt = githubUrl.indexOf(SSH_PREFIX);
   if (sshAt !== -1) {
      const rest = githubUrl.slice(sshAt + SSH_PREFIX.length);
      const firstSlash = rest.indexOf("/");
      if (firstSlash > 0) {
         const owner = rest.slice(0, firstSlash);
         const afterOwner = rest.slice(firstSlash + 1);
         const secondSlash = afterOwner.indexOf("/");
         const repoSeg =
            secondSlash === -1 ? afterOwner : afterOwner.slice(0, secondSlash);
         const afterRepo =
            secondSlash === -1 ? "" : afterOwner.slice(secondSlash);
         // A repo segment containing whitespace is not a GitHub URL.
         if (repoSeg.length > 0 && !/\s/.test(repoSeg)) {
            const repoName =
               repoSeg.length > 4 && repoSeg.endsWith(".git")
                  ? repoSeg.slice(0, -4)
                  : repoSeg;
            if (afterRepo === "") {
               return { owner, repoName };
            }
            const segments = afterRepo.slice(1).split("/");
            // $-anchored: every trailing segment must be non-empty.
            if (!segments.includes("")) {
               return {
                  owner,
                  repoName,
                  packagePath: `/${segments[segments.length - 1]}`,
               };
            }
         }
      }
   }

   return null;
}
