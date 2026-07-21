/**
 * What must not reach the published package, in one place.
 *
 * The copy, the pack audit, and the tests all have to agree exactly. When they
 * drift, one of them silently permits what another forbids: a filter that only
 * the copy applies makes the audit demand a file that will never be there, and
 * a filter only the audit applies lets the copy ship it.
 */
import * as path from "node:path";

/** Path segments of a forward-slash or platform-separated relative path. */
function segments(relative: string): string[] {
   return relative.split(/[\\/]/).filter(Boolean);
}

/**
 * `credible-*` skills are Credible-platform-specific and must never be
 * published (see skills/README.md).
 *
 * Case-insensitive, and matched at every depth: an npm tarball cannot be
 * unpublished, so this errs toward excluding a file it should not rather than
 * shipping one it must not.
 */
export function isCredible(relative: string): boolean {
   return segments(relative).some((segment) =>
      segment.toLowerCase().startsWith("credible-"),
   );
}

/**
 * Editor and OS debris. It must not enter the copy or the audit's
 * expectations: npm strips .DS_Store and friends from every tarball whatever
 * "files" says, so a stray one would be reported as a missing skill file, which
 * is what the audit says when the copy has catastrophically broken.
 *
 * The dotfile rule is deliberately broader than npm's own list (npm packs
 * .prettierrc and the like): skills are markdown, so no dotfile under skills/
 * is content.
 */
export function isNeverPacked(relative: string): boolean {
   const name = path.basename(relative);
   return (
      name.startsWith(".") || name.endsWith(".orig") || name === "npm-debug.log"
   );
}

/**
 * skills/README.md documents the internal upstream-sync process, which is not
 * what a consumer of this package needs. The package ships its own README.
 */
export function isSourceReadme(relative: string): boolean {
   return relative === "README.md";
}

/** True if a path relative to skills/ must not reach the package. */
export function isExcluded(relative: string): boolean {
   return (
      isCredible(relative) ||
      isNeverPacked(relative) ||
      isSourceReadme(relative)
   );
}
