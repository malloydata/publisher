// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

// Imports only node built-ins, `ignore` and the dependency-free compareText, so the skills build scripts can load it by path.
import * as fs from "fs";
import ignore, { type Ignore } from "ignore";
import * as path from "path";
import { compareText } from "../../compare_text";

/** Longest listed asset path; checked by the caller, which knows the skill's prefix. */
export const ASSET_PATH_MAX_CHARS = 200;
/** Directories nested deeper than this are not walked. */
export const ASSET_MAX_DEPTH = 8;
/** Every entry the walk sees counts, kept or not; the rest are not walked. */
export const ASSET_MAX_ENTRIES = 1000;

const SKILL_MD = "SKILL.md";
const SKILLIGNORE = ".skillignore";
const SKILLIGNORE_MAX_BYTES = 16 * 1024;
const SKILLIGNORE_MAX_PATTERNS = 200;
const SKILLIGNORE_PATTERN_MAX_CHARS = 200;
/** Each further single `*` multiplies the match cost (about 8x under bun), so a pattern stops at this many. */
const SKILLIGNORE_MAX_SINGLE_STARS = 2;

export interface SkillFilesResult {
   /** POSIX paths relative to the skill directory, sorted. Candidates only: no per-file checks have run. */
   files: string[];
   warnings: string[];
   /** The `.skillignore` text that was applied, so a hash can fold it in; null when there is none or it was refused. */
   skillignore: string | null;
}

function errorCode(error: unknown): string {
   return (error as NodeJS.ErrnoException).code ?? "unknown error";
}

function shown(pattern: string): string {
   return JSON.stringify(
      pattern.length > 60 ? `${pattern.slice(0, 60)}...` : pattern,
   );
}

/** Dot names are where harness config lives, so no `.skillignore` rule can bring one back. */
function builtInExcluded(name: string, isDirectory: boolean): boolean {
   if (name.startsWith(".")) return true;
   const lower = name.toLowerCase();
   return isDirectory
      ? lower === "node_modules" || lower === "__pycache__"
      : lower.endsWith(".pyc");
}

/** A `**` run is not a single star. */
function singleStars(pattern: string): number {
   return pattern.replace(/\*{2,}/g, "").split("*").length - 1;
}

/** Adds the bounded patterns of `<dir>/.skillignore` to `matcher`; returns its text, or null when none was applied. */
function readSkillignore(
   dir: string,
   matcher: Ignore,
   warnings: string[],
): string | null {
   const file = path.join(dir, SKILLIGNORE);
   let stat: fs.Stats;
   try {
      stat = fs.lstatSync(file);
   } catch (error) {
      if (errorCode(error) !== "ENOENT") {
         warnings.push(`${SKILLIGNORE} cannot be read (${errorCode(error)})`);
      }
      return null;
   }
   if (!stat.isFile()) {
      warnings.push(
         `${SKILLIGNORE} is a link or not a regular file, so it is ignored`,
      );
      return null;
   }
   if (stat.size > SKILLIGNORE_MAX_BYTES) {
      warnings.push(
         `${SKILLIGNORE} is ${stat.size} bytes, over the ${SKILLIGNORE_MAX_BYTES} byte cap, so it is ignored`,
      );
      return null;
   }
   let text: string;
   try {
      text = fs.readFileSync(file, "utf8");
   } catch (error) {
      warnings.push(`${SKILLIGNORE} cannot be read (${errorCode(error)})`);
      return null;
   }

   let patterns = 0;
   let excess = 0;
   for (const line of text.replace(/^\uFEFF/, "").split(/\r?\n/)) {
      if (line.trim() === "" || line.startsWith("#")) continue;
      if (++patterns > SKILLIGNORE_MAX_PATTERNS) {
         excess++;
         continue;
      }
      if (line.length > SKILLIGNORE_PATTERN_MAX_CHARS) {
         warnings.push(
            `${SKILLIGNORE} pattern ${shown(line)} is over ${SKILLIGNORE_PATTERN_MAX_CHARS} characters, so it is skipped`,
         );
         continue;
      }
      if (singleStars(line) > SKILLIGNORE_MAX_SINGLE_STARS) {
         warnings.push(
            `${SKILLIGNORE} pattern ${shown(line)} has more than ${SKILLIGNORE_MAX_SINGLE_STARS} single '*', so it is skipped`,
         );
         continue;
      }
      if (line.startsWith("./")) {
         warnings.push(
            `${SKILLIGNORE} pattern ${shown(line)} starts with './', which matches nothing; drop the './'`,
         );
      }
      matcher.add(line);
   }
   if (excess > 0) {
      warnings.push(
         `${SKILLIGNORE} has more than ${SKILLIGNORE_MAX_PATTERNS} patterns, so the last ${excess} are skipped`,
      );
   }
   return text;
}

/**
 * The files of one skill directory: everything under it, minus the built-in
 * excludes and what its root `.skillignore` matches. A symlink is returned as a
 * candidate and never followed. An ignored directory is not descended, but
 * every entry seen still counts toward ASSET_MAX_ENTRIES; hitting a bound adds
 * a warning and keeps what was found so far.
 */
export function skillFiles(dir: string): SkillFilesResult {
   const warnings: string[] = [];
   const matcher = ignore({ ignorecase: true });
   const skillignore = readSkillignore(dir, matcher, warnings);

   const files: string[] = [];
   let visited = 0;
   let stopped = false;
   const visit = (absolute: string, rel: string, depth: number): void => {
      let entries: fs.Dirent[];
      try {
         entries = fs.readdirSync(absolute, { withFileTypes: true });
      } catch (error) {
         warnings.push(
            `directory '${rel || "."}' cannot be listed (${errorCode(error)}), so its files are not included`,
         );
         return;
      }
      entries.sort((a, b) => compareText(a.name, b.name));
      for (const entry of entries) {
         if (stopped) return;
         const relative = rel ? `${rel}/${entry.name}` : entry.name;
         if (++visited > ASSET_MAX_ENTRIES) {
            stopped = true;
            warnings.push(
               `skill walk stopped at '${relative}': the skill holds more than ${ASSET_MAX_ENTRIES} entries, so the rest are not included`,
            );
            return;
         }
         const isDirectory = entry.isDirectory();
         if (builtInExcluded(entry.name, isDirectory)) continue;
         if (matcher.ignores(isDirectory ? `${relative}/` : relative)) {
            if (relative !== SKILL_MD) continue;
            warnings.push(
               `${SKILLIGNORE} matches ${SKILL_MD}, which a skill always keeps`,
            );
         }
         if (!isDirectory) {
            files.push(relative);
         } else if (depth >= ASSET_MAX_DEPTH) {
            warnings.push(
               `directory '${relative}' is nested deeper than ${ASSET_MAX_DEPTH} levels, so its files are not included`,
            );
         } else {
            visit(path.join(absolute, entry.name), relative, depth + 1);
         }
      }
   };
   visit(dir, "", 1);

   files.sort(compareText);
   return { files, warnings, skillignore };
}
