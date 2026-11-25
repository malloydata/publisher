// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

// Imports only node built-ins, `ignore` and the dependency-free compareText, so the skills build scripts can load it by path.
import fs from "fs";
import ignore, { type Ignore } from "ignore";
import * as path from "path";
import { compareText } from "../../compare_text";

/** Longest listed asset path; checked by the caller, which knows the skill's prefix. */
export const ASSET_PATH_MAX_CHARS = 200;
/** Directories nested deeper than this are not walked. */
export const ASSET_MAX_DEPTH = 8;
/** Entries one walk may see, kept or not; the files `WalkBudget` names as always walked are outside it, and a skill's `reference/` has its own count of Markdown files of this size. */
export const ASSET_MAX_ENTRIES = 1000;
/** Entries of any kind `listBounded` reads (a skill's `reference/`, or a linked `assets/`) before the listing stops. */
export const REFERENCE_MAX_EXAMINED = 10_000;

const SKILL_MD = "SKILL.md";
const REFERENCE_DIR = "reference";
const ASSETS_DIR = "assets";
const SKILLIGNORE = ".skillignore";
const SKILLIGNORE_MAX_BYTES = 16 * 1024;
const SKILLIGNORE_MAX_PATTERNS = 50;
const SKILLIGNORE_PATTERN_MAX_CHARS = 200;
/** Each pattern runs against every path the walk examines, so the sum of their lengths is what bounds the matching time. */
const SKILLIGNORE_MAX_TOTAL_CHARS = 1024;
/** `ignore` backtracks on each further `*` or whole-segment `**` between literals, so a pattern holds at most this many. */
const SKILLIGNORE_MAX_STARS = 2;

export interface SkillFilesResult {
   /** POSIX paths relative to the skill directory, sorted. Candidates only: no per-file checks have run. */
   files: string[];
   warnings: string[];
   /** The `.skillignore` text that was applied, so a hash can fold it in; null when there is none or it was refused. */
   skillignore: string | null;
}

/** How many more entries a walk may take. */
export interface EntryBudget {
   left: number;
}

/**
 * What the walks of one reader's skills may still take together. An entry below
 * `assets/` is charged to `assets` and any other to `other`, so a file outside
 * `assets/` can never cost one inside it. `SKILL.md` and a Markdown file
 * directly in `reference/` are charged to neither: they are always walked, and
 * bounded apart by `reference/`'s own count in each skill.
 */
export interface WalkBudget {
   assets: EntryBudget;
   other: EntryBudget;
   /** Who the budget belongs to, for the warning that it ran out. */
   owner: string;
}

export const newWalkBudget = (owner: string): WalkBudget => ({
   assets: { left: ASSET_MAX_ENTRIES },
   other: { left: ASSET_MAX_ENTRIES },
   owner,
});

export interface SkillFilesOptions {
   /** Shared by the walks of every skill a reader reads. Omitted, one fresh budget bounds this skill alone. */
   budget?: WalkBudget;
   /** Given a root `reference/` that is a link, the directory to read its Markdown from, or undefined to leave the link a plain candidate. */
   followReference?: (absolute: string) => string | undefined;
}

/** A file is text when its bytes are strict UTF-8 without a NUL; anything else is binary and is never served as a body. */
export function decodeText(bytes: Buffer): string | undefined {
   if (bytes.includes(0)) return undefined;
   try {
      // ignoreBOM keeps a leading BOM, so the text is the bytes as they are.
      return new TextDecoder("utf-8", { fatal: true, ignoreBOM: true }).decode(
         bytes,
      );
   } catch {
      return undefined;
   }
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

/** A run of whole-segment `**` means what one does, and costs `ignore` one backtracking point. */
function collapseGlobstars(pattern: string): string {
   const negated = pattern.startsWith("!");
   const segments: string[] = [];
   for (const segment of (negated ? pattern.slice(1) : pattern).split("/")) {
      if (segment !== "**" || segments.at(-1) !== "**") segments.push(segment);
   }
   return (negated ? "!" : "") + segments.join("/");
}

/** The `*` runs and whole-segment `**` of a collapsed pattern; `ignore` reads a `**` inside a segment like `*`, and an escaped star is a literal. */
function stars(pattern: string): number {
   return pattern
      .replace(/^!/, "")
      .split("/")
      .reduce(
         (count, segment) =>
            count +
            (segment === "**"
               ? 1
               : (segment.replace(/\\./g, "").match(/\*+/g)?.length ?? 0)),
         0,
      );
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
   let firstExcess = "";
   let chars = 0;
   let charsExcess = 0;
   let firstCharsExcess = "";
   for (const line of text.replace(/^\uFEFF/, "").split(/\r?\n/)) {
      if (line.trim() === "" || line.startsWith("#")) continue;
      if (++patterns > SKILLIGNORE_MAX_PATTERNS) {
         if (excess++ === 0) firstExcess = line;
         continue;
      }
      if (line.length > SKILLIGNORE_PATTERN_MAX_CHARS) {
         warnings.push(
            `${SKILLIGNORE} pattern ${shown(line)} is over ${SKILLIGNORE_PATTERN_MAX_CHARS} characters, so it is skipped`,
         );
         continue;
      }
      const collapsed = collapseGlobstars(line);
      if (stars(collapsed) > SKILLIGNORE_MAX_STARS) {
         warnings.push(
            `${SKILLIGNORE} pattern ${shown(line)} has more than ${SKILLIGNORE_MAX_STARS} '*' or '**' segments, so it is skipped`,
         );
         continue;
      }
      // Once the sum is spent every later pattern is skipped, so the warning's "last N" is true.
      if (
         charsExcess > 0 ||
         chars + line.length > SKILLIGNORE_MAX_TOTAL_CHARS
      ) {
         if (charsExcess++ === 0) firstCharsExcess = line;
         continue;
      }
      chars += line.length;
      if (line.startsWith("./")) {
         warnings.push(
            `${SKILLIGNORE} pattern ${shown(line)} starts with './', which matches nothing; drop the './'`,
         );
      }
      matcher.add(collapsed);
   }
   if (excess > 0) {
      warnings.push(
         `${SKILLIGNORE} has more than ${SKILLIGNORE_MAX_PATTERNS} patterns, so the last ${excess} are skipped, from ${shown(firstExcess)}`,
      );
   }
   if (charsExcess > 0) {
      warnings.push(
         `${SKILLIGNORE} patterns total more than ${SKILLIGNORE_MAX_TOTAL_CHARS} characters, so the last ${charsExcess} are skipped, from ${shown(firstCharsExcess)}`,
      );
   }
   return text;
}

/**
 * The entries of a directory a link can make huge, in opendir order: at most
 * `REFERENCE_MAX_EXAMINED` of them (`cut` says the bound stopped it), and none
 * after the first that `last` accepts. It reads a skill's `reference/`, real or
 * linked, for the served set and the publisher.json refusal alike, so both read
 * it in the same order to the same bound; and a linked `assets/` for the
 * refusal.
 */
export function listBounded(
   absolute: string,
   last: (entry: fs.Dirent) => boolean,
): { entries: fs.Dirent[]; cut: boolean } {
   const entries: fs.Dirent[] = [];
   const opened = fs.opendirSync(absolute);
   try {
      for (
         let entry = opened.readSync();
         entry !== null;
         entry = opened.readSync()
      ) {
         // opendir does not sort, so which entries survive a directory over the bound is unspecified.
         if (entries.length >= REFERENCE_MAX_EXAMINED) {
            return { entries, cut: true };
         }
         entries.push(entry);
         if (last(entry)) break;
      }
   } finally {
      opened.closeSync();
   }
   return { entries, cut: false };
}

/** Where a directory sits, which decides what an entry in it is charged to. */
type Zone = "root" | "reference" | "linked reference" | "assets" | "other";

/**
 * The files of one skill directory: everything under it, minus the built-in
 * excludes and what its root `.skillignore` matches. A symlink is returned as a
 * candidate and never followed, but for a root `reference/` that
 * `followReference` resolves, which is read for its Markdown alone. At the root
 * `SKILL.md` comes first, then `reference/`, then `assets/`, then the rest, so
 * a bound cuts the newest locations first. An ignored directory is not
 * descended, but every entry seen still counts against the budget; hitting a
 * bound adds a warning and keeps what was found so far. The Markdown files
 * directly in `reference/` are bounded by a count of their own, linked or not
 * and ignored or not, and `reference/` is listed no further than that, nor past
 * `REFERENCE_MAX_EXAMINED` entries of any kind.
 */
export function skillFiles(
   dir: string,
   options: SkillFilesOptions = {},
): SkillFilesResult {
   const warnings: string[] = [];
   const matcher = ignore({ ignorecase: true });
   const skillignore = readSkillignore(dir, matcher, warnings);

   const alone = { left: ASSET_MAX_ENTRIES };
   const budget = options.budget ?? {
      assets: alone,
      other: alone,
      owner: "skill",
   };
   const exhausted = new Set<EntryBudget>();
   const taken = new Set<EntryBudget>();
   const take = (counter: EntryBudget, relative: string): boolean => {
      if (counter.left > 0) {
         counter.left--;
         taken.add(counter);
         return true;
      }
      if (!exhausted.has(counter)) {
         exhausted.add(counter);
         warnings.push(
            taken.has(counter)
               ? `skill walk stopped at '${relative}': more than ${ASSET_MAX_ENTRIES} entries were walked, so the rest are not included`
               : `skill walk stopped at '${relative}': the ${budget.owner}'s walk budget of ${ASSET_MAX_ENTRIES} entries was already spent on earlier skills, so the rest are not included`,
         );
      }
      return false;
   };
   // The always-walked reference/ examines this many Markdown files, ignored or not; what it holds besides is not counted.
   const reference = { left: ASSET_MAX_ENTRIES, warned: false };
   const takeReference = (relative: string): boolean => {
      if (reference.left > 0) {
         reference.left--;
         return true;
      }
      if (!reference.warned) {
         reference.warned = true;
         warnings.push(
            `skill walk stopped at '${relative}': ${REFERENCE_DIR}/ holds more than ${ASSET_MAX_ENTRIES} Markdown files, so the rest are not read`,
         );
      }
      return false;
   };
   // Counted whether or not `.skillignore` matches it, so an ignored file cannot be examined for free.
   const countedMarkdown = (entry: fs.Dirent) =>
      entry.name.endsWith(".md") &&
      !entry.isDirectory() &&
      !builtInExcluded(entry.name, false);
   // Stops one Markdown file past the cap, so a link to a directory of Markdown is not listed in full.
   const listMarkdownCapped = (absolute: string, rel: string) => {
      let markdown = 0;
      const { entries, cut } = listBounded(
         absolute,
         (entry) => countedMarkdown(entry) && ++markdown > ASSET_MAX_ENTRIES,
      );
      if (cut) {
         warnings.push(
            `skill walk stopped in '${rel}': ${REFERENCE_DIR}/ holds more than ${REFERENCE_MAX_EXAMINED} entries, so the rest are not read`,
         );
      }
      return entries;
   };
   const rank = (name: string) => {
      const place = [SKILL_MD, REFERENCE_DIR, ASSETS_DIR].indexOf(name);
      return place < 0 ? 3 : place;
   };

   const files: string[] = [];
   const visit = (
      absolute: string,
      rel: string,
      depth: number,
      zone: Zone,
   ): void => {
      let entries: fs.Dirent[];
      try {
         entries =
            zone === "reference" || zone === "linked reference"
               ? listMarkdownCapped(absolute, rel)
               : fs.readdirSync(absolute, { withFileTypes: true });
      } catch (error) {
         warnings.push(
            `directory '${rel || "."}' cannot be listed (${errorCode(error)}), so its files are not included`,
         );
         return;
      }
      if (zone === "root") {
         entries.sort(
            (a, b) =>
               rank(a.name) - rank(b.name) || compareText(a.name, b.name),
         );
      } else {
         entries.sort((a, b) => compareText(a.name, b.name));
      }
      for (const entry of entries) {
         const relative = rel ? `${rel}/${entry.name}` : entry.name;
         const linked =
            zone === "root" &&
            entry.name === REFERENCE_DIR &&
            entry.isSymbolicLink()
               ? options.followReference?.(path.join(absolute, entry.name))
               : undefined;
         const isDirectory = entry.isDirectory() || linked !== undefined;
         // What this entry costs the budget, and the zone of what it holds.
         let cost: EntryBudget | undefined = budget.other;
         let inside: Zone = "other";
         let childDepth = depth + 1;
         switch (zone) {
            case "linked reference":
               if (isDirectory || !entry.name.endsWith(".md")) continue;
               cost = undefined;
               break;
            case "reference":
               if (!isDirectory && entry.name.endsWith(".md")) cost = undefined;
               break;
            case "assets":
               inside = "assets";
               cost = entry.name.startsWith(".") ? undefined : budget.assets;
               break;
            case "root":
               if (entry.name === SKILL_MD && !isDirectory) {
                  cost = undefined;
               } else if (entry.name === REFERENCE_DIR && isDirectory) {
                  cost = undefined;
                  inside =
                     linked === undefined ? "reference" : "linked reference";
               } else if (entry.name === ASSETS_DIR && isDirectory) {
                  cost = undefined;
                  inside = "assets";
                  // Depth below assets/ is counted from assets/ itself.
                  childDepth = 1;
               }
               break;
         }
         if (cost && !take(cost, relative)) continue;
         if (builtInExcluded(entry.name, isDirectory)) continue;
         if (
            (zone === "reference" || zone === "linked reference") &&
            countedMarkdown(entry) &&
            !takeReference(relative)
         ) {
            continue;
         }
         if (matcher.ignores(isDirectory ? `${relative}/` : relative)) {
            if (relative !== SKILL_MD || isDirectory) continue;
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
            visit(
               linked ?? path.join(absolute, entry.name),
               relative,
               childDepth,
               inside,
            );
         }
      }
   };
   visit(dir, "", 1, "root");

   files.sort(compareText);
   return { files, warnings, skillignore };
}

/**
 * A skill's files for a build that copies or embeds them. `skillFiles` returns
 * a link as a candidate, and a build would follow it to whatever it names, so
 * the root and every file must be a regular file; a walk finding throws too,
 * since it would leave the copy or the manifest quietly short.
 */
export function buildTimeSkillFiles(dir: string): string[] {
   const name = path.basename(dir);
   const only = "a bundled skill holds regular files only";
   if (fs.lstatSync(dir).isSymbolicLink()) {
      throw new Error(`skill '${name}' is a link; ${only}`);
   }
   const { files, warnings } = skillFiles(dir);
   if (warnings.length > 0) {
      throw new Error(`skill '${name}': ${warnings.join("; ")}`);
   }
   for (const file of files) {
      const stat = fs.lstatSync(path.join(dir, file));
      if (!stat.isFile()) {
         throw new Error(
            `skill '${name}': '${file}' is ${stat.isSymbolicLink() ? "a link" : "not a regular file"}; ${only}`,
         );
      }
   }
   return files;
}
