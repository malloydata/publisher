// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { createHash } from "crypto";
import fs from "fs";
import * as path from "path";
import {
   stripLicenseHeader,
   type SkillCatalogEntry,
   type SkillEntry,
} from "./build_skills_bundle";
import { parseFrontmatter } from "./frontmatter";
import {
   ASSET_MAX_DEPTH,
   ASSET_MAX_ENTRIES,
   ASSET_PATH_MAX_CHARS,
   decodeText,
   isInlineMarkdown,
   isReferenceDir,
   listBounded,
   mayChangeSkill,
   newWalkBudget,
   REFERENCE_DIRS,
   SKILL_MD,
   skillFiles,
   underAssets,
   type WalkBudget,
} from "./skill_files";
import { compareText } from "../../compare_text";
import {
   FCJ_MAX_CHARS,
   MODEL_FILE_SUFFIX,
   NOTEBOOK_FILE_SUFFIX,
} from "../../constants";
import { canonicalSha } from "../package_revision";
import {
   confinedPackagePathSync,
   ImportOutsidePackageError,
} from "../../utils";
import { pathToFileURL } from "url";
import { callerVisibleText } from "../../package_relative_text";

/** The root directory an earlier release read as the package's skills; nothing reads it now. */
const PACKAGE_SKILLS_DIR = "skills";

/** Largest single skill file served. A markdown guide never needs more. */
export const PACKAGE_SKILL_FILE_MAX_BYTES = 256 * 1024;

/**
 * `p` resolved inside the package at `packageRoot` (spelled as `p` is: the
 * walk resolves both), by the package's own walk, which never looks outside
 * it; undefined when `p` leads out, to anything or to nothing, so what lies
 * outside decides nothing.
 */
function insidePackage(packageRoot: string, p: string): string | undefined {
   try {
      return confinedPackagePathSync(packageRoot, pathToFileURL(p));
   } catch (error) {
      if (error instanceof ImportOutsidePackageError) return undefined;
      throw error;
   }
}

const sha256 = (data: string | Buffer) =>
   createHash("sha256").update(data).digest("hex");

/** A path that is not there, loops or is too long, which an author wrote wrong; any other errno is a disk that cannot be read, which `strict` reads rethrow so it never refuses a workflow. */
const isAuthorPathError = (error: unknown) =>
   ["ENOENT", "ENOTDIR", "ELOOP", "ENAMETOOLONG"].includes(
      (error as NodeJS.ErrnoException).code ?? "",
   );

/**
 * Where a Markdown file really is and how big, or why it cannot be served; no
 * byte is read. The real path must sit inside the real package root: a symlink
 * in a package can point anywhere, and this text is returned over
 * unauthenticated MCP and REST. An in-package link to a `.md` file is followed.
 */
function markdownInside(
   packageRoot: string,
   file: string,
   strict: boolean,
): { real: string; size: number } | { problem: string; link?: true } {
   try {
      const real = insidePackage(packageRoot, file);
      if (real === undefined) {
         return {
            problem: "resolves outside the package through a link",
            link: true,
         };
      }
      // A link inside the package can still point at publisher.json or data.
      if (!real.toLowerCase().endsWith(".md")) {
         return { problem: "resolves to a file that is not Markdown" };
      }
      const stat = fs.statSync(real);
      // Reading a FIFO would block the main thread.
      if (!stat.isFile()) {
         return { problem: "resolves to something that is not a regular file" };
      }
      return { real, size: stat.size };
   } catch (error) {
      if (strict && !isAuthorPathError(error)) throw error;
      return {
         problem: `cannot be read (${callerVisibleText((error as Error).message, packageRoot)})`,
      };
   }
}

/** A Markdown file's text, or why it cannot be served; `size` is set when the file is over `maxBytes`, which is checked before it is read. */
function readFileInside(
   packageRoot: string,
   file: string,
   maxBytes: number,
   strict = false,
):
   | { text: string; raw: Buffer }
   | { problem: string; link?: true; size?: number } {
   const found = markdownInside(packageRoot, file, strict);
   if ("problem" in found) return found;
   if (found.size > maxBytes) {
      return {
         problem: `is ${found.size} bytes, over the ${maxBytes} byte cap`,
         size: found.size,
      };
   }
   try {
      const raw = fs.readFileSync(found.real);
      return { text: raw.toString("utf8"), raw };
   } catch (error) {
      if (strict && !isAuthorPathError(error)) throw error;
      return {
         problem: `cannot be read (${callerVisibleText((error as Error).message, packageRoot)})`,
      };
   }
}

/** How much of a listed file the first pass looks at to tell a binary one apart. */
const ASSET_SNIFF_BYTES = 8 * 1024;

/**
 * A listed file's bytes, or why they cannot be served. The file may not be a
 * link, and its real path must sit inside the skill's own directory, so a link
 * swapped in after the listing leads nowhere. Given `headBytes`, a file longer
 * than that is read for its first `headBytes` only (`whole` is false).
 */
function readListedBytes(
   packageRoot: string,
   skillDir: string,
   file: string,
   maxBytes: number,
   headBytes?: number,
): { bytes: Buffer; whole: boolean } | { problem: string; link?: true } {
   try {
      if (fs.lstatSync(file).isSymbolicLink()) {
         return { problem: "is a link", link: true };
      }
      const real = insidePackage(skillDir, file);
      if (real === undefined) {
         return {
            problem: "resolves outside the skill through a link",
            link: true,
         };
      }
      const stat = fs.statSync(real);
      if (!stat.isFile()) {
         return { problem: "resolves to something that is not a regular file" };
      }
      if (stat.size > maxBytes) {
         return {
            problem: `is ${stat.size} bytes, over the ${maxBytes} byte cap`,
         };
      }
      if (headBytes !== undefined && stat.size > headBytes) {
         const fd = fs.openSync(real, "r");
         try {
            const head = Buffer.alloc(headBytes);
            const read = fs.readSync(fd, head, 0, headBytes, 0);
            return { bytes: head.subarray(0, read), whole: false };
         } finally {
            fs.closeSync(fd);
         }
      }
      return { bytes: fs.readFileSync(real), whole: true };
   } catch (error) {
      return {
         problem: `cannot be read (${callerVisibleText((error as Error).message, packageRoot)})`,
      };
   }
}

/** Largest instructions file a workflow serves. */
export const WORKFLOW_INSTRUCTIONS_MAX_BYTES = 16 * 1024;

/** Largest total of a skill set's inline Markdown. A file past it is listed instead; SKILL.md files are charged first and a skill whose SKILL.md does not fit is skipped. */
export const SKILL_SET_MARKDOWN_MAX_BYTES = 256 * 1024;

/** A listed file of this size or more is refused; the draft store refuses a 1 MiB file. */
export const PACKAGE_ASSET_FILE_MAX_BYTES = 1024 * 1024;
const SKILL_SET_ASSETS_MAX_BYTES = 4 * 1024 * 1024;
const SKILL_SET_ASSETS_MAX_FILES = 200;
/** Counted once per package path, however many sets share the skill. */
const PACKAGE_ASSETS_MAX_BYTES = 8 * 1024 * 1024;
/** Individually named skip warnings per skill set; the rest are counted in one line. */
const ASSET_WARNINGS_MAX = 20;
/** Skills one set serves; the walk reads no further, so a set cannot outrun the load job's timeout. */
export const SKILLS_PER_SET_MAX = 64;
/** Individually named skill findings per set, capped like its skipped assets because every reply that carries the set carries them. */
export const SKILL_FINDINGS_MAX = 20;
const ASSET_SEGMENT = /^[A-Za-z0-9_][A-Za-z0-9._-]*$/;
/** Casefolded basenames the agent computer reads with authority, refused anywhere below a skill's root (a SKILL.md only below the root one). */
const RESERVED_BASENAMES = new Set([
   "skill.md",
   "claude.md",
   "claude.local.md",
   "agents.md",
   "publisher.json",
]);

/**
 * What a read of one listed file of a skill gives: its text; a message when
 * the server does not carry it; `binary` when it is not text; or `changed`
 * when it is no longer the file that was listed.
 */
export type SkillFileRead =
   | { text: string }
   | { message: string }
   | { binary: true }
   | { changed: true };

/** A skill served from a package or the bundle: its catalog entry and how to read a file the entry lists. */
export interface ServedSkill {
   name: string;
   description: string;
   catalog: SkillCatalogEntry;
   /** Reads a path of `catalog.files`; never joins a caller's path onto the filesystem. */
   read(path: string): Promise<SkillFileRead>;
}

/** One listed file: its path is `<skill>/<path>`, relative to its skill set. */
export interface ListedFile {
   relative_filepath: string;
   bytes: number;
   sha256: string;
   /** Not strict UTF-8 text, so a read answers without opening the file. */
   binary: boolean;
}

/** What one set's assets charge against the package-wide budget, by package-relative path. */
type AssetCharges = Map<string, { bytes: number; sha256: string }>;

/** What every set read in one load shares: the package-wide asset budget, charged once per distinct set in sorted path order. */
interface PackageAssetBudget {
   bytes: number;
   paths: Set<string>;
   /** Each listed file's hash by package path, so a skill's second pass reuses what its first read. */
   reads: Map<string, AssetRead>;
}

type AssetRead =
   | {
        sha256: string;
        bytes: number;
        binary: boolean;
        /** Text `get_skill` cannot return whole. */
        unreturnable: boolean;
     }
   /** A NUL in its first bytes made it binary: not read in full or hashed until a budget takes it. */
   | { unread: true }
   | { problem: string; link?: true };

/** Whether `get_skill` can answer with this text: it travels JSON-escaped inside a payload that is escaped again in the reply. An estimate made at load, which leaves out the reply's own envelope. */
function fitsReply(text: string): boolean {
   // Escaping grows a character at most sevenfold, so a short text needs no measuring.
   return (
      text.length <= Math.floor(FCJ_MAX_CHARS / 7) - 1 ||
      JSON.stringify(JSON.stringify(text)).length <= FCJ_MAX_CHARS
   );
}

const unreturnableWarning = (shown: string) =>
   `Package skill file '${shown}' is over the ${FCJ_MAX_CHARS}-character reply budget of get_skill, so get_skill cannot return it whole`;

/**
 * A listed file as the budgets see it. With `peek` (the first pass) a file whose
 * first bytes hold a NUL is left unread; any other is read whole and hashed,
 * and is binary when its bytes are not strict UTF-8 without a NUL.
 */
function readAsset(
   packageRoot: string,
   skillDir: string,
   file: string,
   peek: boolean,
): AssetRead {
   const maxBytes = PACKAGE_ASSET_FILE_MAX_BYTES - 1;
   let fresh = readListedBytes(
      packageRoot,
      skillDir,
      file,
      maxBytes,
      peek ? ASSET_SNIFF_BYTES : undefined,
   );
   if ("bytes" in fresh && !fresh.whole) {
      if (fresh.bytes.includes(0)) return { unread: true };
      fresh = readListedBytes(packageRoot, skillDir, file, maxBytes);
   }
   if ("problem" in fresh) return fresh;
   const text = decodeText(fresh.bytes);
   return {
      sha256: sha256(fresh.bytes),
      bytes: fresh.bytes.length,
      binary: text === undefined,
      unreturnable: text !== undefined && !fitsReply(text),
   };
}

const RESERVED_WHY =
   "is named like a file the agent computer reads with authority";
/** The agent computer refuses a skill with one of these in its path, and a POSIX file name can carry one. */
// eslint-disable-next-line no-control-regex
const CONTROL_CHARS = /[\u0000-\u001f\u007f]/;
const CONTROL_CHARS_WHY = "has a control character in its path";

/** A path as a warning shows it: control characters escaped, so a file name cannot reshape the message. */
const displayPath = (p: string) =>
   p.replace(
      new RegExp(CONTROL_CHARS, "g"),
      (c) => `\\u${c.charCodeAt(0).toString(16).padStart(4, "0")}`,
   );

/** What one skill set's listed files have taken so far, against its caps and the package's. */
interface AssetWalk {
   files: number;
   bytes: number;
   /** Bytes this walk charged that the package's budget has not recorded yet. */
   newBytes: number;
   /** Each listed or inlined path and its directories, lower-cased, to the spelling that took it: two that differ only by case collide. */
   lower: Map<string, string>;
   charges: AssetCharges;
   skipped: string[];
   /** What every set of the package shares. */
   shared: PackageAssetBudget;
}

const newWalk = (shared: PackageAssetBudget): AssetWalk => ({
   files: 0,
   bytes: 0,
   newBytes: 0,
   lower: new Map(),
   charges: new Map(),
   skipped: [],
   shared,
});

/** The skips a walk names, at most ASSET_WARNINGS_MAX of them, and one line counting the rest. */
function skippedWarnings(walk: AssetWalk): string[] {
   const named = walk.skipped.slice(0, ASSET_WARNINGS_MAX);
   if (walk.skipped.length > ASSET_WARNINGS_MAX) {
      named.push(
         `${walk.skipped.length - ASSET_WARNINGS_MAX} further assets are not served, for the same kinds of reasons`,
      );
   }
   return named;
}

/** The findings a set names, at most SKILL_FINDINGS_MAX of them, and one line counting the rest. */
function cappedFindings(findings: string[]): string[] {
   if (findings.length <= SKILL_FINDINGS_MAX) return findings;
   return [
      ...findings.slice(0, SKILL_FINDINGS_MAX),
      `${findings.length - SKILL_FINDINGS_MAX} further skill findings are not named, for the same kinds of reasons`,
   ];
}

/** A Markdown file directly in `reference/` or `references/`: the places a skill's Markdown beside SKILL.md is always served. */
const isReferenceMarkdown = (relative: string) => {
   const [dir, name, ...deeper] = relative.split("/");
   return (
      deeper.length === 0 &&
      isReferenceDir(dir!) &&
      name !== undefined &&
      name.endsWith(".md")
   );
};

/** `p` resolved, when it leads to a directory inside the package; undefined for anything else. */
function directoryInside(packageRoot: string, p: string): string | undefined {
   try {
      const real = insidePackage(packageRoot, p);
      return real !== undefined && fs.statSync(real).isDirectory()
         ? real
         : undefined;
   } catch {
      return undefined;
   }
}

/**
 * Why a skill is not served when its `assets/` is a link, or a directory
 * that does not resolve inside the skill. Settled on the real tree before any
 * `.skillignore` can hide it. Never throws; what it cannot read, skillFiles says.
 */
function assetsRefusal(
   realSkillDir: string,
   skillDir: string,
   skillRel: string,
): string | undefined {
   const assetsDir = path.join(skillDir, "assets");
   try {
      const stat = fs.lstatSync(assetsDir);
      if (stat.isSymbolicLink()) {
         return `'${skillRel}/assets' is a link; replace it with a real directory`;
      }
      if (
         stat.isDirectory() &&
         fs.realpathSync(assetsDir) !== path.join(realSkillDir, "assets")
      ) {
         return `'${skillRel}/assets' resolves outside the skill's own directory`;
      }
   } catch {
      // Absent is the common case; anything else is reported when the directory is listed.
   }
   return undefined;
}

/** A skill whose SKILL.md has been read; the rest of its directory is named but not yet checked or read. */
interface ScannedSkill {
   dirName: string;
   /** Package-relative skill directory. */
   rel: string;
   dir: string;
   skillMd: { text: string; raw: Buffer };
   /** Name, description and body as the skill serves them: the body without a license header. */
   meta: SkillEntry;
   frontmatter: Record<string, unknown>;
   skillignore: string | null;
   /** What skillFiles found besides SKILL.md. */
   candidates: string[];
   assetsRefusal?: string;
   /** Why a publisher.json inside the skill sets it aside. */
   manifestRefusal?: string;
}

interface Scan {
   packageRoot: string;
   skills: ScannedSkill[];
   warnings: string[];
   /** The set holds more skill directories than {@link SKILLS_PER_SET_MAX}; the rest were not read. */
   pastCap: boolean;
}

const skillNotServed = (rel: string, problem: string) =>
   `Package skill '${rel}/${SKILL_MD}' ${problem}, so it is not served. Fix: add a readable SKILL.md inside the package, or remove the directory.`;

/**
 * Read the skills under `<package>/<dirRel>/<name>/SKILL.md`, and name what else
 * each skill's directory holds. A skill set is read through here, once per load.
 *
 * Parsing is shared with the repo's own skills; selection is not. A package
 * ships what it contains: no manifest gating, and no `credible-*` exclusion,
 * which would silently drop a customer's skill by name. Each skill's walk takes
 * from `budget`, which the caller shares across the set's skills.
 */
function scanSkills(
   packagePath: string,
   dirRel: string,
   budget: WalkBudget,
): Scan {
   const root = path.join(packagePath, dirRel);
   const scan: Scan = {
      packageRoot: path.resolve(packagePath),
      skills: [],
      warnings: [],
      pastCap: false,
   };
   const { packageRoot } = scan;
   const manifestEntries = { left: ASSET_MAX_ENTRIES };
   const unserved = (message: string) => {
      scan.warnings.push(message);
   };

   // Settled before anything is looked at through it: a skills directory
   // that leads out is refused whatever it leads to.
   const inside = insidePackage(packageRoot, root);
   const escapes = inside === undefined;
   // The folder was checked at declaration, so a failure to list it now throws and makes the walk unavailable.
   const entries =
      inside === undefined
         ? []
         : fs.readdirSync(inside, { withFileTypes: true });
   if (escapes) {
      unserved(
         `Skills directory '${dirRel}' resolves outside the package through a link, so it is not served. Fix: copy the skills into the package.`,
      );
      return scan;
   }

   let examined = 0;
   for (const entry of [...entries].sort((a, b) =>
      compareText(a.name, b.name),
   )) {
      if (entry.name.startsWith(".")) continue;
      const skillDir = path.join(root, entry.name);
      // A skill directory that leads out is not looked through: it is refused
      // below, the same whatever it leads to.
      const skillInside = insidePackage(packageRoot, skillDir);
      if (skillInside !== undefined) {
         try {
            if (!fs.statSync(skillInside).isDirectory()) continue;
         } catch {
            continue;
         }
      }
      if (examined === SKILLS_PER_SET_MAX) {
         scan.pastCap = true;
         break;
      }
      examined++;
      const rel = `${dirRel}/${entry.name}`;
      if (skillInside === undefined) {
         unserved(
            skillNotServed(rel, "resolves outside the package through a link"),
         );
         continue;
      }
      // A SKILL.md that is a directory, a link out or unreadable leaves the
      // skill unserved, and nothing else in its directory is looked at.
      const read = readFileInside(
         packageRoot,
         path.join(skillDir, SKILL_MD),
         PACKAGE_SKILL_FILE_MAX_BYTES,
      );
      if ("problem" in read) {
         unserved(skillNotServed(rel, read.problem));
         continue;
      }
      // Its text is served and carried inline, so a lossy decode would be the skill.
      if (decodeText(read.raw) === undefined) {
         unserved(skillNotServed(rel, "is not UTF-8 text"));
         continue;
      }

      const fm = parseFrontmatter(read.text, entry.name);
      for (const w of fm.warnings) {
         scan.warnings.push(`Package skill '${rel}/${SKILL_MD}' ${w}.`);
      }
      if (!fm.description) {
         // The description is what a caller reads before deciding to fetch the
         // body, so a skill without one is invisible in every listing.
         scan.warnings.push(
            `Package skill '${fm.name}' has no description in its frontmatter, so nothing tells a caller when to read it. Fix: add a one-line 'description:' to ${rel}/${SKILL_MD}.`,
         );
      }
      // The directory name is what callers install under, so it must be the name.
      if (fm.name !== entry.name) {
         unserved(
            `Package skill '${rel}' declares the name '${fm.name}', which is not its directory name '${entry.name}', so it is not served. Fix: make 'name:' in ${rel}/${SKILL_MD} equal '${entry.name}'.`,
         );
         continue;
      }

      const found = skillFiles(skillDir, {
         budget,
         followReference: (link) => directoryInside(packageRoot, link),
      });
      for (const w of found.warnings) {
         scan.warnings.push(`Package skill '${rel}': ${w}.`);
      }
      const nested = skillManifest(packageRoot, skillDir, manifestEntries);
      scan.skills.push({
         dirName: entry.name,
         rel,
         dir: skillDir,
         skillMd: read,
         meta: {
            name: fm.name,
            description: fm.description,
            body: stripLicenseHeader(fm.body),
         },
         frontmatter: fm.frontmatter,
         skillignore: found.skillignore,
         candidates: found.files.filter((f) => f !== SKILL_MD),
         assetsRefusal: assetsRefusal(skillInside, skillDir, rel),
         ...(nested !== undefined && {
            manifestRefusal: `'${rel}/${displayPath(nested)}' is a publisher.json, which would make its directory a second package root; remove it or rename it`,
         }),
      });
   }
   return scan;
}

/** One skill's directory, split: Markdown inline, every other file listed. */
export interface ReadSkill {
   dirName: string;
   rel: string;
   meta: SkillEntry;
   skillignore: string | null;
   /** SKILL.md first, then the rest in path order; `relative` is within the skill. */
   inline: Array<{ relative: string; text: string }>;
   /** `relative_filepath` is `<skill>/<path>`. */
   listed: ListedFile[];
   catalog: SkillCatalogEntry;
}

interface FinishContext {
   packageRoot: string;
   /** Whether `get_skill` serves this set, the only tool its reply-size warnings are about. */
   advertised: boolean;
   walk: AssetWalk;
   /** Bytes of inline Markdown the set still has free. */
   inline: { left: number };
   warnings: string[];
   /** What the strict rules for `assets/` set a whole skill aside for. */
   refusals: string[];
}

/**
 * A skill between its two passes. Every skill's first pass runs before any
 * second, so what a skill has always served is charged to the budgets before
 * anything its whole directory adds, and a new file can never push an old one out.
 */
interface SkillDraft {
   s: ScannedSkill;
   inline: ReadSkill["inline"];
   files: SkillCatalogEntry["files"];
   listed: ListedFile[];
   /** What the second pass reads: Markdown outside `reference/`, and every file to list, which includes Markdown that did not fit and a binary under `assets/`. */
   later: { markdown: string[]; list: string[] };
   /** Why a Markdown file is listed in place of inlined; said only when the listing takes it, so a file the listing then skips gets one warning. */
   instead: Map<string, string>;
}

/**
 * Why a path below a skill is not served, from its name alone, and the finding
 * that sets the whole skill aside when the path sits under `assets/`. Spells
 * the path into `walk.lower`, so two paths that differ only by case collide
 * whether they are inline or listed.
 */
function nameProblem(
   s: ScannedSkill,
   relative: string,
   walk: AssetWalk,
): { why: string; refusal?: string } | undefined {
   const shown = `${s.rel}/${relative}`;
   const segments = relative.split("/");
   if (segments.some((segment) => segment.includes(".."))) {
      return {
         why: "its name has '..' in it",
         refusal: `asset '${shown}' has '..' in its name; rename it`,
      };
   }
   let spelled = s.dirName;
   for (const segment of segments) {
      spelled = `${spelled}/${segment}`;
      const taken = walk.lower.get(spelled.toLowerCase());
      if (taken !== undefined && taken !== spelled) {
         return {
            why: "its name differs from another file's only by case",
            refusal: `asset '${shown}' differs from another asset only by case; rename one`,
         };
      }
      walk.lower.set(spelled.toLowerCase(), spelled);
   }
   if (!segments.every((segment) => ASSET_SEGMENT.test(segment))) {
      return { why: "its name is outside [A-Za-z0-9_][A-Za-z0-9._-]*" };
   }
   if (`${s.dirName}/${relative}`.length > ASSET_PATH_MAX_CHARS) {
      return { why: `its path is over ${ASSET_PATH_MAX_CHARS} characters` };
   }
   return undefined;
}

/**
 * Inline a Markdown file while it fits the set's inline budget, judged by its
 * size before it is read, else list it instead. Warn when its name or its
 * bytes cannot be served at all.
 */
function inlineMarkdown(
   d: SkillDraft,
   relative: string,
   ctx: FinishContext,
): void {
   const shown = `${d.s.rel}/${relative}`;
   const problem = nameProblem(d.s, relative, ctx.walk);
   if (problem) {
      ctx.warnings.push(
         `Package skill file '${shown}' ${problem.why}, so it is not served.`,
      );
      return;
   }
   const read = readFileInside(
      ctx.packageRoot,
      path.join(d.s.dir, relative),
      Math.min(ctx.inline.left, PACKAGE_SKILL_FILE_MAX_BYTES),
   );
   if ("problem" in read && read.size === undefined) {
      ctx.warnings.push(
         `Package skill reference '${shown}' ${read.problem}, so it is not served.`,
      );
      return;
   }
   // Decoded text can outgrow the bytes it came from, so the size alone is not the last word.
   const overBudget = `would take the skill set's Markdown over the ${SKILL_SET_MARKDOWN_MAX_BYTES} byte cap`;
   const tooBig =
      "problem" in read
         ? read.size! > PACKAGE_SKILL_FILE_MAX_BYTES
            ? `is ${read.size} bytes, over the ${PACKAGE_SKILL_FILE_MAX_BYTES} byte per-file cap`
            : overBudget
         : Buffer.byteLength(read.text) > ctx.inline.left
           ? overBudget
           : undefined;
   if (tooBig !== undefined) {
      d.instead.set(relative, tooBig);
      d.later.list.push(relative);
      return;
   }
   if (!("text" in read)) return;
   // A .md that is not strict UTF-8 text is listed like any binary file, never served as lossy text.
   if (decodeText(read.raw) === undefined) {
      d.instead.set(relative, "is not UTF-8 text");
      d.later.list.push(relative);
      return;
   }
   ctx.inline.left -= Buffer.byteLength(read.text);
   d.inline.push({ relative, text: read.text });
   d.files[relative] = {
      digest: `sha256:${sha256(read.raw)}`,
      size: read.raw.length,
   };
   if (ctx.advertised && !fitsReply(read.text)) {
      ctx.warnings.push(unreturnableWarning(shown));
   }
}

const recordListed = (d: SkillDraft, relative: string, asset: ListedFile) => {
   d.listed.push(asset);
   d.files[relative] = { digest: `sha256:${asset.sha256}`, size: asset.bytes };
};

/**
 * A scanned skill's first pass: what a skill has always served. A Markdown file
 * directly in `reference/` is inlined while it fits `ctx.inline`, and a text
 * file below `assets/` is listed within the caps; every other file is left for
 * `readRest`. A file named like one the agent computer reads with authority, or
 * with a control character in its path, is skipped here with a warning.
 */
function readFirst(s: ScannedSkill, ctx: FinishContext): SkillDraft {
   const d: SkillDraft = {
      s,
      inline: [{ relative: SKILL_MD, text: s.skillMd.text }],
      // No prototype, so a root file named `__proto__` is listed like any other.
      files: Object.assign(Object.create(null) as SkillDraft["files"], {
         [SKILL_MD]: {
            digest: `sha256:${sha256(s.skillMd.raw)}`,
            size: s.skillMd.raw.length,
         },
      }),
      listed: [],
      later: { markdown: [], list: [] },
      instead: new Map(),
   };
   const references: string[] = [];
   const assetFiles: string[] = [];
   for (const relative of s.candidates) {
      const lowerName = path.posix.basename(relative).toLowerCase();
      const controlChar = CONTROL_CHARS.test(relative);
      if (!controlChar && !RESERVED_BASENAMES.has(lowerName)) {
         if (isReferenceMarkdown(relative)) references.push(relative);
         else if (isInlineMarkdown(relative)) d.later.markdown.push(relative);
         else if (underAssets(relative)) assetFiles.push(relative);
         else d.later.list.push(relative);
         continue;
      }
      const shown = displayPath(`${s.rel}/${relative}`);
      const why = controlChar ? CONTROL_CHARS_WHY : RESERVED_WHY;
      if (isInlineMarkdown(relative)) {
         ctx.warnings.push(
            `Package skill reference '${shown}' ${why}, so it is not served.`,
         );
      } else {
         ctx.walk.skipped.push(`asset '${shown}' is not served: it ${why}`);
         // A publisher.json below assets/ sets the skill aside in scanSkills; so does a SKILL.md there, here.
         if (
            !controlChar &&
            underAssets(relative) &&
            lowerName === "skill.md"
         ) {
            ctx.refusals.push(
               `asset '${shown}' is named like a package manifest or skill; remove or rename it`,
            );
         }
      }
   }
   for (const relative of references) inlineMarkdown(d, relative, ctx);
   for (const relative of assetFiles) {
      const asset = listFile(s, relative, ctx, d.later.list);
      if (asset) recordListed(d, relative, asset);
   }
   return d;
}

/**
 * A skill's second pass: what its whole directory adds. Markdown outside
 * `reference/` is inlined while it fits and listed when it does not; every file
 * left to list has its checks and caps applied and its bytes hashed, not kept.
 * Under `assets/` a link, a `..` name or a case collision sets the skill aside;
 * anywhere else the same finding skips the file with a warning.
 */
function readRest(d: SkillDraft, ctx: FinishContext): ReadSkill {
   for (const relative of d.later.markdown) inlineMarkdown(d, relative, ctx);
   for (const relative of d.later.list.sort(compareText)) {
      const asset = listFile(d.s, relative, ctx);
      if (!asset) continue;
      recordListed(d, relative, asset);
      const why = d.instead.get(relative);
      if (why !== undefined) {
         ctx.warnings.push(
            `Package skill file '${d.s.rel}/${relative}' ${why}, so it is listed instead of inlined.`,
         );
      }
   }
   const { s } = d;
   return {
      dirName: s.dirName,
      rel: s.rel,
      meta: s.meta,
      skillignore: s.skillignore,
      inline: d.inline.sort(
         (a, b) =>
            Number(b.relative === SKILL_MD) - Number(a.relative === SKILL_MD) ||
            compareText(a.relative, b.relative),
      ),
      listed: d.listed.sort((a, b) =>
         compareText(a.relative_filepath, b.relative_filepath),
      ),
      catalog: {
         name: s.meta.name,
         frontmatter: s.frontmatter,
         files: Object.fromEntries(
            Object.entries(d.files).sort(([a], [b]) => compareText(a, b)),
         ),
      },
   };
}

/**
 * One file of a skill listed for an agent to copy: its checks, its caps, then
 * its bytes hashed. Undefined when it is skipped (warned, named) or, under
 * `assets/`, set aside (`ctx.refusals`). A binary file is hashed and then, given
 * `defer`, put there to be charged later instead of now.
 */
function listFile(
   s: ScannedSkill,
   relative: string,
   ctx: FinishContext,
   defer?: string[],
): ListedFile | undefined {
   const { walk } = ctx;
   const shown = `${s.rel}/${relative}`;
   const listed = `${s.dirName}/${relative}`;
   const strict = underAssets(relative);
   const skip = (why: string) =>
      walk.skipped.push(`asset '${shown}' is not served: ${why}`);
   const reject = (refusal: string, why: string) => {
      skip(why);
      // Every refusal is found in the first pass; a later one means the file changed since, and is only skipped.
      if (strict && defer) ctx.refusals.push(refusal);
   };

   // Nothing more fits once the file cap is reached; only a file below assets/ is still looked at, for the findings that set a skill aside.
   if (!strict && walk.files >= SKILL_SET_ASSETS_MAX_FILES) {
      skip(
         `the skill set already has ${SKILL_SET_ASSETS_MAX_FILES} assets, the per-set file cap`,
      );
      return undefined;
   }

   const absolute = path.join(s.dir, relative);
   let stat: fs.Stats;
   try {
      stat = fs.lstatSync(absolute);
   } catch (error) {
      skip(
         `it cannot be read (${callerVisibleText((error as Error).message, ctx.packageRoot)})`,
      );
      return undefined;
   }
   if (stat.isSymbolicLink()) {
      reject(
         `asset '${shown}' is a link; replace it with a regular file`,
         insidePackage(ctx.packageRoot, absolute) === undefined
            ? "it is a link that resolves outside the package"
            : "it is a link",
      );
      return undefined;
   }
   const problem = nameProblem(s, relative, walk);
   if (problem) {
      if (problem.refusal) reject(problem.refusal, problem.why);
      else skip(problem.why);
      return undefined;
   }
   if (!stat.isFile()) {
      skip("it is not a regular file");
      return undefined;
   }
   const lowerName = path.posix.basename(relative).toLowerCase();
   if (
      lowerName.endsWith(MODEL_FILE_SUFFIX) ||
      lowerName.endsWith(NOTEBOOK_FILE_SUFFIX)
   ) {
      skip("a Malloy file cannot be listed");
      return undefined;
   }

   const { size } = stat;
   const charged = walk.shared.paths.has(shown);
   if (size >= PACKAGE_ASSET_FILE_MAX_BYTES) {
      skip(
         `it is ${size} bytes, at or over the ${PACKAGE_ASSET_FILE_MAX_BYTES} byte file cap`,
      );
   } else if (walk.files >= SKILL_SET_ASSETS_MAX_FILES) {
      skip(
         `the skill set already has ${SKILL_SET_ASSETS_MAX_FILES} assets, the per-set file cap`,
      );
   } else if (walk.bytes + size > SKILL_SET_ASSETS_MAX_BYTES) {
      skip(
         `it would take the skill set's assets over the ${SKILL_SET_ASSETS_MAX_BYTES} byte per-set cap`,
      );
   } else if (
      !charged &&
      walk.shared.bytes + walk.newBytes + size > PACKAGE_ASSETS_MAX_BYTES
   ) {
      skip(
         `it would take the package's assets over the ${PACKAGE_ASSETS_MAX_BYTES} byte per-package cap`,
      );
   } else {
      let read = walk.shared.reads.get(shown);
      if (!read || (!defer && "unread" in read)) {
         read = readAsset(
            ctx.packageRoot,
            s.dir,
            absolute,
            defer !== undefined,
         );
         walk.shared.reads.set(shown, read);
      }
      if ("problem" in read) {
         // A link or an escape that appeared after the listing is the same threat as one found in it.
         if (read.link) {
            reject(
               `asset '${shown}' ${read.problem}; replace it with a regular file`,
               read.problem,
            );
         } else {
            skip(read.problem);
         }
         return undefined;
      }
      if (defer && ("unread" in read || read.binary)) {
         defer.push(relative);
         return undefined;
      }
      // Only the first pass leaves a file unread, and it defers it above.
      if ("unread" in read) return undefined;
      walk.files++;
      walk.bytes += read.bytes;
      if (!charged) walk.newBytes += read.bytes;
      walk.charges.set(shown, { bytes: read.bytes, sha256: read.sha256 });
      if (ctx.advertised && read.unreturnable) {
         ctx.warnings.push(unreturnableWarning(shown));
      }
      return {
         relative_filepath: listed,
         bytes: read.bytes,
         sha256: read.sha256,
         binary: read.binary,
      };
   }
   return undefined;
}

const SHA_PREFIX = "sha256:";

/** The load-time digest of each inline file and of the `.skillignore` applied, so the content hash covers the bytes read at load and nothing reads them again. */
function inlineShas(read: ReadSkill, shas: Map<string, string>) {
   for (const f of read.inline) {
      shas.set(
         `${read.rel}/${f.relative}`,
         read.catalog.files[f.relative]!.digest.slice(SHA_PREFIX.length),
      );
   }
   if (read.skillignore !== null) {
      shas.set(`${read.rel}/.skillignore`, sha256(read.skillignore));
   }
}

/** A package skill as `get_skill` and the REST routes read it: inline Markdown from what load read, every listed file from disk against its listed sha. A listed binary file is answered from its listing. */
export function packageSkill(
   packagePath: string,
   read: ReadSkill,
): ServedSkill {
   const texts = new Map(read.inline.map((f) => [f.relative, f.text]));
   const binary = new Set(
      read.listed
         .filter((f) => f.binary)
         .map((f) => f.relative_filepath.slice(read.dirName.length + 1)),
   );
   return {
      name: read.meta.name,
      description: read.meta.description,
      catalog: read.catalog,
      async read(relative) {
         const text = texts.get(relative);
         if (text !== undefined) return { text };
         if (binary.has(relative)) return { binary: true };
         const listed = await readListedFile(
            packagePath,
            read.rel,
            relative,
            read.catalog.files[relative]!.digest.slice(SHA_PREFIX.length),
         );
         return listed ?? { changed: true };
      },
   };
}

/** The workflow whose skill sets are the package's advertised skills. */
export const DEFAULT_WORKFLOW = "default";
const WORKFLOW_NAME = /^[a-z0-9]+(-[a-z0-9]+)*$/;
const WORKFLOW_NAME_MAX = 64;
const WORKFLOW_KEYS = ["description", "instructions", "skill_sets"];
/** Skill sets one workflow lists. */
const SKILL_SETS_MAX = 8;

const isObject = (v: unknown): v is Record<string, unknown> =>
   typeof v === "object" && v !== null && !Array.isArray(v);

/** Keys that are neither known nor an `x-` author note. */
const unknownKeys = (obj: Record<string, unknown>, known: string[]) =>
   Object.keys(obj).filter((k) => !known.includes(k) && !k.startsWith("x-"));

/** Thrown inside one workflow's read to refuse that workflow with a message. */
class WorkflowProblem extends Error {}

/** A manifest path as a normalized package-relative path, or a WorkflowProblem. */
function manifestPath(value: unknown, what: string): string {
   if (typeof value !== "string" || value === "" || value.includes("\0")) {
      throw new WorkflowProblem(
         `${what} must be a non-empty package-relative path`,
      );
   }
   if (path.posix.isAbsolute(value) || path.win32.isAbsolute(value)) {
      throw new WorkflowProblem(`${what} '${value}' is an absolute path`);
   }
   if (value.includes("\\")) {
      throw new WorkflowProblem(
         `${what} '${value}' contains a backslash; write it with forward slashes`,
      );
   }
   const normalized = path.posix.normalize(value).replace(/\/+$/, "");
   if (
      normalized === "." ||
      normalized === ".." ||
      normalized.startsWith("../")
   ) {
      throw new WorkflowProblem(`${what} '${value}' is outside the package`);
   }
   return normalized;
}

const isManifest = (name: string) => name.toLowerCase() === "publisher.json";

/** The name of a publisher.json sitting directly in `dir`, which must resolve inside the package; undefined when there is none. A skill's `reference/` is read as skillFiles reads it, so a link to a huge directory is not listed whole. With `strict`, a directory that exists and cannot be read throws, since not knowing is not the same as none. */
function manifestIn(
   packageRoot: string,
   dir: string,
   reference = false,
   strict = false,
): string | undefined {
   try {
      const inside = insidePackage(packageRoot, dir);
      if (inside === undefined) return undefined;
      const names = reference
         ? listBounded(inside, (entry) => isManifest(entry.name)).entries.map(
              (entry) => entry.name,
           )
         : fs.readdirSync(inside);
      return names.find(isManifest);
   } catch (error) {
      if (strict && !isAuthorPathError(error)) throw error;
      return undefined;
   }
}

/** What one `readPackageWorkflows` call has already looked up, so a path that many workflows name is checked once. */
interface PathChecks {
   /** A directory's publisher.json, by the directory's path. */
   manifests: Map<string, string | undefined>;
   /** A skill set directory's verdict by its manifest path: what refused it, or nothing. */
   roots: Map<string, { problem?: unknown }>;
}

const newPathChecks = (): PathChecks => ({
   manifests: new Map(),
   roots: new Map(),
});

/** {@link manifestIn} for a plain directory, asked once per path in a call. */
function manifestAt(
   checks: PathChecks,
   packageRoot: string,
   dir: string,
): string | undefined {
   if (!checks.manifests.has(dir)) {
      checks.manifests.set(dir, manifestIn(packageRoot, dir, false, true));
   }
   return checks.manifests.get(dir);
}

/**
 * The path, relative to the skill directory, of the first publisher.json inside
 * a skill: in the directory, in its `reference/` or `references/`, or anywhere
 * below its `assets/`; undefined when there is none. It reads the real tree,
 * folders a `.skillignore` ignores included: that file decides what a skill is
 * made of, never what may sit in it. It follows a link only to a directory
 * inside the package, reads a reference folder or a linked `assets/` for at
 * most `REFERENCE_MAX_EXAMINED` entries, and bounds what it walks under
 * `assets/` the way skillFiles does, taking from `entries`, which the caller
 * shares across the set, so a link cannot make it scan the filesystem, list a
 * huge directory whole, or report on files outside the package.
 */
function skillManifest(
   packageRoot: string,
   skillDir: string,
   entries: { left: number },
): string | undefined {
   const below = (
      dir: string,
      rel: string,
      depth: number,
   ): string | undefined => {
      let found: fs.Dirent[];
      try {
         const inside = insidePackage(packageRoot, dir);
         if (inside === undefined) return undefined;
         // Only assets/ itself can be a link here (a linked entry is never descended); it may lead anywhere in the package.
         found = fs.lstatSync(dir).isSymbolicLink()
            ? listBounded(inside, () => false).entries
            : fs.readdirSync(inside, { withFileTypes: true });
      } catch {
         return undefined;
      }
      for (const entry of found.sort((a, b) => compareText(a.name, b.name))) {
         if (entry.name.startsWith(".")) continue;
         if (entries.left-- <= 0) return undefined;
         const relative = `${rel}/${entry.name}`;
         if (isManifest(entry.name)) return relative;
         if (entry.isDirectory() && depth < ASSET_MAX_DEPTH) {
            const deeper = below(
               path.join(dir, entry.name),
               relative,
               depth + 1,
            );
            if (deeper !== undefined) return deeper;
         }
      }
      return undefined;
   };
   for (const place of ["", ...REFERENCE_DIRS]) {
      const found = manifestIn(
         packageRoot,
         path.join(skillDir, place),
         place !== "",
      );
      if (found !== undefined) return path.posix.join(place, found);
   }
   return below(path.join(skillDir, "assets"), "assets", 1);
}

/** Read a Markdown file a workflow serves as its instructions, capped at 16 KiB. */
function readInstructions(
   packagePath: string,
   packageRoot: string,
   value: unknown,
   what: string,
   checks: PathChecks,
): { rel: string; text: string; sha: string } {
   const rel = manifestPath(value, what);
   if (!rel.toLowerCase().endsWith(".md")) {
      throw new WorkflowProblem(`${what} '${rel}' is not a .md file`);
   }
   // A nested manifest beside or above the file would make it a second package
   // root for tools that import by manifest.
   for (
      let dir = path.posix.dirname(rel);
      dir !== ".";
      dir = path.posix.dirname(dir)
   ) {
      if (manifestAt(checks, packageRoot, path.join(packagePath, dir))) {
         throw new WorkflowProblem(
            `${what} '${rel}' sits under a nested publisher.json at '${dir}'`,
         );
      }
   }
   const read = readFileInside(
      packageRoot,
      path.join(packagePath, rel),
      WORKFLOW_INSTRUCTIONS_MAX_BYTES,
      true,
   );
   if ("problem" in read) {
      throw new WorkflowProblem(`${what} '${rel}' ${read.problem}`);
   }
   // Served as text, so a lossy decode would be the instructions.
   if (decodeText(read.raw) === undefined) {
      throw new WorkflowProblem(`${what} '${rel}' is not UTF-8 text`);
   }
   return { rel, text: read.text, sha: sha256(read.raw) };
}

const emitSkillMd = (name: string, description: string, body: string) =>
   `---\nname: ${JSON.stringify(name)}\ndescription: ${JSON.stringify(description)}\n---\n\n${body}\n`;

/**
 * A listed file of the skill at `skillRel`, re-checked on every request and
 * served only when its bytes still hash to `sha`: files can change after load,
 * and a watch-mode edit does not reload. The sha is also the guard against a
 * parent directory swapped for a link, which no path check looks through. The
 * file is opened once and read through that descriptor, never more than its
 * stat size plus one byte, so a FIFO swapped in or a file that grew is refused
 * and never waited on. A binary file answers `{binary: true}`, never its bytes.
 * Undefined (a 404) when it is no longer servable.
 */
async function readListedFile(
   packagePath: string,
   skillRel: string,
   relative: string,
   sha: string,
): Promise<{ text: string } | { binary: true } | undefined> {
   let handle: fs.promises.FileHandle | undefined;
   try {
      // Windows defines neither flag: the regular-file and size checks below are the guard there.
      const flags =
         fs.constants.O_RDONLY |
         (fs.constants.O_NOFOLLOW ?? 0) |
         (fs.constants.O_NONBLOCK ?? 0);
      handle = await fs.promises.open(
         path.join(packagePath, skillRel, relative),
         flags,
      );
      const stat = await handle.stat();
      if (!stat.isFile() || stat.size > PACKAGE_ASSET_FILE_MAX_BYTES - 1) {
         return undefined;
      }
      const bytes = Buffer.allocUnsafe(stat.size + 1);
      let length = 0;
      while (length < bytes.length) {
         const { bytesRead } = await handle.read(
            bytes,
            length,
            bytes.length - length,
            length,
         );
         if (bytesRead === 0) break;
         length += bytesRead;
      }
      // One byte past the size means the file grew after the stat.
      if (length !== stat.size) return undefined;
      const read = bytes.subarray(0, length);
      if (sha256(read) !== sha) return undefined;
      const text = decodeText(read);
      return text === undefined ? { binary: true } : { text };
   } catch {
      return undefined;
   } finally {
      await handle?.close().catch(() => undefined);
   }
}

/** A skill a set serves: what was read, and the skill as `get_skill` and the REST routes read it. */
export interface SetSkill extends ReadSkill {
   served: ServedSkill;
}

/** One skill set, read once per load whichever workflows list it. */
export interface SkillSet {
   /** Its folder's name, which is what a chat picks it by. */
   name: string;
   /** Package-relative, as a workflow's manifest writes it. */
   path: string;
   /** sha256 over the set's path and each skill as a workflow serves it (files by digest, plus the applied `.skillignore`), one that another set shadows included, so it can move when a reply does not. */
   digest: string;
   skills: SetSkill[];
   /** Each finding as it is published, once per set. */
   findings: string[];
}

/** A set as a workflow serves it: a skill name an earlier set of the workflow holds is served from that set only. */
export interface WorkflowSet {
   set: SkillSet;
   skills: SetSkill[];
}

/** One workflow as the manifest declares it, resolved to what a caller adopts. */
export interface PackageWorkflow {
   name: string;
   description?: string;
   instructions?: { path: string; text: string };
   /** In the order the manifest lists them. */
   sets: WorkflowSet[];
   /** A skill name two of its sets both hold, served from the first-listed set when the workflow is read whole; a read of one set serves that set's copy, so it has none of these. */
   shadowing: string[];
   definitionSha: string;
}

export interface PackageWorkflows {
   workflows: Map<string, PackageWorkflow>;
   /** Every skill set a served workflow lists, by path in sorted order. */
   sets: Map<string, SkillSet>;
   /** Package-relative paths of every file a served workflow or set read, for the content hash. */
   paths: string[];
   /** sha256 of the bytes read of each file in `paths`, so the content hash reads nothing again. */
   contentShas: Map<string, string>;
   /** Why each refused workflow is not served, or a served one's shadowing findings, in manifest order; then each set's findings, once. */
   warnings: string[];
   /** Package-relative paths whose edit changes what is served: each skill set and each instructions file. */
   watched: string[];
   /** A root `skills/` directory exists and no workflow lists it, so nothing reads it. */
   skillsDirNotRead: boolean;
   /** Why the walk could not finish, and the workflow names the manifest declares: a declared name answers the reason, an undeclared one is still absent. */
   unavailable?: { reason: string; declared: string[] };
}

/** What a package serves before its manifest has been read: no workflows. */
export const emptyWorkflows = (): PackageWorkflows => ({
   workflows: new Map(),
   sets: new Map(),
   paths: [],
   contentShas: new Map(),
   warnings: [],
   watched: [],
   skillsDirNotRead: false,
});

const emittedSkillMd = new WeakMap<ReadSkill, string>();

/** A skill's SKILL.md as a workflow serves it, emitted once per skill. */
export function servedSkillMd(skill: ReadSkill): string {
   let text = emittedSkillMd.get(skill);
   if (text === undefined) {
      text = emitSkillMd(
         skill.meta.name,
         skill.meta.description,
         skill.meta.body,
      );
      emittedSkillMd.set(skill, text);
   }
   return text;
}

/** A skill's inline files as a workflow serves them: `<skill>/<file>` with its text, a SKILL.md re-emitted with only a quoted name and description. */
export function servedInline(skill: ReadSkill) {
   return skill.inline.map((f) => {
      const isSkillMd = f.relative === SKILL_MD;
      return {
         relative_filepath: `${skill.dirName}/${f.relative}`,
         file_contents: isSkillMd ? servedSkillMd(skill) : f.text,
         ...(isSkillMd && {
            name: skill.meta.name,
            description: skill.meta.description,
         }),
      };
   });
}

/** Records what a walk has charged in the package's budget, once per path, and clears the walk's own tally of bytes that were new. */
function chargePackage(
   budget: PackageAssetBudget,
   walk: AssetWalk,
   contentShas: Map<string, string>,
) {
   for (const [assetPath, { bytes, sha256 }] of walk.charges) {
      contentShas.set(assetPath, sha256);
      if (!budget.paths.has(assetPath)) {
         budget.paths.add(assetPath);
         budget.bytes += bytes;
      }
   }
   walk.newBytes = 0;
}

/** What a skill's first pass changed in the set's walk, so a skill set aside can be taken back out. */
interface WalkMark {
   files: number;
   bytes: number;
   newBytes: number;
   charges: number;
   lower: number;
   skipped: number;
   warnings: number;
   refusals: number;
   inline: number;
}

const markOf = (ctx: FinishContext): WalkMark => ({
   files: ctx.walk.files,
   bytes: ctx.walk.bytes,
   newBytes: ctx.walk.newBytes,
   charges: ctx.walk.charges.size,
   lower: ctx.walk.lower.size,
   skipped: ctx.walk.skipped.length,
   warnings: ctx.warnings.length,
   refusals: ctx.refusals.length,
   inline: ctx.inline.left,
});

/** Takes back what one skill's first pass charged. A Map keeps insertion order, so what the pass added is its tail. */
function rollBack(ctx: FinishContext, mark: WalkMark): void {
   const { walk } = ctx;
   walk.files = mark.files;
   walk.bytes = mark.bytes;
   walk.newBytes = mark.newBytes;
   for (const key of [...walk.charges.keys()].slice(mark.charges)) {
      walk.charges.delete(key);
   }
   for (const key of [...walk.lower.keys()].slice(mark.lower)) {
      walk.lower.delete(key);
   }
   walk.skipped.length = mark.skipped;
   ctx.warnings.length = mark.warnings;
   ctx.refusals.length = mark.refusals;
   ctx.inline.left = mark.inline;
}

const setFinding = (setPath: string, finding: string) =>
   `Skill set '${setPath}' skill finding: ${finding.replace(/\.$/, "")}.`;

const skillSetAside = (s: ScannedSkill, why: string) =>
   `Package skill '${s.rel}' is not served: ${why}`;

/**
 * Read the skill set at `setPath` once: each skill's two passes against the
 * set's own caps, then its charge to the package's. A bad skill, a skill whose
 * `assets/` breaks a strict rule, a publisher.json inside a skill and a SKILL.md
 * past the inline budget are left out with a finding; none of them refuses a
 * workflow. A throw is the caller's to turn into a refusal of the workflows that
 * list the set.
 */
function readSkillSet(
   packagePath: string,
   packageRoot: string,
   setPath: string,
   budget: PackageAssetBudget,
   advertised: boolean,
): { set: SkillSet; paths: string[]; shas: Map<string, string> } {
   const name = path.posix.basename(setPath);
   const scan = scanSkills(packagePath, setPath, newWalkBudget("skill set"));
   const found = [...scan.warnings];
   if (scan.skills.length === 0) {
      found.push(
         "the skill set holds no skills. Fix: add <skill>/SKILL.md under it or remove the entry",
      );
   }
   // A skill the strict assets/ rules set aside is only found after the budget has dropped a later skill for its SKILL.md, so the set is read again without it.
   const refused = new Map<ScannedSkill, string>();
   const fit = () => {
      const findings: string[] = [];
      const newRefusals: Array<[ScannedSkill, string]> = [];
      let overBudget = false;
      // Every SKILL.md is charged before any other Markdown, so the budget can only list a reference, never cost a skill that fit. A skill set aside is charged nothing.
      let inlineLeft = SKILL_SET_MARKDOWN_MAX_BYTES;
      const fitted: ScannedSkill[] = [];
      for (const s of scan.skills) {
         const refusal = refused.get(s) ?? s.assetsRefusal ?? s.manifestRefusal;
         if (refusal) {
            findings.push(skillSetAside(s, refusal));
            continue;
         }
         const bytes = Buffer.byteLength(s.skillMd.text);
         if (bytes > inlineLeft) {
            overBudget = true;
            findings.push(
               `Package skill '${s.rel}/${SKILL_MD}' would take the skill set's Markdown over the ${SKILL_SET_MARKDOWN_MAX_BYTES} byte cap, so it is not served. Fix: shorten the skills, or move one to another skill set`,
            );
            continue;
         }
         inlineLeft -= bytes;
         fitted.push(s);
      }
      const walk = newWalk(budget);
      const ctx: FinishContext = {
         packageRoot,
         advertised,
         walk,
         inline: { left: inlineLeft },
         warnings: [],
         refusals: [],
      };
      const drafts: SkillDraft[] = [];
      for (const s of fitted) {
         const mark = markOf(ctx);
         const draft = readFirst(s, ctx);
         if (ctx.refusals.length > mark.refusals) {
            const refusal = ctx.refusals[mark.refusals]!;
            findings.push(skillSetAside(s, refusal));
            newRefusals.push([s, refusal]);
            rollBack(ctx, mark);
            // Its SKILL.md was charged above, before this refusal was known.
            ctx.inline.left += Buffer.byteLength(s.skillMd.text);
            continue;
         }
         drafts.push(draft);
      }
      return { findings, newRefusals, overBudget, walk, ctx, drafts };
   };
   let pass = fit();
   while (pass.newRefusals.length > 0 && pass.overBudget) {
      for (const [s, refusal] of pass.newRefusals) refused.set(s, refusal);
      pass = fit();
   }
   const { walk, ctx, drafts } = pass;
   found.push(...pass.findings);
   const skills = drafts
      .map((d) => readRest(d, ctx))
      .map(
         (read): SetSkill => ({
            ...read,
            served: packageSkill(packagePath, read),
         }),
      );
   for (const skill of skills) {
      if (advertised && !fitsReply(skill.inline[0]!.text)) {
         ctx.warnings.push(unreturnableWarning(`${skill.rel}/${SKILL_MD}`));
      }
   }

   const paths: string[] = [];
   const shas = new Map<string, string>();
   for (const skill of skills) {
      inlineShas(skill, shas);
      paths.push(...skill.inline.map((f) => `${skill.rel}/${f.relative}`));
      if (skill.skillignore !== null) paths.push(`${skill.rel}/.skillignore`);
   }
   paths.push(...walk.charges.keys());
   chargePackage(budget, walk, shas);

   const digest = canonicalSha({
      path: setPath,
      skills: skills.map((skill) => ({
         name: skill.dirName,
         // What a workflow serves, not what is on disk: an inline file's text as emitted (a SKILL.md is re-emitted), every listed file by its sha.
         files: Object.fromEntries([
            ...servedInline(skill).map((f) => [
               f.relative_filepath.slice(skill.dirName.length + 1),
               sha256(f.file_contents),
            ]),
            ...skill.listed.map((a) => [
               a.relative_filepath.slice(skill.dirName.length + 1),
               a.sha256,
            ]),
         ]),
         ...(skill.skillignore !== null && { skillignore: skill.skillignore }),
      })),
   });
   return {
      set: {
         name,
         path: setPath,
         digest,
         skills,
         findings: [
            ...cappedFindings([...found, ...ctx.warnings]),
            ...(scan.pastCap
               ? [
                    `the skill set holds more than ${SKILLS_PER_SET_MAX} skill directories, so the first ${SKILLS_PER_SET_MAX} by name are served and the rest are not read. Fix: move some to another skill set`,
                 ]
               : []),
            ...skippedWarnings(walk),
         ].map((finding) => setFinding(setPath, finding)),
      },
      paths,
      shas,
   };
}

/** One workflow's declaration after its own checks: what it names, resolved to the files and directories it names. */
interface DeclaredWorkflow {
   name: string;
   description?: string;
   instructions?: { rel: string; text: string; sha: string };
   /** Package-relative skill set paths, in the order listed. */
   sets: string[];
}

/** A skill set directory's own checks, which depend on its path alone: it resolves inside the package, is a directory, has no publisher.json at or above it, and is named as a set must be. Throws a WorkflowProblem. */
function checkSkillSetRoot(
   packagePath: string,
   packageRoot: string,
   root: string,
   checks: PathChecks,
): void {
   // Settled before it is looked at: one that leads out of the package
   // is refused the same whether or not what it names exists.
   const rootInside = insidePackage(packageRoot, path.join(packagePath, root));
   if (rootInside === undefined) {
      throw new WorkflowProblem(
         `skill set directory '${root}' resolves outside the package`,
      );
   }
   let isDirectory: boolean;
   try {
      isDirectory = fs.statSync(rootInside).isDirectory();
   } catch (error) {
      if (!isAuthorPathError(error)) throw error;
      throw new WorkflowProblem(`skill set directory '${root}' does not exist`);
   }
   if (!isDirectory) {
      throw new WorkflowProblem(
         `skill set directory '${root}' is not a folder`,
      );
   }
   // One at the set's root or above makes it a second package; one inside a skill only sets that skill aside.
   for (let dir = root; dir !== "."; dir = path.posix.dirname(dir)) {
      const nested = manifestAt(
         checks,
         packageRoot,
         path.join(packagePath, dir),
      );
      if (nested === undefined) continue;
      throw new WorkflowProblem(
         dir === root
            ? `skill set directory '${root}' contains a nested publisher.json at '${displayPath(nested)}'`
            : `skill set directory '${root}' sits under a nested publisher.json at '${dir}'`,
      );
   }
   const name = path.posix.basename(root);
   if (!WORKFLOW_NAME.test(name) || name.length > WORKFLOW_NAME_MAX) {
      throw new WorkflowProblem(
         `skill set directory '${root}' is named '${name}', which must be 1-64 characters of lowercase letters, digits and single hyphens`,
      );
   }
}

/** `skill_sets` after its checks: each entry's path, in order. Throws a WorkflowProblem naming the first entry that breaks a rule. */
function declareSkillSets(
   packagePath: string,
   packageRoot: string,
   raw: unknown,
   checks: PathChecks,
): string[] {
   if (raw === undefined) return [];
   if (!Array.isArray(raw)) {
      throw new WorkflowProblem("'skill_sets' must be a list of directories");
   }
   if (raw.length > SKILL_SETS_MAX) {
      throw new WorkflowProblem(
         `'skill_sets' lists ${raw.length} directories, over the limit of ${SKILL_SETS_MAX}`,
      );
   }
   const named = new Map<string, string>();
   return raw.map((entry) => {
      const root = manifestPath(entry, "'skill_sets' entry");
      let verdict = checks.roots.get(root);
      if (!verdict) {
         verdict = {};
         try {
            checkSkillSetRoot(packagePath, packageRoot, root, checks);
         } catch (error) {
            verdict = { problem: error };
         }
         checks.roots.set(root, verdict);
      }
      if (verdict.problem !== undefined) throw verdict.problem;
      const name = path.posix.basename(root);
      const clash = named.get(name);
      if (clash !== undefined) {
         throw new WorkflowProblem(
            clash === root
               ? `skill set directory '${root}' is listed twice`
               : `skill set directories '${clash}' and '${root}' are both named '${name}'`,
         );
      }
      named.set(name, root);
      return root;
   });
}

function declareWorkflow(
   packagePath: string,
   packageRoot: string,
   name: string,
   raw: unknown,
   checks: PathChecks,
): DeclaredWorkflow {
   if (!WORKFLOW_NAME.test(name) || name.length > WORKFLOW_NAME_MAX) {
      throw new WorkflowProblem(
         "its name must be 1-64 characters of lowercase letters, digits and single hyphens",
      );
   }
   if (!isObject(raw)) throw new WorkflowProblem("its entry must be an object");
   const unrecognized = unknownKeys(raw, WORKFLOW_KEYS);
   if (unrecognized.length > 0) {
      throw new WorkflowProblem(
         `it sets '${unrecognized.join("', '")}', which this server does not know, and a key it does not know might limit the workflow. Allowed keys: ${WORKFLOW_KEYS.join(", ")}; prefix a note with 'x-' to have it ignored`,
      );
   }
   if (
      raw.description !== undefined &&
      (typeof raw.description !== "string" || raw.description.trim() === "")
   ) {
      throw new WorkflowProblem("'description' must be a non-empty string");
   }
   const instructions =
      raw.instructions === undefined
         ? undefined
         : readInstructions(
              packagePath,
              packageRoot,
              raw.instructions,
              "'instructions'",
              checks,
           );
   const sets = declareSkillSets(
      packagePath,
      packageRoot,
      raw.skill_sets,
      checks,
   );
   if (instructions === undefined && sets.length === 0) {
      throw new WorkflowProblem(
         "it declares nothing to serve: add 'instructions' or 'skill_sets'",
      );
   }
   return {
      name,
      ...(raw.description !== undefined && { description: raw.description }),
      ...(instructions && { instructions }),
      sets,
   };
}

const underSkillsDir = (value: string) => {
   const normalized = path.posix.normalize(value).replace(/\/+$/, "");
   return (
      normalized === PACKAGE_SKILLS_DIR ||
      normalized.startsWith(`${PACKAGE_SKILLS_DIR}/`)
   );
};

/** A link out of the package decides nothing, whatever it leads to. */
function hasSkillsDir(packagePath: string): boolean {
   return (
      directoryInside(
         path.resolve(packagePath),
         path.join(packagePath, PACKAGE_SKILLS_DIR),
      ) !== undefined
   );
}

/** Every set path and instructions file the manifest names, served or not, so fixing a refused workflow recompiles. Lexical: it names what to watch and reads nothing. */
function declaredPaths(raw: Record<string, unknown>): string[] {
   const found = new Set<string>();
   const add = (value: unknown) => {
      try {
         found.add(manifestPath(value, "path"));
      } catch {
         // Not a path inside the package, so there is nothing to watch.
      }
   };
   for (const entry of Object.values(raw)) {
      if (!isObject(entry)) continue;
      if (entry.instructions !== undefined) add(entry.instructions);
      if (Array.isArray(entry.skill_sets)) entry.skill_sets.forEach(add);
   }
   return [...found].sort(compareText);
}

/**
 * Whether an edit to `relative` (package-relative) can change what a workflow serves, given the `watched` paths of its
 * package: an instructions file always, a manifest (the package's own, which declares the workflows, or one at or above
 * a watched path, which refuses it), and a file inside a skill of a set unless that skill never serves it. A file
 * loose in a set's folder is no skill's. `exact` reads the skill's `.skillignore`; a watcher's per-file filter leaves it off.
 */
export function workflowFileServed(
   packagePath: string,
   watched: readonly string[],
   relative: string,
   exact: boolean,
): boolean {
   if (isManifest(path.posix.basename(relative))) {
      const dir = path.posix.dirname(relative);
      if (
         dir === "." ||
         watched.some((w) => w === dir || w.startsWith(`${dir}/`))
      ) {
         return true;
      }
   }
   return watched.some((watchedPath) => {
      if (relative === watchedPath) return true;
      if (!relative.startsWith(`${watchedPath}/`)) return false;
      const [skill, ...within] = relative
         .slice(watchedPath.length + 1)
         .split("/");
      // A dot-named folder is no skill's, as the set's scan skips it.
      return (
         within.length > 0 &&
         !skill!.startsWith(".") &&
         mayChangeSkill(
            path.join(packagePath, watchedPath, skill!),
            within.join("/"),
            exact,
         )
      );
   });
}

const refusal = (name: string, why: string) =>
   `Workflow '${name}' is not served: ${why}. Fix: correct the 'workflows.${name}' entry in publisher.json and the files it names.`;

const cannotRead = (error: unknown, packagePath: string) =>
   callerVisibleText(
      error instanceof Error ? error.message : String(error),
      packagePath,
   );

/** A walk that could not finish: nothing is served, and the declared paths stay watched so fixing the cause reloads. */
function unavailableWorkflows(
   raw: unknown,
   reason: string,
   declared?: string[],
): PackageWorkflows {
   let watched: string[] = [];
   try {
      if (isObject(raw)) {
         watched = declaredPaths(raw);
         declared ??= Object.keys(raw);
      }
   } catch {
      // The declaration is what threw; there is nothing to watch or name.
   }
   return {
      ...emptyWorkflows(),
      watched,
      unavailable: { reason, declared: declared ?? [] },
   };
}

/**
 * The workflows a package's `publisher.json` declares, each resolved to the
 * files it names. `raw` is the manifest's `workflows` value, unvalidated. This
 * never throws: a workflow that is wrong must not take the model offline, so it
 * is left out with a warning saying why and the rest still serve. A listed skill
 * set that cannot be read, or a walk that throws, is `unavailable` instead,
 * which no reader may take for a package without workflows.
 *
 * Each distinct skill set a declared workflow lists is read once, in sorted path
 * order, and charged to the package's 8 MiB cap in that order, so reordering
 * workflows or editing their instructions never moves a set's digest. Adding a
 * set can: one that sorts earlier is charged first.
 */
export function readPackageWorkflows(
   packagePath: string,
   raw: unknown,
): PackageWorkflows {
   try {
      return walkPackageWorkflows(packagePath, raw);
   } catch (error) {
      return unavailableWorkflows(raw, cannotRead(error, packagePath));
   }
}

function walkPackageWorkflows(
   packagePath: string,
   raw: unknown,
): PackageWorkflows {
   const skillsDir = hasSkillsDir(packagePath);
   // Nothing reads it until a set under it is read below.
   const result: PackageWorkflows = {
      ...emptyWorkflows(),
      skillsDirNotRead: skillsDir,
   };
   if (raw === undefined) return result;
   if (!isObject(raw)) {
      result.warnings.push(
         'Package manifest \'workflows\' must be an object keyed by workflow name, so no workflow is served. Fix: write "workflows": { "<name>": { "description": ..., "instructions": ..., "skill_sets": [...] } }.',
      );
      return result;
   }
   let packageRoot: string;
   try {
      fs.realpathSync(packagePath);
      packageRoot = path.resolve(packagePath);
   } catch (error) {
      return unavailableWorkflows(raw, cannotRead(error, packagePath));
   }
   const entries = Object.entries(raw);
   // By manifest position, so the warnings read in manifest order.
   const notes: string[][] = entries.map(() => []);
   const declared = new Map<string, DeclaredWorkflow>();
   const checks = newPathChecks();
   const refused = new Set<string>();
   let unreadable: unknown;
   entries.forEach(([name, entry], i) => {
      try {
         declared.set(
            name,
            declareWorkflow(packagePath, packageRoot, name, entry, checks),
         );
      } catch (error) {
         // Only what the author wrote refuses a workflow; a disk that cannot be read makes the walk unavailable.
         if (!(error instanceof WorkflowProblem)) {
            unreadable ??= error;
            return;
         }
         refused.add(name);
         notes[i]!.push(refusal(name, error.message));
      }
   });
   if (unreadable !== undefined) {
      return unavailableWorkflows(
         raw,
         cannotRead(unreadable, packagePath),
         entries.map(([name]) => name).filter((name) => !refused.has(name)),
      );
   }

   const budget: PackageAssetBudget = {
      bytes: 0,
      paths: new Set(),
      reads: new Map(),
   };
   const advertised = new Set(declared.get(DEFAULT_WORKFLOW)?.sets);
   const findings: string[] = [];
   const setPaths = [...new Set([...declared.values()].flatMap((w) => w.sets))];
   for (const setPath of setPaths.sort(compareText)) {
      let one: ReturnType<typeof readSkillSet>;
      try {
         one = readSkillSet(
            packagePath,
            packageRoot,
            setPath,
            budget,
            advertised.has(setPath),
         );
      } catch (error) {
         return unavailableWorkflows(
            raw,
            `skill set directory '${setPath}' cannot be read (${cannotRead(error, packagePath)})`,
            [...declared.keys()],
         );
      }
      result.sets.set(setPath, one.set);
      result.paths.push(...one.paths);
      for (const [filePath, sha] of one.shas) {
         result.contentShas.set(filePath, sha);
      }
      findings.push(...one.set.findings);
   }

   result.skillsDirNotRead =
      skillsDir && ![...result.sets.keys()].some(underSkillsDir);

   const instructionPaths: string[] = [];
   entries.forEach(([name], i) => {
      const decl = declared.get(name);
      if (!decl) return;
      const shadowing: string[] = [];
      const holder = new Map<string, string>();
      const sets = decl.sets.map((setPath): WorkflowSet => {
         const set = result.sets.get(setPath)!;
         const skills = set.skills.filter((skill) => {
            const first = holder.get(skill.dirName);
            if (first === undefined) {
               holder.set(skill.dirName, setPath);
               return true;
            }
            shadowing.push(
               `Workflow '${name}' lists skill '${skill.dirName}' in both '${first}' and '${setPath}'; the copy in '${first}', listed first, is the one served.`,
            );
            return false;
         });
         return { set, skills };
      });
      result.workflows.set(name, {
         name,
         ...(decl.description !== undefined && {
            description: decl.description,
         }),
         ...(decl.instructions && {
            instructions: {
               path: decl.instructions.rel,
               text: decl.instructions.text,
            },
         }),
         sets,
         shadowing,
         definitionSha: canonicalSha({
            name,
            ...(decl.description !== undefined && {
               description: decl.description,
            }),
            ...(decl.instructions && {
               instructionsPath: decl.instructions.rel,
               instructions: decl.instructions.text,
            }),
            skillSets: sets.map(({ set }) => ({
               path: set.path,
               digest: set.digest,
            })),
         }),
      });
      if (decl.instructions) {
         instructionPaths.push(decl.instructions.rel);
         result.contentShas.set(decl.instructions.rel, decl.instructions.sha);
      }
      notes[i]!.push(...shadowing);
   });
   result.paths.push(...instructionPaths);
   result.watched = declaredPaths(raw);
   result.warnings.push(...notes.flat(), ...findings);
   return result;
}

interface SkillSetWire extends Omit<SkillSet, "skills"> {
   skills: ReadSkill[];
}

interface WorkflowSetWire {
   set: SkillSetWire;
   skills: ReadSkill[];
}

interface PackageWorkflowWire extends Omit<PackageWorkflow, "sets"> {
   sets: WorkflowSetWire[];
}

/** A walk as it crosses the worker boundary: structured clone keeps the maps and every shared skill, but not `served`, a function, which {@link adoptWorkflows} rebuilds. */
export interface PackageWorkflowsWire
   extends Omit<PackageWorkflows, "workflows" | "sets"> {
   workflows: Map<string, PackageWorkflowWire>;
   sets: Map<string, SkillSetWire>;
}

/** `walk` without its readers, sharing each skill between the sets and workflows that hold it as the walk does. */
export function workflowsToWire(walk: PackageWorkflows): PackageWorkflowsWire {
   const skills = new Map<SetSkill, ReadSkill>();
   const skillOf = (skill: SetSkill): ReadSkill => {
      let read = skills.get(skill);
      if (!read) {
         const { served: _served, ...rest } = skill;
         read = rest;
         skills.set(skill, read);
      }
      return read;
   };
   const sets = new Map<string, SkillSetWire>();
   const setOf = (set: SkillSet): SkillSetWire => {
      let wire = sets.get(set.path);
      if (!wire) {
         wire = { ...set, skills: set.skills.map(skillOf) };
         sets.set(set.path, wire);
      }
      return wire;
   };
   for (const set of walk.sets.values()) setOf(set);
   return {
      ...walk,
      sets,
      workflows: new Map(
         [...walk.workflows].map(([name, workflow]) => [
            name,
            {
               ...workflow,
               sets: workflow.sets.map(({ set, skills }) => ({
                  set: setOf(set),
                  skills: skills.map(skillOf),
               })),
            },
         ]),
      ),
   };
}

/** A walk read in the worker, with each skill's reader rebuilt over `packagePath`. */
export function adoptWorkflows(
   packagePath: string,
   wire: PackageWorkflowsWire,
): PackageWorkflows {
   const skills = new Map<ReadSkill, SetSkill>();
   const skillOf = (read: ReadSkill): SetSkill => {
      let skill = skills.get(read);
      if (!skill) {
         skill = { ...read, served: packageSkill(packagePath, read) };
         skills.set(read, skill);
      }
      return skill;
   };
   const sets = new Map<SkillSetWire, SkillSet>();
   const setOf = (wire: SkillSetWire): SkillSet => {
      let set = sets.get(wire);
      if (!set) {
         set = { ...wire, skills: wire.skills.map(skillOf) };
         sets.set(wire, set);
      }
      return set;
   };
   return {
      ...wire,
      sets: new Map([...wire.sets].map(([path, set]) => [path, setOf(set)])),
      workflows: new Map(
         [...wire.workflows].map(([name, workflow]) => [
            name,
            {
               ...workflow,
               sets: workflow.sets.map(({ set, skills }) => ({
                  set: setOf(set),
                  skills: skills.map(skillOf),
               })),
            },
         ]),
      ),
   };
}
