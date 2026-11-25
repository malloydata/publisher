// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { createHash } from "crypto";
import fs from "fs";
import * as path from "path";
import {
   stripLicenseHeader,
   type BundledSkill,
   type SkillCatalogEntry,
   type SkillEntry,
} from "./build_skills_bundle";
import { parseFrontmatter } from "./frontmatter";
import {
   ASSET_MAX_DEPTH,
   ASSET_MAX_ENTRIES,
   ASSET_PATH_MAX_CHARS,
   decodeText,
   listBounded,
   newWalkBudget,
   skillFiles,
   type WalkBudget,
} from "./skill_files";
import skillsBundle from "./skills_bundle.json";
import { compareText } from "../../compare_text";
import { MODEL_FILE_SUFFIX, NOTEBOOK_FILE_SUFFIX } from "../../constants";
import { CronEvaluator } from "../materialization/run/cron_evaluator";
import { canonicalSha } from "../package_revision";
import {
   confinedPackagePathSync,
   ImportOutsidePackageError,
} from "../../utils";
import { pathToFileURL } from "url";
import { callerVisibleText } from "../../package_relative_text";

/** The build-time bundle of the repo's skills: each one's manifest, frontmatter and Markdown text. */
export const BUNDLED_CATALOG = (
   skillsBundle as unknown as { skills: BundledSkill[] }
).skills;

/** The directory a package keeps its own agent skills in. */
export const PACKAGE_SKILLS_DIR = "skills";

/** Where a skill came from, once the bundled and package sets are resolved. */
type SkillOrigin = "bundled" | "package";

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

/** Why a listed file that changed since the package loaded is not served. */
export function fileChangedMessage(relative: string): string {
   return `'${relative}' changed on disk after the package loaded, so it is not served until the package reloads.`;
}

/** A skill served from a package or the bundle: its catalog entry and how to read a file the entry lists. */
export interface ServedSkill {
   name: string;
   description: string;
   catalog: SkillCatalogEntry;
   /** Reads a path of `catalog.files`; never joins a caller's path onto the filesystem. */
   read(path: string): SkillFileRead;
}

export interface ResolvedSkill extends ServedSkill {
   origin: SkillOrigin;
}

interface PackageSkills {
   skills: ServedSkill[];
   /** The inline Markdown read, package-relative path and raw text. */
   files: Array<{ path: string; text: string }>;
   /** Every package-relative path the package's content hash covers: `files`, each listed file and each applied `.skillignore`. */
   paths: string[];
   /** sha256 of each listed file in `paths`, hashed at load so the content hash does not read it again. */
   contentShas: Map<string, string>;
   /**
    * Non-fatal findings. Nothing here throws: a failed package load is skipped
    * entirely, and a malformed markdown file must not cost a working model.
    */
   warnings: string[];
   /** The subset of `warnings` that leave a skill unserved, which an agent cannot be served without. */
   blocking: string[];
}

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

/**
 * Where a Markdown file really is and how big, or why it cannot be served; no
 * byte is read. The real path must sit inside the real package root: a symlink
 * in a package can point anywhere, and this text is returned over
 * unauthenticated MCP and REST. An in-package link to a `.md` file is followed.
 */
function markdownInside(
   packageRoot: string,
   file: string,
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
):
   | { text: string; raw: Buffer }
   | { problem: string; link?: true; size?: number } {
   const found = markdownInside(packageRoot, file);
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

/** Largest instructions or task file an agent serves. */
export const PACKAGE_AGENT_TEXT_MAX_BYTES = 16 * 1024;

/** Largest total of an agent's inline skill files. Markdown past it is listed instead; SKILL.md files that alone pass it drop the agent. */
export const PACKAGE_AGENT_SKILLS_MAX_BYTES = 256 * 1024;

/** A listed file of this size or more is refused; the draft store refuses a 1 MiB file. */
const PACKAGE_ASSET_FILE_MAX_BYTES = 1024 * 1024;
const PACKAGE_AGENT_ASSETS_MAX_BYTES = 4 * 1024 * 1024;
const PACKAGE_AGENT_ASSETS_MAX_FILES = 200;
/** A package's own skills list files against budgets of their own, apart from the agents' package-wide one. */
const PACKAGE_SKILLS_ASSETS_MAX_BYTES = 4 * 1024 * 1024;
const PACKAGE_SKILLS_ASSETS_MAX_FILES = 200;
/** Counted once per package path, however many agents share the skill. */
const PACKAGE_ASSETS_MAX_BYTES = 8 * 1024 * 1024;
/** Individually named skip warnings per agent; the rest are counted in one line. */
const ASSET_WARNINGS_MAX = 20;
const ASSET_SEGMENT = /^[A-Za-z0-9_][A-Za-z0-9._-]*$/;
/** Casefolded basenames the agent computer reads with authority, refused anywhere below a skill's root (a SKILL.md only below the root one). */
const RESERVED_BASENAMES = new Set([
   "skill.md",
   "claude.md",
   "claude.local.md",
   "agents.md",
   "publisher.json",
]);

/** One listed file: its path is `<skill>/<path>`, relative to the agent's skill roots. */
interface PackageAgentAsset {
   relative_filepath: string;
   bytes: number;
   sha256: string;
}

/** What one agent's assets charge against the package-wide budget, by package-relative path. */
type AssetCharges = Map<string, { bytes: number; sha256: string }>;

/** The package-wide asset budget, shared by every agent in manifest order. */
interface PackageAssetBudget {
   bytes: number;
   paths: Set<string>;
   /** Each listed file's hash by package path, so a skill shared by several agents is read and hashed once. */
   reads: Map<string, AssetRead>;
   /** Each skills root by its manifest path, read once for every agent that names it. */
   roots: Map<string, ReadRoot>;
}

type AssetRead =
   | { sha256: string; bytes: number; binary: boolean }
   /** A NUL in its first bytes made it binary: not read in full or hashed until a budget takes it. */
   | { unread: true }
   | { problem: string; link?: true };

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
   return {
      sha256: sha256(fresh.bytes),
      bytes: fresh.bytes.length,
      binary: decodeText(fresh.bytes) === undefined,
   };
}

/**
 * A skills root as every agent naming it sees it: the publisher.json its served
 * tree holds, and its scan for each entry budget it was walked with (the walk
 * takes from the agent's budget, so the same scan holds only for the same one).
 */
interface ReadRoot {
   manifest: string | undefined;
   scans: Map<string, { scan: Scan; used: { assets: number; other: number } }>;
}

export const SKILL_MD = "SKILL.md";
const RESERVED_WHY =
   "is named like a file the agent computer reads with authority";
/** The agent computer refuses a whole agent for one of these in a skill path, and a POSIX file name can carry one. */
// eslint-disable-next-line no-control-regex
const CONTROL_CHARS = /[\u0000-\u001f\u007f]/;
const CONTROL_CHARS_WHY = "has a control character in its path";

/** A path as a warning shows it: control characters escaped, so a file name cannot reshape the message. */
const displayPath = (p: string) =>
   p.replace(
      new RegExp(CONTROL_CHARS, "g"),
      (c) => `\\u${c.charCodeAt(0).toString(16).padStart(4, "0")}`,
   );

/** What one reader's listed files have taken so far, against its caps. */
interface AssetWalk {
   /** Whose caps these are, as a skip words them. */
   scope: { holds: string; owns: string; cap: string };
   maxFiles: number;
   maxBytes: number;
   files: number;
   bytes: number;
   /** Bytes this walk charged that the package's budget has not recorded yet. */
   newBytes: number;
   /** Each listed path and its directories, lower-cased, to the spelling that took it: two that differ only by case collide. */
   lower: Map<string, string>;
   charges: AssetCharges;
   skipped: string[];
   /** Each listed file's read by package path: shared across agents when `shared` is set, so a file the first pass read is not read again. */
   reads: Map<string, AssetRead>;
   /** What agents share across the package; a package's own skills have only the caps above. */
   shared?: PackageAssetBudget;
}

const AGENT_SCOPE: AssetWalk["scope"] = {
   holds: "the agent already has",
   owns: "the agent's assets",
   cap: "per-agent",
};
const SKILLS_SCOPE: AssetWalk["scope"] = {
   holds: "the package's skills already have",
   owns: "the package's skill assets",
   cap: "per-package skills",
};

function newWalk(
   scope: AssetWalk["scope"],
   maxFiles: number,
   maxBytes: number,
   shared?: PackageAssetBudget,
): AssetWalk {
   return {
      scope,
      maxFiles,
      maxBytes,
      files: 0,
      bytes: 0,
      newBytes: 0,
      lower: new Map(),
      charges: new Map(),
      skipped: [],
      reads: shared?.reads ?? new Map(),
      shared,
   };
}

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

const underAssets = (relative: string) => relative.startsWith("assets/");

/** Markdown outside `assets/` is inlined; everything else is listed. */
const isInlineMarkdown = (relative: string) =>
   relative.endsWith(".md") && !underAssets(relative);

/** A Markdown file directly in `reference/`: the one place a skill's Markdown beside SKILL.md was always served. */
const isReferenceMarkdown = (relative: string) =>
   /^reference\/[^/]+\.md$/.test(relative);

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
 * Why an agent must refuse a skill whose `assets/` is a link, or a directory
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
}

interface Scan {
   packageRoot: string;
   skills: ScannedSkill[];
   warnings: string[];
   blocking: string[];
}

const skillNotServed = (rel: string, problem: string) =>
   `Package skill '${rel}/${SKILL_MD}' ${problem}, so it is not served. Fix: add a readable SKILL.md inside the package, or remove the directory.`;

/**
 * Read the skills under `<package>/<dirRel>/<name>/SKILL.md`, and name what else
 * each skill's directory holds. The package's own `skills/` and an agent's skill
 * roots are both read through here.
 *
 * Parsing is shared with the repo's own skills; selection is not. A package
 * ships what it contains: no manifest gating, and no `credible-*` exclusion,
 * which would silently drop a customer's skill by name. Each skill's walk takes
 * from `budget`, which the caller shares across the skills it reads.
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
      blocking: [],
   };
   const { packageRoot } = scan;
   const unserved = (message: string) => {
      scan.warnings.push(message);
      scan.blocking.push(message);
   };

   let entries: fs.Dirent[];
   let escapes: boolean;
   try {
      fs.realpathSync(packagePath);
      // Settled before anything is looked at through it: a skills directory
      // that leads out is refused whatever it leads to.
      const inside = insidePackage(packageRoot, root);
      escapes = inside === undefined;
      if (inside !== undefined) {
         if (!fs.statSync(inside).isDirectory()) return scan;
         entries = fs.readdirSync(inside, { withFileTypes: true });
      } else {
         entries = [];
      }
   } catch {
      // No skills directory is the common case, and one that vanishes mid-load is the same.
      return scan;
   }
   if (escapes) {
      unserved(
         `Skills directory '${dirRel}' resolves outside the package through a link, so it is not served. Fix: copy the skills into the package.`,
      );
      return scan;
   }

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
      });
   }
   return scan;
}

/**
 * The scan of a skills root, reused from an earlier agent that named it when
 * that scan started with the same entries left in its budget, which it then
 * takes from `budget` as the walk would have.
 */
function scanOnce(
   known: ReadRoot,
   packagePath: string,
   root: string,
   budget: WalkBudget,
): Scan {
   const key = `${budget.assets.left}/${budget.other.left}`;
   const earlier = known.scans.get(key);
   if (earlier) {
      budget.assets.left -= earlier.used.assets;
      budget.other.left -= earlier.used.other;
      return earlier.scan;
   }
   const before = { assets: budget.assets.left, other: budget.other.left };
   const scan = scanSkills(packagePath, root, budget);
   known.scans.set(key, {
      scan,
      used: {
         assets: before.assets - budget.assets.left,
         other: before.other - budget.other.left,
      },
   });
   return scan;
}

/** One skill's directory, split: Markdown inline, every other file listed. */
interface ReadSkill {
   dirName: string;
   rel: string;
   meta: SkillEntry;
   skillignore: string | null;
   /** SKILL.md first, then the rest in path order; `relative` is within the skill. */
   inline: Array<{ relative: string; text: string }>;
   /** `relative_filepath` is `<skill>/<path>`. */
   listed: PackageAgentAsset[];
   catalog: SkillCatalogEntry;
}

interface FinishContext {
   packageRoot: string;
   walk: AssetWalk;
   /** Bytes of inline Markdown still free; a package's own skills have no limit. */
   inline: { left: number };
   warnings: string[];
   /** What the strict rules for `assets/` refuse an agent over. A package's own skills only warn, so only an agent acts on these. */
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
   listed: PackageAgentAsset[];
   /** What the second pass reads: Markdown outside `reference/`, and every file to list, which includes Markdown that did not fit and a binary under `assets/`. */
   later: { markdown: string[]; list: string[] };
}

/**
 * Inline a Markdown file while it fits `ctx.inline` and warn that it is listed
 * instead when it does not, judged by its size before it is read. Warn when it
 * cannot be read at all.
 */
function inlineMarkdown(
   d: SkillDraft,
   relative: string,
   ctx: FinishContext,
): void {
   const shown = `${d.s.rel}/${relative}`;
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
   if ("problem" in read || Buffer.byteLength(read.text) > ctx.inline.left) {
      ctx.warnings.push(
         `Package skill file '${shown}' would take the skill Markdown over the ${PACKAGE_AGENT_SKILLS_MAX_BYTES} byte cap, so it is listed instead of inlined.`,
      );
      d.later.list.push(relative);
      return;
   }
   // A .md that is not strict UTF-8 text is listed like any binary file, never served as lossy text.
   if (decodeText(read.raw) === undefined) {
      ctx.warnings.push(
         `Package skill file '${shown}' is not UTF-8 text, so it is listed instead of inlined.`,
      );
      d.later.list.push(relative);
      return;
   }
   ctx.inline.left -= Buffer.byteLength(read.text);
   d.inline.push({ relative, text: read.text });
   d.files[relative] = {
      digest: `sha256:${sha256(read.raw)}`,
      size: read.raw.length,
   };
}

const recordListed = (
   d: SkillDraft,
   relative: string,
   asset: PackageAgentAsset,
) => {
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
         // A publisher.json below assets/ is refused with the rest of the tree, before any skill is read.
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
 * Under `assets/` a link, a `..` name or a case collision refuses an agent;
 * anywhere else the same finding skips the file with a warning.
 */
function readRest(d: SkillDraft, ctx: FinishContext): ReadSkill {
   for (const relative of d.later.markdown) inlineMarkdown(d, relative, ctx);
   for (const relative of d.later.list.sort(compareText)) {
      const asset = listFile(d.s, relative, ctx);
      if (asset) recordListed(d, relative, asset);
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

/** Both passes over every skill, in order. */
const readSkills = (skills: ScannedSkill[], ctx: FinishContext) =>
   skills.map((s) => readFirst(s, ctx)).map((d) => readRest(d, ctx));

/**
 * One file of a skill listed for an agent to copy: its checks, its caps, then
 * its bytes hashed. Undefined when it is skipped (warned, named) or, under
 * `assets/`, refused (`ctx.refusals`). A binary file is hashed and then, given
 * `defer`, put there to be charged later instead of now.
 */
function listFile(
   s: ScannedSkill,
   relative: string,
   ctx: FinishContext,
   defer?: string[],
): PackageAgentAsset | undefined {
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

   // Nothing more fits once the file cap is reached; only a file below assets/ is still looked at, for the findings that refuse an agent.
   if (!strict && walk.files >= walk.maxFiles) {
      skip(
         `${walk.scope.holds} ${walk.maxFiles} assets, the ${walk.scope.cap} file cap`,
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
   const segments = relative.split("/");
   if (segments.some((segment) => segment.includes(".."))) {
      reject(
         `asset '${shown}' has '..' in its name; rename it`,
         "its name has '..' in it",
      );
      return undefined;
   }
   let spelled = s.dirName;
   for (const segment of segments) {
      spelled = `${spelled}/${segment}`;
      const taken = walk.lower.get(spelled.toLowerCase());
      if (taken !== undefined && taken !== spelled) {
         reject(
            `asset '${shown}' differs from another asset only by case; rename one`,
            "its name differs from another file's only by case",
         );
         return undefined;
      }
      walk.lower.set(spelled.toLowerCase(), spelled);
   }
   if (!segments.every((segment) => ASSET_SEGMENT.test(segment))) {
      skip("its name is outside [A-Za-z0-9_][A-Za-z0-9._-]*");
      return undefined;
   }
   if (listed.length > ASSET_PATH_MAX_CHARS) {
      skip(`its path is over ${ASSET_PATH_MAX_CHARS} characters`);
      return undefined;
   }
   if (!stat.isFile()) {
      skip("it is not a regular file");
      return undefined;
   }
   const lowerName = segments[segments.length - 1]!.toLowerCase();
   if (
      lowerName.endsWith(MODEL_FILE_SUFFIX) ||
      lowerName.endsWith(NOTEBOOK_FILE_SUFFIX)
   ) {
      skip("a Malloy file cannot be listed");
      return undefined;
   }

   const { size } = stat;
   const charged = walk.shared?.paths.has(shown) ?? false;
   if (size >= PACKAGE_ASSET_FILE_MAX_BYTES) {
      skip(
         `it is ${size} bytes, at or over the ${PACKAGE_ASSET_FILE_MAX_BYTES} byte file cap`,
      );
   } else if (walk.files >= walk.maxFiles) {
      skip(
         `${walk.scope.holds} ${walk.maxFiles} assets, the ${walk.scope.cap} file cap`,
      );
   } else if (walk.bytes + size > walk.maxBytes) {
      skip(
         `it would take ${walk.scope.owns} over the ${walk.maxBytes} byte ${walk.scope.cap} cap`,
      );
   } else if (
      walk.shared &&
      !charged &&
      walk.shared.bytes + walk.newBytes + size > PACKAGE_ASSETS_MAX_BYTES
   ) {
      skip(
         `it would take the package's assets over the ${PACKAGE_ASSETS_MAX_BYTES} byte per-package cap`,
      );
   } else {
      let read = walk.reads.get(shown);
      if (!read || (!defer && "unread" in read)) {
         read = readAsset(
            ctx.packageRoot,
            s.dir,
            absolute,
            defer !== undefined,
         );
         walk.reads.set(shown, read);
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
      return {
         relative_filepath: listed,
         bytes: read.bytes,
         sha256: read.sha256,
      };
   }
   return undefined;
}

const SHA_PREFIX = "sha256:";

/** A package skill as `get_skill` and the REST routes read it: inline Markdown from what load read, every listed file from disk against its listed sha. */
function packageSkill(packagePath: string, read: ReadSkill): ServedSkill {
   const texts = new Map(read.inline.map((f) => [f.relative, f.text]));
   return {
      name: read.meta.name,
      description: read.meta.description,
      catalog: read.catalog,
      read(relative) {
         const text = texts.get(relative);
         if (text !== undefined) return { text };
         const listed = readListedFile(
            packagePath,
            read.rel,
            relative,
            read.catalog.files[relative]!.digest.slice(SHA_PREFIX.length),
         );
         if (listed === undefined) return { changed: true };
         return "text" in listed ? listed : { binary: true };
      },
   };
}

/**
 * The package's skills under `<dirRel>/`: each skill's SKILL.md inline with its
 * other Markdown, and every other file listed and hashed. `skills` is what
 * `get_skill` and the REST routes read, each with its catalog entry.
 */
export function readSkillsDir(
   packagePath: string,
   dirRel: string = PACKAGE_SKILLS_DIR,
): PackageSkills {
   const scan = scanSkills(packagePath, dirRel, newWalkBudget("package"));
   const walk = newWalk(
      SKILLS_SCOPE,
      PACKAGE_SKILLS_ASSETS_MAX_FILES,
      PACKAGE_SKILLS_ASSETS_MAX_BYTES,
   );
   const result: PackageSkills = {
      skills: [],
      files: [],
      paths: [],
      contentShas: new Map(),
      warnings: scan.warnings,
      blocking: scan.blocking,
   };
   const ctx: FinishContext = {
      packageRoot: scan.packageRoot,
      walk,
      inline: { left: Infinity },
      warnings: scan.warnings,
      refusals: [],
   };
   for (const read of readSkills(scan.skills, ctx)) {
      result.skills.push(packageSkill(packagePath, read));
      result.files.push(
         ...read.inline.map((f) => ({
            path: `${read.rel}/${f.relative}`,
            text: f.text,
         })),
      );
      if (read.skillignore !== null) {
         result.paths.push(`${read.rel}/.skillignore`);
      }
   }
   result.paths.push(
      ...result.files.map((f) => f.path),
      ...walk.charges.keys(),
   );
   for (const [listedPath, { sha256: digest }] of walk.charges) {
      result.contentShas.set(listedPath, digest);
   }
   result.warnings.push(...skippedWarnings(walk));
   return result;
}

interface PackageAgentSchedule {
   cron: string;
   task: string;
   taskContent: string;
}

type PackageAgentBudget = Partial<Record<(typeof BUDGET_KEYS)[number], number>>;

/** One agent as the manifest declares it, resolved to what a caller adopts. */
export interface PackageAgent {
   name: string;
   description: string;
   model: string;
   instructions: string;
   skills: Array<{ relative_filepath: string; file_contents: string }>;
   /** Parsed name and description of each served SKILL.md, by `relative_filepath`; the emitted text quotes them. */
   skillMeta: Map<string, { name: string; description: string }>;
   schedules: PackageAgentSchedule[];
   /** Declared run limits; the platform clamps them to its own maximum. Absent when the manifest sets none. */
   budget?: PackageAgentBudget;
   /** Package-relative directory of each skill that lists files, by skill name. Absent when none do. */
   asset_roots?: Record<string, string>;
   /** Every other file of the agent's skills, listed and never inlined; sorted by `relative_filepath`. Absent when none are served. */
   assets?: PackageAgentAsset[];
   /** Each served skill in the catalog's entry shape. Internal: never on the wire. */
   catalog: SkillCatalogEntry[];
   warnings: string[];
   definitionSha: string;
}

interface PackageAgents {
   agents: Map<string, PackageAgent>;
   /** Package-relative paths of every file a served agent read, for the content hash. */
   paths: string[];
   /** sha256 of each asset in `paths`, so the content hash does not read them again. */
   contentShas: Map<string, string>;
   /** Why each dropped agent is not served, plus any served agent's own warnings. */
   warnings: string[];
}

const AGENT_NAME = /^[a-z0-9]+(-[a-z0-9]+)*$/;
const AGENT_KEYS = [
   "description",
   "instructions",
   "model",
   "skills",
   "schedules",
   "budget",
];
const SCHEDULE_KEYS = ["cron", "task"];
const BUDGET_KEYS = [
   "turns",
   "wall_clock_minutes",
   "llm_tokens",
   "job_minutes",
] as const;

const isObject = (v: unknown): v is Record<string, unknown> =>
   typeof v === "object" && v !== null && !Array.isArray(v);

/** Keys that are neither known nor an `x-` author note. */
const unknownKeys = (obj: Record<string, unknown>, known: string[]) =>
   Object.keys(obj).filter((k) => !known.includes(k) && !k.startsWith("x-"));

/** Thrown inside one agent's read to drop that agent with a message. */
class AgentProblem extends Error {}

/** A manifest path as a normalized package-relative path, or an AgentProblem. */
function agentPath(value: unknown, what: string): string {
   if (typeof value !== "string" || value === "" || value.includes("\0")) {
      throw new AgentProblem(
         `${what} must be a non-empty package-relative path`,
      );
   }
   if (path.posix.isAbsolute(value) || path.win32.isAbsolute(value)) {
      throw new AgentProblem(`${what} '${value}' is an absolute path`);
   }
   if (value.includes("\\")) {
      throw new AgentProblem(
         `${what} '${value}' contains a backslash; write it with forward slashes`,
      );
   }
   const normalized = path.posix.normalize(value).replace(/\/+$/, "");
   if (
      normalized === "." ||
      normalized === ".." ||
      normalized.startsWith("../")
   ) {
      throw new AgentProblem(`${what} '${value}' is outside the package`);
   }
   return normalized;
}

const isManifest = (name: string) => name.toLowerCase() === "publisher.json";

/** The name of a publisher.json sitting directly in `dir`, which must resolve inside the package; undefined when there is none. A skill's `reference/` is read as skillFiles reads it, so a link to a huge directory is not listed whole. */
function manifestIn(
   packageRoot: string,
   dir: string,
   reference = false,
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
   } catch {
      return undefined;
   }
}

const holdsManifest = (packageRoot: string, dir: string) =>
   manifestIn(packageRoot, dir) !== undefined;

/**
 * The path, relative to `root`, of the first publisher.json where one has always
 * refused an agent, or undefined: in the skills root, in a skill directory, in
 * its `reference/`, or anywhere below its `assets/`. A publisher.json anywhere
 * else is skipped with the rest of the files a skill may not hold. It reads the
 * real tree, folders a `.skillignore` ignores included: that file decides what
 * a skill is made of, never what may sit in it. It follows a link only to a
 * directory inside the package, reads a `reference/` or a linked `assets/` for
 * at most `REFERENCE_MAX_EXAMINED` entries, and bounds what it walks under
 * `assets/` the way skillFiles does, so a link cannot make it scan the
 * filesystem, list a huge directory whole, or report on files outside the
 * package.
 */
function servedManifest(packageRoot: string, root: string): string | undefined {
   const atRoot = manifestIn(packageRoot, root);
   if (atRoot !== undefined) return atRoot;
   let skills: fs.Dirent[];
   try {
      const inside = insidePackage(packageRoot, root);
      if (inside === undefined) return undefined;
      skills = fs.readdirSync(inside, { withFileTypes: true });
   } catch {
      return undefined;
   }
   let left = ASSET_MAX_ENTRIES;
   const below = (
      dir: string,
      rel: string,
      depth: number,
   ): string | undefined => {
      let entries: fs.Dirent[];
      try {
         const inside = insidePackage(packageRoot, dir);
         if (inside === undefined) return undefined;
         // Only assets/ itself can be a link here (a linked entry is never descended); it may lead anywhere in the package.
         entries = fs.lstatSync(dir).isSymbolicLink()
            ? listBounded(inside, () => false).entries
            : fs.readdirSync(inside, { withFileTypes: true });
      } catch {
         return undefined;
      }
      for (const entry of entries.sort((a, b) => compareText(a.name, b.name))) {
         if (entry.name.startsWith(".")) continue;
         if (left-- <= 0) return undefined;
         const relative = `${rel}/${entry.name}`;
         if (isManifest(entry.name)) return relative;
         if (entry.isDirectory() && depth < ASSET_MAX_DEPTH) {
            const found = below(
               path.join(dir, entry.name),
               relative,
               depth + 1,
            );
            if (found !== undefined) return found;
         }
      }
      return undefined;
   };
   for (const skill of skills.sort((a, b) => compareText(a.name, b.name))) {
      // A link to a directory is a skill dir that readSkillsDir follows.
      if (!skill.isDirectory() && !skill.isSymbolicLink()) continue;
      const dir = path.join(root, skill.name);
      for (const place of ["", "reference"]) {
         const found = manifestIn(
            packageRoot,
            path.join(dir, place),
            place === "reference",
         );
         if (found !== undefined) {
            return path.posix.join(skill.name, place, found);
         }
      }
      const found = below(path.join(dir, "assets"), `${skill.name}/assets`, 1);
      if (found !== undefined) return found;
   }
   return undefined;
}

/** Read a Markdown file the agent serves as text, capped at 16 KB. */
function readAgentText(
   packagePath: string,
   packageRoot: string,
   value: unknown,
   what: string,
): { rel: string; text: string } {
   const rel = agentPath(value, what);
   if (!rel.toLowerCase().endsWith(".md")) {
      throw new AgentProblem(`${what} '${rel}' is not a .md file`);
   }
   // A nested manifest beside or above the file would make it a second package
   // root for tools that import by manifest.
   for (
      let dir = path.posix.dirname(rel);
      dir !== ".";
      dir = path.posix.dirname(dir)
   ) {
      if (holdsManifest(packageRoot, path.join(packagePath, dir))) {
         throw new AgentProblem(
            `${what} '${rel}' sits under a nested publisher.json at '${dir}'`,
         );
      }
   }
   const read = readFileInside(
      packageRoot,
      path.join(packagePath, rel),
      PACKAGE_AGENT_TEXT_MAX_BYTES,
   );
   if ("problem" in read) {
      throw new AgentProblem(`${what} '${rel}' ${read.problem}`);
   }
   return { rel, text: read.text };
}

const emitSkillMd = (name: string, description: string, body: string) =>
   `---\nname: ${JSON.stringify(name)}\ndescription: ${JSON.stringify(description)}\n---\n\n${body}\n`;

/** A served skill file without its text; a SKILL.md also carries the name and description its frontmatter holds. */
export function summarizeAgentSkillFile(
   file: PackageAgent["skills"][number],
   meta: PackageAgent["skillMeta"],
): {
   relative_filepath: string;
   name?: string;
   description?: string;
   bytes: number;
} {
   const { relative_filepath, file_contents } = file;
   const bytes = Buffer.byteLength(file_contents);
   const parsed = meta.get(relative_filepath);
   return parsed
      ? { relative_filepath, ...parsed, bytes }
      : { relative_filepath, bytes };
}

function readAgentSchedules(
   packagePath: string,
   packageRoot: string,
   raw: unknown,
   paths: string[],
): PackageAgentSchedule[] {
   if (raw === undefined) return [];
   if (!Array.isArray(raw))
      throw new AgentProblem("'schedules' must be a list");
   return raw.map((entry) => {
      if (!isObject(entry)) {
         throw new AgentProblem("each schedule must be an object");
      }
      const bad = unknownKeys(entry, SCHEDULE_KEYS);
      if (bad.length > 0) {
         throw new AgentProblem(
            `a schedule sets '${bad.join("', '")}'; allowed keys: ${SCHEDULE_KEYS.join(", ")}`,
         );
      }
      if (
         typeof entry.cron !== "string" ||
         !new CronEvaluator().isValid(entry.cron)
      ) {
         throw new AgentProblem(
            `schedule cron ${JSON.stringify(entry.cron)} is not a valid 5-field UTC cron expression`,
         );
      }
      const task = readAgentText(
         packagePath,
         packageRoot,
         entry.task,
         "schedule 'task'",
      );
      paths.push(task.rel);
      return {
         cron: entry.cron,
         task: entry.task as string,
         taskContent: task.text,
      };
   });
}

function readAgentBudget(raw: unknown): PackageAgentBudget | undefined {
   if (raw === undefined) return undefined;
   if (!isObject(raw)) throw new AgentProblem("'budget' must be an object");
   const bad = unknownKeys(raw, [...BUDGET_KEYS]);
   if (bad.length > 0) {
      throw new AgentProblem(
         `'budget' sets '${bad.join("', '")}'; allowed keys: ${BUDGET_KEYS.join(", ")}`,
      );
   }
   const budget: PackageAgentBudget = {};
   for (const key of BUDGET_KEYS) {
      const value = raw[key];
      if (value === undefined) continue;
      if (
         typeof value !== "number" ||
         !Number.isSafeInteger(value) ||
         value < 1
      ) {
         throw new AgentProblem(
            `budget '${key}' must be a positive integer, got ${JSON.stringify(value)}`,
         );
      }
      budget[key] = value;
   }
   return Object.keys(budget).length > 0 ? budget : undefined;
}

/**
 * A listed file of the skill at `skillRel`, re-checked on every request with
 * the checks that listed it, and only when its bytes still hash to `sha`:
 * files can change after load, and a watch-mode edit does not reload. A binary
 * file answers with its size, never its bytes. Undefined (a 404) when it is no
 * longer servable.
 */
function readListedFile(
   packagePath: string,
   skillRel: string,
   relative: string,
   sha: string,
): { text: string } | { binarySize: number } | undefined {
   try {
      const skillDir = path.join(packagePath, skillRel);
      // The skill directory is resolved by the package's own walk first, so
      // one swapped for a link out is not looked through.
      if (insidePackage(packagePath, skillDir) === undefined) return undefined;
      const read = readListedBytes(
         packagePath,
         skillDir,
         path.join(skillDir, relative),
         PACKAGE_ASSET_FILE_MAX_BYTES - 1,
      );
      if (!("bytes" in read) || sha256(read.bytes) !== sha) return undefined;
      const text = decodeText(read.bytes);
      return text === undefined ? { binarySize: read.bytes.length } : { text };
   } catch {
      return undefined;
   }
}

/** One listed file of an agent's skills: its text, or a message naming its size when it is binary; undefined when it is not served. */
export function readAgentAsset(
   packagePath: string,
   agent: PackageAgent,
   relativeFilepath: string,
): { text: string } | { message: string } | undefined {
   const skill = relativeFilepath.split("/")[0]!;
   const listing = agent.assets?.find(
      (a) => a.relative_filepath === relativeFilepath,
   );
   const skillRel =
      agent.asset_roots && Object.hasOwn(agent.asset_roots, skill)
         ? agent.asset_roots[skill]
         : undefined;
   if (skillRel === undefined || !listing) return undefined;
   const read = readListedFile(
      packagePath,
      skillRel,
      relativeFilepath.slice(skill.length + 1),
      listing.sha256,
   );
   if (read === undefined || "text" in read) return read;
   return {
      message: `'${relativeFilepath}' is a binary file of ${read.binarySize} bytes. It is listed for the agent computer to copy, and is not served as text.`,
   };
}

/** What an agent's skill roots read into, sorted by relative path. */
interface AgentSkills {
   roots: string[];
   files: PackageAgent["skills"];
   meta: PackageAgent["skillMeta"];
   catalog: SkillCatalogEntry[];
   /** Each skill's applied `.skillignore` text, by skill name; only the skills that have one. */
   skillignores: Record<string, string>;
   assets: PackageAgentAsset[];
   assetRoots: Record<string, string>;
}

/**
 * An agent's skill roots in two phases. This validates and scans them and runs
 * every skill's first pass, which holds every finding that refuses the agent,
 * so `finish` cannot refuse it. `walk` holds what the first pass charged.
 */
function startAgentSkills(
   packagePath: string,
   packageRoot: string,
   raw: unknown,
   paths: string[],
   warnings: string[],
   assetBudget: PackageAssetBudget,
): { walk: AssetWalk; finish: () => AgentSkills } {
   if (raw !== undefined && !Array.isArray(raw)) {
      throw new AgentProblem("'skills' must be a list of directories");
   }
   const roots = ((raw as unknown[] | undefined) ?? []).map((entry) =>
      agentPath(entry, "'skills' entry"),
   );
   const walk = newWalk(
      AGENT_SCOPE,
      PACKAGE_AGENT_ASSETS_MAX_FILES,
      PACKAGE_AGENT_ASSETS_MAX_BYTES,
      assetBudget,
   );
   const entryBudget = newWalkBudget("agent");
   const scanned: ScannedSkill[] = [];
   for (const root of roots) {
      // Settled before it is looked at: one that leads out of the package
      // is refused the same whether or not what it names exists.
      const rootInside = insidePackage(
         packageRoot,
         path.join(packagePath, root),
      );
      if (rootInside === undefined) {
         throw new AgentProblem(
            `skills directory '${root}' resolves outside the package`,
         );
      }
      try {
         if (!fs.statSync(rootInside).isDirectory()) {
            throw new Error("not a directory");
         }
      } catch {
         throw new AgentProblem(`skills directory '${root}' does not exist`);
      }
      let known = assetBudget.roots.get(root);
      if (!known) {
         known = {
            manifest: servedManifest(packageRoot, path.join(packagePath, root)),
            scans: new Map(),
         };
         assetBudget.roots.set(root, known);
      }
      for (let dir = root; dir !== "."; dir = path.posix.dirname(dir)) {
         if (dir === root) {
            if (known.manifest !== undefined) {
               throw new AgentProblem(
                  `skills directory '${root}' contains a nested publisher.json at '${displayPath(known.manifest)}'`,
               );
            }
         } else if (holdsManifest(packageRoot, path.join(packagePath, dir))) {
            throw new AgentProblem(
               `skills directory '${root}' contains a nested publisher.json`,
            );
         }
      }
      const scan = scanOnce(known, packagePath, root, entryBudget);
      if (scan.blocking.length > 0) {
         throw new AgentProblem(scan.blocking.join(" "));
      }
      // Served but flagged: what is left is a missing description, a frontmatter finding or a skipped file.
      for (const w of scan.warnings) {
         warnings.push(`skill finding: ${w.replace(/\.$/, "")}`);
      }
      if (scan.skills.length === 0) {
         warnings.push(
            `its skills directory '${root}' holds no skills. Fix: add <skill>/SKILL.md under it or remove the entry`,
         );
      }
      for (const s of scan.skills) {
         // Two roots holding one skill name collide on its SKILL.md.
         if (scanned.some((other) => other.dirName === s.dirName)) {
            throw new AgentProblem(
               `skills directories collide on '${s.dirName}/${SKILL_MD}'`,
            );
         }
         if (s.assetsRefusal) throw new AgentProblem(s.assetsRefusal);
         scanned.push(s);
      }
   }

   // Every SKILL.md is charged before any other Markdown, so the cap can only
   // list a reference, never cost the agent a skill.
   const skillMdBytes = scanned.reduce(
      (total, s) => total + Buffer.byteLength(s.skillMd.text),
      0,
   );
   if (skillMdBytes > PACKAGE_AGENT_SKILLS_MAX_BYTES) {
      throw new AgentProblem(
         `its skill files total ${skillMdBytes} bytes, over the ${PACKAGE_AGENT_SKILLS_MAX_BYTES} byte cap`,
      );
   }
   const ctx: FinishContext = {
      packageRoot,
      walk,
      inline: { left: PACKAGE_AGENT_SKILLS_MAX_BYTES - skillMdBytes },
      warnings: [],
      refusals: [],
   };
   const drafts = scanned.map((s) => readFirst(s, ctx));
   if (ctx.refusals.length > 0) throw new AgentProblem(ctx.refusals[0]!);
   return {
      walk,
      finish: () => finishAgentSkills(drafts, ctx, roots, paths, warnings),
   };
}

/** The second pass of every skill of an agent, assembled into what the agent serves. */
function finishAgentSkills(
   drafts: SkillDraft[],
   ctx: FinishContext,
   roots: string[],
   paths: string[],
   warnings: string[],
): AgentSkills {
   const served = new Map<string, string>();
   const meta: PackageAgent["skillMeta"] = new Map();
   const catalog: SkillCatalogEntry[] = [];
   const skillignores = new Map<string, string>();
   const assets = new Map<string, PackageAgentAsset>();
   const assetRoots = new Map<string, string>();
   for (const read of drafts.map((d) => readRest(d, ctx))) {
      for (const f of read.inline) {
         const relative = `${read.dirName}/${f.relative}`;
         paths.push(`${read.rel}/${f.relative}`);
         if (f.relative === SKILL_MD) {
            meta.set(relative, {
               name: read.meta.name,
               description: read.meta.description,
            });
            served.set(
               relative,
               emitSkillMd(
                  read.meta.name,
                  read.meta.description,
                  read.meta.body,
               ),
            );
         } else {
            served.set(relative, f.text);
         }
      }
      catalog.push(read.catalog);
      if (read.skillignore !== null) {
         paths.push(`${read.rel}/.skillignore`);
         skillignores.set(read.dirName, read.skillignore);
      }
      for (const asset of read.listed) {
         assets.set(asset.relative_filepath, asset);
         paths.push(
            `${read.rel}${asset.relative_filepath.slice(read.dirName.length)}`,
         );
      }
      if (read.listed.length > 0) assetRoots.set(read.dirName, read.rel);
   }
   for (const w of ctx.warnings) {
      warnings.push(`skill finding: ${w.replace(/\.$/, "")}`);
   }
   warnings.push(...skippedWarnings(ctx.walk));
   return {
      assets: [...assets.values()].sort((a, b) =>
         compareText(a.relative_filepath, b.relative_filepath),
      ),
      assetRoots: Object.fromEntries(assetRoots),
      roots,
      catalog,
      skillignores: Object.fromEntries(skillignores),
      files: [...served]
         .sort(([a], [b]) => compareText(a, b))
         .map(([relative_filepath, file_contents]) => ({
            relative_filepath,
            file_contents,
         })),
      meta,
   };
}

/** An agent between its two phases: `walk` holds what the first charged, and `finish` reads the rest. */
interface StartedAgent {
   walk: AssetWalk;
   finish: () => { agent: PackageAgent; paths: string[] };
}

function startAgent(
   packagePath: string,
   packageRoot: string,
   name: string,
   raw: unknown,
   assetBudget: PackageAssetBudget,
): StartedAgent {
   if (!AGENT_NAME.test(name) || name.length > 64) {
      throw new AgentProblem(
         "its name must be 1-64 characters of lowercase letters, digits and single hyphens",
      );
   }
   if (!isObject(raw)) throw new AgentProblem("its entry must be an object");
   const unrecognized = unknownKeys(raw, AGENT_KEYS);
   if (unrecognized.length > 0) {
      throw new AgentProblem(
         `it sets '${unrecognized.join("', '")}', which this server does not know, and a key it does not know might limit the agent. Allowed keys: ${AGENT_KEYS.join(", ")}; prefix a note with 'x-' to have it ignored`,
      );
   }
   if (typeof raw.description !== "string" || raw.description.trim() === "") {
      throw new AgentProblem("'description' must be a non-empty string");
   }
   if (
      raw.model !== undefined &&
      (typeof raw.model !== "string" || raw.model === "")
   ) {
      throw new AgentProblem("'model' must be a non-empty string");
   }
   const model = raw.model ?? "inherit";
   const warnings: string[] = [];
   const paths: string[] = [];

   const instructions = readAgentText(
      packagePath,
      packageRoot,
      raw.instructions,
      "'instructions'",
   );
   paths.push(instructions.rel);
   const schedules = readAgentSchedules(
      packagePath,
      packageRoot,
      raw.schedules,
      paths,
   );
   const budget = readAgentBudget(raw.budget);
   const started = startAgentSkills(
      packagePath,
      packageRoot,
      raw.skills,
      paths,
      warnings,
      assetBudget,
   );
   return {
      walk: started.walk,
      finish: () => {
         const skills = started.finish();
         const definitionSha = canonicalSha({
            name,
            description: raw.description,
            model,
            instructionsPath: instructions.rel,
            skillRoots: skills.roots,
            schedules,
            ...(budget && { budget }),
            // Folded in only when present, so an agent without listed files or a .skillignore keeps its sha.
            ...(skills.assets.length > 0 && {
               assets: skills.assets.map(({ relative_filepath, sha256 }) => ({
                  relative_filepath,
                  sha256,
               })),
            }),
            ...(Object.keys(skills.skillignores).length > 0 && {
               skillignores: skills.skillignores,
            }),
            instructions: instructions.text,
            skills: skills.files,
         });
         return {
            agent: {
               name,
               description: raw.description as string,
               model: model as string,
               instructions: instructions.text,
               skills: skills.files,
               skillMeta: skills.meta,
               catalog: skills.catalog,
               schedules,
               ...(budget && { budget }),
               ...(skills.assets.length > 0 && {
                  asset_roots: skills.assetRoots,
                  assets: skills.assets,
               }),
               warnings,
               definitionSha,
            },
            paths,
         };
      },
   };
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

/**
 * The agents a package's `publisher.json` declares, each resolved to the files
 * it names. `raw` is the manifest's `agents` value, unvalidated. This never
 * throws: an agent that is wrong must not take the model offline, so it is
 * left out with a warning saying why and the rest still serve.
 */
export function readPackageAgents(
   packagePath: string,
   raw: unknown,
): PackageAgents {
   const result: PackageAgents = {
      agents: new Map(),
      paths: [],
      contentShas: new Map(),
      warnings: [],
   };
   // Charged in manifest order, only by an agent that is served, first phase before second.
   const assetBudget: PackageAssetBudget = {
      bytes: 0,
      paths: new Set(),
      reads: new Map(),
      roots: new Map(),
   };
   if (raw === undefined) return result;
   if (!isObject(raw)) {
      result.warnings.push(
         'Package manifest \'agents\' must be an object keyed by agent name, so no agent is served. Fix: write "agents": { "<name>": { "description": ..., "instructions": ... } }.',
      );
      return result;
   }
   let packageRoot: string;
   try {
      fs.realpathSync(packagePath);
      packageRoot = path.resolve(packagePath);
   } catch {
      return result;
   }
   const entries = Object.entries(raw);
   // By manifest position, so the warnings read in manifest order across the two phases.
   const notes: string[][] = entries.map(() => []);
   const dropped = (i: number, name: string, error: unknown) => {
      const why =
         error instanceof AgentProblem
            ? error.message
            : `it cannot be read (${callerVisibleText((error as Error).message, packagePath)})`;
      notes[i]!.push(
         `Package agent '${name}' is not served: ${why}. Fix: correct the 'agents.${name}' entry in publisher.json and the files it names.`,
      );
   };
   // Every agent's first phase runs before any second, so what an agent has always served is charged to the package's budget before any agent's newly covered files.
   const started = new Map<number, StartedAgent>();
   entries.forEach(([name, entry], i) => {
      try {
         const agent = startAgent(
            packagePath,
            packageRoot,
            name,
            entry,
            assetBudget,
         );
         chargePackage(assetBudget, agent.walk, result.contentShas);
         started.set(i, agent);
      } catch (error) {
         dropped(i, name, error);
      }
   });
   entries.forEach(([name], i) => {
      const agent = started.get(i);
      if (!agent) return;
      try {
         const read = agent.finish();
         chargePackage(assetBudget, agent.walk, result.contentShas);
         result.agents.set(name, read.agent);
         result.paths.push(...read.paths);
         for (const w of read.agent.warnings) {
            notes[i]!.push(`Package agent '${name}' ${w}.`);
         }
      } catch (error) {
         dropped(i, name, error);
      }
   });
   result.warnings.push(...notes.flat());
   return result;
}

/** Where a skill's files are installed from when this server does not carry them. */
const SKILLS_PACKAGE = "@malloy-publisher/skills";

/** A bundled skill read as a package's is: its Markdown from the bundle, everything else a manifest entry only. */
function bundledSkill({ bodies, ...catalog }: BundledSkill): ServedSkill {
   const { description } = catalog.frontmatter;
   return {
      name: catalog.name,
      description: typeof description === "string" ? description.trim() : "",
      catalog,
      read: (relative) =>
         Object.hasOwn(bodies, relative)
            ? { text: bodies[relative]! }
            : {
                 message: `'${relative}' (${catalog.files[relative]!.size} bytes) is part of the bundled skill '${catalog.name}' but this server does not carry it. Install \`${SKILLS_PACKAGE}\` to get it.`,
              },
   };
}

/**
 * The skills in force for one package: the bundled set, with the package's own
 * skills shadowing any bundled skill of the same name and adding the rest.
 *
 * Shadowing by name is the whole mechanism: Publisher does not know which
 * skills the agent already holds, so it cannot merge, only choose what a name
 * returns. Nothing is merged, a shadowed skill's bundled files go with it.
 * `origin` is on every entry because a replaced skill is otherwise
 * indistinguishable from the original.
 */
export function resolveSkills(
   bundled: BundledSkill[],
   packageSkills: ServedSkill[],
): ResolvedSkill[] {
   const shadows = new Map(packageSkills.map((s) => [s.name, s]));
   const bundledNames = new Set(bundled.map((s) => s.name));
   // Bundled order first, each skill replaced in place by the package's copy
   // when there is one, then the package's new names in their own order. Two
   // packages' listings therefore line up row for row wherever they agree.
   return [
      ...bundled.map((b): ResolvedSkill => {
         const shadow = shadows.get(b.name);
         // A bundled manifest is informational: it lists a template's publisher.json, which the package reserved-name and listed-file rules would skip.
         return shadow
            ? { ...shadow, origin: "package" }
            : { ...bundledSkill(b), origin: "bundled" };
      }),
      ...packageSkills
         .filter((s) => !bundledNames.has(s.name))
         .map((s): ResolvedSkill => ({ ...s, origin: "package" })),
   ];
}

/** A path as a caller wrote it, as a path of the skill: without a leading `./`, and `SKILL.md` when empty. */
export function skillFilePath(raw: string | null | undefined): string {
   let relative = (raw ?? "").trim();
   while (relative.startsWith("./")) relative = relative.slice(2);
   return relative || SKILL_MD;
}

/** Every file of a skill as path and size, SKILL.md first, then by path. */
export function skillFileList(
   skill: ServedSkill,
): Array<{ path: string; size: number }> {
   return Object.entries(skill.catalog.files)
      .map(([filePath, { size }]) => ({ path: filePath, size }))
      .sort(
         (a, b) =>
            Number(b.path === SKILL_MD) - Number(a.path === SKILL_MD) ||
            compareText(a.path, b.path),
      );
}

/** An unknown path's answer names this many of the skill's files; the rest are counted. */
const NAMED_FILES_MAX = 25;

/** The skill's first files by name, for a message about a path it does not have. */
export function namedSkillFiles(skill: ServedSkill): string {
   const files = skillFileList(skill);
   const more = files.length - NAMED_FILES_MAX;
   return (
      files
         .slice(0, NAMED_FILES_MAX)
         .map((f) => f.path)
         .join(", ") +
      (more > 0
         ? ` (and ${more} more; see \`files\` on SKILL.md, or the paths its text names)`
         : "")
   );
}

/**
 * One read of `relative` as it is answered: `content` when there is text, its
 * `size` when there is none, and for a SKILL.md the frontmatter and the files
 * (`entries`, every one unless the caller trims them; the frontmatter unless
 * `withFrontmatter` is false).
 */
export function skillFileView(
   skill: ResolvedSkill,
   relative: string,
   content: string | undefined,
   entries: Array<{ path: string; size: number }> = skillFileList(skill),
   withFrontmatter = true,
) {
   return {
      name: skill.name,
      path: relative,
      ...(content === undefined
         ? { size: skill.catalog.files[relative]!.size }
         : { content }),
      origin: skill.origin,
      ...(relative === SKILL_MD
         ? {
              ...(withFrontmatter
                 ? { frontmatter: skill.catalog.frontmatter }
                 : {}),
              files: entries,
           }
         : {}),
   };
}
