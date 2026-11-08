// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { createHash } from "crypto";
import * as fs from "fs";
import * as path from "path";
import {
   parseReference,
   parseSkill,
   referenceFiles,
   referencePointer,
   type SkillEntry,
} from "./build_skills_bundle";
import skillsBundle from "./skills_bundle.json";
import { compareText } from "../../compare_text";
import { MODEL_FILE_SUFFIX, NOTEBOOK_FILE_SUFFIX } from "../../constants";
import { CronEvaluator } from "../materialization/run/cron_evaluator";
import { canonicalSha } from "../package_revision";
import { stepsOutside } from "../../path_safety";

/** The build-time bundle of the repo's skills, served as MCP prompts and through get_skill. */
export const BUNDLED_SKILLS = (skillsBundle as { skills: SkillEntry[] }).skills;

/** The directory a package keeps its own agent skills in. */
export const PACKAGE_SKILLS_DIR = "skills";

/** Where a skill came from, once the bundled and package sets are resolved. */
type SkillOrigin = "bundled" | "package";

export interface ResolvedSkill extends SkillEntry {
   origin: SkillOrigin;
}

interface PackageSkills {
   skills: SkillEntry[];
   /**
    * Every file read, package-relative path and raw text, so the caller can
    * fold the paths into the package's content hash and an agent can serve the
    * raw files. Produced by the same walk that produced `skills`, so the two
    * cannot drift.
    */
   files: Array<{ path: string; text: string }>;
   /**
    * Non-fatal findings. Nothing here throws: a failed package load is skipped
    * entirely, and a malformed markdown file must not cost a working model.
    */
   warnings: string[];
   /** The subset of `warnings` that leave a skill unserved, which an agent cannot be served without. */
   blocking: string[];
}

const ASSET_SNIFF_BYTES = 8 * 1024;

/** Largest single skill file served. A markdown guide never needs more. */
export const PACKAGE_SKILL_FILE_MAX_BYTES = 256 * 1024;

/**
 * A file's text, or why it cannot be served. The real path must sit inside the
 * real package root: a symlink in a package can point anywhere, and this text
 * is returned over unauthenticated MCP and REST.
 *
 * `asset` reads a non-Markdown skill asset instead: `realRoot` is then the
 * skill's real `assets/` directory, the file itself may not be a link, and its
 * bytes must be UTF-8 text without a NUL.
 */
function readFileInside(
   realRoot: string,
   file: string,
   maxBytes: number,
   asset = false,
): { text: string } | { problem: string; link?: true } {
   try {
      if (asset && fs.lstatSync(file).isSymbolicLink()) {
         return { problem: "is a link", link: true };
      }
      const real = fs.realpathSync(file);
      if (stepsOutside(path.relative(realRoot, real))) {
         return {
            problem: "resolves outside the package through a link",
            link: true,
         };
      }
      // A link inside the package can still point at publisher.json or data.
      if (!asset && !real.toLowerCase().endsWith(".md")) {
         return { problem: "resolves to a file that is not Markdown" };
      }
      const stat = fs.statSync(real);
      // Reading a FIFO would block the main thread.
      if (!stat.isFile()) {
         return { problem: "resolves to something that is not a regular file" };
      }
      if (stat.size > maxBytes) {
         return {
            problem: `is ${stat.size} bytes, over the ${maxBytes} byte cap`,
         };
      }
      if (!asset) return { text: fs.readFileSync(real, "utf8") };
      // A binary shows a NUL early; reading the head first spares a full read of a rejected file.
      const head = Buffer.alloc(ASSET_SNIFF_BYTES);
      const fd = fs.openSync(real, "r");
      let headBytes: number;
      try {
         headBytes = fs.readSync(fd, head, 0, ASSET_SNIFF_BYTES, 0);
      } finally {
         fs.closeSync(fd);
      }
      if (head.subarray(0, headBytes).includes(0)) {
         return { problem: "contains a NUL byte" };
      }
      const bytes = fs.readFileSync(real);
      if (bytes.includes(0)) return { problem: "contains a NUL byte" };
      try {
         // ignoreBOM keeps a leading BOM, so the text hashes and serves as the bytes do.
         return {
            text: new TextDecoder("utf-8", {
               fatal: true,
               ignoreBOM: true,
            }).decode(bytes),
         };
      } catch {
         return { problem: "is not valid UTF-8 text" };
      }
   } catch (error) {
      return { problem: `cannot be read (${(error as Error).message})` };
   }
}

/** The skill's reference files, or none when `reference/` is a link out of the package. */
function referenceFilesInside(
   realRoot: string,
   skillDir: string,
   rel: string,
   warnings: string[],
): string[] {
   try {
      const real = fs.realpathSync(path.join(skillDir, "reference"));
      if (stepsOutside(path.relative(realRoot, real))) {
         warnings.push(
            `Package skill reference directory '${rel}/reference' resolves outside the package through a link, so it is not served. Fix: copy the files into the package.`,
         );
         return [];
      }
      return referenceFiles(skillDir);
   } catch (error) {
      // No reference/ is the common case; a file or unreadable directory there is not.
      if ((error as NodeJS.ErrnoException).code === "ENOENT") return [];
      warnings.push(
         `Package skill reference directory '${rel}/reference' cannot be listed (${(error as Error).message}), so its references are not served. Fix: make it a readable directory of .md files, or remove it.`,
      );
      return [];
   }
}

/**
 * Read the skills under `<package>/<dirRel>/<name>/SKILL.md` plus each skill's
 * optional `reference/*.md`. The package's own `skills/` and an agent's skill
 * roots are both read through here.
 *
 * Parsing is shared with the repo's own skills; selection is not. A package
 * ships what it contains: no manifest gating, and no `credible-*` exclusion,
 * which would silently drop a customer's skill by name.
 */
export function readSkillsDir(
   packagePath: string,
   dirRel: string = PACKAGE_SKILLS_DIR,
): PackageSkills {
   const root = path.join(packagePath, dirRel);
   const skills: SkillEntry[] = [];
   const files: PackageSkills["files"] = [];
   const warnings: string[] = [];
   const blocking: string[] = [];
   const unserved = (message: string) => {
      warnings.push(message);
      blocking.push(message);
   };

   let realRoot: string;
   let entries: fs.Dirent[];
   let escapes: boolean;
   try {
      realRoot = fs.realpathSync(packagePath);
      if (!fs.statSync(root).isDirectory()) {
         return { skills, files, warnings, blocking };
      }
      escapes = stepsOutside(path.relative(realRoot, fs.realpathSync(root)));
      entries = fs.readdirSync(root, { withFileTypes: true });
   } catch {
      // No skills directory is the common case, and one that vanishes mid-load is the same.
      return { skills, files, warnings, blocking };
   }
   if (escapes) {
      unserved(
         `Skills directory '${dirRel}' resolves outside the package through a link, so it is not served. Fix: copy the skills into the package.`,
      );
      return { skills, files, warnings, blocking };
   }

   for (const entry of [...entries].sort((a, b) =>
      a.name < b.name ? -1 : a.name > b.name ? 1 : 0,
   )) {
      if (entry.name.startsWith(".")) continue;
      const skillDir = path.join(root, entry.name);
      try {
         if (!fs.statSync(skillDir).isDirectory()) continue;
      } catch {
         continue;
      }
      const rel = `${dirRel}/${entry.name}`;
      const read = readFileInside(
         realRoot,
         path.join(skillDir, "SKILL.md"),
         PACKAGE_SKILL_FILE_MAX_BYTES,
      );
      if ("problem" in read) {
         unserved(
            `Package skill '${rel}/SKILL.md' ${read.problem}, so it is not served. Fix: add a readable SKILL.md inside the package, or remove the directory.`,
         );
         continue;
      }

      const skill = parseSkill(read.text, entry.name);
      if (!skill.description) {
         // The description is what a caller reads before deciding to fetch the
         // body, so a skill without one is invisible in every listing.
         warnings.push(
            `Package skill '${skill.name}' has no description in its frontmatter, so nothing tells a caller when to read it. Fix: add a one-line 'description:' to ${rel}/SKILL.md.`,
         );
      }
      // The directory name is what callers install under, so it must be the name.
      if (skill.name !== entry.name) {
         unserved(
            `Package skill '${rel}' declares the name '${skill.name}', which is not its directory name '${entry.name}', so it is not served. Fix: make 'name:' in ${rel}/SKILL.md equal '${entry.name}'.`,
         );
         continue;
      }

      const references: SkillEntry[] = [];
      const refFiles: PackageSkills["files"] = [];
      const served: string[] = [];
      for (const file of referenceFilesInside(
         realRoot,
         skillDir,
         rel,
         warnings,
      )) {
         const relative = `${rel}/reference/${file}`;
         const ref = readFileInside(
            realRoot,
            path.join(skillDir, "reference", file),
            PACKAGE_SKILL_FILE_MAX_BYTES,
         );
         if ("problem" in ref) {
            warnings.push(
               `Package skill reference '${relative}' ${ref.problem}, so it is not served.`,
            );
            continue;
         }
         references.push(parseReference(ref.text, skill.name, file));
         refFiles.push({ path: relative, text: ref.text });
         served.push(file);
      }
      if (references.length > 0) {
         skill.body = `${skill.body}\n\n${referencePointer(skill.name, served, "package")}`;
      }
      skills.push(skill, ...references);
      files.push({ path: `${rel}/SKILL.md`, text: read.text }, ...refFiles);
   }

   return { skills, files, warnings, blocking };
}

/** Largest instructions or task file an agent serves. */
export const PACKAGE_AGENT_TEXT_MAX_BYTES = 16 * 1024;

/** Largest total of an agent's skill files. Over it drops the agent rather than truncating. */
export const PACKAGE_AGENT_SKILLS_MAX_BYTES = 256 * 1024;

/** An asset of this size or more is refused; the draft store refuses a 1 MiB file. */
const PACKAGE_ASSET_FILE_MAX_BYTES = 1024 * 1024;
const PACKAGE_AGENT_ASSETS_MAX_BYTES = 4 * 1024 * 1024;
const PACKAGE_AGENT_ASSETS_MAX_FILES = 200;
/** Counted once per package path, however many agents share the skill. */
const PACKAGE_ASSETS_MAX_BYTES = 8 * 1024 * 1024;
const ASSET_PATH_MAX_CHARS = 200;
const ASSET_MAX_DEPTH = 8;
const ASSET_MAX_ENTRIES = 1000;
/** Individually named skip warnings per agent; the rest are counted in one line. */
const ASSET_WARNINGS_MAX = 20;
const ASSET_SEGMENT = /^[A-Za-z0-9_][A-Za-z0-9._-]*$/;

/** One listed asset: its path is `<skill>/assets/...`, relative to the agent's skill roots. */
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
   /** Each asset's read result by package path, so a skill shared by several agents is read and hashed once. */
   reads: Map<string, AssetRead>;
}

type AssetRead =
   | { text: string; sha256: string }
   | { problem: string; link?: true };

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
   /** Package-relative directory of each skill that ships assets, by skill name. Absent when none do. */
   asset_roots?: Record<string, string>;
   /** Listed, never read by the agent; sorted by `relative_filepath`. Absent when none are served. */
   assets?: PackageAgentAsset[];
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

/** True when a publisher.json sits directly in `dir`, which must resolve inside the package. */
function holdsManifest(realRoot: string, dir: string): boolean {
   try {
      if (stepsOutside(path.relative(realRoot, fs.realpathSync(dir)))) {
         return false;
      }
      return fs
         .readdirSync(dir)
         .some((f) => f.toLowerCase() === "publisher.json");
   } catch {
      return false;
   }
}

/**
 * True when a publisher.json sits in a skills root, a skill directory under it,
 * or its reference/: the only places a served file can be. It never walks
 * deeper, and follows a link only to a directory inside the package, so a link
 * cannot make this scan the filesystem or report on files outside it.
 */
function servedTreeHoldsManifest(realRoot: string, root: string): boolean {
   if (holdsManifest(realRoot, root)) return true;
   try {
      if (stepsOutside(path.relative(realRoot, fs.realpathSync(root)))) {
         return false;
      }
      return fs.readdirSync(root, { withFileTypes: true }).some(
         (e) =>
            // A link to a directory is a skill dir readSkillsDir follows.
            (e.isDirectory() || e.isSymbolicLink()) &&
            (holdsManifest(realRoot, path.join(root, e.name)) ||
               holdsManifest(realRoot, path.join(root, e.name, "reference"))),
      );
   } catch {
      return false;
   }
}

/** Read a Markdown file the agent serves as text, capped at 16 KB. */
function readAgentText(
   packagePath: string,
   realRoot: string,
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
      if (holdsManifest(realRoot, path.join(packagePath, dir))) {
         throw new AgentProblem(
            `${what} '${rel}' sits under a nested publisher.json at '${dir}'`,
         );
      }
   }
   const read = readFileInside(
      realRoot,
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
   realRoot: string,
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
         realRoot,
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

/** One agent's running totals across every skill's assets. */
interface AssetWalk {
   visited: number;
   files: number;
   bytes: number;
   /** Bytes no earlier agent already charged to the package. */
   newBytes: number;
   /** Lower-cased listed paths, so two that differ only by case collide. */
   lower: Set<string>;
   charges: AssetCharges;
   skipped: string[];
   stopped: boolean;
   /** Shown whatever the cap on named skips, since it explains every asset after it. */
   limitNote?: string;
}

/** The skill's real `assets/`, which must be a plain directory inside the skill's own real directory. */
function realAssetsDir(skillDir: string, skillRel: string): string {
   const assetsDir = path.join(skillDir, "assets");
   if (fs.lstatSync(assetsDir).isSymbolicLink()) {
      throw new AgentProblem(
         `'${skillRel}/assets' is a link; replace it with a real directory`,
      );
   }
   const real = fs.realpathSync(assetsDir);
   if (real !== path.join(fs.realpathSync(skillDir), "assets")) {
      throw new AgentProblem(
         `'${skillRel}/assets' resolves outside the skill's own directory`,
      );
   }
   return real;
}

/**
 * The text files under one served skill's `assets/`, listed and hashed but not
 * returned. A link, an escape or a manifest-shaped name drops the whole agent;
 * anything merely unservable (binary, too big, over a budget) is skipped with a
 * note, so an agent that is served today stays served.
 */
function assetFilesInside(
   skillDir: string,
   skillRel: string,
   skill: string,
   budget: PackageAssetBudget,
   walk: AssetWalk,
): PackageAgentAsset[] {
   const assetsDir = path.join(skillDir, "assets");
   let link: fs.Stats;
   try {
      link = fs.lstatSync(assetsDir);
   } catch (error) {
      if ((error as NodeJS.ErrnoException).code !== "ENOENT") {
         walk.skipped.push(
            `assets of '${skillRel}' cannot be listed (${(error as Error).message})`,
         );
      }
      return [];
   }
   if (link.isSymbolicLink()) {
      throw new AgentProblem(
         `'${skillRel}/assets' is a link; replace it with a real directory`,
      );
   }
   if (!link.isDirectory()) {
      walk.skipped.push(`'${skillRel}/assets' is not a directory`);
      return [];
   }
   const realAssets = realAssetsDir(skillDir, skillRel);

   const out: PackageAgentAsset[] = [];
   const skip = (shown: string, why: string) =>
      walk.skipped.push(`asset '${shown}' is not served: ${why}`);
   const visit = (dir: string, rel: string, depth: number): void => {
      let entries: fs.Dirent[];
      try {
         entries = fs.readdirSync(dir, { withFileTypes: true });
      } catch (error) {
         skip(
            `${skillRel}/assets/${rel}`,
            `its directory cannot be listed (${(error as Error).message})`,
         );
         return;
      }
      entries.sort((a, b) => compareText(a.name, b.name));
      for (const entry of entries) {
         if (walk.stopped) return;
         if (entry.name.startsWith(".")) continue;
         const relative = rel ? `${rel}/${entry.name}` : entry.name;
         const shown = `${skillRel}/assets/${relative}`;
         if (++walk.visited > ASSET_MAX_ENTRIES) {
            walk.stopped = true;
            walk.limitNote = `asset walk stopped at '${shown}': the agent's assets hold more than ${ASSET_MAX_ENTRIES} entries, so the rest are not served`;
            return;
         }
         const lowerName = entry.name.toLowerCase();
         if (entry.isSymbolicLink()) {
            throw new AgentProblem(
               `asset '${shown}' is a link; replace it with a regular file`,
            );
         }
         if (entry.name.includes("..")) {
            throw new AgentProblem(
               `asset '${shown}' has '..' in its name; rename it`,
            );
         }
         if (lowerName === "publisher.json" || lowerName === "skill.md") {
            throw new AgentProblem(
               `asset '${shown}' is named like a package manifest or skill; remove or rename it`,
            );
         }
         const listed = `${skill}/assets/${relative}`;
         if (walk.lower.has(listed.toLowerCase())) {
            throw new AgentProblem(
               `asset '${shown}' differs from another asset only by case; rename one`,
            );
         }
         walk.lower.add(listed.toLowerCase());

         if (!ASSET_SEGMENT.test(entry.name)) {
            skip(shown, "its name is outside [A-Za-z0-9_][A-Za-z0-9._-]*");
            continue;
         }
         if (listed.length > ASSET_PATH_MAX_CHARS) {
            skip(shown, `its path is over ${ASSET_PATH_MAX_CHARS} characters`);
            continue;
         }
         const absolute = path.join(dir, entry.name);
         if (entry.isDirectory()) {
            if (depth >= ASSET_MAX_DEPTH) {
               skip(
                  shown,
                  `it is nested deeper than ${ASSET_MAX_DEPTH} levels`,
               );
            } else {
               visit(absolute, relative, depth + 1);
            }
            continue;
         }
         if (!entry.isFile()) {
            skip(shown, "it is not a regular file");
            continue;
         }
         if (
            lowerName.endsWith(MODEL_FILE_SUFFIX) ||
            lowerName.endsWith(NOTEBOOK_FILE_SUFFIX)
         ) {
            skip(shown, "a Malloy file cannot be an asset");
            continue;
         }

         let size: number;
         try {
            size = fs.lstatSync(absolute).size;
         } catch (error) {
            skip(shown, `it cannot be read (${(error as Error).message})`);
            continue;
         }
         const packagePath = `${skillRel}/assets/${relative}`;
         const charged = budget.paths.has(packagePath);
         if (size >= PACKAGE_ASSET_FILE_MAX_BYTES) {
            skip(
               shown,
               `it is ${size} bytes, at or over the ${PACKAGE_ASSET_FILE_MAX_BYTES} byte file cap`,
            );
         } else if (walk.files >= PACKAGE_AGENT_ASSETS_MAX_FILES) {
            skip(
               shown,
               `the agent already has ${PACKAGE_AGENT_ASSETS_MAX_FILES} assets, the per-agent file cap`,
            );
         } else if (walk.bytes + size > PACKAGE_AGENT_ASSETS_MAX_BYTES) {
            skip(
               shown,
               `it would take the agent's assets over the ${PACKAGE_AGENT_ASSETS_MAX_BYTES} byte per-agent cap`,
            );
         } else if (
            !charged &&
            budget.bytes + walk.newBytes + size > PACKAGE_ASSETS_MAX_BYTES
         ) {
            skip(
               shown,
               `it would take the package's assets over the ${PACKAGE_ASSETS_MAX_BYTES} byte per-package cap`,
            );
         } else {
            let read = budget.reads.get(packagePath);
            if (!read) {
               const fresh = readFileInside(
                  realAssets,
                  absolute,
                  PACKAGE_ASSET_FILE_MAX_BYTES - 1,
                  true,
               );
               read =
                  "problem" in fresh
                     ? fresh
                     : {
                          text: fresh.text,
                          sha256: createHash("sha256")
                             .update(fresh.text)
                             .digest("hex"),
                       };
               budget.reads.set(packagePath, read);
            }
            if ("problem" in read) {
               // A link or an escape that appeared after the listing is the same threat as one found in it.
               if (read.link) {
                  throw new AgentProblem(
                     `asset '${shown}' ${read.problem}; replace it with a regular file`,
                  );
               }
               skip(shown, read.problem);
               continue;
            }
            const bytes = Buffer.byteLength(read.text);
            const { sha256 } = read;
            walk.files++;
            walk.bytes += bytes;
            if (!charged) walk.newBytes += bytes;
            walk.charges.set(packagePath, { bytes, sha256 });
            out.push({ relative_filepath: listed, bytes, sha256 });
         }
      }
   };
   visit(assetsDir, "", 1);
   return out;
}

/**
 * A listed asset's text, re-checked on every request with the checks that
 * listed it, and only when its bytes still hash to the listed sha256: files can
 * change after load, and a watch-mode edit does not reload. Undefined (a 404)
 * when it is no longer servable.
 */
export function readAgentAsset(
   packagePath: string,
   agent: PackageAgent,
   relativeFilepath: string,
): string | undefined {
   const skill = relativeFilepath.split("/")[0]!;
   const listing = agent.assets?.find(
      (a) => a.relative_filepath === relativeFilepath,
   );
   const skillRel =
      agent.asset_roots && Object.hasOwn(agent.asset_roots, skill)
         ? agent.asset_roots[skill]
         : undefined;
   if (skillRel === undefined || !listing) return undefined;
   try {
      const skillDir = path.join(packagePath, skillRel);
      const read = readFileInside(
         realAssetsDir(skillDir, skillRel),
         path.join(skillDir, relativeFilepath.slice(skill.length)),
         PACKAGE_ASSET_FILE_MAX_BYTES - 1,
         true,
      );
      if (!("text" in read)) return undefined;
      return createHash("sha256").update(read.text).digest("hex") ===
         listing.sha256
         ? read.text
         : undefined;
   } catch {
      return undefined;
   }
}

/** The agent's skill roots read into served files, sorted by relative path. */
function readAgentSkills(
   packagePath: string,
   realRoot: string,
   raw: unknown,
   paths: string[],
   warnings: string[],
   assetBudget: PackageAssetBudget,
): {
   roots: string[];
   files: PackageAgent["skills"];
   meta: PackageAgent["skillMeta"];
   assets: PackageAgentAsset[];
   assetRoots: Record<string, string>;
   charges: AssetCharges;
} {
   if (raw !== undefined && !Array.isArray(raw)) {
      throw new AgentProblem("'skills' must be a list of directories");
   }
   const roots = ((raw as unknown[] | undefined) ?? []).map((entry) =>
      agentPath(entry, "'skills' entry"),
   );
   const served = new Map<string, string>();
   const meta: PackageAgent["skillMeta"] = new Map();
   const assets = new Map<string, PackageAgentAsset>();
   const assetRoots = new Map<string, string>();
   const walk: AssetWalk = {
      visited: 0,
      files: 0,
      bytes: 0,
      newBytes: 0,
      lower: new Set(),
      charges: new Map(),
      skipped: [],
      stopped: false,
   };
   let bytes = 0;
   for (const root of roots) {
      try {
         if (!fs.statSync(path.join(packagePath, root)).isDirectory()) {
            throw new Error("not a directory");
         }
      } catch {
         throw new AgentProblem(`skills directory '${root}' does not exist`);
      }
      for (let dir = root; dir !== "."; dir = path.posix.dirname(dir)) {
         const holds =
            dir === root
               ? servedTreeHoldsManifest(realRoot, path.join(packagePath, dir))
               : holdsManifest(realRoot, path.join(packagePath, dir));
         if (holds) {
            throw new AgentProblem(
               `skills directory '${root}' contains a nested publisher.json`,
            );
         }
      }
      const read = readSkillsDir(packagePath, root);
      if (read.blocking.length > 0) {
         throw new AgentProblem(read.blocking.join(" "));
      }
      // Served but flagged: what is left is a missing description or a skipped reference.
      for (const w of read.warnings) {
         warnings.push(`skill finding: ${w.replace(/\.$/, "")}`);
      }
      if (read.files.length === 0) {
         warnings.push(
            `its skills directory '${root}' holds no skills. Fix: add <skill>/SKILL.md under it or remove the entry`,
         );
      }
      const skillNames: string[] = [];
      for (const file of read.files) {
         const relative = file.path.slice(root.length + 1);
         if (served.has(relative)) {
            throw new AgentProblem(
               `skills directories collide on '${relative}'`,
            );
         }
         bytes += Buffer.byteLength(file.text);
         paths.push(file.path);
         if (
            relative.split("/").length === 2 &&
            relative.endsWith("/SKILL.md")
         ) {
            skillNames.push(relative.split("/")[0]!);
         }
         if (relative.endsWith("/SKILL.md")) {
            const skill = parseSkill(file.text, relative.split("/")[0]!);
            meta.set(relative, {
               name: skill.name,
               description: skill.description,
            });
            served.set(
               relative,
               emitSkillMd(skill.name, skill.description, skill.body),
            );
         } else {
            served.set(relative, file.text);
         }
      }
      // Only a skill this agent serves ships assets; the package's own skills/ never does.
      for (const skill of skillNames) {
         const skillRel = `${root}/${skill}`;
         const listed = assetFilesInside(
            path.join(packagePath, skillRel),
            skillRel,
            skill,
            assetBudget,
            walk,
         );
         for (const asset of listed) {
            // Two roots holding one skill name already collided on its SKILL.md.
            assets.set(asset.relative_filepath, asset);
            assetRoots.set(skill, skillRel);
            paths.push(
               `${skillRel}${asset.relative_filepath.slice(skill.length)}`,
            );
         }
      }
   }
   if (bytes > PACKAGE_AGENT_SKILLS_MAX_BYTES) {
      throw new AgentProblem(
         `its skill files total ${bytes} bytes, over the ${PACKAGE_AGENT_SKILLS_MAX_BYTES} byte cap`,
      );
   }
   for (const note of walk.skipped.slice(0, ASSET_WARNINGS_MAX)) {
      warnings.push(note);
   }
   if (walk.limitNote) warnings.push(walk.limitNote);
   if (walk.skipped.length > ASSET_WARNINGS_MAX) {
      warnings.push(
         `${walk.skipped.length - ASSET_WARNINGS_MAX} further assets are not served, for the same kinds of reasons`,
      );
   }
   return {
      assets: [...assets.values()].sort((a, b) =>
         compareText(a.relative_filepath, b.relative_filepath),
      ),
      assetRoots: Object.fromEntries(assetRoots),
      charges: walk.charges,
      roots,
      files: [...served]
         .sort(([a], [b]) => compareText(a, b))
         .map(([relative_filepath, file_contents]) => ({
            relative_filepath,
            file_contents,
         })),
      meta,
   };
}

function readAgent(
   packagePath: string,
   realRoot: string,
   name: string,
   raw: unknown,
   assetBudget: PackageAssetBudget,
): { agent: PackageAgent; paths: string[]; charges: AssetCharges } {
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
      realRoot,
      raw.instructions,
      "'instructions'",
   );
   paths.push(instructions.rel);
   const schedules = readAgentSchedules(
      packagePath,
      realRoot,
      raw.schedules,
      paths,
   );
   const budget = readAgentBudget(raw.budget);
   const skills = readAgentSkills(
      packagePath,
      realRoot,
      raw.skills,
      paths,
      warnings,
      assetBudget,
   );

   const definitionSha = canonicalSha({
      name,
      description: raw.description,
      model,
      instructionsPath: instructions.rel,
      skillRoots: skills.roots,
      schedules,
      ...(budget && { budget }),
      // Folded in only when present, so an agent without assets keeps its sha.
      ...(skills.assets.length > 0 && {
         assets: skills.assets.map(({ relative_filepath, sha256 }) => ({
            relative_filepath,
            sha256,
         })),
      }),
      instructions: instructions.text,
      skills: skills.files,
   });
   return {
      agent: {
         name,
         description: raw.description,
         model: model as string,
         instructions: instructions.text,
         skills: skills.files,
         skillMeta: skills.meta,
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
      charges: skills.charges,
   };
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
   // Charged in manifest order, and only for an agent that is served.
   const assetBudget: PackageAssetBudget = {
      bytes: 0,
      paths: new Set(),
      reads: new Map(),
   };
   if (raw === undefined) return result;
   if (!isObject(raw)) {
      result.warnings.push(
         'Package manifest \'agents\' must be an object keyed by agent name, so no agent is served. Fix: write "agents": { "<name>": { "description": ..., "instructions": ... } }.',
      );
      return result;
   }
   let realRoot: string;
   try {
      realRoot = fs.realpathSync(packagePath);
   } catch {
      return result;
   }
   for (const [name, entry] of Object.entries(raw)) {
      try {
         const { agent, paths, charges } = readAgent(
            packagePath,
            realRoot,
            name,
            entry,
            assetBudget,
         );
         result.agents.set(name, agent);
         result.paths.push(...paths);
         for (const [assetPath, { bytes, sha256 }] of charges) {
            result.contentShas.set(assetPath, sha256);
            if (!assetBudget.paths.has(assetPath)) {
               assetBudget.paths.add(assetPath);
               assetBudget.bytes += bytes;
            }
         }
         for (const w of agent.warnings) {
            result.warnings.push(`Package agent '${name}' ${w}.`);
         }
      } catch (error) {
         const why =
            error instanceof AgentProblem
               ? error.message
               : `it cannot be read (${(error as Error).message})`;
         result.warnings.push(
            `Package agent '${name}' is not served: ${why}. Fix: correct the 'agents.${name}' entry in publisher.json and the files it names.`,
         );
      }
   }
   return result;
}

/**
 * The skills in force for one package: the bundled set, with the package's own
 * skills shadowing any bundled skill of the same name and adding the rest.
 *
 * Shadowing by name is the whole mechanism: Publisher does not know which
 * skills the agent already holds, so it cannot merge, only choose what a name
 * returns. `origin` is on every entry because a replaced skill is otherwise
 * indistinguishable from the original.
 */
export function resolveSkills(
   bundled: SkillEntry[],
   packageSkills: SkillEntry[],
): ResolvedSkill[] {
   // A reference entry is `<skill>/<stem>`; a package skill's own name carries no slash.
   const root = (name: string) => name.split("/")[0]!;
   const shadows = new Map(
      packageSkills
         .filter((s) => !s.name.includes("/"))
         .map((s) => [s.name, s]),
   );
   const bundledNames = new Set(bundled.map((s) => s.name));
   // Bundled order first, each skill replaced in place by the package's copy
   // when there is one, then the package's new names in their own order. Two
   // packages' listings therefore line up row for row wherever they agree.
   return [
      ...bundled.flatMap((s): ResolvedSkill[] => {
         const shadow = shadows.get(root(s.name));
         if (!shadow) return [{ ...s, origin: "bundled" }];
         // Nothing is merged: a shadowed skill's bundled references go with it.
         return s.name === shadow.name
            ? [{ ...shadow, origin: "package" }]
            : [];
      }),
      ...packageSkills
         .filter((s) => s.name.includes("/") || !bundledNames.has(s.name))
         .map((s): ResolvedSkill => ({ ...s, origin: "package" })),
   ];
}
