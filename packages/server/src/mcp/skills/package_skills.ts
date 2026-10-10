// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import * as fs from "fs";
import * as path from "path";
import {
   parseReference,
   parseSkill,
   referenceFiles,
   referencePointer,
   type SkillEntry,
} from "./build_skills_bundle";
import { CronEvaluator } from "../../service/cron_evaluator";
import { canonicalSha } from "../../service/package_revision";
import { stepsOutside } from "../../service/package_retrieval";

/** The directory a package keeps its own agent skills in. */
export const PACKAGE_SKILLS_DIR = "skills";

/** Where a skill came from, once the bundled and package sets are resolved. */
export type SkillOrigin = "bundled" | "package";

export interface ResolvedSkill extends SkillEntry {
   origin: SkillOrigin;
}

export interface PackageSkills {
   skills: SkillEntry[];
   /**
    * Every file read, package-relative path and raw text, so the caller can
    * fold the paths into the package's content hash and an agent can serve the
    * raw files. Produced by the same walk that produced `skills`, so the two
    * cannot drift.
    */
   files: Array<{ path: string; text: string }>;
   /**
    * Non-fatal findings. A package that fails to load is skipped entirely, and
    * losing a working model over a malformed markdown file is the wrong trade,
    * so nothing here throws.
    */
   warnings: string[];
}

/** Largest single skill file served. A markdown guide never needs more. */
export const PACKAGE_SKILL_FILE_MAX_BYTES = 256 * 1024;

/**
 * A file's text, or why it cannot be served. The real path must sit inside the
 * real package root: a symlink in a package can point anywhere, and this text
 * is returned over unauthenticated MCP and REST.
 */
export function readFileInside(
   realRoot: string,
   file: string,
   maxBytes: number,
): { text: string } | { problem: string } {
   try {
      const real = fs.realpathSync(file);
      if (stepsOutside(path.relative(realRoot, real))) {
         return { problem: "resolves outside the package through a link" };
      }
      // A link inside the package can still point at publisher.json or data.
      if (!real.toLowerCase().endsWith(".md")) {
         return { problem: "resolves to a file that is not Markdown" };
      }
      const { size } = fs.statSync(real);
      if (size > maxBytes) {
         return { problem: `is ${size} bytes, over the ${maxBytes} byte cap` };
      }
      return { text: fs.readFileSync(real, "utf8") };
   } catch (error) {
      return { problem: `cannot be read (${(error as Error).message})` };
   }
}

/**
 * Read the skills under `<package>/<dirRel>/<name>/SKILL.md` plus each skill's
 * optional `reference/*.md`. The package's own `skills/` and an agent's skill
 * roots are both read through here.
 *
 * Parsing is shared with the repo's own skills (`parseSkill`, `parseReference`)
 * so one definition decides what a SKILL.md means. Selection deliberately is
 * NOT shared: `buildSkills` gates on `manifests/publisher-local.json`, which
 * answers "what does this server ship" and has no counterpart in a package. A
 * package ships what it contains, and the repo's `credible-*` exclusion is
 * likewise not applied: it exists for a gitignored install target inside this
 * repo, and silently dropping a customer's skill by name would be a trap.
 */
export function readSkillsDir(
   packagePath: string,
   dirRel: string = PACKAGE_SKILLS_DIR,
): PackageSkills {
   const root = path.join(packagePath, dirRel);
   const skills: SkillEntry[] = [];
   const files: PackageSkills["files"] = [];
   const warnings: string[] = [];

   let realRoot: string;
   let entries: fs.Dirent[];
   let escapes: boolean;
   try {
      realRoot = fs.realpathSync(packagePath);
      if (!fs.statSync(root).isDirectory()) return { skills, files, warnings };
      escapes = stepsOutside(path.relative(realRoot, fs.realpathSync(root)));
      entries = fs.readdirSync(root, { withFileTypes: true });
   } catch {
      // No skills directory is the common case, and one that vanishes mid-load is the same.
      return { skills, files, warnings };
   }
   if (escapes) {
      warnings.push(
         `Skills directory '${dirRel}' resolves outside the package through a link, so it is not served. Fix: copy the skills into the package.`,
      );
      return { skills, files, warnings };
   }

   const seen = new Map<string, string>();
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
         warnings.push(
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
      const previous = seen.get(skill.name);
      if (previous) {
         warnings.push(
            `Package skills '${previous}' and '${entry.name}' both declare the name '${skill.name}'; only '${previous}' is served. Fix: give each skill a distinct 'name:'.`,
         );
         continue;
      }
      seen.set(skill.name, entry.name);

      const references: SkillEntry[] = [];
      const refFiles: PackageSkills["files"] = [];
      const served: string[] = [];
      for (const file of referenceFiles(skillDir)) {
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
         skill.body = `${skill.body}\n\n${referencePointer(skill.name, served)}`;
      }
      skills.push(skill, ...references);
      files.push({ path: `${rel}/SKILL.md`, text: read.text }, ...refFiles);
   }

   return { skills, files, warnings };
}

/** Largest instructions or task file an agent serves. */
export const PACKAGE_AGENT_TEXT_MAX_BYTES = 16 * 1024;

/** Largest total of an agent's skill files. Over it drops the agent rather than truncating. */
export const PACKAGE_AGENT_SKILLS_MAX_BYTES = 256 * 1024;

export interface PackageAgentSchedule {
   cron: string;
   task: string;
   taskContent: string;
}

/** One agent as the manifest declares it, resolved to what a caller adopts. */
export interface PackageAgent {
   name: string;
   description: string;
   model: string;
   instructions: string;
   skills: Array<{ relative_filepath: string; file_contents: string }>;
   schedules: PackageAgentSchedule[];
   warnings: string[];
   definitionSha: string;
}

export interface PackageAgents {
   agents: Map<string, PackageAgent>;
   /** Package-relative paths of every file a served agent read, for the content hash. */
   paths: string[];
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
];
const SCHEDULE_KEYS = ["cron", "task"];

const isObject = (v: unknown): v is Record<string, unknown> =>
   typeof v === "object" && v !== null && !Array.isArray(v);

/** Keys that are neither known nor an `x-` author note. */
const unknownKeys = (obj: Record<string, unknown>, known: string[]) =>
   Object.keys(obj).filter((k) => !known.includes(k) && !k.startsWith("x-"));

const byCodepoint = (a: string, b: string) => (a < b ? -1 : a > b ? 1 : 0);

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

/** True when a publisher.json sits in `dir` or, when `deep`, anywhere under it. */
function holdsManifest(dir: string, deep: boolean): boolean {
   try {
      return fs
         .readdirSync(dir, { recursive: deep })
         .some(
            (f) => path.basename(String(f)).toLowerCase() === "publisher.json",
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
      if (holdsManifest(path.join(packagePath, dir), false)) {
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

/** The agent's skill roots read into served files, sorted by relative path. */
function readAgentSkills(
   packagePath: string,
   raw: unknown,
   paths: string[],
   warnings: string[],
): { roots: string[]; files: PackageAgent["skills"] } {
   if (raw !== undefined && !Array.isArray(raw)) {
      throw new AgentProblem("'skills' must be a list of directories");
   }
   const roots = ((raw as unknown[] | undefined) ?? []).map((entry) =>
      agentPath(entry, "'skills' entry"),
   );
   const served = new Map<string, string>();
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
         if (holdsManifest(path.join(packagePath, dir), dir === root)) {
            throw new AgentProblem(
               `skills directory '${root}' contains a nested publisher.json`,
            );
         }
      }
      const read = readSkillsDir(packagePath, root);
      if (read.warnings.length > 0) {
         throw new AgentProblem(read.warnings.join(" "));
      }
      if (read.files.length === 0) {
         warnings.push(
            `its skills directory '${root}' holds no skills. Fix: add <skill>/SKILL.md under it or remove the entry`,
         );
      }
      for (const file of read.files) {
         const relative = file.path.slice(root.length + 1);
         if (served.has(relative)) {
            throw new AgentProblem(
               `skills directories collide on '${relative}'`,
            );
         }
         bytes += Buffer.byteLength(file.text);
         paths.push(file.path);
         if (relative.endsWith("/SKILL.md")) {
            const skill = parseSkill(file.text, relative.split("/")[0]!);
            served.set(
               relative,
               emitSkillMd(skill.name, skill.description, skill.body),
            );
         } else {
            served.set(relative, file.text);
         }
      }
   }
   if (bytes > PACKAGE_AGENT_SKILLS_MAX_BYTES) {
      throw new AgentProblem(
         `its skill files total ${bytes} bytes, over the ${PACKAGE_AGENT_SKILLS_MAX_BYTES} byte cap`,
      );
   }
   return {
      roots,
      files: [...served]
         .sort(([a], [b]) => byCodepoint(a, b))
         .map(([relative_filepath, file_contents]) => ({
            relative_filepath,
            file_contents,
         })),
   };
}

function readAgent(
   packagePath: string,
   realRoot: string,
   name: string,
   raw: unknown,
): { agent: PackageAgent; paths: string[] } {
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
   const skills = readAgentSkills(packagePath, raw.skills, paths, warnings);

   const definitionSha = canonicalSha({
      name,
      description: raw.description,
      model,
      instructionsPath: instructions.rel,
      skillRoots: skills.roots,
      schedules,
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
         schedules,
         warnings,
         definitionSha,
      },
      paths,
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
   const result: PackageAgents = { agents: new Map(), paths: [], warnings: [] };
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
         const { agent, paths } = readAgent(packagePath, realRoot, name, entry);
         result.agents.set(name, agent);
         result.paths.push(...paths);
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
 * Shadowing by name is the whole mechanism. Publisher does not know which
 * skills the calling agent already holds, so it cannot patch or merge one; it
 * can only decide what to return when asked for a name. A package that wants
 * to change how an agent uses IT ships a file under that agent-facing name, and
 * gets its version back.
 *
 * `origin` rides on every entry because a replaced base skill is otherwise
 * indistinguishable from the original, which is exactly the thing a reader
 * debugging an agent's behaviour needs to see.
 */
export function resolveSkills(
   bundled: SkillEntry[],
   packageSkills: SkillEntry[],
): ResolvedSkill[] {
   const shadows = new Map(packageSkills.map((s) => [s.name, s]));
   const bundledNames = new Set(bundled.map((s) => s.name));
   // Bundled order first, each entry replaced in place by the package's copy
   // when there is one, then the package's new names in their own order. Two
   // packages' listings therefore line up row for row wherever they agree.
   return [
      ...bundled.map((s): ResolvedSkill => {
         const shadow = shadows.get(s.name);
         return shadow
            ? { ...shadow, origin: "package" }
            : { ...s, origin: "bundled" };
      }),
      ...packageSkills
         .filter((s) => !bundledNames.has(s.name))
         .map((s): ResolvedSkill => ({ ...s, origin: "package" })),
   ];
}
