// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import type { components } from "../../api";
import type { BundledSkill } from "./build_skills_bundle";
import {
   DEFAULT_SKILL_SET,
   skillSetRows,
   type ServedSkill,
   type ServedWorkflows,
   type SetSkill,
   type SkillSet,
   type SkillSetRows,
} from "./package_workflows";
import { SKILL_MD } from "./skill_files";
import skillsBundle from "./skills_bundle.json";
import { compareText } from "../../compare_text";

type ApiSkillSet = components["schemas"]["SkillSet"];
type ApiSkillSetSummary = components["schemas"]["SkillSetSummary"];

/** The build-time bundle of the repo's skills: each one's manifest, frontmatter and Markdown text. */
export const BUNDLED_CATALOG = (
   skillsBundle as unknown as { skills: BundledSkill[] }
).skills;

/** The names of the bundled skills, which the package-load worker reads a package's skills against; computed once so the worker never imports the bundle. */
export const BUNDLED_SKILL_NAMES = BUNDLED_CATALOG.map((skill) => skill.name);

export type { ServedSkill, SkillFileRead } from "./package_workflows";

/** Where a skill came from, once the bundled and package sets are resolved. */
type SkillOrigin = "bundled" | "package";

/** Why a listed file that changed since the package loaded is not served. */
export function fileChangedMessage(relative: string): string {
   return `'${relative}' changed on disk after the package loaded, so it is not served until the package reloads.`;
}

export interface ResolvedSkill extends ServedSkill {
   origin: SkillOrigin;
}

/** A set's rows, built on the first read of its load: a reload adopts new set objects, so they last exactly as long as the content they hold. */
const rowsOfSet = new WeakMap<SkillSet, SkillSetRows>();

/** The one projection of a skill set a reply carries. The full view holds each inline file's text and every listed file; the summary swaps each text for its size on disk. Files are sorted by path. Every route embeds this same value, so a set inside a workflow is the set route's reply. */
export function skillSetView(
   set: SkillSet,
   view: "full" | "summary",
): ApiSkillSet {
   let rows = rowsOfSet.get(set);
   if (!rows) {
      rows = skillSetRows(set);
      rowsOfSet.set(set, rows);
   }
   return {
      name: set.name,
      ...(set.description !== undefined && { description: set.description }),
      path: set.path,
      digest: set.digest,
      skills: view === "full" ? rows.full : rows.summary,
      ...(view === "full" && rows.assets.length > 0 && { assets: rows.assets }),
      warnings: set.findings,
   };
}

/** A set without its skills: its name, description and digest, which is also the pin a cut reply keeps. */
export function skillSetSummary(set: SkillSet): ApiSkillSetSummary {
   return {
      name: set.name,
      ...(set.description !== undefined && { description: set.description }),
      digest: set.digest,
   };
}

/** One file of a set's skill by its `<skill>/<file>` path: an inline file as written, or a listed file as the skill reads it (a binary one answers with a message naming its size, one that changed since load with `changed`). Undefined when it is not served. */
export async function readSkillSetFile(
   skill: SetSkill,
   relativeFilepath: string,
): Promise<
   { text: string } | { message: string } | { changed: true } | undefined
> {
   const prefix = `${skill.dirName}/`;
   if (!relativeFilepath.startsWith(prefix)) return undefined;
   const relative = relativeFilepath.slice(prefix.length);
   const inline = skill.inline.find((f) => f.relative === relative);
   if (inline) return { text: inline.text };
   const listing = skill.listed.find(
      (a) => a.relative_filepath === relativeFilepath,
   );
   if (!listing) return undefined;
   const read = await skill.served.read(relative);
   if ("text" in read || "changed" in read) return read;
   return {
      message: binaryFileMessage(relativeFilepath, listing.bytes),
   };
}

/** The skills a package advertises: those of its `default` skill set, and the names that set's `overrides` lets them replace. */
export interface DefaultSkills {
   skills: ServedSkill[];
   overrides: readonly string[];
}

/** What a package with no `default` set advertises. */
export const NO_DEFAULT_SKILLS: DefaultSkills = { skills: [], overrides: [] };

/** The `default` skill set as advertised, whole: which of its skills are served where a bundled one has the name is {@link resolveSkills}'s to say. */
export function advertisedSkills(workflows: ServedWorkflows): DefaultSkills {
   const set = workflows.sets.get(DEFAULT_SKILL_SET);
   return set
      ? {
           skills: set.skills.map((skill) => skill.served),
           overrides: set.overrides,
        }
      : NO_DEFAULT_SKILLS;
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
      read: async (relative) =>
         Object.hasOwn(bodies, relative)
            ? { text: bodies[relative]! }
            : {
                 message: `'${relative}' (${catalog.files[relative]!.size} bytes) is part of the bundled skill '${catalog.name}' but this server does not carry it. Install \`${SKILLS_PACKAGE}\` to get it.`,
              },
   };
}

/**
 * The skills in force for one package: the bundled set, with the `default`
 * set's skills added, and a `default` skill named like a bundled one served in
 * its place only when `default`'s `overrides` lists the name. Otherwise the
 * bundled skill is served and the package's copy is left out here; the set
 * routes still serve it.
 *
 * Choosing what a name returns is the whole mechanism: Publisher does not know
 * which skills the agent already holds, so it cannot merge, only choose. A
 * replaced skill's bundled files go with it. `origin` is on every entry because
 * a replaced skill is otherwise indistinguishable from the original.
 */
export function resolveSkills(
   bundled: BundledSkill[],
   own: DefaultSkills = NO_DEFAULT_SKILLS,
): ResolvedSkill[] {
   const byName = new Map(own.skills.map((s) => [s.name, s]));
   const overrides = new Set(own.overrides);
   const bundledNames = new Set(bundled.map((s) => s.name));
   // Bundled order first, each skill replaced in place by the package's copy
   // when it overrides, then the package's new names in their own order. Two
   // packages' listings therefore line up row for row wherever they agree.
   return [
      ...bundled.map((b): ResolvedSkill => {
         const replacement = overrides.has(b.name)
            ? byName.get(b.name)
            : undefined;
         // A bundled manifest is informational: it lists a template's publisher.json, which the package reserved-name and listed-file rules would skip.
         return replacement
            ? { ...replacement, origin: "package" }
            : { ...bundledSkill(b), origin: "bundled" };
      }),
      ...own.skills
         .filter((s) => !bundledNames.has(s.name))
         .map((s): ResolvedSkill => ({ ...s, origin: "package" })),
   ];
}

/** A path without a leading `./`, which names the same file. */
export function withoutDotSlash(raw: string): string {
   let relative = raw;
   while (relative.startsWith("./")) relative = relative.slice(2);
   return relative;
}

/** A path as a caller wrote it, as a path of the skill: without a leading `./`, and `SKILL.md` when blank. Never trimmed otherwise, because a listed name may end in a space. */
export function skillFilePath(raw: string | null | undefined): string {
   if (!raw?.trim()) return SKILL_MD;
   return withoutDotSlash(raw) || SKILL_MD;
}

/** A skill set file path as a caller wrote it, `<skill>/<file>`: a blank file part (`<skill>/`) names the skill's SKILL.md, as a blank `path` does in `get_skill`. */
export function skillSetFilePath(raw: string): string {
   const relative = withoutDotSlash(raw);
   const slash = relative.indexOf("/");
   return slash > 0
      ? `${relative.slice(0, slash)}/${skillFilePath(relative.slice(slash + 1))}`
      : relative;
}

/** The answer for a listed file that is not text; the one sentence every surface uses. */
export function binaryFileMessage(relative: string, size: number): string {
   return `'${relative}' is a binary file (${size} bytes), and only text files are served.`;
}

/** One listed file of a skill read for an answer: its text, a message saying why there is none, or `changed` when it is no longer the file that was listed. */
export async function readSkillFile(
   skill: ServedSkill,
   relative: string,
): Promise<{ text: string } | { message: string } | { changed: true }> {
   const read = await skill.read(relative);
   return "binary" in read
      ? {
           message: binaryFileMessage(
              relative,
              skill.catalog.files[relative]!.size,
           ),
        }
      : read;
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
