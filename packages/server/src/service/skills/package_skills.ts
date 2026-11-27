// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import type { BundledSkill, SkillCatalogEntry } from "./build_skills_bundle";
import {
   DEFAULT_WORKFLOW,
   servedInline,
   type PackageWorkflow,
   type PackageWorkflows,
   type SetSkill,
   type WorkflowSet,
} from "./package_workflows";
import { SKILL_MD } from "./skill_files";
import skillsBundle from "./skills_bundle.json";
import { compareText } from "../../compare_text";

/** The build-time bundle of the repo's skills: each one's manifest, frontmatter and Markdown text. */
export const BUNDLED_CATALOG = (
   skillsBundle as unknown as { skills: BundledSkill[] }
).skills;

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

/** The sets a read covers: the workflow's own, each without a skill name an earlier set holds, or with `skillSet` that one set whole, since a reply of one set has nothing to collide with and its digest then pins exactly what it serves. Undefined when the workflow lists no set of that name. */
export function pickedSets(
   workflow: PackageWorkflow,
   skillSet?: string,
): WorkflowSet[] | undefined {
   if (skillSet === undefined) return workflow.sets;
   const picked = workflow.sets.find(({ set }) => set.name === skillSet);
   return picked && [{ set: picked.set, skills: picked.set.skills }];
}

/**
 * A workflow's set as the wire carries it. The full view holds each inline
 * file's text and every listed file; the summary swaps each text for its size
 * and gives a SKILL.md its name and description. Files are sorted by path.
 */
export function workflowSkillSet(
   { set, skills }: WorkflowSet,
   view: "full" | "summary",
) {
   const files = skills
      .flatMap(servedInline)
      .sort((a, b) => compareText(a.relative_filepath, b.relative_filepath));
   const assets = skills
      .flatMap((skill) => skill.listed)
      .map(({ relative_filepath, bytes, sha256 }) => ({
         relative_filepath,
         bytes,
         sha256,
      }))
      .sort((a, b) => compareText(a.relative_filepath, b.relative_filepath));
   return {
      name: set.name,
      path: set.path,
      digest: set.digest,
      skills:
         view === "full"
            ? files.map(({ relative_filepath, file_contents }) => ({
                 relative_filepath,
                 file_contents,
              }))
            : files.map(
                 ({ relative_filepath, file_contents, name, description }) => ({
                    relative_filepath,
                    ...(name !== undefined && { name, description }),
                    bytes: Buffer.byteLength(file_contents),
                 }),
              ),
      ...(view === "full" && assets.length > 0 && { assets }),
   };
}

/** One file of a workflow's skill by its `<skill>/<file>` path: an inline file as the workflow serves it, or a listed file as the skill reads it (a binary one answers with a message naming its size, one that changed since load with `changed`). Undefined when it is not served. */
export function readWorkflowFile(
   skill: SetSkill,
   relativeFilepath: string,
): { text: string } | { message: string } | { changed: true } | undefined {
   const inline = servedInline(skill).find(
      (f) => f.relative_filepath === relativeFilepath,
   );
   if (inline) return { text: inline.file_contents };
   const listing = skill.listed.find(
      (a) => a.relative_filepath === relativeFilepath,
   );
   if (!listing) return undefined;
   const read = skill.served.read(
      relativeFilepath.slice(skill.dirName.length + 1),
   );
   if ("text" in read || "changed" in read) return read;
   return {
      message: binaryFileMessage(relativeFilepath, listing.bytes),
   };
}

/** The skills a package advertises: its `default` workflow's, each skill name served from the first set that holds it. */
export function advertisedSkills(workflows: PackageWorkflows): ServedSkill[] {
   return (
      workflows.workflows
         .get(DEFAULT_WORKFLOW)
         ?.sets.flatMap(({ skills }) => skills.map((skill) => skill.served)) ??
      []
   );
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

/** A workflow file path as a caller wrote it, `<skill>/<file>`: a blank file part (`<skill>/`) names the skill's SKILL.md, as a blank `path` does in `get_skill`. */
export function workflowFilePath(raw: string): string {
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
export function readSkillFile(
   skill: ServedSkill,
   relative: string,
): { text: string } | { message: string } | { changed: true } {
   const read = skill.read(relative);
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
