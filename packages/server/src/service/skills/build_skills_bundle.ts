// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * Build a bundled JSON of the agent skills so the MCP server can expose them as
 * MCP prompts (dual-channel): skill-aware hosts read the skill files directly, while
 * hosts that only ingest MCP get the same guidance through these prompts.
 *
 * Every file of a skill is in its manifest; only Markdown text is embedded, which keeps the bundle small.
 *
 * Committed as a build-time asset so the server has no runtime dependency on the
 * skills directory layout. Regenerate when the skills change:
 *
 *   bun run packages/server/src/service/skills/build_skills_bundle.ts <skills-dir> [out.json]
 *
 * <skills-dir> points at the repo's top-level skills/ directory.
 *
 * Indented rather than minified so concurrent edits to different skills land on
 * different lines and merge cleanly. The width is 3 to match tabWidth in
 * packages/server/.prettierrc, so prettier leaves the file byte-identical;
 * change it only alongside that setting.
 */
import { createHash } from "crypto";
import * as fs from "fs";
import * as path from "path";
import { parseFrontmatter } from "./frontmatter";
import { buildTimeSkillFiles, decodeText } from "./skill_files";
import { compareText } from "../../compare_text";

export interface SkillEntry {
   name: string;
   description: string;
   body: string;
}

/** One served skill, in the MCP skills extension's entry shape: `get_skill` is served from it. */
export interface SkillCatalogEntry {
   name: string;
   /** The SKILL.md frontmatter as JSON (dates as ISO strings, keys as strings); empty when it did not parse. */
   frontmatter: Record<string, unknown>;
   /** Every served file by path relative to the skill, inline or listed; `digest` is `sha256:` and the hex of the file's raw bytes. */
   files: Record<string, { digest: string; size: number }>;
}

/** A bundled skill: the catalog entry a package skill has, plus its Markdown files' verbatim text by path. */
export interface BundledSkill extends SkillCatalogEntry {
   bodies: Record<string, string>;
}

/**
 * Drop the license header (an HTML comment at the top of the Markdown body) and
 * trim. The header is for the file; the agent reading the prompt should not
 * see it as the first line.
 */
export function stripLicenseHeader(body: string): string {
   return body
      .replace(/^\s*<!--[\s\S]*?SPDX-License-Identifier:[\s\S]*?-->/, "")
      .trim();
}

/** Parse a SKILL.md into its frontmatter name/description and Markdown body. */
export function parseSkill(md: string, dirName: string): SkillEntry {
   const { name, description, body } = parseFrontmatter(md, dirName);
   return { name, description, body: stripLicenseHeader(body) };
}

/**
 * `credible-*` skills never ship (see skills/README.md) but can sit in skills/
 * as a gitignored local install, so the bundle must skip them or a regeneration
 * would bake them in. Duplicates packages/skills/scripts/exclusions.ts, because
 * the server does not depend on that package.
 */
export function isCredible(dirName: string): boolean {
   return dirName.toLowerCase().startsWith("credible-");
}

/** The subdirectory a skill keeps its on-demand detail in. */
const REFERENCE_DIR = "reference";

/** Prompt name for one reference file: `<skill>/<file stem>`. */
function referenceName(skillName: string, file: string): string {
   return `${skillName}/${path.basename(file, ".md")}`;
}

/**
 * Turn a reference file into an entry. These carry no frontmatter, so the
 * description comes from the first H1.
 */
export function parseReference(
   md: string,
   skillName: string,
   file: string,
): SkillEntry {
   const body = stripLicenseHeader(md.replace(/\r\n/g, "\n"));
   const heading = body.match(/^#\s+(.+)$/m)?.[1]?.trim();
   return {
      name: referenceName(skillName, file),
      description: heading
         ? `${heading}. Reference detail for the ${skillName} skill.`
         : `Reference detail for the ${skillName} skill.`,
      body,
   };
}

/**
 * Tell an agent that reached this body over MCP where the reference files went.
 * The skill's relative `reference/...` paths are unresolvable over MCP, and
 * rewriting them in the source would break the filesystem channel, so the
 * mapping is appended to the served copy only.
 */
export function referencePointer(
   skillName: string,
   files: string[],
   channel: "bundled" | "package" = "bundled",
): string {
   const stems = files.map((f) => path.basename(f, ".md"));
   // Prompts are registered once at server start, for bundled skills only.
   const how =
      channel === "bundled"
         ? `served as separate prompts, one per file, fetched only when you ask for them. Where the text above says to read \`${REFERENCE_DIR}/<name>.md\`, get the prompt named \`${skillName}/<name>\` instead.`
         : `served as separate guides, one per file, fetched only when you ask for them. Where the text above says to read \`${REFERENCE_DIR}/<name>.md\`, call get_skill with skill_name \`${skillName}/<name>\` and the same \`scopes\` you used for this skill instead.`;
   return [
      `## Reference files over MCP`,
      ``,
      `This skill's \`${REFERENCE_DIR}/\` files are ${how}`,
      ``,
      `Available: ${stems.join(", ")}.`,
   ].join("\n");
}

/**
 * The skills the deployment manifest ships, as a set. The manifest is the single
 * answer to "what ships", so a skill merely present in the tree does not reach
 * agents. Read rather than imported (see `isCredible`), and resolved relative to
 * the given skills directory so a spec's tree gets its own manifest.
 */
function manifestSkillNames(skillsDir: string): Set<string> {
   const manifestFile = path.join(
      path.dirname(skillsDir),
      "manifests",
      "publisher-local.json",
   );
   const manifest = JSON.parse(fs.readFileSync(manifestFile, "utf8")) as {
      auto_discovered?: string[];
      supporting?: string[];
   };
   return new Set([
      ...(manifest.auto_discovered ?? []),
      ...(manifest.supporting ?? []),
   ]);
}

/** One skill directory as the bundle holds it; a finding of the frontmatter fails the build, like one of the walk. */
function bundleSkill(skillDir: string, dirName: string): BundledSkill {
   const files: BundledSkill["files"] = {};
   const bodies: BundledSkill["bodies"] = {};
   for (const relative of buildTimeSkillFiles(skillDir)) {
      const bytes = fs.readFileSync(path.join(skillDir, relative));
      files[relative] = {
         digest: `sha256:${createHash("sha256").update(bytes).digest("hex")}`,
         size: bytes.length,
      };
      // Decision 1's inline rule: every Markdown file outside assets/.
      const text =
         relative.endsWith(".md") && !relative.startsWith("assets/")
            ? decodeText(bytes)
            : undefined;
      if (text !== undefined) bodies[relative] = text;
   }
   const fm = parseFrontmatter(bodies["SKILL.md"] ?? "", dirName);
   if (fm.warnings.length > 0) {
      throw new Error(`skill '${dirName}': ${fm.warnings.join("; ")}`);
   }
   return { name: fm.name, frontmatter: fm.frontmatter, files, bodies };
}

/**
 * Read every skill the manifest ships under a skills directory, in codepoint
 * order of name: the committed artifact's order must be identical on every
 * machine, and localeCompare follows the runtime locale.
 */
export function buildSkills(skillsDir: string): BundledSkill[] {
   const shipping = manifestSkillNames(skillsDir);
   const skills: BundledSkill[] = [];
   for (const entry of fs.readdirSync(skillsDir, { withFileTypes: true })) {
      if (isCredible(entry.name) || !shipping.has(entry.name)) continue;
      // A link is kept so that bundleSkill refuses it rather than the bundle quietly lacking the skill.
      if (!entry.isDirectory() && !entry.isSymbolicLink()) continue;
      const skillDir = path.join(skillsDir, entry.name);
      if (!fs.existsSync(path.join(skillDir, "SKILL.md"))) continue;
      skills.push(bundleSkill(skillDir, entry.name));
   }
   return skills.sort((a, b) => compareText(a.name, b.name));
}

/**
 * The flat `{name, description, body}` entries `get_skill`, the REST route and
 * the MCP prompts still read: each skill's `SKILL.md` body with a pointer
 * section, and each `reference/<stem>.md` as an entry named `<skill>/<stem>`.
 */
export function flatSkills(bundled: BundledSkill[]): SkillEntry[] {
   const skills: SkillEntry[] = [];
   for (const { name, bodies } of bundled) {
      const skill = parseSkill(bodies["SKILL.md"] ?? "", name);
      const references = Object.keys(bodies).filter((p) =>
         /^reference\/[^/]+$/.test(p),
      );
      if (references.length > 0) {
         skill.body = `${skill.body}\n\n${referencePointer(skill.name, references)}`;
      }
      skills.push(
         skill,
         ...references.map((p) => parseReference(bodies[p]!, skill.name, p)),
      );
   }
   return skills.sort((a, b) => compareText(a.name, b.name));
}

function main(): void {
   const skillsDir = process.argv[2];
   const outFile =
      process.argv[3] || path.join(import.meta.dir, "skills_bundle.json");
   if (!skillsDir || !fs.existsSync(skillsDir)) {
      console.error(
         `skills dir not found: ${skillsDir}\nUsage: bun run build_skills_bundle.ts <skills-dir> [out.json]`,
      );
      process.exit(1);
   }

   const skills = buildSkills(skillsDir);

   fs.writeFileSync(outFile, `${JSON.stringify({ skills }, null, 3)}\n`);
   const files = skills.reduce((n, s) => n + Object.keys(s.files).length, 0);
   console.log(
      `Wrote ${skills.length} skills (${files} files in their manifests) to ${outFile}`,
   );
}

// Only run when invoked directly (bun run ...), not when imported by a test.
if (import.meta.main) {
   main();
}
