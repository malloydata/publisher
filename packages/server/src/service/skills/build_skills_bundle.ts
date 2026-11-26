// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * Build a bundled JSON of the agent skills so the MCP server can expose them as
 * MCP prompts (dual-channel): skill-aware hosts read the skill files directly, while
 * hosts that only ingest MCP get the same guidance through these prompts.
 *
 * Every file of a skill is in its manifest; only Markdown outside `assets/` is embedded, which keeps the bundle small.
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
import {
   buildTimeSkillFiles,
   decodeText,
   isCredible,
   isInlineMarkdown,
} from "./skill_files";
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
   // No prototype, so a root file named `__proto__` is an entry like any other.
   const files: BundledSkill["files"] = Object.create(null);
   const bodies: BundledSkill["bodies"] = Object.create(null);
   for (const relative of buildTimeSkillFiles(skillDir)) {
      const bytes = fs.readFileSync(path.join(skillDir, relative));
      files[relative] = {
         digest: `sha256:${createHash("sha256").update(bytes).digest("hex")}`,
         size: bytes.length,
      };
      const text = isInlineMarkdown(relative) ? decodeText(bytes) : undefined;
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
      // A link is kept so that bundleSkill throws on it; a link to nothing has no SKILL.md and is skipped below.
      if (!entry.isDirectory() && !entry.isSymbolicLink()) continue;
      const skillDir = path.join(skillsDir, entry.name);
      if (!fs.existsSync(path.join(skillDir, "SKILL.md"))) continue;
      skills.push(bundleSkill(skillDir, entry.name));
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
