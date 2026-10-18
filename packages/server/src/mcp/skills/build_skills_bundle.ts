// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * Build a bundled JSON of the agent skills so the MCP server can expose them as
 * MCP prompts (dual-channel): skill-aware hosts read the skill files directly, while
 * hosts that only ingest MCP get the same guidance through these prompts.
 *
 * Committed as a build-time asset so the server has no runtime dependency on the
 * skills directory layout. Regenerate when the skills change:
 *
 *   bun run packages/server/src/mcp/skills/build_skills_bundle.ts <skills-dir> [out.json]
 *
 * <skills-dir> points at the repo's top-level skills/ directory.
 *
 * Indented rather than minified so concurrent edits to different skills land on
 * different lines and merge cleanly. The width is 3 to match tabWidth in
 * packages/server/.prettierrc, so prettier leaves the file byte-identical;
 * change it only alongside that setting.
 */
import * as fs from "fs";
import * as path from "path";

export interface SkillEntry {
   name: string;
   description: string;
   body: string;
}

export function unquote(s: string): string {
   return s.trim().replace(/^["']|["']$/g, "");
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
   const text = md.replace(/\r\n/g, "\n"); // tolerate CRLF-committed files
   const fm = text.match(/^---\n([\s\S]*?)\n---\n?([\s\S]*)$/);
   const front = fm ? fm[1] : "";
   const body = stripLicenseHeader(fm ? fm[2] : text);
   const name = unquote(front.match(/^name:\s*(.+)$/m)?.[1] ?? dirName);
   const description = unquote(
      front.match(/^description:\s*(.+)$/m)?.[1] ?? "",
   );
   return { name, description, body };
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
export function referenceName(skillName: string, file: string): string {
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

/** Reference files for one skill, sorted, or [] when it has none. */
export function referenceFiles(skillDir: string): string[] {
   const dir = path.join(skillDir, REFERENCE_DIR);
   if (!fs.existsSync(dir)) return [];
   return fs
      .readdirSync(dir)
      .filter((f) => f.endsWith(".md"))
      .sort();
}

/**
 * Read every skill under a skills directory, in the bundle's own order.
 *
 * A skill's `reference/` files become entries of their own rather than being
 * folded into the parent body, so they stay on-demand detail over MCP.
 */
export function buildSkills(skillsDir: string): SkillEntry[] {
   const shipping = manifestSkillNames(skillsDir);
   const skills: SkillEntry[] = [];
   for (const entry of fs.readdirSync(skillsDir, { withFileTypes: true })) {
      if (!entry.isDirectory() || isCredible(entry.name)) continue;
      if (!shipping.has(entry.name)) continue;
      const skillDir = path.join(skillsDir, entry.name);
      const skillFile = path.join(skillDir, "SKILL.md");
      if (!fs.existsSync(skillFile)) continue;

      const skill = parseSkill(fs.readFileSync(skillFile, "utf8"), entry.name);
      const files = referenceFiles(skillDir);
      if (files.length > 0) {
         skill.body = `${skill.body}\n\n${referencePointer(skill.name, files)}`;
      }
      skills.push(skill);

      for (const file of files) {
         skills.push(
            parseReference(
               fs.readFileSync(
                  path.join(skillDir, REFERENCE_DIR, file),
                  "utf8",
               ),
               skill.name,
               file,
            ),
         );
      }
   }
   // Codepoint order, not localeCompare: the committed artifact's order must be
   // identical on every machine, and localeCompare follows the runtime locale.
   return skills.sort((a, b) =>
      a.name < b.name ? -1 : a.name > b.name ? 1 : 0,
   );
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
   console.log(
      `Wrote ${skills.length} entries (${skills.filter((s) => !s.name.includes("/")).length} skills plus ${skills.filter((s) => s.name.includes("/")).length} reference files) to ${outFile}`,
   );
}

// Only run when invoked directly (bun run ...), not when imported by a test.
if (import.meta.main) {
   main();
}
