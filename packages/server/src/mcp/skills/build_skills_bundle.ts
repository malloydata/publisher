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
 * Indented rather than minified, and that is load-bearing rather than cosmetic.
 * One line per entry field gives git something to merge: two branches that edit
 * different skills land on different lines and merge without a conflict, where a
 * single-line bundle made every pair of concurrent skills edits collide on the
 * one line the whole file consists of. A skill body stays on one line either
 * way, since a JSON string cannot hold a raw newline, so what remains is a
 * normal line-scoped conflict: the two branches touched the same region, either
 * the same entry or two entries inserted at the same sorted position.
 *
 * The width is 3 to match tabWidth in packages/server/.prettierrc, which makes
 * this output byte-identical to what prettier emits. At any other width the file
 * is permanently unformatted by its own package's config, so a contributor with
 * format-on-save rewrites every line of it just by opening the file. Change this
 * only alongside that setting.
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
 * `credible-*` skills are Credible-platform-specific and never ship (see
 * skills/README.md). They can legitimately sit in skills/ uncommitted: the
 * directory is a gitignored local install target (`skills/credible-*` in
 * .gitignore), so the bundle must skip them or a regeneration on a machine
 * with Credible installed would bake them into the committed bundle, and the
 * regenerate-and-compare spec would fail on a clean checkout.
 *
 * The canonical predicate lives in packages/skills/scripts/exclusions.ts;
 * duplicated here because the server does not depend on that package.
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
 * description comes from the first H1, which every reference file in the tree
 * has ("# Rubric: Correctness", "# Severity, Confidence, Categorization").
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
 *
 * The skill text points at them by relative path ("see `reference/rubric-*.md`"),
 * which is correct for a host reading the skill off disk and unresolvable for
 * one that received this as a prompt: nothing in the prompt says which directory
 * to stand in. Rewriting the paths in the source file is not an option, since
 * that would break the filesystem channel, so the mapping is appended to the
 * bundled copy only. One source file, one correct rendering per channel.
 */
export function referencePointer(skillName: string, files: string[]): string {
   const stems = files.map((f) => path.basename(f, ".md"));
   return [
      `## Reference files over MCP`,
      ``,
      `This skill's \`${REFERENCE_DIR}/\` files are served as separate prompts, one per file, fetched only when you ask for them. Where the text above says to read \`${REFERENCE_DIR}/<name>.md\`, get the prompt named \`${skillName}/<name>\` instead.`,
      ``,
      `Available: ${stems.join(", ")}.`,
   ].join("\n");
}

/** Reference files for one skill, sorted, or [] when it has none. */
function referenceFiles(skillDir: string): string[] {
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
 * folded into the parent body: they are the skill's on-demand detail (a review
 * loads the rubrics its scope needs, not all ten), and one prompt per file
 * keeps that property over MCP. Folding them in would put 83k characters of
 * malloy-review rubrics in front of an agent that wanted the workflow.
 */
export function buildSkills(skillsDir: string): SkillEntry[] {
   const skills: SkillEntry[] = [];
   for (const entry of fs.readdirSync(skillsDir, { withFileTypes: true })) {
      if (!entry.isDirectory() || isCredible(entry.name)) continue;
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
   // Codepoint order, not localeCompare: entry order decides line positions in a
   // committed artifact, so it has to be identical on every contributor's machine.
   // localeCompare follows the runtime's locale (under cs-CZ the `ch` digraph sorts
   // after `h`, moving malloy-charts), which made a regeneration produce a spurious
   // reordering diff unrelated to the skill the contributor actually edited.
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
