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

/** Parse a SKILL.md into its frontmatter name/description and Markdown body. */
export function parseSkill(md: string, dirName: string): SkillEntry {
   const text = md.replace(/\r\n/g, "\n"); // tolerate CRLF-committed files
   const fm = text.match(/^---\n([\s\S]*?)\n---\n?([\s\S]*)$/);
   const front = fm ? fm[1] : "";
   const body = (fm ? fm[2] : text).trim();
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

/** Read every skill under a skills directory, in the bundle's own order. */
export function buildSkills(skillsDir: string): SkillEntry[] {
   const skills: SkillEntry[] = [];
   for (const entry of fs.readdirSync(skillsDir, { withFileTypes: true })) {
      if (!entry.isDirectory() || isCredible(entry.name)) continue;
      const skillFile = path.join(skillsDir, entry.name, "SKILL.md");
      if (!fs.existsSync(skillFile)) continue;
      skills.push(parseSkill(fs.readFileSync(skillFile, "utf8"), entry.name));
   }
   return skills.sort((a, b) => a.name.localeCompare(b.name));
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

   fs.writeFileSync(outFile, JSON.stringify({ skills }));
   console.log(`Wrote ${skills.length} skills to ${outFile}`);
}

// Only run when invoked directly (bun run ...), not when imported by a test.
if (import.meta.main) {
   main();
}
