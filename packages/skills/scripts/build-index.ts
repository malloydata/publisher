/**
 * Emit skills/malloy/SKILL.md from each skill's frontmatter.
 *
 * Grouping is editorial and lives in INDEX_GROUPS below, not in a manifest.
 * A skill that exists on disk but is missing from a group (or a group that
 * names a skill that does not exist) fails the build rather than drifting.
 *
 *   bun run --cwd packages/skills build-index
 */
import * as fs from "node:fs";
import * as path from "node:path";
import { fileURLToPath } from "node:url";
import { isCredible } from "./exclusions";

export const INDEX_NAME = "malloy";

export interface IndexGroup {
   heading: string;
   skills: string[];
}

/**
 * Editorial grouping for the index tables. Membership is the contract:
 * every on-disk skill except the index itself must appear in exactly one
 * group, and every name here must exist on disk.
 */
export const INDEX_GROUPS: IndexGroup[] = [
   {
      heading: "Start here",
      skills: [
         "malloy-publisher-setup",
         "malloy-getting-started",
         "malloy-modeling",
         "malloy-analysis",
      ],
   },
   {
      heading: "Modeling phases",
      skills: [
         "malloy-discover",
         "malloy-scope",
         "malloy-define",
         "malloy-model",
         "malloy-document",
         "malloy-lookml-review",
         "malloy-model-as-you-go",
      ],
   },
   {
      heading: "Analysis and presentation",
      skills: [
         "malloy-charts",
         "malloy-dashboards",
         "malloy-notebooks",
         "malloy-analysis-report",
         "malloy-analysis-pitfalls",
         "malloy-notebook-chat",
         "malloy-phrase-detection",
      ],
   },
   {
      heading: "Writing correct Malloy",
      skills: [
         "malloy-queries",
         "malloy-gotchas-modeling",
         "malloy-gotchas-queries",
         "malloy-gotchas-rendering",
         "malloy-debug",
         "malloy-patterns",
         "malloy-review",
      ],
   },
   {
      heading: "Serving and operating a package",
      skills: [
         "malloy-publish",
         "malloy-html-data-apps",
         "malloy-html-data-app-runtime",
         "malloy-html-data-app-embedding",
         "malloy-materialization",
         "malloy-materialization-tuning",
      ],
   },
];

export interface SkillFrontmatter {
   name: string;
   description: string;
}

function unquote(value: string): string {
   const trimmed = value.trim();
   if (
      trimmed.startsWith("'") &&
      trimmed.endsWith("'") &&
      trimmed.length >= 2
   ) {
      return trimmed.slice(1, -1).replace(/''/g, "'");
   }
   if (
      trimmed.startsWith('"') &&
      trimmed.endsWith('"') &&
      trimmed.length >= 2
   ) {
      return trimmed.slice(1, -1).replace(/\\"/g, '"');
   }
   return trimmed;
}

/** Read name and description from every SKILL.md under skillsDir. */
export function listSkillFrontmatter(skillsDir: string): SkillFrontmatter[] {
   const skills: SkillFrontmatter[] = [];
   for (const entry of fs.readdirSync(skillsDir, { withFileTypes: true })) {
      if (!entry.isDirectory() || isCredible(entry.name)) continue;
      const skillFile = path.join(skillsDir, entry.name, "SKILL.md");
      if (!fs.existsSync(skillFile)) continue;
      const markdown = fs
         .readFileSync(skillFile, "utf8")
         .replace(/\r\n/g, "\n");
      const frontmatter = markdown.match(/^---\n([\s\S]*?)\n---/)?.[1] ?? "";
      skills.push({
         name: unquote(frontmatter.match(/^name:\s*(.+)$/m)?.[1] ?? entry.name),
         description: unquote(
            frontmatter.match(/^description:\s*(.+)$/m)?.[1] ?? "",
         ),
      });
   }
   return skills.sort((a, b) =>
      a.name < b.name ? -1 : a.name > b.name ? 1 : 0,
   );
}

function cell(text: string): string {
   return text.replace(/\|/g, "\\|");
}

function coverageErrors(onDisk: string[], groups: IndexGroup[]): string[] {
   const grouped: string[] = [];
   const seen = new Set<string>();
   const errors: string[] = [];
   for (const group of groups) {
      for (const name of group.skills) {
         if (seen.has(name)) {
            errors.push(`'${name}' appears in more than one group`);
         }
         seen.add(name);
         grouped.push(name);
      }
   }
   const disk = new Set(onDisk);
   for (const name of grouped) {
      if (!disk.has(name)) {
         errors.push(`group names '${name}', which is not on disk`);
      }
   }
   for (const name of onDisk) {
      if (!seen.has(name)) {
         errors.push(`'${name}' is on disk but in no group`);
      }
   }
   return errors;
}

/**
 * The committed index, regenerated from frontmatter plus the editorial
 * grouping. Throws if INDEX_GROUPS and the tree disagree.
 */
export function buildIndex(skillsDir: string): string {
   const skills = listSkillFrontmatter(skillsDir);
   const byName = new Map(skills.map((s) => [s.name, s]));
   const catalogued = skills
      .map((s) => s.name)
      .filter((name) => name !== INDEX_NAME);
   const errors = coverageErrors(catalogued, INDEX_GROUPS);
   if (errors.length > 0) {
      throw new Error(
         `skills/malloy/SKILL.md grouping is stale:\n${errors
            .map((e) => `  - ${e}`)
            .join("\n")}`,
      );
   }

   const tables: string[] = [];
   for (const group of INDEX_GROUPS) {
      const rows = group.skills.map((name) => {
         const skill = byName.get(name);
         if (!skill) {
            throw new Error(`group names '${name}', which is not on disk`);
         }
         return `| \`skill:${name}\` | ${cell(skill.description)} |`;
      });
      tables.push(
         `**${group.heading}**\n\n| Skill | Use when... |\n|-------|-------------|\n${rows.join("\n")}`,
      );
   }

   return `---
name: malloy
description: Index of all Malloy skills. Use when user asks "malloy help", "what malloy skills are available", "how do I use malloy", or needs guidance on which Malloy skill to use.
---

# Malloy Skills Index

## First-Time Setup

**Tools missing or server not running?**
Load \`skill:malloy-publisher-setup\`.

**No .malloy files in workspace?**
Say "model my data" and the agent will orchestrate the full modeling workflow automatically. Make sure the Malloy Publisher MCP tools are configured first.

## Skill Reference

Every skill in this deployment, by what it is for. Start at a driver; it routes to the rest.

${tables.join("\n\n")}

> **Adapter pattern:** Each prior art adapter (LookML, future dbt) follows the same structure: a coordinator SKILL.md plus reference files under \`reference/\` dispatched by phase skills.

## Workflows

Two top-level workflows orchestrate the phase and support skills above:

- **Model data from scratch:** load \`skill:malloy-modeling\`. It drives the full pipeline (discover, scope, define, build, review, curate) and routes to the phase skills.
- **Answer a data question or explore:** load \`skill:malloy-analysis\`. It drives discovery, query construction, verification, and open-ended exploration. Use \`skill:malloy-charts\` for visualization and \`skill:malloy-notebooks\` or \`skill:malloy-dashboards\` to persist the result.

Publishing is out of scope for open-source Publisher v1. Self-hosters move a finished model into a served package via git and the host's publish path; see \`skill:malloy-publish\`.

## Syntax Help

Call \`malloy_searchDocs\` with your question. Use \`skill:malloy-patterns\` to discover available topics.
`;
}

const packageDir = path.dirname(path.dirname(fileURLToPath(import.meta.url)));
export const repoSkillsDir = path.join(packageDir, "..", "..", "skills");
export const indexPath = path.join(repoSkillsDir, INDEX_NAME, "SKILL.md");

if (import.meta.main) {
   fs.writeFileSync(indexPath, buildIndex(repoSkillsDir));
   console.log(`Wrote ${indexPath}`);
}
