// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import * as fs from "node:fs";
import * as path from "node:path";
import { parseDocument } from "yaml";

function unquote(value: string): string {
   return value.trim().replace(/^["']|["']$/g, "");
}

/** agentskills.io's name rule: 1-64 characters of lowercase letters, digits and single hyphens. */
const SKILL_NAME = /^[a-z0-9]+(-[a-z0-9]+)*$/;
const SKILL_NAME_MAX_CHARS = 64;

/** Alias-free output stays under 5x its source; an alias, even of a scalar, can exceed it. */
const EMIT_FACTOR = 5;
const EMIT_SLACK = 10_000;

/** YAML's parse time grows faster than its input, so a longer frontmatter is line-read; kept equal to the server reader's cap. */
const FRONTMATTER_MAX_BYTES = 4 * 1024;

/** The frontmatter as a JSON object, or undefined when YAML cannot be trusted to read it. */
function parseYaml(front: string): Record<string, unknown> | undefined {
   if (Buffer.byteLength(front) > FRONTMATTER_MAX_BYTES) return undefined;
   try {
      const doc = parseDocument(front, { logLevel: "silent" });
      if (doc.errors.length > 0) return undefined;
      let emitted = 0;
      const limit = EMIT_FACTOR * front.length + EMIT_SLACK;
      const bounded = (_key: string, node: unknown): unknown => {
         emitted += 1;
         if (typeof node === "string") emitted += node.length;
         else if (node === null || typeof node !== "object") {
            emitted += JSON.stringify(node)?.length ?? 0;
         } else if (!Array.isArray(node)) {
            for (const k of Object.keys(node)) emitted += k.length;
         }
         if (emitted > limit) throw new Error("expands past its source size");
         return node;
      };
      const value: unknown = JSON.parse(
         JSON.stringify(doc.toJS() ?? {}, bounded),
      );
      if (typeof value !== "object" || value === null || Array.isArray(value)) {
         return undefined;
      }
      return value as Record<string, unknown>;
   } catch {
      return undefined;
   }
}

/**
 * A skill's `name` and `description` from its frontmatter: read the YAML, and
 * on a parse failure or a name outside the spec fall back to a line read. A
 * second reader beside the server's `parseFrontmatter`
 * (packages/server/src/service/skills/frontmatter.ts), which a published
 * runtime package cannot import; skill_frontmatter.spec.ts holds the two to
 * the same answers. The fallback is silent here because a listing has no
 * channel for the warning the server raises.
 */
export function readFrontmatter(
   frontmatter: string,
   dirName: string,
): { name: string; description: string } {
   const line = (key: string) => {
      const value = frontmatter.match(
         new RegExp(`^${key}:[ \\t]*(\\S.*)$`, "m"),
      )?.[1];
      return value === undefined ? undefined : unquote(value);
   };
   const lineRead = {
      name: line("name") ?? dirName,
      description: line("description") ?? "",
   };
   const value = parseYaml(frontmatter);
   if (value === undefined) return lineRead;
   const declared = value.name ?? undefined;
   if (
      declared !== undefined &&
      !(
         typeof declared === "string" &&
         declared.length <= SKILL_NAME_MAX_CHARS &&
         SKILL_NAME.test(declared)
      )
   ) {
      return lineRead;
   }
   const description = value.description ?? "";
   return {
      name: typeof declared === "string" ? declared : dirName,
      description:
         typeof description === "string"
            ? description.trim()
            : lineRead.description,
   };
}

/** A skill's frontmatter, plus where its files live. */
export interface Skill {
   name: string;
   description: string;
   /** Absolute path to the skill's directory: SKILL.md and any reference/. */
   dir: string;
}

/**
 * Every skill under `root`, parsed on demand rather than baked into a generated
 * manifest, so there is no second copy of this metadata to drift. `listSkills`
 * reads the shipped files; the parity spec points `root` at skills it wrote,
 * which is why this is not on the package's entry point.
 */
export function readSkills(root: string): Skill[] {
   const skills: Skill[] = [];
   for (const entry of fs.readdirSync(root, { withFileTypes: true })) {
      if (!entry.isDirectory()) continue;
      const dir = path.join(root, entry.name);
      const skillFile = path.join(dir, "SKILL.md");
      if (!fs.existsSync(skillFile)) continue;
      const markdown = fs
         .readFileSync(skillFile, "utf8")
         .replace(/^\uFEFF/, "")
         .replace(/\r\n/g, "\n");
      const frontmatter = markdown.match(/^---\n([\s\S]*?)\n---/)?.[1] ?? "";
      skills.push({ ...readFrontmatter(frontmatter, entry.name), dir });
   }
   // Codepoint order, not localeCompare: the result is compared across
   // platforms and must not depend on the runtime's locale.
   return skills.sort((a, b) =>
      a.name < b.name ? -1 : a.name > b.name ? 1 : 0,
   );
}
