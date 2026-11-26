// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

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

/** Resolving each alias scans the anchors before it, so distinct anchors cost quadratic time that the alias and emit bounds do not see. */
const MAX_ANCHORS_AND_ALIASES = 1_000;
/** Counted on the text because the stall is in `parseDocument` itself; over-counts a stray `*word` in a string, which only costs the line read. */
const ANCHOR_OR_ALIAS = /[&*][^\s,[\]{}]/g;

function hasTooManyAnchors(front: string): boolean {
   let count = 0;
   for (const _ of front.matchAll(ANCHOR_OR_ALIAS)) {
      count += 1;
      if (count > MAX_ANCHORS_AND_ALIASES) return true;
   }
   return false;
}

/** The frontmatter as a JSON object, or undefined when YAML cannot be trusted to read it. */
function parseYaml(front: string): Record<string, unknown> | undefined {
   if (hasTooManyAnchors(front)) return undefined;
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
         new RegExp(`^${key}:\\s*(.+)$`, "m"),
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
