// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { parseDocument } from "yaml";

function unquote(value: string): string {
   return value.trim().replace(/^["']|["']$/g, "");
}

/** agentskills.io's name rule: 1-64 characters of lowercase letters, digits and single hyphens. */
const SKILL_NAME = /^[a-z0-9]+(-[a-z0-9]+)*$/;

/**
 * A second call site of the server's frontmatter reader
 * (packages/server/src/service/skills/frontmatter.ts), which a published runtime
 * package cannot import. Same rule: read the YAML, and on a parse failure or a
 * name outside the spec fall back to a line read of `name` and `description`.
 * The fallback is silent here because a listing has no channel for the warning
 * the server's reader raises.
 */
export function readFrontmatter(
   frontmatter: string,
   dirName: string,
): { name: string; description: string } {
   const line = (key: string) =>
      frontmatter.match(new RegExp(`^${key}:\\s*(.+)$`, "m"))?.[1];
   const lineRead = {
      name: unquote(line("name") ?? dirName),
      description: unquote(line("description") ?? ""),
   };
   try {
      const doc = parseDocument(frontmatter, { logLevel: "silent" });
      const value: unknown = doc.errors.length === 0 ? doc.toJS() : undefined;
      if (typeof value !== "object" || value === null || Array.isArray(value)) {
         return lineRead;
      }
      const { name = dirName, description = "" } = value as Record<
         string,
         unknown
      >;
      if (
         typeof name !== "string" ||
         name.length > 64 ||
         !SKILL_NAME.test(name)
      ) {
         return lineRead;
      }
      return {
         name,
         description:
            typeof description === "string"
               ? description.trim()
               : lineRead.description,
      };
   } catch {
      return lineRead;
   }
}
