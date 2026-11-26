// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { parseDocument } from "yaml";

/** agentskills.io: 1-64 characters of lowercase letters, digits and single hyphens, none leading or trailing. */
const SKILL_NAME = /^[a-z0-9]+(-[a-z0-9]+)*$/;
const SKILL_NAME_MAX_CHARS = 64;

export interface Frontmatter {
   /** The declared name; the directory's when none is declared. */
   name: string;
   description: string;
   /** The frontmatter as JSON (dates as ISO strings, keys as strings); empty when there is none or it did not parse. */
   frontmatter: Record<string, unknown>;
   /** The Markdown after the frontmatter, line endings normalized to LF. */
   body: string;
   /** What went wrong, as a clause to follow the file it is about; empty when nothing did. */
   warnings: string[];
}

export function unquote(s: string): string {
   return s.trim().replace(/^["']|["']$/g, "");
}

/** A `key: value` line read without YAML: the fallback for frontmatter YAML cannot be trusted to read. */
function lineRead(front: string, key: string): string | undefined {
   const value = front.match(new RegExp(`^${key}:[ \\t]*(\\S.*)$`, "m"))?.[1];
   return value === undefined ? undefined : unquote(value);
}

/** Alias-free output stays under 5x its source (a number like `1e20` prints 21 digits); an alias, even of a scalar, can exceed it. */
const EMIT_FACTOR = 5;
const EMIT_SLACK = 10_000;

/** YAML's parse time grows faster than its input (plain keys, anchors and errors alike), so the text it is given is capped; the largest bundled frontmatter is under 1 KiB. */
const FRONTMATTER_MAX_BYTES = 4 * 1024;

/** The frontmatter block as a JSON object, or why it is not one; `bound` marks YAML that parsed but is past an expansion limit. Never throws. */
function parseYaml(
   front: string,
): { value: Record<string, unknown> } | { problem: string; bound?: true } {
   try {
      // Silent so a warning is not printed; the document's errors are read below.
      const doc = parseDocument(front, { logLevel: "silent" });
      if (doc.errors.length > 0) {
         return { problem: doc.errors[0]!.message.split("\n")[0]! };
      }
      // Each alias is walked again, so the replacer sees (and stops) an expansion before its string is built.
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
         if (emitted > limit) {
            throw new Error("frontmatter expands past its source size");
         }
         return node;
      };
      // A JSON round trip drops what JSON cannot hold and turns a Date into its ISO string.
      const value: unknown = JSON.parse(
         JSON.stringify(doc.toJS() ?? {}, bounded),
      );
      if (typeof value !== "object" || value === null || Array.isArray(value)) {
         return { problem: "it is not a mapping" };
      }
      return { value: value as Record<string, unknown> };
   } catch (error) {
      return { problem: (error as Error).message.split("\n")[0]!, bound: true };
   }
}

/**
 * A SKILL.md's frontmatter and body. The reader never throws: YAML that does
 * not parse, a frontmatter over 4 KiB, or a `name` outside the spec's
 * character set, warns and falls back to a line-by-line read of `name` and
 * `description`.
 */
export function parseFrontmatter(md: string, dirName: string): Frontmatter {
   const text = md.replace(/\r\n/g, "\n"); // tolerate CRLF-committed files
   const fence = text.match(/^---\n([\s\S]*?)\n---\n?([\s\S]*)$/);
   if (!fence) {
      return {
         name: dirName,
         description: "",
         frontmatter: {},
         body: text,
         warnings: [],
      };
   }
   const [, front = "", body = ""] = fence;
   const fallback = (): Pick<Frontmatter, "name" | "description"> => ({
      name: lineRead(front, "name") ?? dirName,
      description: lineRead(front, "description") ?? "",
   });
   const bytes = Buffer.byteLength(front);
   if (bytes > FRONTMATTER_MAX_BYTES) {
      return {
         ...fallback(),
         frontmatter: {},
         body,
         warnings: [
            `has frontmatter of ${bytes} bytes, over 4 KiB, so it is not read as YAML and its name and description are read line by line`,
         ],
      };
   }
   const parsed = parseYaml(front);
   if ("problem" in parsed) {
      return {
         ...fallback(),
         frontmatter: {},
         body,
         warnings: [
            parsed.bound
               ? `has frontmatter that is valid YAML but too large to read once its aliases are expanded (${parsed.problem}), so its name and description are read line by line`
               : `has frontmatter that is not valid YAML (${parsed.problem}), so its name and description are read line by line`,
         ],
      };
   }
   const { value } = parsed;
   const declared = value.name ?? undefined;
   if (
      declared !== undefined &&
      !(
         typeof declared === "string" &&
         declared.length <= SKILL_NAME_MAX_CHARS &&
         SKILL_NAME.test(declared)
      )
   ) {
      return {
         ...fallback(),
         frontmatter: value,
         body,
         warnings: [
            `declares the name ${JSON.stringify(declared)}, which is not 1-${SKILL_NAME_MAX_CHARS} lowercase letters, digits and single hyphens, so its name and description are read line by line`,
         ],
      };
   }
   const description = value.description ?? "";
   return {
      name: typeof declared === "string" ? declared : dirName,
      description:
         typeof description === "string"
            ? description.trim()
            : (lineRead(front, "description") ?? ""),
      frontmatter: value,
      body,
      warnings: [],
   };
}
