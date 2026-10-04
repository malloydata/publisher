// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * The `retrieval` block of a package's publisher.json: how THIS package is
 * searched and indexed. The server operator's side (credentials, egress,
 * ceilings) is publisher.config.json; see ../retrieval_config.ts. A package
 * can switch a feature on, never past what the operator allows.
 *
 * Only keys whose code exists are accepted. Any other key is an error naming
 * the valid ones, so a typo (or a key from a later release) does not silently
 * do nothing.
 */

import { createHash } from "crypto";
import * as fs from "fs";
import * as path from "path";
import { PackageManifestError } from "../errors";

export const PACKAGE_REPRESENTATIONS = ["single", "facets"] as const;
export type PackageRepresentation = (typeof PACKAGE_REPRESENTATIONS)[number];

export const KEYPHRASE_MODES = ["auto", "never", "always"] as const;
export type KeyphraseMode = (typeof KEYPHRASE_MODES)[number];

const RETRIEVAL_KEYS = ["representation", "keyphrases", "prompts"] as const;
const PROMPT_KEYS = ["keyphrase"] as const;

/** A prompt a package overrides: the text, read at package load. */
export interface PackagePrompt {
   /** The path as written in publisher.json, for messages. */
   path: string;
   text: string;
   /** sha256 of the text; part of the key that invalidates stored output. */
   hash: string;
}

export interface PackageRetrievalSettings {
   /** `single`: one row per entity. `facets`: a name row plus doc chunks. */
   representation: PackageRepresentation;
   /**
    * `auto`: a short description is its own keyphrase, anything else gets an
    * LLM-written one (needs the operator's LLM; otherwise acts as `never`).
    * `always`: an LLM keyphrase for every entity. `never`: none.
    */
   keyphrases: KeyphraseMode;
   prompts: { keyphrase?: PackagePrompt };
}

export const DEFAULT_PACKAGE_RETRIEVAL: PackageRetrievalSettings = {
   representation: "single",
   keyphrases: "auto",
   prompts: {},
};

function fail(message: string): never {
   throw new PackageManifestError(message);
}

function describe(value: unknown): string {
   return JSON.stringify(value) ?? "nothing";
}

/**
 * Whether a path relative to the package root steps outside it: `..` itself or
 * a path that starts with `..` as a whole segment. A name that merely begins
 * with two dots (`..prompts`) is an ordinary name and stays inside.
 */
function stepsOutside(within: string): boolean {
   return (
      within === ".." ||
      within.startsWith(`..${path.sep}`) ||
      path.isAbsolute(within)
   );
}

/**
 * `rel` as a path inside `packageRoot`, or a PackageManifestError. Rejects an
 * absolute path, a NUL byte, and anything that resolves outside the package
 * directory, including through a symlink: the check runs on the real paths
 * once the file exists.
 */
export function resolvePromptPath(packageRoot: string, rel: string): string {
   const key = "retrieval.prompts.keyphrase";
   const fix = `Fix: use a path inside the package, e.g. "prompts/keyphrase.md".`;
   if (
      rel.includes("\0") ||
      path.isAbsolute(rel) ||
      /^[A-Za-z]:[\\/]/.test(rel) ||
      rel.startsWith("\\")
   ) {
      fail(
         `Invalid publisher.json ${key}: ${describe(rel)} is an absolute path. ${fix}`,
      );
   }
   const root = path.resolve(packageRoot);
   const resolved = path.resolve(root, rel);
   const within = path.relative(root, resolved);
   if (within === "" || stepsOutside(within)) {
      fail(
         `Invalid publisher.json ${key}: ${describe(rel)} resolves outside the package directory. ${fix}`,
      );
   }
   return resolved;
}

/**
 * Validate the block's shape and return what it says, with the prompt path
 * still unread. Throws a PackageManifestError (424: the package is not served
 * until the author fixes the file) naming the key and a fix.
 */
export function parsePackageRetrieval(raw: unknown): {
   representation: PackageRepresentation;
   keyphrases: KeyphraseMode;
   promptPaths: { keyphrase?: string };
} {
   if (raw === undefined || raw === null) {
      return {
         representation: DEFAULT_PACKAGE_RETRIEVAL.representation,
         keyphrases: DEFAULT_PACKAGE_RETRIEVAL.keyphrases,
         promptPaths: {},
      };
   }
   if (typeof raw !== "object" || Array.isArray(raw)) {
      fail(
         `Invalid publisher.json retrieval: expected an object, got ${describe(raw)}. ` +
            `Fix: "retrieval": { "representation": "single" }`,
      );
   }
   const obj = raw as Record<string, unknown>;
   const unknown = Object.keys(obj).filter(
      (k) => !(RETRIEVAL_KEYS as readonly string[]).includes(k),
   );
   if (unknown.length > 0) {
      fail(
         `Invalid publisher.json retrieval: unknown key ${unknown.map((k) => `'${k}'`).join(", ")}. ` +
            `Valid keys: ${RETRIEVAL_KEYS.join(", ")}.`,
      );
   }

   const representation = obj.representation ?? "single";
   if (
      typeof representation !== "string" ||
      !(PACKAGE_REPRESENTATIONS as readonly string[]).includes(representation)
   ) {
      fail(
         `Invalid publisher.json retrieval.representation: expected one of ${PACKAGE_REPRESENTATIONS.join(", ")}, got ${describe(obj.representation)}. ` +
            `Fix: "representation": "single" (one row per entity) or "facets" (a name row plus doc chunks).`,
      );
   }
   const keyphrases = obj.keyphrases ?? "auto";
   if (
      typeof keyphrases !== "string" ||
      !(KEYPHRASE_MODES as readonly string[]).includes(keyphrases)
   ) {
      fail(
         `Invalid publisher.json retrieval.keyphrases: expected one of ${KEYPHRASE_MODES.join(", ")}, got ${describe(obj.keyphrases)}. ` +
            `Fix: "keyphrases": "auto".`,
      );
   }

   const promptPaths: { keyphrase?: string } = {};
   if (obj.prompts !== undefined && obj.prompts !== null) {
      if (typeof obj.prompts !== "object" || Array.isArray(obj.prompts)) {
         fail(
            `Invalid publisher.json retrieval.prompts: expected an object, got ${describe(obj.prompts)}. ` +
               `Fix: "prompts": { "keyphrase": "prompts/keyphrase.md" }`,
         );
      }
      const prompts = obj.prompts as Record<string, unknown>;
      const badPrompt = Object.keys(prompts).filter(
         (k) => !(PROMPT_KEYS as readonly string[]).includes(k),
      );
      if (badPrompt.length > 0) {
         fail(
            `Invalid publisher.json retrieval.prompts: unknown key ${badPrompt.map((k) => `'${k}'`).join(", ")}. ` +
               `Valid keys: ${PROMPT_KEYS.join(", ")}.`,
         );
      }
      if (prompts.keyphrase !== undefined && prompts.keyphrase !== null) {
         if (
            typeof prompts.keyphrase !== "string" ||
            prompts.keyphrase.trim() === ""
         ) {
            fail(
               `Invalid publisher.json retrieval.prompts.keyphrase: expected a file path inside the package, got ${describe(prompts.keyphrase)}. ` +
                  `Fix: "keyphrase": "prompts/keyphrase.md"`,
            );
         }
         promptPaths.keyphrase = prompts.keyphrase;
      }
   }
   return {
      representation: representation as PackageRepresentation,
      keyphrases: keyphrases as KeyphraseMode,
      promptPaths,
   };
}

/**
 * Parse the block and read the prompt files it names. Called when the package
 * loads, so an edit takes effect on reload and a bad path stops the load with
 * a message naming the file.
 */
export async function readPackageRetrieval(
   packageRoot: string,
   raw: unknown,
): Promise<PackageRetrievalSettings> {
   const parsed = parsePackageRetrieval(raw);
   const prompts: PackageRetrievalSettings["prompts"] = {};
   if (parsed.promptPaths.keyphrase !== undefined) {
      const rel = parsed.promptPaths.keyphrase;
      const file = resolvePromptPath(packageRoot, rel);
      let text: string;
      try {
         // The real path must also be inside the real package directory: a
         // symlink in the package can point anywhere.
         const [realRoot, realFile] = await Promise.all([
            fs.promises.realpath(packageRoot),
            fs.promises.realpath(file),
         ]);
         const within = path.relative(realRoot, realFile);
         if (stepsOutside(within)) {
            fail(
               `Invalid publisher.json retrieval.prompts.keyphrase: ${describe(rel)} resolves outside the package directory through a link. ` +
                  `Fix: copy the file into the package.`,
            );
         }
         text = await fs.promises.readFile(realFile, "utf8");
      } catch (error) {
         if (error instanceof PackageManifestError) throw error;
         return fail(
            `Invalid publisher.json retrieval.prompts.keyphrase: cannot read ${describe(rel)} (${(error as Error).message}). ` +
               `Fix: create the file inside the package or remove the key.`,
         );
      }
      if (text.trim() === "") {
         fail(
            `Invalid publisher.json retrieval.prompts.keyphrase: ${describe(rel)} is empty. ` +
               `Fix: write the prompt, or remove the key to use the built-in one.`,
         );
      }
      prompts.keyphrase = {
         path: rel,
         text,
         hash: createHash("sha256").update(text).digest("hex"),
      };
   }
   return {
      representation: parsed.representation,
      keyphrases: parsed.keyphrases,
      prompts,
   };
}
