// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { describeSource } from "./errors";
import { REGISTRY, type RegisteredSetting, type SettingKey } from "./registry";
import { namesOf, type Settings } from "./settings";
import { BooleanType, IntegerType, ListType, NumberType } from "./types";

/**
 * What the registry generates: the settings table of docs/configuration.md,
 * `--help`, and the startup line naming each setting given. Each is built from
 * the entries alone, so none can drift from what the server reads;
 * reference.spec.ts fails if the checked-in table does.
 *
 * A description or default text names another setting as `{key}`, which the
 * table shows as the key and `--help` as the flag that sets it, so neither
 * reads in the other's terms.
 */

/** Settings a variable or flag can give; the structured ones are file-only. */
const NAMED = REGISTRY.filter((entry) => entry.kind !== "structured");

export const REFERENCE_START =
   "<!-- settings-reference:start (generated from src/settings/registry; run `bun run generate-settings-reference`) -->";
export const REFERENCE_END = "<!-- settings-reference:end -->";

/** The markdown table of every setting a variable or flag gives. */
export function settingsReferenceTable(): string {
   const rows = NAMED.map((entry) => {
      const names = namesOf(entry);
      const deprecated = [
         ...names.env.filter(({ old }) => old),
         ...names.flags.filter(({ old }) => old),
      ].map(({ name }) => code(name));
      return [
         code(entry.key),
         cell(
            names.env
               .filter(({ old }) => !old)
               .map(({ name }) => code(name))
               .join(", "),
         ),
         cell(flagSpecs(entry).map(code).join(", ")),
         cell(deprecated.join(", ")),
         cell(defaultText(entry, "markdown")),
         cell(withReferences(entry.notes ?? entry.description, "markdown")),
      ].join(" | ");
   });
   return [
      "| Setting | Env var | CLI flag | Deprecated names | Default | Meaning |",
      "| --- | --- | --- | --- | --- | --- |",
      ...rows.map((row) => `| ${row} |`),
   ].join("\n");
}

/** `doc` with its generated settings table replaced by the registry's. */
export function withReferenceTable(doc: string): string {
   const start = doc.indexOf(REFERENCE_START);
   const end = doc.indexOf(REFERENCE_END);
   if (start < 0 || end < start) return doc;
   return (
      doc.slice(0, start + REFERENCE_START.length) +
      "\n\n" +
      settingsReferenceTable() +
      "\n\n" +
      doc.slice(end)
   );
}

/**
 * `publisher.config.json.example`: every setting the file can hold, at its
 * key, with its default, so a copy of it changes nothing. A launch setting is
 * left out, since it decides which file is read. `null`, which the file reads
 * as unset, stands where the default is unset, follows another setting
 * (`mcp.host`), or is a secret's, which the file can give only as a `${VAR}`
 * reference. It also stands for a setting another default follows (`host`,
 * named `{host}` in `mcp.host`'s default text): given its own default, it
 * would still count as given, and move the other with it.
 *
 * A structured setting whose leaves are settings of their own (`retrieval`)
 * is left out with its leaves. Its own parser reads the block as a whole, and
 * a block of unset leaves is not the same as no block: an `llm` without the
 * provider and model, which are not settings, is refused.
 */
export function exampleConfig(): Record<string, unknown> {
   const followed = new Set(
      REGISTRY.flatMap((entry) =>
         [
            ...(entry.defaultText ?? "").matchAll(/\{([a-zA-Z][a-zA-Z.]*)\}/g),
         ].map((match) => match[1]),
      ),
   );
   const example: Record<string, unknown> = {};
   for (const entry of REGISTRY) {
      if (entry.kind === "launch" || inBlockWithLeaves(entry.key)) continue;
      const fallback = entry.default;
      const value =
         entry.secret ||
         followed.has(entry.key) ||
         fallback === undefined ||
         typeof fallback === "function"
            ? null
            : fallback;
      let node = example;
      const parts = entry.key.split(".");
      for (const part of parts.slice(0, -1)) {
         node = (node[part] ??= {}) as Record<string, unknown>;
      }
      node[parts[parts.length - 1]] = value;
   }
   return example;
}

/** {@link exampleConfig} as the checked-in file holds it. */
export function exampleConfigText(): string {
   return `${JSON.stringify(exampleConfig(), null, 2)}\n`;
}

/**
 * Whether `key` is a structured setting whose leaves are settings of their
 * own (`retrieval`), or one of those leaves: what {@link exampleConfig} leaves
 * out.
 */
export function inBlockWithLeaves(key: string): boolean {
   return REGISTRY.some(
      (entry) =>
         entry.kind === "structured" &&
         REGISTRY.some((other) => other.key.startsWith(`${entry.key}.`)) &&
         (key === entry.key || key.startsWith(`${entry.key}.`)),
   );
}

/** `--help`: today's flags, with what each sets and its default. */
export function helpText(): string {
   const flagged = NAMED.filter((entry) => flagSpecs(entry).length > 0).map(
      (entry) => ({ entry, spec: flagSpecs(entry).join(", ") }),
   );
   // A spec longer than the column goes on its own line, its text below it.
   const width = 40;
   const lines = flagged.map(({ entry, spec }) => {
      const fallback = defaultText(entry, "text");
      const was = namesOf(entry)
         .flags.filter(({ old }) => old)
         .map(({ name }) => name);
      const text = `${withReferences(entry.description, "text")}${fallback ? ` (default: ${fallback})` : ""}${was.length > 0 ? ` (was ${was.join(", ")})` : ""}`;
      return spec.length < width
         ? `  ${spec.padEnd(width)}${text}`
         : `  ${spec}\n  ${"".padEnd(width)}${text}`;
   });
   return [
      "Malloy Publisher Server",
      "",
      "Usage: malloy-publisher [options]",
      "",
      "Options:",
      ...lines,
      `  ${"--help, -h".padEnd(width)}Show this help message`,
      "",
      "Every setting can also be given as an environment variable or in publisher.config.json; see docs/configuration.md.",
   ].join("\n");
}

/**
 * One line naming each setting a flag, variable or the file gave, and where it
 * came from, whether or not the value given is the default. A secret, or a
 * value too close to one (`masked`), shows as `(set)`; a URL's user and
 * password show as `***`; and a structured setting shows by its source alone.
 */
export function settingsLine(settings: Settings): string {
   const given = REGISTRY.filter((entry) =>
      settings.isSet(entry.key as SettingKey),
   ).map((entry) => {
      const key = entry.key as SettingKey;
      const from = describeSource(settings.source(key));
      if (entry.kind === "structured") return `${entry.key} from ${from}`;
      let shown: string;
      try {
         shown =
            entry.secret || entry.masked
               ? "(set)"
               : JSON.stringify(
                    withoutUrlCredentials(settings.get(key) as unknown),
                 );
      } catch {
         shown = "(invalid)";
      }
      return `${entry.key}=${shown} from ${from}`;
   });
   return given.length === 0
      ? "Settings: every setting is at its default."
      : `Settings given: ${given.join(", ")}`;
}

/**
 * `https://user:pass@host` -> `https://***@host`, for a value or each value
 * of a list. Everything between the scheme and the last `@` goes, so a
 * password holding `/` or `@`, which `new URL` either refuses or splits at the
 * wrong `@`, is hidden too. A path with an `@` in it loses its host as well,
 * which only hides more.
 */
function withoutUrlCredentials(value: unknown): unknown {
   if (Array.isArray(value)) return value.map(withoutUrlCredentials);
   if (typeof value !== "string") return value;
   const scheme = /^[a-z][a-z0-9+.-]*:\/\//i.exec(value)?.[0];
   const at = value.lastIndexOf("@");
   if (scheme === undefined || at < scheme.length) return value;
   return `${scheme}***${value.slice(at)}`;
}

/**
 * `text` with each `{key}` naming a registered setting written for `style`:
 * the key in the table, and in `--help` the derived flag that sets it (its
 * derived variable, where it has no flag).
 */
function withReferences(text: string, style: "markdown" | "text"): string {
   return text.replace(/\{([a-zA-Z][a-zA-Z.]*)\}/g, (whole, key: string) => {
      const entry = REGISTRY.find((candidate) => candidate.key === key);
      if (!entry) return whole;
      if (style === "markdown") return code(key);
      const names = namesOf(entry);
      const named =
         names.flags.find(({ old }) => !old) ??
         names.env.find(({ old }) => !old);
      return named?.name ?? key;
   });
}

/** `--port <n>`, or a boolean's switches: `--serving-api-enabled`, `--no-...`. */
function flagSpecs(entry: RegisteredSetting): string[] {
   const flags = namesOf(entry)
      .flags.filter(({ old }) => !old)
      .map(({ name }) => name);
   if (entry.type instanceof BooleanType) return flags;
   return flags.map((flag) => `${flag} <${valueName(entry)}>`);
}

function valueName(entry: RegisteredSetting): string {
   if (entry.type instanceof IntegerType || entry.type instanceof NumberType) {
      return "n";
   }
   if (entry.type instanceof ListType) return "name";
   if (entry.key === "root") return "dir";
   if (entry.key === "config") return "path";
   if (entry.key.endsWith("host")) return "addr";
   return "value";
}

function defaultText(
   entry: RegisteredSetting,
   style: "markdown" | "text",
): string {
   // A row's names are the derived ones, which say what the key says, so the
   // key's default reads the right way round even where a deprecated name is
   // negated (`PUBLISHER_NO_MCP_CONFIG`).
   if (entry.defaultText !== undefined) {
      return style === "markdown"
         ? withReferences(entry.defaultText, style)
         : withReferences(entry.defaultText.replace(/[`_]/g, ""), style);
   }
   const fallback = entry.default;
   if (fallback === undefined) return style === "markdown" ? "_unset_" : "";
   if (typeof fallback === "boolean" && style === "text") {
      return fallback ? "on" : "off";
   }
   if (Array.isArray(fallback) && fallback.length === 0) {
      return style === "markdown" ? "_none_" : "";
   }
   const shown = Array.isArray(fallback)
      ? fallback.join(",")
      : String(fallback);
   return style === "markdown" ? code(shown) : shown;
}

function code(text: string): string {
   return `\`${text}\``;
}

/** A table cell: one line, with its own pipes escaped. */
function cell(text: string): string {
   return text.replace(/\n+/g, " ").replace(/(?<!\\)\|/g, "\\|");
}
