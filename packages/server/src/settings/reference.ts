// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { describeSource } from "./errors";
import { REGISTRY, type RegisteredSetting, type SettingKey } from "./registry";
import type { Settings } from "./settings";
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

/** Settings a variable or flag can give; the file-only ones have their own tables. */
const NAMED = REGISTRY.filter(
   (entry) =>
      (entry.aliases.env ?? []).length > 0 ||
      (entry.aliases.flags ?? []).length > 0,
);

export const REFERENCE_START =
   "<!-- settings-reference:start (generated from src/settings/registry; run `bun run generate-settings-reference`) -->";
export const REFERENCE_END = "<!-- settings-reference:end -->";

/** The markdown table of every setting a variable or flag gives. */
export function settingsReferenceTable(): string {
   const rows = NAMED.map((entry) =>
      [
         code(entry.key),
         cell((entry.aliases.env ?? []).map(code).join(", ")),
         cell(flagSpecs(entry).map(code).join(", ")),
         cell(defaultText(entry, "markdown")),
         cell(withReferences(entry.notes ?? entry.description, "markdown")),
      ].join(" | "),
   );
   return [
      "| Setting | Env var | CLI flag | Default | Meaning |",
      "| --- | --- | --- | --- | --- |",
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

/** `--help`: today's flags, with what each sets and its default. */
export function helpText(): string {
   const flagged = NAMED.filter(
      (entry) => (entry.aliases.flags ?? []).length > 0,
   ).map((entry) => ({ entry, spec: flagSpecs(entry).join(", ") }));
   const width = Math.max(...flagged.map(({ spec }) => spec.length)) + 2;
   const lines = flagged.map(({ entry, spec }) => {
      const fallback = defaultText(entry, "text");
      return `  ${spec.padEnd(width)}${withReferences(entry.description, "text")}${fallback ? ` (default: ${fallback})` : ""}`;
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
      "Every setting can also be given as an environment variable; see docs/configuration.md.",
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
 * the key in the table, and in `--help` the flag that sets it (its first
 * variable, where it has no flag).
 */
function withReferences(text: string, style: "markdown" | "text"): string {
   return text.replace(/\{([a-zA-Z][a-zA-Z.]*)\}/g, (whole, key: string) => {
      const entry = REGISTRY.find((candidate) => candidate.key === key);
      if (!entry) return whole;
      if (style === "markdown") return code(key);
      return entry.aliases.flags?.[0] ?? entry.aliases.env?.[0] ?? key;
   });
}

/** `--port <n>`, or a boolean's switches: `--serving-api-enabled`, `--no-...`. */
function flagSpecs(entry: RegisteredSetting): string[] {
   const flags = entry.aliases.flags ?? [];
   if (entry.type instanceof BooleanType) return [...flags];
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
   // A negated name says the opposite of the key (`--no-mcp-config` sets
   // `mcp.configFileEnabled` off), so the key's default would read backwards
   // beside it. Unset is the default; the description says what that does.
   if (entry.aliases.negated) return style === "markdown" ? "_unset_" : "";
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
   // malloy-text: markdown code formatting in a generated doc, not Malloy
   return `\`${text}\``;
}

/** A table cell: one line, with its own pipes escaped. */
function cell(text: string): string {
   return text.replace(/\n+/g, " ").replace(/(?<!\\)\|/g, "\\|");
}
