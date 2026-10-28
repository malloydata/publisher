// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { describeSource } from "./errors";
import { REGISTRY, type RegisteredSetting, type SettingKey } from "./registry";
import type { Settings } from "./settings";
import { BooleanType, IntegerType, ListType, NumberType } from "./types";

/**
 * What the registry generates: the settings table of docs/configuration.md,
 * `--help`, and the startup line naming each setting not at its default. Each
 * is built from the entries alone, so none can drift from what the server
 * reads; reference.spec.ts fails if the checked-in table does.
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
         cell(entry.notes ?? entry.description),
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
      return `  ${spec.padEnd(width)}${entry.description}${fallback ? ` (default: ${fallback})` : ""}`;
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
 * came from. A secret shows as `(set)`, and a structured setting by its source
 * alone.
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
         shown = entry.secret
            ? "(set)"
            : JSON.stringify(settings.get(key) as unknown);
      } catch {
         shown = "(invalid)";
      }
      return `${entry.key}=${shown} from ${from}`;
   });
   return given.length === 0
      ? "Settings: every setting is at its default."
      : `Settings not at their defaults: ${given.join(", ")}`;
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
   if (entry.defaultText !== undefined) {
      return style === "markdown"
         ? entry.defaultText
         : entry.defaultText.replace(/[`_]/g, "");
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
