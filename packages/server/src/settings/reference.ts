// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { describeSource } from "./errors";
import { REGISTRY, type RegisteredSetting, type SettingKey } from "./registry";
import { namesOf, type Settings } from "./settings";
import { BooleanType, IntegerType, ListType, NumberType } from "./types";

/**
 * What the registry generates: the settings table of docs/configuration.md,
 * `--help`, and the startup line naming each setting not at its default. Each
 * is built from the entries alone, so none can drift from what the server
 * reads; reference.spec.ts fails if the checked-in table does.
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
         cell(entry.notes ?? entry.description),
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
      const text = `${entry.description}${fallback ? ` (default: ${fallback})` : ""}${was.length > 0 ? ` (was ${was.join(", ")})` : ""}`;
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
