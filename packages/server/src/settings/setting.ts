// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import type { SettingType } from "./types";

/**
 * What a default that depends on other settings can ask while settings
 * resolve. Keys are plain strings here: the registry's own key type is built
 * from the entries that use this.
 */
export interface Resolving {
   get(key: string): unknown;
   /** Whether some source gave the setting, rather than its default. */
   isSet(key: string): boolean;
   /** A variable from the same startup, for a default that depends on one. */
   env(name: string): string | undefined;
}

/**
 * How today's names are read where that differs from the setting's type. Step 1
 * of the configuration design keeps these, so no value changes meaning: where
 * a boolean's reading here and the common reader disagree, today's wins, with a
 * warning. Each is removed, from here, by the step that ends it.
 */
export interface TodayReading {
   /**
    * How a boolean's variable is read today:
    * - `exactly-true`: on only for the exact text `true`; anything else is off.
    * - `true-or-1`: on only for `true` or `1`.
    * - `on-off`: only `on` or `off`, any case; anything else fails.
    */
   reads?: "exactly-true" | "true-or-1" | "on-off";
   /**
    * How a value in the file is read today:
    * - `truthy`: JavaScript truthiness, so the string `"false"` is on.
    * - `boolean-or-reference`: a JSON boolean, or a `${VAR}` whose value is
    *   one; a literal string fails.
    * - `json-boolean`: a JSON boolean only.
    */
   file?: "truthy" | "boolean-or-reference" | "json-boolean";
   /** A value that does not parse is the default today, with a warning. */
   invalid?: "warn";
}

/**
 * Today's names for a setting, read with a deprecation warning where the
 * design renamed them, and removed in step 3.
 */
export interface Aliases {
   env?: readonly string[];
   /**
    * Flags. A boolean's flag takes no value: `--no-x` sets it off and any other
    * name sets it on. Every other flag takes the next argument.
    */
   flags?: readonly string[];
   /** Today's paths in publisher.config.json, where the setting is read from the file today. */
   file?: readonly string[];
   /** The variable says the opposite of the setting (`PUBLISHER_NO_MCP_CONFIG`). */
   negated?: true;
   today?: TodayReading;
}

/**
 * The configuration design's exceptions: `structured` settings are file-only
 * as a whole, `launch` settings decide which file is read (or must not come
 * from one), and every other setting is `server`.
 */
export type SettingKind = "server" | "launch" | "structured";

export type DefaultValue<T> = T | ((resolving: Resolving) => T);

export interface Setting<K extends string = string, T = unknown> {
   readonly key: K;
   readonly type: SettingType<NonNullable<T>>;
   readonly default: DefaultValue<T> | undefined;
   readonly description: string;
   /** The reference's longer text (markdown), where one line does not say enough. */
   readonly notes?: string;
   /** How the reference shows the default, where its value alone does not say. */
   readonly defaultText?: string;
   readonly kind: SettingKind;
   /** Short flags the design keeps beside the derived one (`--init`). */
   readonly shortFlags?: readonly string[];
   /** No flag; never logged, reported, or echoed in an error. */
   readonly secret: boolean;
   readonly aliases: Aliases;
   /** Carries the value type for `settings.get`; never set. */
   readonly valueType?: { value: T };
}

interface Entry<K extends string, T> {
   key: K;
   type: SettingType<T>;
   description: string;
   notes?: string;
   defaultText?: string;
   kind?: SettingKind;
   shortFlags?: readonly string[];
   secret?: boolean;
   aliases?: Aliases;
}

/** One setting, in the configuration design's shape. */
export function setting<K extends string, T>(
   entry: Entry<K, T> & { default: DefaultValue<NoInfer<T>> },
): Setting<K, T>;
export function setting<K extends string, T>(
   entry: Entry<K, T> & { default?: undefined },
): Setting<K, T | undefined>;
export function setting<K extends string, T>(
   entry: Entry<K, T> & { default?: DefaultValue<NoInfer<T>> },
): Setting<K, T | undefined> {
   return {
      key: entry.key,
      type: entry.type as SettingType<NonNullable<T>>,
      default: entry.default,
      description: entry.description,
      ...(entry.notes !== undefined ? { notes: entry.notes } : {}),
      ...(entry.defaultText !== undefined
         ? { defaultText: entry.defaultText }
         : {}),
      kind: entry.kind ?? "server",
      ...(entry.shortFlags !== undefined
         ? { shortFlags: entry.shortFlags }
         : {}),
      secret: entry.secret ?? false,
      aliases: entry.aliases ?? {},
   };
}

/** `mcp.port` -> `PUBLISHER_MCP_PORT`: the variable step 2 reads. */
export function derivedEnvName(key: string): string {
   return `PUBLISHER_${key
      .replace(/\./g, "_")
      .replace(/([a-z0-9])([A-Z])/g, "$1_$2")
      .toUpperCase()}`;
}

/** `mcp.port` -> `--mcp-port`: the flag step 2 reads. */
export function derivedFlagName(key: string): string {
   return `--${key
      .replace(/\./g, "-")
      .replace(/([a-z0-9])([A-Z])/g, "$1-$2")
      .toLowerCase()}`;
}
