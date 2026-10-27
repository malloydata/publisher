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
 * of the configuration design keeps every one of these, so no value changes
 * meaning; each is removed by the step that ends it, from here.
 */
export interface TodayReading {
   /**
    * How a variable or flag value is read today:
    * - `exactly-true`: on only for the exact text `true`; anything else is off.
    * - `true-or-1`: on only for `true` or `1`.
    * - `on-off`: only `on` or `off`, any case; anything else fails.
    * - `Number`: `Number()`, then the type's checks.
    * - `unchecked-Number`: `Number()` and nothing else, so a typo is `NaN`.
    * - `parseInt`: `Number.parseInt`, which reads `5abc` as 5, then the type's checks.
    * - `parseInt-or-default`: as `parseInt`, but a value that fails is the default.
    */
   reads?:
      | "exactly-true"
      | "true-or-1"
      | "on-off"
      | "Number"
      | "unchecked-Number"
      | "parseInt"
      | "parseInt-or-default";
   /**
    * How a value in the file is read today:
    * - `truthy`: JavaScript truthiness, so the string `"false"` is on.
    * - `boolean-or-reference`: a JSON boolean, or a `${VAR}` whose value is
    *   one; a literal string fails.
    * - `json-boolean`: a JSON boolean only.
    */
   file?: "truthy" | "boolean-or-reference" | "json-boolean";
   /** A blank variable fails today, rather than reading as unset. */
   blankFails?: true;
   /** A value that does not parse is the default today: silently, or with a warning. */
   invalid?: "default" | "warn";
   /** The file's value wins over the variable today. */
   fileFirst?: true;
   /** Each flag adds to the variable's list today, rather than replacing it. */
   flagsAppend?: true;
   /**
    * Only flags count as giving the settings this default depends on, and the
    * default wins over the variable whenever it is on.
    */
   defaultFromFlags?: true;
}

/** Today's names for a setting. In step 1 they are the only names read. */
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
   readonly kind: SettingKind;
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
   kind?: SettingKind;
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
      kind: entry.kind ?? "server",
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
