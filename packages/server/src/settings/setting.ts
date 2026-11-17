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
   /** No flag, though no secret: the design gives it none (`packageVersioning`). */
   readonly noFlag?: true;
   /**
    * Not a secret, but too close to one to log: the startup line shows it as
    * `(set)`, as it does a secret. An access key id, say.
    */
   readonly masked?: true;
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
   noFlag?: true;
   masked?: true;
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
      ...(entry.noFlag ? { noFlag: true as const } : {}),
      ...(entry.masked ? { masked: true as const } : {}),
   };
}

/** `mcp.port` -> `PUBLISHER_MCP_PORT`: the setting's variable. */
export function derivedEnvName(key: string): string {
   return `PUBLISHER_${key
      .replace(/\./g, "_")
      .replace(/([a-z0-9])([A-Z])/g, "$1_$2")
      .toUpperCase()}`;
}

/** `mcp.port` -> `--mcp-port`: the setting's flag. */
export function derivedFlagName(key: string): string {
   return `--${key
      .replace(/\./g, "-")
      .replace(/([a-z0-9])([A-Z])/g, "$1-$2")
      .toLowerCase()}`;
}
