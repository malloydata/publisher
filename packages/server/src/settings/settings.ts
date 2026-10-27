// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import fs from "fs";
import path from "path";
import { isMainThread, workerData } from "worker_threads";
import { PUBLISHER_CONFIG_NAME } from "../constants";
import { describeSource, SettingError, type SettingSource } from "./errors";
import {
   REGISTRY,
   type RegisteredSetting,
   type SettingKey,
   type SettingValue,
} from "./registry";
import {
   derivedEnvName,
   derivedFlagName,
   type Resolving,
   type Setting,
} from "./setting";
import { BooleanType, ListType, Mismatch } from "./types";

export type Env = Readonly<Record<string, string | undefined>>;

/** One startup's sources. Settings never read anything else. */
export interface LoadInput {
   /** Command-line arguments, without the runtime and script. */
   argv?: readonly string[];
   /** The variables; this process's by default. */
   env?: Env;
   /** What `root` and a relative `config` resolve against; the process's cwd by default. */
   cwd?: string;
   /** The settings to read; the registry. A test passes it altered. */
   registry?: readonly RegisteredSetting[];
}

/** Something to say at startup about how a setting was read. */
export interface SettingWarning {
   key?: string;
   /** A deprecated name in use: an old variable, flag or file key. */
   kind?: "deprecated";
   message: string;
}

/** A value a higher-precedence source hid, kept so it can be reported. */
export interface ShadowedValue {
   source: SettingSource;
   value: unknown;
}

type Slot =
   | { value: unknown; source: SettingSource; shadowed: ShadowedValue[] }
   | { error: Error; source: SettingSource };

/** The settings a package-load worker thread receives from the thread that started it. */
export interface SerializedSettings {
   slots: Array<
      [
         string,
         (
            | {
                 value: unknown;
                 source: SettingSource;
                 shadowed: ShadowedValue[];
              }
            | { error: string; source: SettingSource }
         ),
      ]
   >;
   configPath?: string;
}

let installed: Settings | undefined;

/**
 * Every Publisher setting, read once from one startup's flags, variables and
 * config file, through the registry (registry/). Code asks for a value by key
 * (`settings.get("mcp.port")`), never from `process.env`.
 *
 * From step 2 of the configuration design the names read are the derived
 * ones (`PUBLISHER_MCP_PORT`, `--mcp-port`, the key in the file), with one
 * precedence: flag, variable, file. Today's names, kept on each entry's
 * `aliases`, still work, each with a deprecation warning.
 */
export class Settings {
   private constructor(
      private readonly slots: ReadonlyMap<string, Slot>,
      /** The config file read, if one was found. */
      readonly configPath: string | undefined,
      /** Why the config file could not be read, if it could not. */
      readonly fileError: Error | undefined,
      readonly warnings: readonly SettingWarning[],
      /** The deprecated names this startup used, for the deprecated-usage metric. */
      readonly deprecatedNames: readonly string[],
   ) {}

   static load(input: LoadInput = {}): Settings {
      const loaded = new Loader(input).load();
      return new Settings(
         loaded.slots,
         loaded.configPath,
         loaded.fileError,
         loaded.warnings,
         loaded.deprecated,
      );
   }

   /**
    * The settings this process started with: what `server.ts` installed, or,
    * in a package-load worker, what the pool passed, or else this process's
    * own variables and config file.
    */
   static current(): Settings {
      installed ??= Settings.fromProcess();
      return installed;
   }

   /** Make `next` the current settings; returns the ones it replaced. */
   static install(next: Settings | undefined): Settings | undefined {
      const previous = installed;
      installed = next;
      return previous;
   }

   private static fromProcess(): Settings {
      const passed = isMainThread
         ? undefined
         : (workerData as { settings?: SerializedSettings } | null)?.settings;
      return passed ? Settings.fromWorkerData(passed) : Settings.load();
   }

   /** The setting's value; throws the setting's error if it did not parse. */
   get<K extends SettingKey>(key: K): SettingValue<K> {
      const slot = this.slot(key);
      if ("error" in slot) throw slot.error;
      return slot.value as SettingValue<K>;
   }

   /** Where the value came from: a flag, a variable, the file, or the default. */
   source(key: SettingKey): SettingSource {
      return this.slot(key).source;
   }

   /** Whether a flag, variable or the file gave the setting, valid or not. */
   isSet(key: SettingKey): boolean {
      return this.slot(key).source.kind !== "default";
   }

   /** Values from lower-precedence sources that the value in force hid. */
   shadowed(key: SettingKey): readonly ShadowedValue[] {
      const slot = this.slot(key);
      return "shadowed" in slot ? slot.shadowed : [];
   }

   /** Every setting that did not parse, each error once. */
   errors(): Error[] {
      return [
         ...new Set(
            [...this.slots.values()].flatMap((slot) =>
               "error" in slot ? [slot.error] : [],
            ),
         ),
      ];
   }

   toWorkerData(): SerializedSettings {
      return {
         slots: [...this.slots].map(([key, slot]) => [
            key,
            "error" in slot
               ? { error: slot.error.message, source: slot.source }
               : slot,
         ]),
         configPath: this.configPath,
      };
   }

   static fromWorkerData(data: SerializedSettings): Settings {
      return new Settings(
         new Map(
            data.slots.map(([key, slot]) => [
               key,
               "error" in slot
                  ? { error: new Error(slot.error), source: slot.source }
                  : slot,
            ]),
         ),
         data.configPath,
         undefined,
         [],
         [],
      );
   }

   private slot(key: string): Slot {
      const slot = this.slots.get(key);
      if (!slot) throw new Error(`No setting named ${key}`);
      return slot;
   }
}

const VAR_REFERENCE = /\$\{([A-Z_][A-Z0-9_]*)\}/g;

/**
 * A raw value from one source, before it is read as the setting's type.
 * `old` marks a deprecated name, one of today's that the design renamed;
 * `today` marks a name read as it was before step 1, which every old name is,
 * and so is a name the design left unchanged.
 */
interface Candidate {
   kind: "flag" | "env" | "file";
   /** The flag or variable, or the file's path. */
   name: string;
   /** The key or old key in the file, for a file value. */
   path?: string;
   raw: unknown;
   old: boolean;
   today: boolean;
}

interface FlagName {
   entry: RegisteredSetting;
   old: boolean;
   /** The value a switch sets; undefined for a flag that takes one. */
   switches?: boolean;
}

type Parsed = { value: unknown } | { error: Error };

/** A name a setting is read under; `old` marks a deprecated one. */
export interface SettingName {
   name: string;
   old: boolean;
}

/**
 * Every name a setting is read under, current first: the derived variable,
 * flag (with `--no-` for a boolean) and file key; the short flags the design
 * keeps; and today's names where the design renamed them, which are
 * deprecated. A secret has no flag, a structured setting is file-only and a
 * launch setting has no file key.
 */
export function namesOf(entry: RegisteredSetting): {
   env: SettingName[];
   flags: SettingName[];
   file: Array<{ path: string; old: boolean }>;
} {
   const structured = entry.kind === "structured";
   const derivedEnv = derivedEnvName(entry.key);
   const derivedFlag = derivedFlagName(entry.key);
   const env = structured
      ? []
      : [
           { name: derivedEnv, old: false },
           ...(entry.aliases.env ?? [])
              .filter((name) => name !== derivedEnv)
              .map((name) => ({ name, old: true })),
        ];
   const flags: SettingName[] = [];
   const addFlag = (name: string, old: boolean) => {
      if (!flags.some((flag) => flag.name === name)) flags.push({ name, old });
   };
   if (!structured) {
      if (!entry.secret) {
         addFlag(derivedFlag, false);
         if (entry.type instanceof BooleanType) {
            addFlag(`--no-${derivedFlag.slice(2)}`, false);
         }
      }
      for (const name of entry.shortFlags ?? []) addFlag(name, false);
      for (const name of entry.aliases.flags ?? []) addFlag(name, true);
   }
   const file =
      entry.kind === "launch"
         ? []
         : [
              { path: entry.key, old: false },
              ...(entry.aliases.file ?? [])
                 .filter((path) => path !== entry.key)
                 .map((path) => ({ path, old: true })),
           ];
   return { env, flags, file };
}

/**
 * The flags that take the argument after them as their value: every flag but
 * a boolean's, which is a switch. create-malloy-package keeps a copy of this
 * list to tell which --host a start script hands the server.
 */
export function flagsTakingAValue(
   registry: readonly RegisteredSetting[] = REGISTRY,
): string[] {
   return registry.flatMap((entry) =>
      entry.type instanceof BooleanType
         ? []
         : namesOf(entry).flags.map((flag) => flag.name),
   );
}

/** Reads one startup's sources into a {@link Settings}. */
class Loader {
   private readonly registry: readonly RegisteredSetting[];
   private readonly byKey: ReadonlyMap<string, RegisteredSetting>;
   private readonly byFlag = new Map<string, FlagName>();
   private readonly argv: readonly string[];
   private readonly env: Env;
   private readonly cwd: string;
   private readonly flags = new Map<string, Candidate[]>();
   private readonly slots = new Map<string, Slot>();
   private readonly resolving = new Set<string>();
   private readonly warnings: SettingWarning[] = [];
   private readonly deprecated = new Set<string>();
   private readonly unknownFlags: string[] = [];
   private readonly flagsWithoutValue: string[] = [];
   private file: Record<string, unknown> = {};
   private configPath: string | undefined;
   private fileError: Error | undefined;

   constructor(input: LoadInput) {
      this.registry = input.registry ?? REGISTRY;
      this.byKey = new Map(this.registry.map((entry) => [entry.key, entry]));
      for (const entry of this.registry) this.nameFlags(entry);
      this.argv = input.argv ?? [];
      this.env = input.env ?? { ...process.env };
      this.cwd = input.cwd ?? process.cwd();
   }

   load(): {
      slots: Map<string, Slot>;
      configPath: string | undefined;
      fileError: Error | undefined;
      warnings: SettingWarning[];
      deprecated: string[];
   } {
      this.readFlags();
      // The launch settings say which file to read and have no file key.
      this.readFile(
         this.valueOf("root") as string,
         this.valueOf("config") as string | undefined,
      );
      for (const entry of this.registry) this.resolve(entry);
      this.warnAboutFlags();
      this.warnAboutFileKeys();
      return {
         slots: this.slots,
         configPath: this.configPath,
         fileError: this.fileError,
         warnings: this.warnings,
         deprecated: [...this.deprecated],
      };
   }

   private valueOf(key: string): unknown {
      const slot = this.resolve(this.byKey.get(key)!);
      if ("error" in slot) throw slot.error;
      return slot.value;
   }

   private nameFlags(entry: RegisteredSetting): void {
      const isSwitch = entry.type instanceof BooleanType;
      for (const { name, old } of namesOf(entry).flags) {
         if (this.byFlag.has(name)) continue;
         this.byFlag.set(name, {
            entry,
            old,
            ...(isSwitch ? { switches: !name.startsWith("--no-") } : {}),
         });
      }
   }

   /** Every variable the setting is read from; `today` where today's reading applies. */
   private envNames(
      entry: RegisteredSetting,
   ): Array<{ name: string; old: boolean; today: boolean }> {
      const aliases = entry.aliases.env ?? [];
      return namesOf(entry).env.map(({ name, old }) => ({
         name,
         old,
         today: old || aliases.includes(name),
      }));
   }

   private filePaths(
      entry: RegisteredSetting,
   ): Array<{ path: string; old: boolean }> {
      return namesOf(entry).file;
   }

   /**
    * An exact flag name; a switch alone; any other flag with the next argument
    * as its value when there is one. A flag it does not know, and a flag with
    * nothing after it, are skipped and warned about.
    */
   private readFlags(): void {
      for (let i = 0; i < this.argv.length; i++) {
         const name = this.argv[i];
         const flag = this.byFlag.get(name);
         if (!flag) {
            if (name.startsWith("-") && name !== "--help" && name !== "-h") {
               this.unknownFlags.push(name);
            }
            continue;
         }
         let raw: unknown;
         if (flag.switches !== undefined) {
            raw = flag.switches;
         } else if (this.argv[i + 1]) {
            raw = this.argv[i + 1];
            i++;
         } else {
            this.flagsWithoutValue.push(name);
            continue;
         }
         const hits = this.flags.get(flag.entry.key) ?? [];
         hits.push({ kind: "flag", name, raw, old: flag.old, today: false });
         this.flags.set(flag.entry.key, hits);
      }
   }

   private warnAboutFlags(): void {
      const current = [...this.byFlag]
         .filter(([, flag]) => !flag.old)
         .map(([name]) => name);
      for (const name of this.unknownFlags) {
         const suggestion = nearest(name, current);
         this.warnings.push({
            message: `Unknown flag ${name}, so it is ignored.${suggestion ? ` Did you mean ${suggestion}?` : ""}`,
         });
      }
      for (const name of this.flagsWithoutValue) {
         this.warnings.push({
            message: `${name} has no value after it, so it is ignored.`,
         });
      }
   }

   /**
    * A key the file holds that no setting reads: a typo (`adminApiEnable`), or
    * a launch setting, which the file cannot give. Checked at the top level and
    * in each section that holds settings; a structured setting's own keys are
    * its parser's to check.
    */
   private warnAboutFileKeys(): void {
      const read = this.registry.flatMap((entry) =>
         this.filePaths(entry).map(({ path }) => path),
      );
      const whole = new Set(
         this.registry
            .filter((entry) => entry.kind === "structured")
            .flatMap((entry) => this.filePaths(entry).map(({ path }) => path)),
      );
      const file = this.configPath ?? PUBLISHER_CONFIG_NAME;
      const visit = (node: Record<string, unknown>, prefix: string) => {
         for (const [name, value] of Object.entries(node)) {
            const key = prefix ? `${prefix}.${name}` : name;
            if (whole.has(key) || read.includes(key)) continue;
            if (read.some((path) => path.startsWith(`${key}.`))) {
               if (
                  value &&
                  typeof value === "object" &&
                  !Array.isArray(value)
               ) {
                  visit(value as Record<string, unknown>, key);
               }
               continue;
            }
            const launch = this.byKey.get(key);
            if (launch?.kind === "launch") {
               this.warnings.push({
                  message: `"${key}" in ${file} is ignored: it decides which file is read, or must not come from one, so it is a variable or a flag only (${derivedEnvName(key)}, ${derivedFlagName(key)}).`,
               });
               continue;
            }
            const siblings = read
               .filter((path) => parentOf(path) === prefix)
               .map((path) => path.slice(prefix ? prefix.length + 1 : 0));
            const suggestion = nearest(name, siblings);
            this.warnings.push({
               message: `${file} has "${key}", which no setting reads, so it is ignored.${suggestion ? ` Did you mean "${prefix ? `${prefix}.` : ""}${suggestion}"?` : ""}`,
            });
         }
      };
      visit(this.file, "");
   }

   private readFile(root: string, config: string | undefined): void {
      const file = config
         ? path.resolve(this.cwd, config)
         : path.join(path.resolve(this.cwd, root), PUBLISHER_CONFIG_NAME);
      // A missing file is the config code's to report: it falls back or says
      // the named path is missing.
      if (!fs.existsSync(file)) return;
      this.configPath = file;
      try {
         const parsed: unknown = JSON.parse(fs.readFileSync(file, "utf8"));
         if (parsed && typeof parsed === "object" && !Array.isArray(parsed)) {
            this.file = parsed as Record<string, unknown>;
         }
      } catch (error) {
         const message = error instanceof Error ? error.message : String(error);
         this.fileError = new Error(`Failed to parse ${file}: ${message}`);
      }
   }

   private resolve(entry: RegisteredSetting): Slot {
      const done = this.slots.get(entry.key);
      if (done) return done;
      if (this.resolving.has(entry.key)) {
         throw new Error(`The default of ${entry.key} depends on itself`);
      }
      this.resolving.add(entry.key);
      const slot = this.read(entry);
      this.resolving.delete(entry.key);
      this.slots.set(entry.key, slot);
      return slot;
   }

   /**
    * One precedence for every setting: flag, then variable, then file, then
    * the default. A value given under an old name and a different one under a
    * new name fails, rather than one silently winning.
    */
   private read(entry: RegisteredSetting): Slot {
      const parsed: Array<{ candidate: Candidate; value: unknown }> = [];
      for (const candidate of this.candidates(entry)) {
         const result = this.parseCandidate(entry, candidate);
         if ("error" in result) {
            return { error: result.error, source: sourceOf(candidate) };
         }
         parsed.push({ candidate, value: result.value });
      }
      for (const { candidate } of parsed) {
         if (candidate.old) this.warnDeprecated(entry, candidate);
      }
      const olds = parsed.filter(({ candidate }) => candidate.old);
      const currents = parsed.filter(({ candidate }) => !candidate.old);
      for (const old of olds) {
         const differing = currents.find(
            (current) => !sameValue(current.value, old.value),
         );
         if (differing) {
            return {
               error: conflict(entry, old, differing),
               source: sourceOf(differing.candidate),
            };
         }
      }
      for (const kind of ["flag", "env", "file"] as const) {
         const given = parsed.filter(
            ({ candidate }) => candidate.kind === kind,
         );
         if (given.length === 0) continue;
         // A repeated flag adds to a list; elsewhere the last one wins, and a
         // current name over an old one that agrees with it.
         const winner =
            [...given].reverse().find(({ candidate }) => !candidate.old) ??
            given[given.length - 1];
         return {
            value:
               entry.type instanceof ListType && kind === "flag"
                  ? given.flatMap(({ value }) => value as string[])
                  : winner.value,
            source: sourceOf(winner.candidate),
            shadowed: parsed
               .filter(({ candidate }) => candidate.kind !== kind)
               .map(({ candidate, value }) => ({
                  source: sourceOf(candidate),
                  value,
               })),
         };
      }
      try {
         return {
            value: this.defaultOf(entry),
            source: { kind: "default" },
            shadowed: [],
         };
      } catch (error) {
         // A default that reads another setting carries that setting's error.
         return { error: error as Error, source: { kind: "default" } };
      }
   }

   /** Every source that gave the setting: flags, variables, the file. */
   private candidates(entry: RegisteredSetting): Candidate[] {
      const flags = this.flags.get(entry.key) ?? [];
      const env: Candidate[] = [];
      for (const { name, old, today } of this.envNames(entry)) {
         const raw = this.env[name];
         // Blank is unset, so `-e NAME=` undoes a value an image sets.
         if (raw === undefined || raw.trim() === "") continue;
         env.push({ kind: "env", name, raw, old, today });
      }
      const file: Candidate[] = [];
      for (const { path: key, old } of this.filePaths(entry)) {
         const { value: raw, notObject } = valueAt(this.file, key);
         if (notObject !== undefined) this.notAnObject(notObject);
         if (raw === undefined || raw === null) continue;
         if (typeof raw === "string" && raw.trim() === "") continue;
         file.push({
            kind: "file",
            name: this.configPath ?? PUBLISHER_CONFIG_NAME,
            path: key,
            raw,
            old,
            // Only a key the file held before step 1 keeps today's reading.
            today: (entry.aliases.file ?? []).includes(key),
         });
      }
      return [...flags, ...env, ...file];
   }

   /** Read one source's value; a name read as it always was keeps that reading. */
   private parseCandidate(
      entry: RegisteredSetting,
      candidate: Candidate,
   ): Parsed {
      const today = entry.aliases.today ?? {};
      const source = sourceOf(candidate);
      if (
         entry.secret &&
         candidate.kind === "file" &&
         !(
            typeof candidate.raw === "string" &&
            new RegExp(VAR_REFERENCE.source).test(candidate.raw)
         )
      ) {
         return {
            error: new Error(
               `${entry.key} from ${source.kind === "default" ? "" : source.name}: a secret in the file must be a \${VAR} reference, not the value itself`,
            ),
         };
      }
      const readsAsToday =
         candidate.today &&
         entry.type instanceof BooleanType &&
         (candidate.kind === "file"
            ? today.file !== undefined
            : candidate.kind === "env" && today.reads !== undefined);
      if (readsAsToday) return this.readBoolean(entry, candidate);
      try {
         const value =
            candidate.kind === "file"
               ? this.parseFileValue(entry, candidate.raw, candidate.today)
               : typeof candidate.raw === "boolean"
                 ? candidate.raw
                 : entry.type.fromString(candidate.raw as string);
         return { value: this.negate(entry, candidate, value) };
      } catch (error) {
         if (!(error instanceof Mismatch)) return { error: error as Error };
         const mismatch = new SettingError(
            entry.key,
            source,
            this.typeExpected(entry, candidate),
            candidate.raw,
            entry.secret,
         );
         if (candidate.today && today.invalid === "warn") {
            const fallback = this.defaultOf(entry);
            this.warnings.push({
               key: entry.key,
               message: `${mismatch.message}; using its default, ${JSON.stringify(fallback)}.`,
            });
            return { value: fallback };
         }
         return { error: mismatch };
      }
   }

   /**
    * Step 1's rule for a boolean today reads its own way, still in force for
    * today's names: a value the common reader does not accept fails; one
    * today's reader refuses is still refused; and where the two read it
    * differently, today's meaning stays, with a warning.
    */
   private readBoolean(entry: RegisteredSetting, candidate: Candidate): Parsed {
      const source = sourceOf(candidate);
      const refusal = (expected: string): Parsed => ({
         error: new SettingError(
            entry.key,
            source,
            expected,
            candidate.raw,
            entry.secret,
         ),
      });
      let common: boolean;
      let today: boolean;
      try {
         common = this.commonBoolean(entry, candidate);
      } catch (error) {
         if (!(error instanceof Mismatch)) return { error: error as Error };
         return refusal(this.typeExpected(entry, candidate));
      }
      try {
         today = this.parseToday(entry, candidate) as boolean;
      } catch (error) {
         if (!(error instanceof Mismatch)) return { error: error as Error };
         if (entry.aliases.today?.invalid !== "warn") {
            return refusal(this.expected(entry, candidate));
         }
         today = this.negate(
            entry,
            candidate,
            this.defaultOf(entry),
         ) as boolean;
      }
      if (today !== common) {
         this.warnings.push({
            key: entry.key,
            message: disagreement(entry.key, candidate, today, common),
         });
      }
      return { value: this.negate(entry, candidate, today) };
   }

   /** The value as every boolean setting without a reading of its own reads it. */
   private commonBoolean(
      entry: RegisteredSetting,
      candidate: Candidate,
   ): boolean {
      const type = entry.type as BooleanType;
      return candidate.kind === "file"
         ? type.fromJson(
              typeof candidate.raw === "string"
                 ? this.substitute(entry, candidate.raw)
                 : candidate.raw,
           )
         : type.fromString(candidate.raw as string);
   }

   /** A boolean's value as today's reader reads it, before any negation. */
   private parseToday(entry: RegisteredSetting, candidate: Candidate): unknown {
      if (candidate.kind === "file") {
         return this.parseFileValue(entry, candidate.raw, true);
      }
      const raw = candidate.raw as string;
      switch (entry.aliases.today?.reads) {
         case "exactly-true":
            return raw === "true";
         case "true-or-1":
            return raw === "true" || raw === "1";
         case "on-off": {
            const spelled = raw.trim().toLowerCase();
            if (spelled === "on") return true;
            if (spelled === "off") return false;
            throw new Mismatch();
         }
         default:
            return entry.type.fromString(raw);
      }
   }

   /** An old negated variable (`PUBLISHER_NO_MCP_CONFIG`) says the opposite of its setting. */
   private negate(
      entry: RegisteredSetting,
      candidate: Candidate,
      value: unknown,
   ): unknown {
      return entry.aliases.negated && candidate.kind === "env" && candidate.old
         ? !value
         : value;
   }

   private warnDeprecated(
      entry: RegisteredSetting,
      candidate: Candidate,
   ): void {
      const name = candidate.kind === "file" ? candidate.path! : candidate.name;
      if (this.deprecated.has(name)) return;
      this.deprecated.add(name);
      const instead =
         candidate.kind === "flag"
            ? `use ${derivedFlagName(entry.key)}`
            : candidate.kind === "env"
              ? `set ${derivedEnvName(entry.key)}`
              : `write "${entry.key}"`;
      this.warnings.push({
         key: entry.key,
         kind: "deprecated",
         message: `${candidate.kind === "file" ? `"${name}" in ${candidate.name}` : name} is deprecated: ${instead} instead (${entry.key}). A later release stops reading it.`,
      });
   }

   /** A section of the file that should hold settings but is not an object. */
   private notAnObject(section: string): void {
      const message = `Invalid "${section}" in ${PUBLISHER_CONFIG_NAME}: expected an object. Ignoring.`;
      if (!this.warnings.some((warning) => warning.message === message)) {
         this.warnings.push({ message });
      }
   }

   private parseFileValue(
      entry: RegisteredSetting,
      raw: unknown,
      today: boolean,
   ): unknown {
      const substituted =
         typeof raw === "string" ? this.substitute(entry, raw) : raw;
      switch (today ? entry.aliases.today?.file : undefined) {
         case "truthy":
            return Boolean(substituted);
         case "boolean-or-reference":
            if (typeof raw === "boolean") return raw;
            if (
               typeof raw === "string" &&
               new RegExp(VAR_REFERENCE.source).test(raw)
            ) {
               return entry.type.fromString(substituted as string);
            }
            throw new Mismatch();
         case "json-boolean":
            if (typeof raw === "boolean") return raw;
            throw new Mismatch();
         default:
            return entry.type.fromJson(substituted);
      }
   }

   /** `${VAR}` in a file value, from this startup's variables. */
   private substitute(entry: RegisteredSetting, raw: string): string {
      return raw.replace(VAR_REFERENCE, (_match, name: string) => {
         const value = this.env[name];
         if (value === undefined) {
            throw new Error(
               `${entry.key} from ${this.configPath ?? PUBLISHER_CONFIG_NAME}: \${${name}} is not set`,
            );
         }
         return value;
      });
   }

   private defaultOf(entry: RegisteredSetting): unknown {
      const fallback = (entry as Setting).default;
      if (typeof fallback !== "function") return fallback;
      const resolving: Resolving = {
         get: (key) => this.valueOf(key),
         isSet: (key) =>
            this.resolve(this.byKey.get(key)!).source.kind !== "default",
         env: (name) => this.env[name],
      };
      return (fallback as (resolving: Resolving) => unknown)(resolving);
   }

   private expected(entry: RegisteredSetting, candidate: Candidate): string {
      return entry.aliases.today?.reads === "on-off"
         ? 'one of "on" or "off"'
         : this.typeExpected(entry, candidate);
   }

   /**
    * What the setting's type expects of this value. A string in the file is
    * read as a variable is; any other JSON value is held to the file's own
    * spelling, so `"frozenConfig": 1` is told `true or false`.
    */
   private typeExpected(
      entry: RegisteredSetting,
      candidate: Candidate,
   ): string {
      return candidate.kind === "file" && typeof candidate.raw !== "string"
         ? (entry.type.expectedInFile ?? entry.type.expected)
         : entry.type.expected;
   }
}

/** Whether two parsed values are the same setting value. */
function sameValue(a: unknown, b: unknown): boolean {
   return JSON.stringify(a) === JSON.stringify(b);
}

/** The failure for a setting given different values under an old and a new name. */
function conflict(
   entry: RegisteredSetting,
   old: { candidate: Candidate; value: unknown },
   current: { candidate: Candidate; value: unknown },
): Error {
   const shown = (side: { candidate: Candidate; value: unknown }) => {
      const where =
         side.candidate.kind === "file"
            ? `"${side.candidate.path}" in ${side.candidate.name}`
            : side.candidate.name;
      return entry.secret ? where : `${where} (${JSON.stringify(side.value)})`;
   };
   return new Error(
      `${entry.key} is given different values under its old and new names: ${shown(old)} and ${shown(current)}. Remove the old one.`,
   );
}

/**
 * The warning for a boolean today reads differently from every other boolean
 * setting: `INITIALIZE_STORAGE=1`, `"frozenConfig": "false"`.
 */
function disagreement(
   key: string,
   candidate: Candidate,
   today: boolean,
   common: boolean,
): string {
   const raw = JSON.stringify(candidate.raw);
   const given =
      candidate.kind === "file"
         ? `"${key}": ${raw} in ${candidate.name}`
         : `${candidate.name}=${typeof candidate.raw === "string" ? candidate.raw : raw}`;
   const onOff = (value: boolean) => (value ? "on" : "off");
   return (
      `${key}: ${given} is read as ${onOff(today)}, as it always has been, ` +
      `though other boolean settings read ${raw} as ${onOff(common)}. ` +
      `Write true or false: a later release refuses ${raw} here.`
   );
}

function parentOf(key: string): string {
   const dot = key.lastIndexOf(".");
   return dot < 0 ? "" : key.slice(0, dot);
}

/**
 * The closest of `names` to `name`, if one is close enough to be a typo of it:
 * a few edits away, or the start of it (`includeHiddenFiles`).
 */
function nearest(name: string, names: readonly string[]): string | undefined {
   const lower = name.toLowerCase();
   const prefixed = names.find(
      (candidate) =>
         lower.length >= 4 && candidate.toLowerCase().startsWith(lower),
   );
   if (prefixed) return prefixed;
   let best: string | undefined;
   let bestDistance = Infinity;
   for (const candidate of names) {
      const d = distance(lower, candidate.toLowerCase());
      if (d < bestDistance) {
         best = candidate;
         bestDistance = d;
      }
   }
   return bestDistance <= Math.max(2, Math.floor(name.length / 3))
      ? best
      : undefined;
}

/** Edit distance: insertions, deletions and substitutions. */
function distance(a: string, b: string): number {
   const row = Array.from({ length: b.length + 1 }, (_, i) => i);
   for (let i = 1; i <= a.length; i++) {
      let diagonal = row[0];
      row[0] = i;
      for (let j = 1; j <= b.length; j++) {
         const above = row[j];
         row[j] = Math.min(
            row[j] + 1,
            row[j - 1] + 1,
            diagonal + (a[i - 1] === b[j - 1] ? 0 : 1),
         );
         diagonal = above;
      }
   }
   return row[b.length];
}

function sourceOf(candidate: Candidate): SettingSource {
   return { kind: candidate.kind, name: candidate.name };
}

/**
 * The value at a dotted path of the config file, or undefined. `notObject`
 * names the section on the way that holds something other than an object.
 */
function valueAt(
   file: Record<string, unknown>,
   key: string,
): { value: unknown; notObject?: string } {
   let node: unknown = file;
   const parts = key.split(".");
   for (const [i, part] of parts.entries()) {
      if (node === undefined || node === null) return { value: undefined };
      if (typeof node !== "object" || Array.isArray(node)) {
         return { value: undefined, notObject: parts.slice(0, i).join(".") };
      }
      node = (node as Record<string, unknown>)[part];
   }
   return { value: node };
}

/** How a source reads in a message; re-exported for the config code's notices. */
export { describeSource };
