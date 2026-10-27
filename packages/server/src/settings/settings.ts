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
import { derivedFlagName, type Resolving, type Setting } from "./setting";
import { BooleanType, Mismatch } from "./types";

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
 * In step 1 of the configuration design the names read are today's, kept on
 * each entry's `aliases` with today's precedence and today's readings.
 */
export class Settings {
   private constructor(
      private readonly slots: ReadonlyMap<string, Slot>,
      /** The config file read, if one was found. */
      readonly configPath: string | undefined,
      /** Why the config file could not be read, if it could not. */
      readonly fileError: Error | undefined,
      readonly warnings: readonly SettingWarning[],
   ) {}

   static load(input: LoadInput = {}): Settings {
      const loaded = new Loader(input).load();
      return new Settings(
         loaded.slots,
         loaded.configPath,
         loaded.fileError,
         loaded.warnings,
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

   /** Every setting that did not parse. */
   errors(): Error[] {
      return [...this.slots.values()].flatMap((slot) =>
         "error" in slot ? [slot.error] : [],
      );
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
 * The flags that take the argument after them as their value: every flag but
 * a boolean's, which is a switch. create-malloy-package keeps a copy of this
 * list to tell which --host a start script hands the server.
 */
export function flagsTakingAValue(
   registry: readonly RegisteredSetting[] = REGISTRY,
): string[] {
   return registry.flatMap((entry) =>
      entry.type instanceof BooleanType ? [] : (entry.aliases.flags ?? []),
   );
}

/** A raw value from one source, before it is read as the setting's type. */
type Candidate =
   | { kind: "flag"; name: string; raw: string | boolean }
   | { kind: "env"; name: string; raw: string }
   | { kind: "file"; name: string; raw: unknown };

/** Reads one startup's sources into a {@link Settings}. */
class Loader {
   private readonly registry: readonly RegisteredSetting[];
   private readonly byKey: ReadonlyMap<string, RegisteredSetting>;
   private readonly byFlag: ReadonlyMap<string, RegisteredSetting>;
   private readonly argv: readonly string[];
   private readonly env: Env;
   private readonly cwd: string;
   private readonly flags = new Map<
      string,
      Array<{ name: string; raw: string | boolean }>
   >();
   private readonly slots = new Map<string, Slot>();
   private readonly resolving = new Set<string>();
   private readonly warnings: SettingWarning[] = [];
   private readonly unknownFlags: string[] = [];
   private readonly flagsWithoutValue: string[] = [];
   private file: Record<string, unknown> = {};
   private configPath: string | undefined;
   private fileError: Error | undefined;

   constructor(input: LoadInput) {
      this.registry = input.registry ?? REGISTRY;
      this.byKey = new Map(this.registry.map((entry) => [entry.key, entry]));
      this.byFlag = new Map(
         this.registry.flatMap((entry) =>
            (entry.aliases.flags ?? []).map((flag) => [flag, entry] as const),
         ),
      );
      this.argv = input.argv ?? [];
      this.env = input.env ?? { ...process.env };
      this.cwd = input.cwd ?? process.cwd();
   }

   load(): {
      slots: Map<string, Slot>;
      configPath: string | undefined;
      fileError: Error | undefined;
      warnings: SettingWarning[];
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
      };
   }

   private valueOf(key: string): unknown {
      const slot = this.resolve(this.byKey.get(key)!);
      if ("error" in slot) throw slot.error;
      return slot.value;
   }

   /**
    * Today's parser: an exact flag name, a boolean's flag alone, any other
    * flag with the next argument as its value when there is one. A flag it
    * does not know, and a flag with nothing after it, are skipped and warned
    * about.
    */
   private readFlags(): void {
      for (let i = 0; i < this.argv.length; i++) {
         const name = this.argv[i];
         const entry = this.byFlag.get(name);
         if (!entry) {
            if (name.startsWith("-") && name !== "--help" && name !== "-h") {
               this.unknownFlags.push(name);
            }
            continue;
         }
         const hits = this.flags.get(entry.key) ?? [];
         if (entry.type instanceof BooleanType) {
            hits.push({ name, raw: !name.startsWith("--no-") });
         } else if (this.argv[i + 1]) {
            hits.push({ name, raw: this.argv[i + 1] });
            i++;
         } else {
            this.flagsWithoutValue.push(name);
            continue;
         }
         this.flags.set(entry.key, hits);
      }
   }

   private warnAboutFlags(): void {
      const known = [...this.byFlag.keys()];
      for (const name of this.unknownFlags) {
         const later = this.registry.find(
            (entry) => derivedFlagName(entry.key) === name,
         );
         const suggestion = nearest(name, known);
         this.warnings.push({
            message: later
               ? `${name} is not read before a later release, so it is ignored; ${todayNames(later)}.`
               : `Unknown flag ${name}, so it is ignored.${suggestion ? ` Did you mean ${suggestion}?` : ""}`,
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
    * a setting read from the file only from a later step. Checked at the top
    * level and in each section that holds settings; a structured setting's
    * own keys are its parser's to check.
    */
   private warnAboutFileKeys(): void {
      const read = this.registry.flatMap((entry) => entry.aliases.file ?? []);
      const whole = new Set(
         this.registry
            .filter((entry) => entry.kind === "structured")
            .flatMap((entry) => entry.aliases.file ?? []),
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
            const later = this.registry.find(
               (entry) => entry.key === key || entry.key.startsWith(`${key}.`),
            );
            if (later) {
               this.warnings.push({
                  message:
                     later.key === key
                        ? `"${key}" in ${file} is not read from the file before a later release, so it is ignored; ${todayNames(later)}.`
                        : `"${key}" in ${file} holds settings that are not read from the file before a later release, so it is ignored; set them by their variables or flags.`,
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

   private read(entry: RegisteredSetting): Slot {
      const today = entry.aliases.today ?? {};
      const candidates = this.candidates(entry);

      if (today.defaultFromFlags) {
         const fallback = this.defaultOf(entry, true);
         if (fallback === true) {
            return {
               value: true,
               source: { kind: "default" },
               shadowed: this.parsedQuietly(entry, candidates),
            };
         }
      }

      if (today.flagsAppend) {
         const flags = candidates.filter((c) => c.kind === "flag");
         if (flags.length > 0) {
            const fromEnv = candidates.find((c) => c.kind === "env");
            const listed = fromEnv
               ? (this.parse(entry, fromEnv) as string[])
               : [];
            return {
               value: [
                  ...listed,
                  ...flags.flatMap((c) => this.parse(entry, c) as string[]),
               ],
               source: { kind: "flag", name: flags[flags.length - 1].name },
               shadowed: [],
            };
         }
      }

      const [first, ...rest] = candidates;
      if (!first) {
         return {
            value: this.defaultOf(entry),
            source: { kind: "default" },
            shadowed: [],
         };
      }
      const source = sourceOf(first);
      if (this.readsBooleanAsToday(entry, first)) {
         return this.readBoolean(entry, first, rest);
      }
      try {
         return {
            value: this.parse(entry, first),
            source,
            shadowed: this.parsedQuietly(entry, rest),
         };
      } catch (error) {
         if (!(error instanceof Mismatch)) {
            return { error: error as Error, source };
         }
         const mismatch = new SettingError(
            entry.key,
            source,
            this.expected(entry),
            first.raw,
            entry.secret,
         );
         if (today.invalid === "warn") {
            const fallback = this.defaultOf(entry);
            this.warnings.push({
               key: entry.key,
               message: `${mismatch.message}; using its default, ${JSON.stringify(fallback)}.`,
            });
            return {
               value: fallback,
               source: { kind: "default" },
               shadowed: [],
            };
         }
         return { error: mismatch, source };
      }
   }

   /** Whether this source's value is a boolean that today reads its own way. */
   private readsBooleanAsToday(
      entry: RegisteredSetting,
      candidate: Candidate,
   ): boolean {
      const today = entry.aliases.today ?? {};
      if (!(entry.type instanceof BooleanType)) return false;
      return candidate.kind === "file"
         ? today.file !== undefined
         : typeof candidate.raw === "string" && today.reads !== undefined;
   }

   /**
    * Step 1's rule for a boolean today reads its own way: a value the common
    * reader does not accept fails; one today's reader refuses is still
    * refused; and where the two read it differently, today's meaning stays,
    * with a warning that a later release refuses the value.
    */
   private readBoolean(
      entry: RegisteredSetting,
      first: Candidate,
      rest: Candidate[],
   ): Slot {
      const source = sourceOf(first);
      const refusal = (expected: string): Slot => ({
         error: new SettingError(
            entry.key,
            source,
            expected,
            first.raw,
            entry.secret,
         ),
         source,
      });
      let common: boolean;
      let today: boolean;
      try {
         common = this.commonBoolean(entry, first);
      } catch (error) {
         if (!(error instanceof Mismatch))
            return { error: error as Error, source };
         return refusal(entry.type.expected);
      }
      try {
         today = this.parseRaw(entry, first) as boolean;
      } catch (error) {
         if (!(error instanceof Mismatch))
            return { error: error as Error, source };
         if (entry.aliases.today?.invalid !== "warn") {
            return refusal(this.expected(entry));
         }
         today = this.unnegated(entry, first, this.defaultOf(entry) as boolean);
      }
      if (today !== common) {
         this.warnings.push({
            key: entry.key,
            message: disagreement(entry.key, first, today, common),
         });
      }
      return {
         value: this.negate(entry, first, today),
         source,
         shadowed: this.parsedQuietly(entry, rest),
      };
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

   /** A negated variable (`PUBLISHER_NO_MCP_CONFIG`) says the opposite of its setting. */
   private negate(
      entry: RegisteredSetting,
      candidate: Candidate,
      value: unknown,
   ): unknown {
      return entry.aliases.negated && candidate.kind === "env" ? !value : value;
   }

   private unnegated(
      entry: RegisteredSetting,
      candidate: Candidate,
      value: boolean,
   ): boolean {
      return this.negate(entry, candidate, value) as boolean;
   }

   /** A section of the file that should hold settings but is not an object. */
   private notAnObject(section: string): void {
      const message = `Invalid "${section}" in ${PUBLISHER_CONFIG_NAME}: expected an object. Ignoring.`;
      if (!this.warnings.some((warning) => warning.message === message)) {
         this.warnings.push({ message });
      }
   }

   /** Every source that gave the setting, highest precedence first. */
   private candidates(entry: RegisteredSetting): Candidate[] {
      const today = entry.aliases.today ?? {};
      const hits = this.flags.get(entry.key) ?? [];
      const flags: Candidate[] = today.flagsAppend
         ? hits.map((hit) => ({ kind: "flag" as const, ...hit }))
         : hits.slice(-1).map((hit) => ({ kind: "flag" as const, ...hit }));

      const env: Candidate[] = [];
      for (const name of entry.aliases.env ?? []) {
         const raw = this.env[name];
         // Blank is unset, so `-e NAME=` undoes a value an image sets.
         if (raw === undefined || raw.trim() === "") continue;
         env.push({ kind: "env", name, raw });
         break;
      }

      const file: Candidate[] = [];
      for (const key of entry.aliases.file ?? []) {
         const { value: raw, notObject } = valueAt(this.file, key);
         if (notObject !== undefined) this.notAnObject(notObject);
         if (raw === undefined || raw === null) continue;
         if (typeof raw === "string" && raw.trim() === "") continue;
         file.push({
            kind: "file",
            name: this.configPath ?? PUBLISHER_CONFIG_NAME,
            raw,
         });
         break;
      }

      return today.fileFirst
         ? [...file, ...flags, ...env]
         : [...flags, ...env, ...file];
   }

   /** Read one source's value as today's reader reads it. Throws {@link Mismatch}. */
   private parse(entry: RegisteredSetting, candidate: Candidate): unknown {
      return this.negate(entry, candidate, this.parseRaw(entry, candidate));
   }

   /** As {@link parse}, before a negated variable is turned around. */
   private parseRaw(entry: RegisteredSetting, candidate: Candidate): unknown {
      return candidate.kind === "file"
         ? this.parseFileValue(entry, candidate.raw)
         : typeof candidate.raw === "boolean"
           ? candidate.raw
           : this.parseString(entry, candidate.raw);
   }

   private parseString(entry: RegisteredSetting, raw: string): unknown {
      const type = entry.type;
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
            return type.fromString(raw);
      }
   }

   private parseFileValue(entry: RegisteredSetting, raw: unknown): unknown {
      const substituted =
         typeof raw === "string" ? this.substitute(entry, raw) : raw;
      switch (entry.aliases.today?.file) {
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

   /** Lower-precedence values, for reporting; one that does not parse is kept raw. */
   private parsedQuietly(
      entry: RegisteredSetting,
      candidates: Candidate[],
   ): ShadowedValue[] {
      return candidates.map((candidate) => {
         let value: unknown;
         try {
            value = this.parse(entry, candidate);
         } catch {
            value = candidate.raw;
         }
         return { source: sourceOf(candidate), value };
      });
   }

   private defaultOf(entry: RegisteredSetting, flagsOnly = false): unknown {
      const fallback = (entry as Setting).default;
      if (typeof fallback !== "function") return fallback;
      const resolving: Resolving = {
         get: (key) => this.valueOf(key),
         isSet: (key) => {
            const slot = this.resolve(this.byKey.get(key)!);
            return flagsOnly
               ? slot.source.kind === "flag"
               : slot.source.kind !== "default";
         },
         env: (name) => this.env[name],
      };
      return (fallback as (resolving: Resolving) => unknown)(resolving);
   }

   private expected(entry: RegisteredSetting): string {
      return entry.aliases.today?.reads === "on-off"
         ? 'one of "on" or "off"'
         : entry.type.expected;
   }
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

/** How to give a setting today, for a name that is read only from a later release. */
function todayNames(entry: RegisteredSetting): string {
   const flags = entry.aliases.flags ?? [];
   const env = entry.aliases.env ?? [];
   if (flags.length > 0) return `until then use ${flags[0]}`;
   if (env.length > 0) return `until then set ${env[0]}`;
   return `until then set it in ${PUBLISHER_CONFIG_NAME}`;
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
