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
 * Each setting is read under the names derived from its key
 * (`PUBLISHER_MCP_PORT`, `--mcp-port`, the key in the file) and the short
 * flags the design keeps, with one precedence: flag, variable, file.
 */
export class Settings {
   private constructor(
      private readonly slots: ReadonlyMap<string, Slot>,
      /** The config file read, if one was found. */
      readonly configPath: string | undefined,
      /** Why the config file could not be read, if it could not. */
      readonly fileError: Error | undefined,
      /**
       * What fails startup without belonging to one setting: an unknown flag, a
       * flag with no value or with a value it does not take, and a key the file
       * holds that no setting reads.
       */
      private readonly problems: readonly Error[] = [],
   ) {}

   static load(input: LoadInput = {}): Settings {
      const loaded = new Loader(input).load();
      return new Settings(
         loaded.slots,
         loaded.configPath,
         loaded.fileError,
         loaded.problems,
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

   /**
    * Every setting that did not parse, each error once, then what failed
    * without belonging to one setting.
    */
   errors(): Error[] {
      return [
         ...new Set(
            [...this.slots.values()].flatMap((slot) =>
               "error" in slot ? [slot.error] : [],
            ),
         ),
         ...this.problems,
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
      );
   }

   private slot(key: string): Slot {
      const slot = this.slots.get(key);
      if (!slot) throw new Error(`No setting named ${key}`);
      return slot;
   }
}

const VAR_REFERENCE = /\$\{([A-Z_][A-Z0-9_]*)\}/g;
/** A value that is one `${VAR}` reference and nothing else, as a secret in the file must be. */
const WHOLE_REFERENCE = /^\$\{[A-Z_][A-Z0-9_]*\}$/;

/** A `${VAR}` in a file value whose variable is not set. */
class UnsetReference extends Error {}

/** A raw value from one source, before it is read as the setting's type. */
interface Candidate {
   kind: "flag" | "env" | "file";
   /** The flag or variable, or the file's path. */
   name: string;
   /** The key in the file, for a file value. */
   path?: string;
   raw: unknown;
}

interface FlagName {
   entry: RegisteredSetting;
   /** The value a switch sets; undefined for a flag that takes one. */
   switches?: boolean;
}

type Parsed = { value: unknown } | { error: Error };

/**
 * Every name a setting is read under: the derived variable, flag (with
 * `--no-` for a boolean) and file key, and the short flags the design keeps.
 * A secret has no flag, nor has a setting the design gives none (`noFlag`); a
 * structured setting is file-only and a launch setting has no file key.
 */
export function namesOf(entry: RegisteredSetting): {
   env: string[];
   flags: string[];
   file: string[];
} {
   const structured = entry.kind === "structured";
   const derivedFlag = derivedFlagName(entry.key);
   const flags: string[] = [];
   if (!structured) {
      if (!entry.secret && !entry.noFlag) {
         flags.push(derivedFlag);
         if (entry.type instanceof BooleanType) {
            flags.push(`--no-${derivedFlag.slice(2)}`);
         }
      }
      for (const name of entry.shortFlags ?? []) {
         if (!flags.includes(name)) flags.push(name);
      }
   }
   return {
      env: structured ? [] : [derivedEnvName(entry.key)],
      flags,
      file: entry.kind === "launch" ? [] : [entry.key],
   };
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
      entry.type instanceof BooleanType ? [] : namesOf(entry).flags,
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
   private readonly problems: Error[] = [];
   private readonly unknownFlags: string[] = [];
   private readonly flagsWithoutValue: string[] = [];
   private readonly valuesAfterSwitches: Array<{
      name: string;
      flag: FlagName;
      next: string;
   }> = [];
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
      problems: Error[];
   } {
      this.readFlags();
      // The launch settings say which file to read and have no file key. One
      // that fails is reported with every other setting's error, so startup
      // still prints PUBLISHER_INIT_FAILED; no file is read, since which one
      // is unknown.
      const root = this.resolve(this.byKey.get("root")!);
      const config = this.resolve(this.byKey.get("config")!);
      if (!("error" in root) && !("error" in config)) {
         this.readFile(
            root.value as string,
            config.value as string | undefined,
         );
      }
      for (const entry of this.registry) this.resolve(entry);
      this.refuseFlags();
      this.refuseFileKeys();
      return {
         slots: this.slots,
         configPath: this.configPath,
         fileError: this.fileError,
         problems: this.problems,
      };
   }

   private valueOf(key: string): unknown {
      const slot = this.resolve(this.byKey.get(key)!);
      if ("error" in slot) throw slot.error;
      return slot.value;
   }

   private nameFlags(entry: RegisteredSetting): void {
      const isSwitch = entry.type instanceof BooleanType;
      for (const name of namesOf(entry).flags) {
         if (this.byFlag.has(name)) continue;
         this.byFlag.set(name, {
            entry,
            ...(isSwitch ? { switches: !name.startsWith("--no-") } : {}),
         });
      }
   }

   /**
    * An exact flag name; a switch alone; any other flag with the next argument
    * as its value when there is one. A flag it does not know, a flag with
    * nothing after it, and a value after a switch are kept to fail startup.
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
            const next = this.argv[i + 1];
            if (next !== undefined && readsAsBoolean(flag.entry, next)) {
               this.valuesAfterSwitches.push({ name, flag, next });
            }
         } else if (this.argv[i + 1]) {
            raw = this.argv[i + 1];
            i++;
         } else {
            this.flagsWithoutValue.push(name);
            continue;
         }
         const hits = this.flags.get(flag.entry.key) ?? [];
         hits.push({ kind: "flag", name, raw });
         this.flags.set(flag.entry.key, hits);
      }
   }

   private refuseFlags(): void {
      const known = [...this.byFlag.keys()];
      for (const name of this.unknownFlags) {
         const suggestion = nearest(name, known);
         this.problems.push(
            new Error(
               `Unknown flag ${name}.${suggestion ? ` Did you mean ${suggestion}?` : ""}`,
            ),
         );
      }
      for (const name of this.flagsWithoutValue) {
         this.problems.push(new Error(`${name} has no value after it.`));
      }
      for (const { name, flag, next } of this.valuesAfterSwitches) {
         const on = flag.switches!;
         const opposite = namesOf(flag.entry).flags.find(
            (other) =>
               this.byFlag.get(other)?.switches === !on &&
               this.byFlag.get(other)?.entry === flag.entry,
         );
         this.problems.push(
            new Error(
               `${name} takes no value, so it cannot be given "${next}": ${name} alone turns ${flag.entry.key} ${on ? "on" : "off"}.` +
                  (opposite
                     ? ` To turn it ${on ? "off" : "on"}, write ${opposite}.`
                     : ""),
            ),
         );
      }
   }

   /**
    * A key the file holds that no setting reads: a typo (`adminApiEnable`), or
    * a launch setting, which the file cannot give. Checked at the top level and
    * in each section that holds settings; a structured setting's own keys are
    * its parser's to check.
    */
   private refuseFileKeys(): void {
      const read = this.registry.flatMap((entry) => namesOf(entry).file);
      const whole = new Set(
         this.registry
            .filter((entry) => entry.kind === "structured")
            .flatMap((entry) => namesOf(entry).file),
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
               this.problems.push(
                  new Error(
                     `"${key}" in ${file} cannot be set there: it decides which file is read, or must not come from one, so it is a variable or a flag only (${derivedEnvName(key)}, ${derivedFlagName(key)}).`,
                  ),
               );
               continue;
            }
            const siblings = read
               .filter((path) => parentOf(path) === prefix)
               .map((path) => path.slice(prefix ? prefix.length + 1 : 0));
            const suggestion = nearest(name, siblings);
            this.problems.push(
               new Error(
                  `${file} has "${key}", which no setting reads.${suggestion ? ` Did you mean "${prefix ? `${prefix}.` : ""}${suggestion}"?` : ""}`,
               ),
            );
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
    * the default.
    */
   private read(entry: RegisteredSetting): Slot {
      const parsed: Array<{ candidate: Candidate; value: unknown }> = [];
      for (const candidate of this.candidates(entry)) {
         const result = this.parseCandidate(entry, candidate);
         if ("error" in result) {
            // A file value whose ${VAR} is unset gives nothing. It fails
            // startup only when no flag or variable gives the setting.
            if (
               result.error instanceof UnsetReference &&
               parsed.some(({ candidate }) => candidate.kind !== "file")
            ) {
               continue;
            }
            return { error: result.error, source: sourceOf(candidate) };
         }
         parsed.push({ candidate, value: result.value });
      }
      for (const kind of ["flag", "env", "file"] as const) {
         const given = parsed.filter(
            ({ candidate }) => candidate.kind === kind,
         );
         if (given.length === 0) continue;
         // A repeated flag adds to a list; elsewhere the last one wins.
         const winner = given[given.length - 1];
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
      for (const name of namesOf(entry).env) {
         const raw = this.env[name];
         // Blank is unset, so `-e NAME=` undoes a value an image sets.
         if (raw === undefined || raw.trim() === "") continue;
         env.push({ kind: "env", name, raw });
      }
      const file: Candidate[] = [];
      for (const key of namesOf(entry).file) {
         const { value: raw, notObject } = valueAt(this.file, key);
         if (notObject !== undefined) this.notAnObject(notObject);
         if (raw === undefined || raw === null) continue;
         if (typeof raw === "string" && raw.trim() === "") continue;
         file.push({
            kind: "file",
            name: this.configPath ?? PUBLISHER_CONFIG_NAME,
            path: key,
            raw,
         });
      }
      return [...flags, ...env, ...file];
   }

   /** Read one source's value as the setting's type. */
   private parseCandidate(
      entry: RegisteredSetting,
      candidate: Candidate,
   ): Parsed {
      const source = sourceOf(candidate);
      if (
         entry.secret &&
         candidate.kind === "file" &&
         !(
            typeof candidate.raw === "string" &&
            WHOLE_REFERENCE.test(candidate.raw.trim())
         )
      ) {
         return {
            error: new Error(
               `${entry.key} from ${source.kind === "default" ? "" : source.name}: a secret in the file must be one \${VAR} reference and nothing else, not the value itself or part of it`,
            ),
         };
      }
      try {
         const raw = candidate.raw;
         const value =
            candidate.kind === "file"
               ? entry.type.fromJson(
                    typeof raw === "string" ? this.substitute(entry, raw) : raw,
                 )
               : typeof raw === "boolean"
                 ? raw
                 : entry.type.fromString(raw as string);
         return { value };
      } catch (error) {
         if (!(error instanceof Mismatch)) return { error: error as Error };
         return {
            error: new SettingError(
               entry.key,
               source,
               this.typeExpected(entry, candidate),
               candidate.raw,
               entry.secret,
               serviceLinkHint(candidate),
            ),
         };
      }
   }

   /** A section of the file that should hold settings but is not an object. */
   private notAnObject(section: string): void {
      const message = `Invalid "${section}" in ${PUBLISHER_CONFIG_NAME}: expected an object.`;
      if (!this.problems.some((problem) => problem.message === message)) {
         this.problems.push(new Error(message));
      }
   }

   /** `${VAR}` in a file value, from this startup's variables. */
   private substitute(entry: RegisteredSetting, raw: string): string {
      return raw.replace(VAR_REFERENCE, (_match, name: string) => {
         const value = this.env[name];
         if (value === undefined) {
            throw new UnsetReference(
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

/** Whether an argument after a switch reads as a boolean: `--x false`. */
function readsAsBoolean(entry: RegisteredSetting, next: string): boolean {
   try {
      (entry.type as BooleanType).fromString(next);
      return true;
   } catch {
      return false;
   }
}

/**
 * Kubernetes gives every pod a `<SERVICE>_PORT=tcp://<ip>:<port>` variable for
 * each Service in its namespace unless the pod sets `enableServiceLinks:
 * false`, so a Service named `publisher-mcp` sets `PUBLISHER_MCP_PORT`. "Expected
 * an integer" alone would not lead anyone there.
 */
function serviceLinkHint(candidate: Candidate): string | undefined {
   if (candidate.kind !== "env" || !candidate.name.endsWith("_PORT")) {
      return undefined;
   }
   if (
      typeof candidate.raw !== "string" ||
      !/^tcp:\/\/[^\s/]+:\d+$/.test(candidate.raw.trim())
   ) {
      return undefined;
   }
   const service = candidate.name
      .slice(0, -"_PORT".length)
      .toLowerCase()
      .replace(/_/g, "-");
   return `That is the variable Kubernetes sets for a Service named "${service}" in the pod's namespace: set enableServiceLinks: false in the pod spec, or rename the Service.`;
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
