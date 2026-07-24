import * as fs from "node:fs";
import { ScaffoldError } from "./errors";
import { printable } from "./names";

/**
 * Render a value read off the user's disk so that printing it can only add
 * characters to the terminal, never commands to it, and cannot fill the screen.
 *
 * Every message in this file is printed to a terminal, and the values in them
 * come out of a publisher.config.json that may have been cloned rather than
 * written here. A `location` holding `ESC [2K CR ESC [32m ...` repaints the line
 * above it, which is how an "Error: ... already registered" over a run that
 * wrote nothing was turned into a green "Created package" that never happened.
 * The comment above assertConfigShape already stated the policy; nothing in the
 * file applied it.
 */
function shown(value: unknown, max = 120): string {
   const text = typeof value === "string" ? value : String(value);
   return printable(text.length > max ? `${text.slice(0, max)}...` : text);
}

/** A package registered in an environment: name plus a relative location. */
export interface PackageEntry {
   name: string;
   location: string;
}

export interface EnvironmentEntry {
   name: string;
   packages: PackageEntry[];
   connections?: unknown[];
   [key: string]: unknown;
}

export interface PublisherConfig {
   frozenConfig?: boolean;
   environments: EnvironmentEntry[];
   [key: string]: unknown;
}

/** A fresh config with a single environment, optionally holding one package. */
export function defaultConfig(
   envName: string,
   pkg?: PackageEntry,
): PublisherConfig {
   return {
      frozenConfig: false,
      environments: [
         {
            name: envName,
            packages: pkg ? [pkg] : [],
            connections: [],
         },
      ],
   };
}

/**
 * JSON.parse, but tolerant of the UTF-8 byte order mark editors on Windows (and
 * PowerShell's `>` redirect) leave at the front of a file. Node's `require()`,
 * `npm pkg get`, and every other reader in this toolchain accept a BOM, so
 * treating one as a parse failure made this tool the only thing that concluded
 * the user's package.json, .mcp.json or publisher.config.json held nothing worth
 * keeping, and then acted on that conclusion by overwriting the file.
 */
export function parseJson(text: string): unknown {
   return JSON.parse(text.replace(/^\uFEFF/, ""));
}

/** The value as a plain JSON object, or undefined if it is anything else. */
export function asJsonObject(
   value: unknown,
): Record<string, unknown> | undefined {
   return typeof value === "object" && value !== null && !Array.isArray(value)
      ? (value as Record<string, unknown>)
      : undefined;
}

/**
 * Name a JSON value by its kind, for an error about the wrong kind. "an object"
 * is a real case: a "packages" key holding one is the wrong kind for a list.
 */
export function describeJsonValue(value: unknown): string {
   if (value === null) {
      return "null";
   }
   if (Array.isArray(value)) {
      return "an array";
   }
   if (typeof value === "object") {
      return "an object";
   }
   return `a ${typeof value}`;
}

export function readConfig(configPath: string): PublisherConfig {
   let parsed: unknown;
   try {
      parsed = parseJson(fs.readFileSync(configPath, "utf8"));
   } catch (err) {
      throw new ScaffoldError(
         // The parse message quotes the file it failed on, so it carries
         // whatever the file holds.
         `Could not parse ${shown(configPath)}: ` +
            `${shown((err as Error).message, 200)}. ` +
            `Fix or remove it, then run again.`,
      );
   }
   if (
      typeof parsed !== "object" ||
      parsed === null ||
      !Array.isArray((parsed as PublisherConfig).environments)
   ) {
      throw new ScaffoldError(
         `${shown(configPath)} is not a Publisher config (no "environments" array). ` +
            `Fix or remove it, then run again.`,
      );
   }
   const config = parsed as PublisherConfig;
   assertConfigShape(configPath, config);
   return config;
}

/**
 * Refuse the two malformed shapes an "environments" array can still hold once it
 * is an array: an entry that is not an object, and an entry whose "packages" is
 * not an array of objects.
 *
 * Every one of those used to reach the code that reads `.name` off the entry and
 * come back out as `TypeError: Cannot read properties of null (reading 'name')`,
 * which this tool then reported as "This looks like a bug in
 * create-malloy-package. Re-run with DEBUG=1 for the stack trace." That is the
 * wrong diagnosis for a config file, and it is the only bad shape in this file
 * that got one: every neighbour is named and told how to fix it.
 *
 * Checked rather than filtered. Skipping the bad entry would let the run finish
 * green over a config the server cannot load, and a non-array "packages" is
 * worse than that: addPackage replaces it with a fresh array, so filtering would
 * write the user's own data out of their config and report a package created.
 *
 * The entries are named by position, never by echoing a name back: the value is
 * whatever the file holds, and this message is printed to a terminal.
 */
function assertConfigShape(configPath: string, config: PublisherConfig): void {
   const total = config.environments.length;
   config.environments.forEach((env, index) => {
      const where = `entry ${index + 1} of ${total} in "environments"`;
      if (!asJsonObject(env)) {
         throw new ScaffoldError(
            `${shown(configPath)} has an environment that is ` +
               `${describeJsonValue(env)} rather than a JSON object ` +
               `(${where}). Publisher cannot load that file, and this tool ` +
               `cannot register a package in it. Fix or remove it, then run ` +
               `again.`,
         );
      }
      if (env.packages === undefined) {
         return;
      }
      if (!Array.isArray(env.packages)) {
         throw new ScaffoldError(
            `${shown(configPath)} has an environment whose "packages" is ` +
               `${describeJsonValue(env.packages)} rather than an array ` +
               `(${where}). Registering a package there would replace that ` +
               `value with a list holding only the new package, so nothing was ` +
               `written. Fix or remove it, then run again.`,
         );
      }
      env.packages.forEach((pkg, pkgIndex) => {
         if (!asJsonObject(pkg)) {
            throw new ScaffoldError(
               `${shown(configPath)} has a package that is ${describeJsonValue(pkg)} ` +
                  `rather than a JSON object (entry ${pkgIndex + 1} of ` +
                  `${(env.packages as unknown[]).length} in the "packages" of ` +
                  `${where}). Publisher cannot load that file, and this tool ` +
                  `cannot tell whether the name you asked for is already taken ` +
                  `by it. Fix or remove it, then run again.`,
            );
         }
      });
   });
}

export function writeConfig(configPath: string, config: PublisherConfig): void {
   fs.writeFileSync(configPath, JSON.stringify(config, null, 2) + "\n");
}

/**
 * The environment envName lands in: the named one, else the first, else none.
 * Exported so a caller can inspect that entry as it is written on disk, rather
 * than through resolveEnvironmentName, which substitutes envName for a `name`
 * that is missing or null and so hides an entry this tool cannot work with.
 */
export function targetEnvironment(
   config: PublisherConfig,
   envName: string,
): EnvironmentEntry | undefined {
   // Both halves skip anything that is not an object, so this can only ever
   // return something with a `.name` to read. readConfig refuses those entries
   // outright, which is where a user hears about them; this keeps a config
   // built in memory from turning into a TypeError here instead.
   const environments = config.environments.filter(
      (candidate): candidate is EnvironmentEntry =>
         asJsonObject(candidate) !== undefined,
   );
   return (
      environments.find((candidate) => candidate.name === envName) ??
      environments[0]
   );
}

/**
 * The name of the environment a package would land in. Callers need this because
 * an existing config's first environment is often not named envName (Publisher's
 * own shipped config calls it "examples"), and the start command, the REST URLs,
 * and the agent instructions all have to name the environment the package is
 * actually in, not the one we would have created.
 */
export function resolveEnvironmentName(
   config: PublisherConfig,
   envName: string,
): string {
   return targetEnvironment(config, envName)?.name ?? envName;
}

/**
 * How many packages the environment envName resolves to already holds. Callers
 * need this because "this run created a package" and "the environment the server
 * will serve has packages in it" are different questions, and everything a user
 * is told about an empty server (nothing to serve, the packages endpoint 404s)
 * is only true of the second one. A config whose first environment already lists
 * packages answers that endpoint, whether or not this run added anything.
 */
export function environmentPackageCount(
   config: PublisherConfig,
   envName: string,
): number {
   const env = targetEnvironment(config, envName);
   return Array.isArray(env?.packages) ? env.packages.length : 0;
}

/**
 * The names of the packages that environment holds. The agent briefing is
 * regenerated in full on every run, so a briefing written from this run's
 * package alone describes a workspace that has one package in it when the
 * config says it serves three, and the two it does not mention are the ones
 * nothing else in the workspace describes either.
 *
 * Entries with no usable name are dropped rather than rendered: this list is
 * read out of a file that ships in repositories, and every name in it is
 * written into a document an agent reads as instructions.
 */
export function environmentPackageNames(
   config: PublisherConfig,
   envName: string,
): string[] {
   const env = targetEnvironment(config, envName);
   if (!Array.isArray(env?.packages)) {
      return [];
   }
   return env.packages
      .map((entry) => asJsonObject(entry)?.name)
      .filter((name): name is string => typeof name === "string");
}

/**
 * Throw if a package cannot be registered: the config is frozen, or the name is
 * already taken at a different location. Same name and location is allowed (a
 * `--force` re-run). Pure check, no mutation, so callers can validate before
 * creating anything on disk.
 */
export function assertCanAddPackage(
   config: PublisherConfig,
   envName: string,
   pkg: PackageEntry,
): void {
   if (config.frozenConfig === true) {
      throw new ScaffoldError(
         `publisher.config.json has "frozenConfig": true, which forbids adding ` +
            `packages. Set it to false, or register "${shown(pkg.name)}" by hand.`,
      );
   }
   const env = targetEnvironment(config, envName);
   const existing = Array.isArray(env?.packages)
      ? env.packages.find((entry) => asJsonObject(entry)?.name === pkg.name)
      : undefined;
   if (existing && existing.location !== pkg.location) {
      throw new ScaffoldError(
         `Package "${shown(pkg.name)}" is already registered in environment ` +
            `"${shown(env?.name)}" at a different location ` +
            `("${shown(existing.location)}"). ` +
            `Choose another name.`,
      );
   }
}

export interface AddPackageResult {
   /** True when the config was changed and has to be written back. */
   added: boolean;
   /** The environment the package is registered in, which may not be envName. */
   envName: string;
}

/**
 * Register a package in a config by adding one entry. Prefers the environment
 * named envName so the generated `--watch-env` keeps matching; falls back to the
 * first environment, or creates envName if there are none. Re-registering the
 * identical entry (same name and location) is a no-op. Reports the environment it
 * landed in so the caller can name that one everywhere instead of assuming
 * envName. Assumes assertCanAddPackage has been (or is) satisfied.
 */
export function addPackage(
   config: PublisherConfig,
   envName: string,
   pkg: PackageEntry,
): AddPackageResult {
   assertCanAddPackage(config, envName, pkg);

   let env = targetEnvironment(config, envName);
   if (!env) {
      env = { name: envName, packages: [], connections: [] };
      config.environments.push(env);
   }
   // Only a missing key reaches this: readConfig refuses a "packages" that is
   // present and not an array, because replacing it here would write whatever
   // it held out of the user's config.
   if (!Array.isArray(env.packages)) {
      env.packages = [];
   }
   if (env.packages.some((entry) => asJsonObject(entry)?.name === pkg.name)) {
      // Same name and location (a different location already threw): no-op.
      return { added: false, envName: env.name };
   }
   env.packages.push(pkg);
   return { added: true, envName: env.name };
}
