import * as fs from "node:fs";
import * as path from "node:path";
import {
   addPackage,
   asJsonObject,
   assertCanAddPackage,
   defaultConfig,
   describeJsonValue,
   environmentPackageCount,
   environmentPackageNames,
   parseJson,
   readConfig,
   resolveEnvironmentName,
   targetEnvironment,
   writeConfig,
   type PackageEntry,
   type PublisherConfig,
} from "./config";
import { ScaffoldError } from "./errors";
import {
   printable,
   toMalloyIdentifier,
   validateEnvironmentName,
   validatePackageName,
} from "./names";
import {
   assertSkillsAvailable,
   installSkills,
   isWithinDirectory,
} from "./skills";
import { renderTemplate, templatesDir } from "./templates";

export type Host = "claude-code" | "cursor";

/**
 * A `start` or `reset` script already in the workspace's package.json that this
 * tool found and did not adopt, with the reason it did not. The caller prints
 * this, because "not adopted" covers two very different situations: a script
 * that has nothing to do with Publisher (nothing to say), and a script that
 * boots Publisher onto an address other than loopback, which is the moment this
 * tool detects an unauthenticated REST and MCP endpoint about to be served to
 * the network and is the one thing worth interrupting the user for.
 */
export interface DeclinedScript {
   /** The scripts entry as it is written in package.json. */
   script: string;
   /** Why it was not adopted. */
   reason:
      | "not-publisher"
      // The server is named inside something larger: a pipeline, a chain, more
      // than one command. Distinct from "different-invocation" below, which is
      // a single clean invocation that simply is not the one we would write.
      | "unrecognised-shape"
      // Shell syntax this tool does not model, so what the server is handed was
      // never established. Separate from "unrecognised-shape" because there is
      // no longer command line to describe: `--watch-env default # note` is one
      // command, and saying otherwise is a claim the code did not check.
      | "unmodelled-shell-syntax"
      // sh refuses the script outright, so it boots nothing at all. Separate
      // again because the others describe an unknown binding and this one has
      // no binding to be unknown about.
      | "unparseable"
      | "different-invocation"
      | "no-host-flag"
      | "non-loopback-host"
      | "no-init-flag";
   /** The --host value the script carries, when it carries one. */
   host?: string;
   /**
    * The value written as `--host=<value>`, which Publisher's own argument
    * parser does not accept (server.ts matches `arg === "--host"` and takes the
    * next argv entry), so the flag is dropped in silence and the bind falls back
    * to 0.0.0.0. Carried alongside `reason: "no-host-flag"`, which is what such a
    * script really is as far as the server is concerned, so a caller can say
    * which of the two shapes it saw without guessing.
    */
   joinedHostFlag?: string;
}

/**
 * Where the generated Publisher briefing ends up when the workspace already has
 * an AGENTS.md of its own. That file is left alone (it is the user's), so the
 * briefing needs somewhere else to live: a CLAUDE.md pointing at an AGENTS.md
 * that holds someone else's project notes sends the next agent session to a file
 * with no start command, no ports, no MCP reconnect, and no skills index in it.
 */
const MALLOY_AGENTS_FILE = "AGENTS.malloy.md";

/**
 * The line every generated briefing carries, in AGENTS.md and in
 * MALLOY_AGENTS_FILE alike. It is how a later run tells a briefing this tool
 * wrote from an AGENTS.md that belongs to the project.
 *
 * Deciding that from "the file exists" alone was wrong in exactly one place, and
 * it was this tool's own workspace. Scaffolding a second package into a
 * directory the first run set up left AGENTS.md skipped as if it were the
 * user's, the new briefing diverted to AGENTS.malloy.md, and the CLAUDE.md from
 * run one still pointing at AGENTS.md: a workspace whose only description of the
 * package that was just created is referenced by nothing, and whose entry point
 * describes a package that is one run out of date. The two-step flow was worse
 * still, because a setup-only run writes an AGENTS.md with no package section at
 * all, and that is the file the agent was then sent to.
 *
 * The marker has to be a whole line of the template, on one line, and generic
 * (no substituted value), so it is identical in every file this tool has ever
 * written. assertBriefingIsRecognisable checks the rendered file still holds it.
 */
const BRIEFING_MARKER =
   "This directory is a [Malloy Publisher](https://github.com/malloydata/publisher)";

export interface ScaffoldOptions {
   /** Package name; omit to only wire the agent workspace into cwd. */
   name?: string;
   /** Directory the workspace files are written to; also the server root. */
   cwd: string;
   /** A CSV, Parquet, or XLSX file to seed the package from instead of the sample. */
   dataFile?: string;
   host: Host;
   /** Overwrite existing workspace files instead of leaving them in place. */
   force: boolean;
}

export interface ScaffoldResult {
   cwd: string;
   packageCreated: boolean;
   packageName?: string;
   sourceName?: string;
   modelFile?: string;
   dataPath?: string;
   /** The --data file's own name, set only when copying it in had to rename it. */
   dataFileRenamedFrom?: string;
   /** The environment the package is registered in, which may not be "default". */
   envName: string;
   /**
    * How many packages that environment holds once this run's registration is
    * applied. Zero is the only state in which a booted server really has nothing
    * to serve and the packages endpoint really 404s, and "this run created a
    * package" is not the same question: a setup-only run over a workspace that
    * already has packages leaves a server with plenty to serve.
    */
   envPackageCount: number;
   publisherPort: number;
   mcpPort: number;
   host: Host;
   /** Workspace-relative paths that were created or updated. */
   written: string[];
   /** Workspace-relative paths that already existed and were left in place. */
   skipped: string[];
   /** Workspace-relative paths whose existing contents --force overwrote. */
   replaced: string[];
   /**
    * The file this run's Publisher briefing was written to: "AGENTS.md", or
    * MALLOY_AGENTS_FILE when the workspace already had an AGENTS.md of its own.
    */
   agentsFile: string;
   configExtended: boolean;
   /**
    * True when a package was created into a workspace a Publisher has already
    * booted in, so `npm start` will not mount it and only the --init boot will.
    * Measured off publisher_data/ and publisher.db, which only a server writes.
    *
    * This is deliberately not `configExtended`. That flag means "this run wrote a
    * new entry into a pre-existing config", which is false whenever the name was
    * already registered (a --force re-run, or a second run of the same name), and
    * the persisted package list still wins on those runs exactly as it does on a
    * fresh registration. Keying the reset advice off the write rather than off
    * the state is how a --force re-run came to promise watch mode over a package
    * the server would never mount.
    *
    * It is not "a publisher.config.json is here" either. That file is this
    * tool's own footprint (a setup-only run writes one), and reading it as proof
    * of a boot is how a workspace with nothing persisted in it was told to run
    * the --init boot and warned that it would drop materialized tables.
    */
   needsReset: boolean;
   /**
    * True when `npm start` boots Publisher on a loopback address: our own start
    * script, or one already in package.json that does the same thing. A Publisher
    * script that binds anything else does not count, because everything this tool
    * says about the endpoint being private would then be wrong.
    */
   hasStartScript: boolean;
   /** The command that boots Publisher, for when `npm start` is not available. */
   startCommand: string;
   /**
    * Why this directory's package.json could not be read, when it is there and
    * could not be. Set only on a run without --force, which leaves the manifest
    * alone; --force aborts the whole run on the same three conditions.
    *
    * The caller needs it because "no start script" and "no readable manifest to
    * look for one in" are different facts, and only the first of them is
    * something this tool has established.
    */
   packageJsonProblem?: string;
   /** The `start` script that was there and was not adopted, and why. */
   declinedStartScript?: DeclinedScript;
   /** True when `npm run reset` will boot that same loopback server with --init. */
   hasResetScript: boolean;
   /** The same boot, told to rebuild its state from publisher.config.json. */
   resetCommand: string;
   /** The `reset` script that was there and was not adopted, and why. */
   declinedResetScript?: DeclinedScript;
   /** The host's MCP config, workspace-relative. */
   mcpConfigPath: string;
   /** True when that file now holds a `malloy` server pointing at this endpoint. */
   mcpWired: boolean;
   /** True when the `malloy` server was added to a file that was already there. */
   mcpMerged: boolean;
   /**
    * How many servers other than `malloy` that file already held. Zero for a file
    * this run created, and zero for an existing `{}` or `{"mcpServers":{}}`, which
    * is why "alongside the servers already in it" cannot be said from mcpMerged.
    */
   mcpOtherServers: number;
   /**
    * True when a `malloy` server was already in that file and pointed somewhere
    * else, so this run replaced it rather than adding one. AGENTS.md creates that
    * state itself when it tells the reader to edit the url after moving ports.
    */
   mcpEntryReplaced: boolean;
   /** Why that file could not be merged into, when it could not be. */
   mcpConfigProblem?: string;
   /** The JSON to paste when we could not write the MCP config ourselves. */
   mcpPasteBlock?: string;
   skillsInstalled: number;
}

const ENV_NAME = "default";
const PUBLISHER_PORT = 4000;
const MCP_PORT = 4040;

/**
 * The ports AGENTS.md tells a reader to move to when 4000 and 4040 are already
 * held by another workspace's server. Nothing binds them here; they exist so the
 * command in that file is a real command rather than a sentence about one.
 */
const ALT_PUBLISHER_PORT = PUBLISHER_PORT + 100;
const ALT_MCP_PORT = MCP_PORT + 100;

/**
 * Publisher itself defaults to 0.0.0.0, which suits a deployed server. This
 * scaffold is a laptop workspace, and neither the REST API nor the MCP endpoint
 * has any authentication in front of it, so that default hands every machine on
 * the network a read of the user's data. Both listeners take this one flag. To
 * expose it deliberately, change --host in the start script to 0.0.0.0 (or a
 * specific interface) and put something that authenticates in front of it.
 */
const BIND_HOST = "127.0.0.1";

/**
 * The server version every generated workspace boots.
 *
 * `npx -y @malloy-publisher/server` with no version re-resolves `latest` on
 * every start, forever, and there is no lockfile behind it: a workspace
 * generated months ago boots whatever was published this morning. That is a
 * silent behaviour change on someone else's machine, and the flag it would be
 * felt through first is `--host`. The server accepts an unknown flag without a
 * word and falls back to binding 0.0.0.0, so the day a release renames or drops
 * `--host`, every generated workspace starts serving an unauthenticated REST API
 * and MCP endpoint on every interface, while its own package.json, AGENTS.md and
 * README all still say 127.0.0.1.
 *
 * Exact, not a range: the server is on 0.0.x, where `^` already means exact and
 * `~` spans every version there is, so a range pins nothing. Bump this when
 * releasing create-malloy-package (npm view @malloy-publisher/server dist-tags),
 * and confirm the new version still honours --host. A generated workspace can
 * move itself off it by editing the scripts in its own package.json.
 */
export const SERVER_VERSION = "0.0.231";

function startCommandFor(envName: string): string {
   return (
      `npx -y @malloy-publisher/server@${SERVER_VERSION} --server_root . ` +
      `--config ./publisher.config.json --host ${BIND_HOST} ` +
      `--watch-env ${envName}`
   );
}

/**
 * The same boot with --init, which rebuilds the server's persisted state from
 * publisher.config.json. Publisher reads that file only when publisher_data/
 * holds no database yet; from the second boot on, the persisted state wins. So a
 * package added to the config later is registered, reported, and never served,
 * with nothing in the log to say why. --init is deliberately not in `start`,
 * because it also clears runtime state on every boot.
 */
function resetCommandFor(envName: string): string {
   return `${startCommandFor(envName)} --init`;
}

/**
 * The characters DuckDB accepts in an unquoted file path (DUCKDB_FILE_PATH_RE in
 * malloy's dialect/duckdb/table-path-parser.ts). Publisher absolutizes the model's
 * relative `duckdb.table('data/sales.csv')` against the package directory before
 * that check runs, so one space or accent anywhere in the workspace path makes
 * every model in it fail to load. The server still reports serving and puts the
 * reason in its log, so the user's only symptom is an empty package list.
 */
const DUCKDB_SAFE_PATH_CHAR = /[A-Za-z0-9._~:/?#@!$&*+,=%-]/;

/**
 * Names the workspace itself uses. A package on one of these either crashes
 * (mkdir over publisher.config.json fails with EISDIR, after the directory is
 * made) or wins the name and quietly breaks the workspace (a package directory
 * called publisher_data shadows the server's data directory). All of them printed
 * green checks before.
 *
 * The agent-briefing names are taken from the constants that write them rather
 * than restated here, because restating them is how MALLOY_AGENTS_FILE came to
 * be missing: scaffolding a package called AGENTS.malloy.md into a directory
 * with an AGENTS.md of its own made the package directory first and then died on
 * EISDIR when the diverted briefing was written to the same path, leaving a
 * half-written workspace behind.
 */
const RESERVED_PACKAGE_NAMES = new Set(
   [
      "AGENTS.md",
      MALLOY_AGENTS_FILE,
      "CLAUDE.md",
      "node_modules",
      "package.json",
      "package-lock.json",
      "publisher.config.json",
      "publisher.db",
      "publisher_data",
   ].map((name) => name.toLowerCase()),
);

/** Scaffold a package (if named) and wire the agent workspace into cwd. */
export function scaffold(options: ScaffoldOptions): ScaffoldResult {
   // Everything below writes files, so the reasons a finished scaffold could not
   // work go first.
   assertServablePath(options.cwd);
   assertSkillsAvailable();
   if (options.name === undefined && options.dataFile !== undefined) {
      throw new ScaffoldError(
         `--data seeds a new package, so it needs a package name: ` +
            `create-malloy-package <name> --data ${options.dataFile}`,
      );
   }
   if (options.name !== undefined) {
      validatePackageName(options.name);
      assertNotReservedName(options.name);
   }
   const mcpConfigPath = mcpConfigPathFor(options.host);
   assertWorkspacePathsContained(options, mcpConfigPath);
   // Reading it here, and discarding it, is what makes --force able to promise it
   // keeps the manifest: an unusable one aborts before any file is written.
   if (options.force) {
      readMergeablePackageJson(options.cwd);
   }

   const result: ScaffoldResult = {
      cwd: options.cwd,
      packageCreated: false,
      envName: ENV_NAME,
      envPackageCount: 0,
      publisherPort: PUBLISHER_PORT,
      mcpPort: MCP_PORT,
      host: options.host,
      written: [],
      skipped: [],
      replaced: [],
      agentsFile: "AGENTS.md",
      configExtended: false,
      needsReset: false,
      hasStartScript: false,
      startCommand: startCommandFor(ENV_NAME),
      hasResetScript: false,
      resetCommand: resetCommandFor(ENV_NAME),
      mcpConfigPath,
      mcpWired: false,
      mcpMerged: false,
      mcpOtherServers: 0,
      mcpEntryReplaced: false,
      skillsInstalled: 0,
   };

   const configPath = path.join(options.cwd, "publisher.config.json");
   const configExists = fs.existsSync(configPath);

   // Load and validate an existing config up front, before anything is written to
   // disk, so a malformed or frozen config or a duplicate name aborts cleanly
   // rather than leaving an orphaned package directory behind.
   const config: PublisherConfig = configExists
      ? readConfig(configPath)
      : defaultConfig(ENV_NAME);
   // Before anything is written, and before that name reaches a command line.
   // The raw entry, not resolveEnvironmentName's answer: that one substitutes
   // ENV_NAME for a `name` which is missing or null, while addPackage goes on to
   // report the raw value, which is how "--watch-env undefined" got written. An
   // empty environments array has no entry to check, and the name then written
   // is ENV_NAME, which is this tool's own.
   const targetEnv = targetEnvironment(config, ENV_NAME);
   if (targetEnv !== undefined) {
      validateEnvironmentName(targetEnv.name);
   }
   result.envName = resolveEnvironmentName(config, ENV_NAME);

   let packageEntry: PackageEntry | undefined;
   if (options.name !== undefined) {
      packageEntry = { name: options.name, location: `./${options.name}` };
      if (configExists) {
         assertCanAddPackage(config, ENV_NAME, packageEntry);
      }
      createPackage(options, result);
   }

   wireWorkspace(
      options,
      config,
      configExists,
      configPath,
      packageEntry,
      result,
   );
   // Read off the config as it now stands, so it counts the package this run
   // registered and the ones that were already there alike.
   result.envPackageCount = environmentPackageCount(config, ENV_NAME);
   return result;
}

/** Create the package directory: publisher.json, the sample data, the model. */
function createPackage(options: ScaffoldOptions, result: ScaffoldResult): void {
   const name = options.name as string;

   // Validate the data file before creating anything, so a bad --data leaves no
   // half-written package directory behind.
   if (options.dataFile !== undefined) {
      validateDataFile(options.dataFile);
   }

   const sourceName = toMalloyIdentifier(name);
   const modelFile = `${sourceName}.malloy`;
   const packageDir = path.join(options.cwd, name);

   const packageDirExists = fs.existsSync(packageDir);
   // --force overwrites a directory, but it cannot turn a file into one, so
   // recommending it here would send the user into an ENOTDIR from mkdir. Check
   // the type before offering the remedy.
   if (packageDirExists && !fs.lstatSync(packageDir).isDirectory()) {
      throw new ScaffoldError(
         `"${name}" already exists here and is not a directory, so the package ` +
            `cannot be created there. --force does not help: it overwrites files ` +
            `inside a package directory, it does not replace something that is ` +
            `in the way.\n\n` +
            `Choose another name, or move or remove "${name}" first.`,
      );
   }
   if (packageDirExists && !options.force) {
      throw new ScaffoldError(
         `Directory "${name}" already exists. Choose another name, or re-run ` +
            `with --force.\n\n` +
            forceDescription(name, modelFile, options.host) +
            `\n\n` +
            // A --force typed after `npm create` never gets here, so the error
            // that recommends it has to say so, or the user is in a loop.
            `If you passed --force to \`npm create\`, npm read it as one of its ` +
            `own settings and it never reached this tool. Options need a \`--\` ` +
            `in front of them there:\n` +
            `  npm create malloy-package ${name} -- --force`,
      );
   }

   const dataDir = path.join(packageDir, "data");
   assertWithinWorkspace(dataDir, options.cwd, `${name}/data`);
   fs.mkdirSync(dataDir, { recursive: true });

   writeFile(
      path.join(packageDir, "publisher.json"),
      JSON.stringify({ name }, null, 2) + "\n",
      options.cwd,
   );

   let dataPath: string;
   if (options.dataFile !== undefined) {
      const sourceBase = path.basename(options.dataFile);
      const destBase = sanitizeFileName(sourceBase);
      copyFile(
         options.dataFile,
         path.join(dataDir, destBase),
         options.cwd,
         `${name}/data/${destBase}`,
      );
      dataPath = `data/${destBase}`;
      if (destBase !== sourceBase) {
         // The copy is what the model reads, so the name in the package is the
         // one the user has to look for. Silently renaming it hid their file.
         result.dataFileRenamedFrom = sourceBase;
      }
      writeFile(
         path.join(packageDir, modelFile),
         renderTemplate("model.custom.malloy", { sourceName, dataPath }),
         options.cwd,
      );
   } else {
      copyFile(
         path.join(templatesDir, "sales.csv"),
         path.join(dataDir, "sales.csv"),
         options.cwd,
         `${name}/data/sales.csv`,
      );
      dataPath = "data/sales.csv";
      writeFile(
         path.join(packageDir, modelFile),
         renderTemplate("model.default.malloy", { sourceName }),
         options.cwd,
      );
   }

   result.packageCreated = true;
   result.packageName = name;
   result.sourceName = sourceName;
   result.modelFile = modelFile;
   result.dataPath = dataPath;
   result.written.push(`${name}/`);
   if (packageDirExists) {
      result.replaced.push(`${name}/`);
   }
}

/**
 * What --force really does, file by file. It used to be summarized as
 * "overwrites package.json and the MCP config", which is not what the code does:
 * both of those are merged, and a user who believed the summary would either
 * lose nothing and be surprised, or keep a backup they never needed.
 */
function forceDescription(name: string, modelFile: string, host: Host): string {
   const agentFiles =
      host === "cursor" ? "AGENTS.md" : "AGENTS.md and CLAUDE.md";
   const mcpPath = mcpConfigPathFor(host);
   return (
      `--force does not empty the directory. It rewrites ${name}/publisher.json, ` +
      `${name}/${modelFile} and the data file it copies into ${name}/data/, and ` +
      `leaves anything else in there alone. It also refreshes .claude/skills/ ` +
      `from the bundled copies, as every run does. Outside the package it ` +
      `replaces ${agentFiles}; in package.json it sets only the "start" and ` +
      `"reset" scripts and keeps the rest of the file as it is; in ${mcpPath} it ` +
      `sets only the "malloy" server and keeps the others. publisher.config.json ` +
      `is extended, never rewritten, and .gitignore only gains the lines it is ` +
      `missing, with or without --force. None of those merges is a rewrite even ` +
      `when the file cannot be read: a package.json that is not valid JSON aborts ` +
      `the run before anything is written, and an unreadable ${mcpPath} or ` +
      `.gitignore is left alone and reported. If this directory has an AGENTS.md ` +
      `of its own, --force is not what you want for it: a run without --force ` +
      `keeps that file and writes the Publisher briefing to ${MALLOY_AGENTS_FILE} ` +
      `beside it instead, regenerating that one on every run.`
   );
}

function mcpConfigPathFor(host: Host): string {
   return host === "cursor" ? ".cursor/mcp.json" : ".mcp.json";
}

/** Write the config, agent files, and skills into cwd. */
function wireWorkspace(
   options: ScaffoldOptions,
   config: PublisherConfig,
   configExists: boolean,
   configPath: string,
   packageEntry: PackageEntry | undefined,
   result: ScaffoldResult,
): void {
   const { cwd } = options;

   // publisher.config.json: register the package in the config loaded up front
   // (a fresh one, or the validated existing one), then write. Re-registering the
   // same package (a --force re-run) leaves an existing config untouched.
   if (packageEntry) {
      const registration = addPackage(config, ENV_NAME, packageEntry);
      result.envName = registration.envName;
      if (!configExists || registration.added) {
         writeConfig(configPath, config);
         result.configExtended = configExists;
         result.written.push(
            configExists
               ? "publisher.config.json (extended)"
               : "publisher.config.json",
         );
      } else {
         result.skipped.push("publisher.config.json (already registered)");
      }
   } else if (!configExists) {
      writeConfig(configPath, config);
      result.written.push("publisher.config.json");
   } else {
      result.skipped.push("publisher.config.json");
   }

   // Whether the package this run created will actually be mounted by a plain
   // boot. Publisher reads publisher.config.json only while publisher_data/ holds
   // no database; after that its own persisted package list wins.
   //
   // So the question is not "did we write a new entry into the config"
   // (registration.added, which is false for a name already registered), and it
   // is not "was there a config here" either: this tool writes one itself on a
   // setup-only run, so `configExists` counted its own footprint as a server's
   // and told a workspace no server has ever read that a plain start would not
   // serve the new package. Everything that comes with that answer, the
   // persisted list that wins, the --init boot, the warning that it drops
   // materialized tables and saved themes, is false of a workspace with nothing
   // persisted in it. Both checks below are the server's own evidence of a boot.
   result.needsReset =
      result.packageCreated &&
      (fs.existsSync(path.join(cwd, "publisher_data")) ||
         fs.existsSync(path.join(cwd, "publisher.db")));

   // Every URL, the watch flag, and the agent instructions have to name the
   // environment the package actually landed in.
   result.startCommand = startCommandFor(result.envName);
   result.resetCommand = resetCommandFor(result.envName);

   writeWorkspacePackageJson(cwd, options, result);

   // `npm start` only works if the effective package.json carries a Publisher
   // start script (ours). In an existing project we leave package.json alone, so
   // the caller, and AGENTS.md, fall back to the explicit start command. When
   // there is a script and it was not adopted, keep why: "no Publisher start
   // script" is the wrong sentence over a script that boots Publisher onto the
   // LAN, and that case is the one worth telling the user about.
   const scripts = workspaceScripts(cwd);
   const declinedStart = declineReasonFor(
      scripts?.start,
      result.envName,
      false,
   );
   const declinedReset = declineReasonFor(scripts?.reset, result.envName, true);
   result.hasStartScript =
      typeof scripts?.start === "string" && declinedStart === undefined;
   result.declinedStartScript = declinedStart;
   result.hasResetScript =
      typeof scripts?.reset === "string" && declinedReset === undefined;
   result.declinedResetScript = declinedReset;

   // Before AGENTS.md, which tells the agent whether the endpoint is wired.
   writeMcpConfig(cwd, result.mcpConfigPath, result);

   // Skills are copied every run so an existing workspace picks up updates, and
   // before AGENTS.md, which states how many are there.
   installWorkspaceSkills(cwd, result);

   mergeGitignore(cwd, result);
   // Read off the config as it now stands, so the briefing names the packages
   // that were already registered here as well as the one this run added.
   writeAgentsFile(
      cwd,
      renderAgentsFile(
         result,
         options.host,
         environmentPackageNames(config, ENV_NAME),
      ),
      options,
      result,
   );
   if (options.host !== "cursor") {
      writeIfAbsent(
         cwd,
         "CLAUDE.md",
         claudeFile(result.agentsFile),
         options,
         result,
      );
   }
}

/**
 * Put the Publisher briefing somewhere a reader will actually find it.
 *
 * A workspace that already has an AGENTS.md of *its own* keeps it: it is the
 * user's file, and in an existing project it is usually a briefing for something
 * else entirely. Skipping it and stopping there is what made the CLAUDE.md
 * written in the same run false, because that file's one line points at AGENTS.md
 * for the start command, the ports, the MCP reconnect trap and the skills index,
 * none of which are in a Node project's AGENTS.md and never will be. So the
 * briefing goes to AGENTS.malloy.md instead and everything that points at it
 * points there.
 *
 * "Of its own" is read out of the file, not inferred from its existence: an
 * AGENTS.md carrying BRIEFING_MARKER is one of ours from an earlier run, and it
 * is regenerated in place. Nothing of the user's is lost by that, because the
 * briefing is rendered in full from this run's state exactly like
 * AGENTS.malloy.md and the skills, and it is reported as overwritten.
 *
 * Either file, once it is ours, is rewritten every run: it is generated in full
 * from this run's state and a stale copy is the failure this exists to prevent.
 */
function writeAgentsFile(
   cwd: string,
   content: string,
   options: ScaffoldOptions,
   result: ScaffoldResult,
): void {
   assertBriefingIsRecognisable(content);
   const relPath = "AGENTS.md";
   const target = path.join(cwd, relPath);
   const exists = fs.existsSync(target);
   const ours = exists && isGeneratedBriefing(target);
   if (!exists || ours || options.force) {
      writeFile(target, content, cwd);
      result.written.push(relPath);
      if (ours) {
         // Named as a regeneration rather than left as a bare filename: the
         // file it replaced was this tool's own output, and every run replaces
         // it. Under --force over someone else's AGENTS.md it is a real
         // overwrite, and that one is reported as one.
         result.replaced.push(`${relPath} (regenerated, as every run does)`);
      } else if (exists) {
         result.replaced.push(relPath);
      }
      // AGENTS.md holds the briefing, so a copy left in AGENTS.malloy.md by an
      // earlier run is now a second, older answer to the same question.
      if (fs.existsSync(path.join(cwd, MALLOY_AGENTS_FILE))) {
         result.skipped.push(
            `${MALLOY_AGENTS_FILE} (left from an earlier run, when AGENTS.md ` +
               `was not ours to write; AGENTS.md now holds the briefing, so ` +
               `this copy is stale and can be deleted)`,
         );
      }
      return;
   }
   result.skipped.push(relPath);
   const existed = fs.existsSync(path.join(cwd, MALLOY_AGENTS_FILE));
   writeFile(path.join(cwd, MALLOY_AGENTS_FILE), content, cwd);
   result.agentsFile = MALLOY_AGENTS_FILE;
   result.written.push(
      `${MALLOY_AGENTS_FILE} (the Publisher briefing, because this ` +
         `directory's own AGENTS.md was left alone)`,
   );
   if (existed) {
      result.replaced.push(
         `${MALLOY_AGENTS_FILE} (regenerated, as every run does)`,
      );
   }
}

/** Whether the file at target is a briefing this tool wrote. */
function isGeneratedBriefing(target: string): boolean {
   try {
      return fs.readFileSync(target, "utf8").includes(BRIEFING_MARKER);
   } catch {
      // Unreadable, or a directory. Not ours to overwrite either way.
      return false;
   }
}

/**
 * Refuse to write a briefing a later run could not recognise as one. This is a
 * bug in the template, not in the user's workspace, and it is silent by nature:
 * the run that drops the marker line still writes a perfectly good AGENTS.md,
 * and every run after it diverts to AGENTS.malloy.md and orphans the file
 * CLAUDE.md points at. Throwing here is what makes editing the template out from
 * under this a failure now rather than a support thread later.
 */
function assertBriefingIsRecognisable(content: string): void {
   if (!content.includes(BRIEFING_MARKER)) {
      throw new Error(
         `The rendered agent briefing no longer holds the line "` +
            `${BRIEFING_MARKER}". That line is how a later run recognises a ` +
            `briefing this tool wrote and regenerates it in place, so without ` +
            `it every AGENTS.md written so far would be read as the user's own ` +
            `and the briefing diverted to ${MALLOY_AGENTS_FILE}. Restore the ` +
            `line in templates/AGENTS.md, or update BRIEFING_MARKER to a line ` +
            `the template still has.`,
      );
   }
}

/**
 * Install the skills and report every way the result differs from "all of them,
 * freshly written": a refusal, a symlink left alone, and the ones whose contents
 * were replaced. The last used to be invisible, so a run that discarded a user's
 * edit to a skill printed a "Left these existing files alone" block and nothing
 * else, which reads as a promise it did not keep.
 */
function installWorkspaceSkills(cwd: string, result: ScaffoldResult): void {
   const relDir = path.join(".claude", "skills");
   const skills = installSkills(path.join(cwd, relDir), cwd);
   result.skillsInstalled = skills.installed;
   if (skills.refused) {
      result.skipped.push(
         `${relDir}${path.sep} (${skills.refused}; no skills were installed)`,
      );
   }
   for (const name of skills.skipped) {
      result.skipped.push(
         `${path.join(relDir, name)} (symlink, not written through)`,
      );
   }
   if (skills.refreshed.length > 0) {
      const count = skills.refreshed.length;
      // Says what the code does, which is not what "refreshed" is read to
      // mean. Every run deletes each skill directory and writes the bundled
      // copy back over it, so an edit to a skill file is reverted and a file
      // the user added inside one is gone. A reader took the old sentence for
      // the first of those only, and it happens with or without --force.
      result.replaced.push(
         `${relDir}${path.sep} (${count} skill${count === 1 ? "" : "s"} ` +
            `refreshed: every run deletes each skill directory and writes the ` +
            `bundled copy back, so edits to a skill are reverted and anything ` +
            `else you put inside one is removed, not just changed)`,
      );
   }
   if (skills.removed.length > 0) {
      // The paths that make the sentence above concrete: they were in a skill
      // directory, no bundled skill holds them, and nothing wrote them back.
      // Named because a user who put them there has no other way to learn
      // they are gone. printable(), because these come off the user's disk.
      const shown = skills.removed
         .slice(0, 5)
         .map((entry) => printable(path.join(relDir, entry)));
      const rest = skills.removed.length - shown.length;
      result.replaced.push(
         `${shown.join(", ")}${rest > 0 ? `, and ${rest} more` : ""} (not ` +
            `part of any bundled skill, so nothing wrote ` +
            `${shown.length === 1 && rest === 0 ? "it" : "them"} back)`,
      );
   }
}

/**
 * Refuse a workspace path DuckDB cannot read data files from. The scaffold cannot
 * work around it by quoting, because the model already single-quotes the path and
 * Publisher rewrites it to an absolute one before DuckDB sees it.
 */
function assertServablePath(cwd: string): void {
   const absolute = path.resolve(cwd);
   // Windows separators are normalized before the path reaches DuckDB, so only
   // the rest of the path has to be in the safe set.
   const candidate =
      path.sep === "\\" ? absolute.replace(/\\/g, "/") : absolute;
   for (const character of candidate) {
      if (!DUCKDB_SAFE_PATH_CHAR.test(character)) {
         throw new ScaffoldError(
            // printable(), because the path being refused is a path holding a
            // character this tool has just decided it cannot handle, and an ESC
            // or a CR in it would be sent to the terminal rather than shown.
            `This workspace cannot live in ${printable(absolute)}: DuckDB ` +
               `cannot read a ` +
               `data file under a path containing ${describeCharacter(
                  character,
               )}, so every model here would fail to load with the server ` +
               `still reporting healthy. Move to a path made of letters, ` +
               `digits, "-", "_", "." and "/" (for example ` +
               `~/malloy-workspace) and run again.`,
         );
      }
   }
}

/** Name the offending character, since several of them do not print usefully. */
function describeCharacter(character: string): string {
   const named: Record<string, string> = {
      " ": "a space",
      "'": "an apostrophe",
      '"': "a double quote",
      "\\": "a backslash",
      "\t": "a tab",
   };
   const label = named[character] ?? `"${character}"`;
   const codePoint = character.codePointAt(0) as number;
   return codePoint < 0x20 || codePoint > 0x7e
      ? `${label} (U+${codePoint.toString(16).toUpperCase().padStart(4, "0")})`
      : label;
}

/**
 * Refuse before writing anything if one of the paths we are about to write leaves
 * the workspace. fs.existsSync says false for a dangling symlink and
 * fs.writeFileSync follows one, so `ln -s /etc/something AGENTS.md` in the
 * workspace used to be enough to make this tool write through it, report the file
 * created, and leave nothing in the workspace.
 */
function assertWorkspacePathsContained(
   options: ScaffoldOptions,
   mcpConfigPath: string,
): void {
   const relatives = [
      "publisher.config.json",
      "package.json",
      ".gitignore",
      "AGENTS.md",
      // Written only when AGENTS.md is left alone, but checked always: a refusal
      // here costs nothing, and finding out mid-run costs a half-written
      // workspace.
      MALLOY_AGENTS_FILE,
      mcpConfigPath,
   ];
   if (options.host !== "cursor") {
      relatives.push("CLAUDE.md");
   }
   if (options.name !== undefined) {
      relatives.push(options.name);
   }
   for (const relative of relatives) {
      assertWithinWorkspace(
         path.join(options.cwd, relative),
         options.cwd,
         relative,
      );
   }
}

function assertWithinWorkspace(
   target: string,
   root: string,
   label: string,
): void {
   if (!isWithinDirectory(target, root)) {
      throw new ScaffoldError(
         `"${label}" in ${root} is a symlink (or sits under one) pointing ` +
            `outside the workspace, so writing it would change a file somewhere ` +
            `else on this machine. Nothing was written. Remove or repoint it, ` +
            `then run again.`,
      );
   }
}

function assertNotReservedName(name: string): void {
   if (RESERVED_PACKAGE_NAMES.has(name.toLowerCase())) {
      throw new ScaffoldError(
         `Package name "${name}" is reserved: the workspace itself uses that ` +
            `path. Choose another name.`,
      );
   }
}

/**
 * What is at cwd/package.json, as one of three answers rather than two.
 *
 * "There is no manifest" and "there is one and this tool could not read it" were
 * collapsed into a single `undefined` for the whole no---force path, and every
 * sentence downstream was written for the first of them. A directory holding
 * `{ not json`, or `[1,2,3]`, or a package.json with mode 000, therefore scaffolded
 * to exit 0 under three green checks, listed `package.json` among the files left
 * alone with no reason beside it, and then asserted "this directory's package.json
 * has no start script" about a file that had not been opened. The same three
 * conditions are named exactly under --force, and .mcp.json and .gitignore are
 * each named in the same run, which is what made this a defect rather than a
 * decision.
 */
type WorkspaceManifest =
   | { kind: "absent" }
   | { kind: "object"; pkg: Record<string, unknown> }
   /** There is a file there and it cannot be merged into. `problem` is a phrase. */
   | { kind: "unusable"; problem: string };

function readWorkspacePackageJson(cwd: string): WorkspaceManifest {
   const target = path.join(cwd, "package.json");
   if (!fs.existsSync(target)) {
      return { kind: "absent" };
   }
   let text: string;
   try {
      text = fs.readFileSync(target, "utf8");
   } catch (err) {
      return {
         kind: "unusable",
         // Every phrase here reads after both "package.json in <dir> is ..." and
         // "package.json (..., so the Publisher start script was not added)".
         problem: `unreadable: ${printable((err as Error).message)}`,
      };
   }
   let parsed: unknown;
   try {
      parsed = parseJson(text);
   } catch (err) {
      // The parse message quotes the file, so it comes off the user's disk.
      return {
         kind: "unusable",
         problem: `not valid JSON: ${printable((err as Error).message)}`,
      };
   }
   const pkg = asJsonObject(parsed);
   return pkg
      ? { kind: "object", pkg }
      : {
           kind: "unusable",
           problem: `${describeJsonValue(parsed)}, not a JSON object`,
        };
}

/** The scripts block of cwd/package.json, if there is a readable one. */
function workspaceScripts(cwd: string): Record<string, unknown> | undefined {
   const manifest = readWorkspacePackageJson(cwd);
   return manifest.kind === "object"
      ? asJsonObject(manifest.pkg.scripts)
      : undefined;
}

/**
 * Whether a scripts entry boots Publisher the way this tool documents: the
 * server, bound to a loopback address.
 *
 * Naming the server used to be the whole test, which made any pre-existing script
 * mentioning it "ours" and put it in AGENTS.md as `npm start`. AGENTS.md then told
 * the reader, in the same file, that the server binds to 127.0.0.1 and that
 * exposing it on the network means changing `--host` to 0.0.0.0, over a script
 * already running `--host 0.0.0.0`: the unauthenticated REST and MCP endpoints
 * were on the LAN and the briefing said the opposite. Publisher's own default is
 * 0.0.0.0 as well, so a script carrying no --host at all is not loopback either.
 *
 * A script that fails this test is left alone and simply not adopted: the caller
 * falls back to the explicit command, which does bind loopback, so every command
 * this tool prints or writes matches what it says about the binding.
 */
function isPublisherScript(
   script: unknown,
   envName: string,
   requireInit = false,
): boolean {
   return (
      typeof script === "string" &&
      declineReasonFor(script, envName, requireInit) === undefined
   );
}

/**
 * Why a scripts entry was not adopted, or undefined when it was (and undefined
 * when there is no entry at all: nothing was declined, so there is nothing to
 * report). requireInit is for `reset`, which is the same boot plus --init.
 *
 * The reason is carried rather than collapsed to a boolean because the caller
 * prints it, and "this directory's package.json has no Publisher start script"
 * was printed over a package.json whose start script boots Publisher on
 * 0.0.0.0. That is not the same fact, and it is the one moment this tool sees an
 * unauthenticated REST and MCP endpoint about to be served to the network.
 */
function declineReasonFor(
   script: unknown,
   envName: string,
   requireInit = false,
): DeclinedScript | undefined {
   if (typeof script !== "string") {
      return undefined;
   }
   if (!script.includes("@malloy-publisher/server")) {
      return { script, reason: "not-publisher" };
   }
   // Ordered so each reason describes what was actually detected. The
   // unmodelled-syntax test must come first: hasUnterminatedQuote reads a
   // backslash-escaped quote as opening a region, so it reports `\'` as
   // unterminated where sh is perfectly happy.
   if (UNMODELLED_SHELL_SYNTAX.test(script)) {
      return { script, reason: "unmodelled-shell-syntax" };
   }
   if (hasUnterminatedQuote(script)) {
      return { script, reason: "unparseable" };
   }
   if (!isSingleServerInvocation(script)) {
      return { script, reason: "unrecognised-shape" };
   }
   const host = hostFlagOf(script);
   if (host === undefined) {
      // Publisher's own default is 0.0.0.0, so no flag is not loopback. A
      // `--host=<value>` counts as no flag here, because that is what it is to
      // the server: it matches none of its argument branches, is dropped without
      // a word, and the bind falls back to 0.0.0.0.
      const joined = joinedHostFlagOf(script);
      return joined === undefined
         ? { script, reason: "no-host-flag" }
         : { script, reason: "no-host-flag", joinedHostFlag: joined };
   }
   if (!isLoopbackHost(host)) {
      return { script, reason: "non-loopback-host", host };
   }
   if (requireInit && !script.includes("--init")) {
      return { script, reason: "no-init-flag", host };
   }
   // Everything above establishes that the script boots the server on a loopback
   // address. Nothing above establishes *which* server: the ports it listens on,
   // the workspace it mounts, or whether it watches anything. Those are the three
   // facts the rest of this run then states as fact, unconditionally, over the
   // adopted script: AGENTS.md says "http://localhost:4000 ... MCP endpoint on
   // http://localhost:4040/mcp, and mounts this workspace's packages in watch
   // mode", .mcp.json is written with that url, and the CLI prints both curls
   // against 4000. A script carrying `--port 4100 --mcp_port 4140` (which this
   // tool's own AGENTS.md tells the reader to add, on the documented
   // second-package path) satisfies every check above, so all of it was false and
   // every command printed answered connection-refused, or worse, answered from
   // some other server already holding 4000.
   //
   // So the last check is the only one that keeps those claims honest: adopt a
   // script only when it is a script this tool could have written itself.
   // Anything else is left exactly as it is and the caller falls back to the
   // explicit command, which does boot this workspace on these ports.
   if (!matchesGeneratedInvocation(script, envName, requireInit)) {
      return { script, reason: "different-invocation" };
   }
   return undefined;
}

/**
 * The flags a generated boot carries, and how the value of each is allowed to
 * differ from the one this tool would have written.
 *
 * A flag not in this table is a flag nothing here has read, and the reason this
 * check exists at all is that `--port` was exactly that: read by the server,
 * unread here, and contradicted by every port this tool went on to print.
 */
const GENERATED_FLAGS: Record<
   string,
   (value: string, envName: string) => boolean
> = {
   // Relative to the workspace root, which is where npm runs the script from.
   // Any other root mounts someone else's packages under this workspace's name.
   "--server_root": (value) => value === "." || value === "./",
   "--config": (value) =>
      value === "./publisher.config.json" || value === "publisher.config.json",
   // Already checked above, and checked again here so this function stands on
   // its own: an adopted script binds an address this tool describes correctly.
   "--host": (value) => isLoopbackHost(value),
   // The environment the package this run touched actually landed in. Watching
   // another one leaves AGENTS.md's whole recompile-on-save section false.
   "--watch-env": (value, envName) => value === envName,
};

/** Flags that take no value in a generated boot. */
const GENERATED_BOOLEAN_FLAGS = new Set(["--init"]);

/**
 * Whether the script is, token for token, a boot this tool could have written:
 * startCommandFor(envName), or resetCommandFor(envName) when requireInit.
 *
 * Deliberately whole-command rather than a scan for the flags that would be
 * wrong. A scan can only reject the misconfigurations someone thought of, and
 * the server takes flags this file has never heard of (a future --base-path
 * would move every URL printed here without matching any pattern). An unknown
 * flag therefore declines. Order is not significant, since none of these flags
 * interact, but presence is: each of the four must be there, and --init exactly
 * when the caller is looking at `reset`. A flag written twice is read the way
 * the server reads it, last one wins, which is how hostFlagOf above reads --host
 * as well.
 *
 * The version on the server spec is the one thing free to differ: a generated
 * workspace pins SERVER_VERSION and is told, in its own package.json comment
 * trail and README, that it can move itself off that pin.
 */
function matchesGeneratedInvocation(
   script: string,
   envName: string,
   requireInit: boolean,
): boolean {
   const tokens = script.trim().split(/\s+/).filter(Boolean);
   const index = tokens.findIndex((token) =>
      unquote(token).startsWith("@malloy-publisher/server"),
   );
   if (index === -1) {
      return false;
   }
   const values = new Map<string, string>();
   let init = false;
   const rest = tokens.slice(index + 1).map(unquote);
   for (let i = 0; i < rest.length; i++) {
      const flag = rest[i];
      if (GENERATED_BOOLEAN_FLAGS.has(flag)) {
         init = init || flag === "--init";
         continue;
      }
      const accepts = GENERATED_FLAGS[flag];
      if (accepts === undefined) {
         // A joined `--flag=value`, a positional argument, and a flag this tool
         // does not write all land here.
         return false;
      }
      const value = rest[i + 1];
      if (value === undefined) {
         return false;
      }
      values.set(flag, value);
      i++;
   }
   for (const [flag, accepts] of Object.entries(GENERATED_FLAGS)) {
      const value = values.get(flag);
      if (value === undefined || !accepts(value, envName)) {
         return false;
      }
   }
   return init === requireInit;
}

function unquote(token: string): string {
   return token.replace(/^["']|["']$/g, "");
}

/**
 * Characters that make a scripts entry more than the one command it looks like.
 * npm runs the entry through a shell, so `&&`, `;`, `|`, `&`, a newline, a
 * backtick, `$(...)` and a redirect all chain, substitute, or background further
 * commands that nothing here has looked at.
 *
 * `$` and the parentheses are in the set on their own: `--host $HOST` is not a
 * bind address this tool can read, and a script it cannot read is a script it
 * must not describe.
 */
// Two reject sets rather than one, because the decline message has to say what
// was actually found. These genuinely put the server inside something larger:
// a chain, a pipeline, a redirect.
const SHELL_CHAINING_CHARACTERS = /[&|;<>\n\r]/;

// These do not. Each is shell syntax shellTokens does not model, in a command
// that may well be a single Publisher boot, and each has been caught reporting a
// bind address the server never receives: sh reads
// `--host 0.0.0.0 # --host 127.0.0.1` as a comment and binds 0.0.0.0 while the
// text says loopback, and `\'` emits a literal quote where a quote-only
// tokenizer sees a region opening and swallows the rest of the line.
const UNMODELLED_SHELL_SYNTAX = /[`$()#\\]/;

/**
 * Command words a Publisher boot can legitimately be reached through. Anything
 * else in front of the server package (a wrapper script, `node <path>`, another
 * binary entirely) is a command this tool has not read, so the script is left
 * alone rather than adopted and described.
 */
const SCRIPT_RUNNERS = new Set([
   "npx",
   "bunx",
   "pnpx",
   "pnpm",
   "npm",
   "yarn",
   "bun",
   "dlx",
   "exec",
   "run",
]);

/**
 * The runner flags a Publisher boot can carry in front of the server spec.
 *
 * An allowlist, not `token.startsWith("-")`. Every runner here takes flags that
 * choose what actually runs, and in their joined form those are a single token
 * that begins with a dash: `--package=evil` and `-p=evil` pick a different
 * package, `--call=evil` replaces the command outright, and
 * `--node-options=--require=/tmp/evil.js` preloads a file into the real server —
 * that last one carries no space, so tokenizing never splits it, `=` is not in
 * SHELL_CONTROL_CHARACTERS, and the boot that follows is a genuine Publisher on
 * a loopback address, so every claim this tool goes on to make stays true while
 * the script it adopted is not the one it read.
 *
 * The separated forms were already refused, because the value lands as a bare
 * token that is neither a flag nor a runner. Joining them was the whole bypass.
 *
 * So this mirrors what matchesGeneratedInvocation does on the other side of the
 * spec: an unknown flag declines. Only boolean flags belong here — anything that
 * takes a value can carry one in its joined form, which is the bug. Nothing is
 * lost by being strict: a declined script is left exactly as it is and the
 * caller falls back to the explicit command.
 */
const RUNNER_BOOLEAN_FLAGS = new Set([
   "-y",
   "--yes",
   "-q",
   "--quiet",
   "--silent",
]);

/**
 * Whether the whole scripts entry is one invocation of the Publisher server and
 * nothing else.
 *
 * The check used to be `script.includes("@malloy-publisher/server")`, over a
 * string that is handed to a shell. That made
 * `npx @malloy-publisher/server --host 127.0.0.1 && curl -s http://x | sh` a
 * script this tool adopted, printed as `npm start`, wrote into AGENTS.md as the
 * command an agent runs, and described as "Publisher (web UI + MCP) with the
 * package in watch mode" on a loopback address. Two of those three claims were
 * about a command the code had never parsed, and the bind claim was defeated by
 * a second command in the same line (`... --host 127.0.0.1 & nc -l 0.0.0.0 4000`
 * carries exactly one --host).
 *
 * Nothing is lost by being strict: a script that fails this is left exactly as it
 * is, and the caller falls back to the explicit command, which does boot
 * Publisher on 127.0.0.1.
 */
function isSingleServerInvocation(script: string): boolean {
   if (SHELL_CHAINING_CHARACTERS.test(script)) {
      return false;
   }
   const tokens = script.trim().split(/\s+/).filter(Boolean);
   const index = tokens.findIndex((token) =>
      token.includes("@malloy-publisher/server"),
   );
   if (index === -1) {
      return false;
   }
   // The server has to be the thing being run, not an argument to something
   // else: bare, or with a version tag npx resolves.
   const command = tokens[index].replace(/^["']|["']$/g, "");
   if (command !== "@malloy-publisher/server") {
      if (!command.startsWith("@malloy-publisher/server@")) {
         return false;
      }
      // What follows the `@` has to be something npm resolves out of the
      // registry. The allowance was added so `@0.0.228` would pass, and as
      // written it admitted every other spec form npx takes as well:
      // `@file:/tmp/evil`, `@git+ssh://...`, a tarball URL. Each of those runs
      // code that is not Publisher, under a script this tool then adopts and
      // describes as booting Publisher on a loopback address.
      const spec = command.slice("@malloy-publisher/server@".length);
      if (!/^[A-Za-z0-9.-]+$/.test(spec)) {
         return false;
      }
   }
   // Everything in front of it is a package runner or one of the boolean flags
   // that cannot redirect what runs.
   return tokens
      .slice(0, index)
      .every(
         (token) =>
            RUNNER_BOOLEAN_FLAGS.has(token) || SCRIPT_RUNNERS.has(token),
      );
}

/**
 * The value of the last `--host <value>` in a command line, if it carries one.
 *
 * Whitespace only, because that is the whole of what Publisher accepts:
 * `packages/server/src/server.ts` walks argv with
 * `} else if (arg === "--host" && args[i + 1]) {`, a strict equality against a
 * separate argv entry. `--host=127.0.0.1` matches no branch in that chain, and
 * the chain has no unknown-flag error, so the flag is dropped in silence and
 * PUBLISHER_HOST falls back to "0.0.0.0" for both the REST and the MCP listener.
 *
 * Accepting the `=` form here was worse than useless: it made the one shape that
 * looks private and is not the one shape this tool called safest, suppressing the
 * exposure warning that a script with no --host at all correctly gets.
 */
/**
 * The server flags that consume the following argv entry, mirroring the
 * `arg === "--x" && args[i + 1]` chain in `packages/server/src/server.ts`.
 * `--init` and `--help` take no value and so are deliberately absent.
 */
export const SERVER_VALUE_FLAGS = new Set([
   "--port",
   "--host",
   "--server_root",
   "--config",
   "--mcp_port",
   "--shutdown_drain_duration_seconds",
   "--shutdown_graceful_close_timeout_seconds",
   "--watch-env",
]);

/** True when a quote is left open, which sh refuses to run at all. */
function hasUnterminatedQuote(script: string): boolean {
   let quote: string | undefined;
   for (const char of script) {
      if (quote !== undefined) {
         if (char === quote) quote = undefined;
         continue;
      }
      if (char === '"' || char === "'") quote = char;
   }
   return quote !== undefined;
}

/**
 * Split a command the way `sh -c` would, which is what npm hands the script to.
 * Quoting is all this models, and SHELL_CONTROL_CHARACTERS is what makes that
 * enough: everything else with shell meaning is declined before reaching here.
 * `#` and `\` are in that reject set for exactly this reason, having each been
 * caught reading a bind address the server never receives.
 *
 * Splitting on whitespace instead lets a quoted flag name (`"--host"`) read as a
 * different token than the one the server is actually handed.
 *
 * `~` and `*` are the known gap: sh expands them and this does not. Neither can
 * hide an exposed bind, because an unexpanded value is not a loopback literal
 * and so warns, and a glob's result depends on a directory this cannot see.
 */
export function shellTokens(script: string): string[] {
   const tokens: string[] = [];
   let current = "";
   let quote: string | undefined;
   let open = false;
   for (const char of script) {
      if (quote !== undefined) {
         if (char === quote) quote = undefined;
         else current += char;
         continue;
      }
      if (char === '"' || char === "'") {
         quote = char;
         // A quoted empty string is still an argv entry, and a falsy one, which
         // is the whole reason the server drops the flag in front of it.
         open = true;
         continue;
      }
      if (/\s/.test(char)) {
         if (open) tokens.push(current);
         current = "";
         open = false;
         continue;
      }
      current += char;
      open = true;
   }
   if (open) tokens.push(current);
   return tokens;
}

function hostFlagOf(script: string): string | undefined {
   // A positional walk rather than a scan, because that is what the server
   // does. Every value-taking flag consumes the NEXT token, so a flag whose own
   // value is missing swallows `--host` as that value and the server never sees
   // a host at all. A scan reads `--server_root --host 127.0.0.1` as loopback;
   // the server binds 0.0.0.0. This is what decides whether the exposure
   // warning is suppressed, so it has to agree with the server rather than with
   // the text.
   const tokens = shellTokens(script);
   // Start past the package spec: a value flag among the runner's own arguments
   // belongs to npx, not to Publisher.
   const spec = tokens.findIndex((token) =>
      token.includes("@malloy-publisher/server"),
   );
   if (spec === -1) {
      return undefined;
   }
   let value: string | undefined;
   for (let i = spec + 1; i < tokens.length; i++) {
      const token = tokens[i];
      const next = tokens[i + 1];
      // Mirrors `arg === "--x" && args[i + 1]`: a value flag followed by
      // nothing, or by the one falsy argv entry there is, matches no branch and
      // so consumes nothing.
      if (!SERVER_VALUE_FLAGS.has(token) || !next) {
         continue;
      }
      if (token === "--host") {
         // Last one wins, as it does on a real command line.
         value = next;
      }
      i++;
   }
   return value;
}

/** The value of the last `--host=<value>`, the form the server ignores. */
function joinedHostFlagOf(script: string): string | undefined {
   let value: string | undefined;
   for (const match of script.matchAll(
      /(?:^|\s)--host=("[^"]*"|'[^']*'|\S*)/g,
   )) {
      value = match[1].replace(/^["']|["']$/g, "");
   }
   return value;
}

function isLoopbackHost(host: string | undefined): boolean {
   if (host === undefined) {
      return false;
   }
   const bare = host.replace(/^\[|\]$/g, "").toLowerCase();
   return (
      bare === "localhost" ||
      bare === "::1" ||
      /^127\.\d{1,3}\.\d{1,3}\.\d{1,3}$/.test(bare)
   );
}

/**
 * Read an existing cwd/package.json that --force is about to merge into, and
 * refuse the whole run if it cannot be merged into. Returns undefined when there
 * is no manifest yet.
 *
 * Treating a parse failure as "nothing worth keeping" and writing a stub over it
 * was silent data loss, and it fired on a file the rest of the toolchain reads
 * fine: a UTF-8 BOM. That specific case now parses (parseJson strips it), and
 * anything still unreadable stops the run instead, because --force's own
 * description promises the file is kept.
 */
function readMergeablePackageJson(
   cwd: string,
): Record<string, unknown> | undefined {
   const manifest = readWorkspacePackageJson(cwd);
   if (manifest.kind === "absent") {
      return undefined;
   }
   if (manifest.kind === "unusable") {
      throw new ScaffoldError(
         `package.json in ${cwd} is ${manifest.problem}. --force adds the ` +
            `Publisher scripts to an existing manifest and keeps everything ` +
            `else, which needs the file to be a JSON object this tool can read. ` +
            `Nothing was written. Fix or move it, then run again.`,
      );
   }
   return manifest.pkg;
}

/**
 * Write our workspace manifest, or under --force add our scripts to the one
 * already there. Replacing an existing manifest wholesale would drop the user's
 * dependencies and scripts, and --force is asked for often enough (our own
 * "directory already exists" error suggests it) that it must not be a data-loss
 * button for a file we did not write.
 */
function writeWorkspacePackageJson(
   cwd: string,
   options: ScaffoldOptions,
   result: ScaffoldResult,
): void {
   const relPath = "package.json";
   const target = path.join(cwd, relPath);
   const manifest = readWorkspacePackageJson(cwd);
   if (manifest.kind === "absent") {
      writeFile(target, workspacePackageJson(cwd, result), cwd);
      result.written.push(relPath);
      return;
   }
   if (manifest.kind === "unusable") {
      // Under --force the run already aborted in scaffold(), before anything was
      // written, so only the default path reaches this. Say what was found, the
      // way .mcp.json and .gitignore already do: the alternative was a bare
      // `package.json` in the left-alone list followed by an assertion about the
      // contents of a file this tool could not open.
      result.packageJsonProblem = manifest.problem;
      result.skipped.push(
         `${relPath} (${manifest.problem}, so the Publisher start script was ` +
            `not added)`,
      );
      return;
   }
   if (!options.force) {
      result.skipped.push(relPath);
      return;
   }

   const pkg = manifest.pkg as { scripts?: Record<string, unknown> };
   const scripts = asJsonObject(pkg.scripts) ?? {};
   pkg.scripts = {
      ...scripts,
      start: result.startCommand,
      reset: result.resetCommand,
   };
   writeFile(target, JSON.stringify(pkg, null, 2) + "\n", cwd);
   result.written.push(`${relPath} (start and reset scripts set)`);
   for (const name of ["start", "reset"] as const) {
      const previous = scripts[name];
      if (
         typeof previous === "string" &&
         // `reset` is the same boot plus --init, so it has to be recognised
         // with requireInit or this tool reports its own generated reset as
         // someone else's script it overwrote.
         !isPublisherScript(previous, result.envName, name === "reset")
      ) {
         // Only someone else's script is worth reporting as lost. It is echoed
         // through printable() because it came out of a package.json in a
         // directory the user may have cloned: a script carrying ESC or CR is
         // otherwise sent to the terminal, where it can repaint the line this
         // tool just printed.
         result.replaced.push(
            `${relPath} ${name} script ("${printable(previous)}")`,
         );
      }
   }
}

/**
 * Put the `malloy` server in the host's MCP config, merging into an existing file
 * rather than skipping it. An existing .mcp.json is the common case in a project
 * an agent already works in, and skipping it left the agent with no `malloy_*`
 * tools while the CLI reported the endpoint wired.
 */
function writeMcpConfig(
   cwd: string,
   relPath: string,
   result: ScaffoldResult,
): void {
   const target = path.join(cwd, relPath);
   if (!fs.existsSync(target)) {
      writeFile(target, mcpConfig(), cwd);
      result.written.push(relPath);
      result.mcpWired = true;
      return;
   }

   let parsed: unknown;
   let parses = true;
   try {
      parsed = parseJson(fs.readFileSync(target, "utf8"));
   } catch {
      parses = false;
   }
   const config = asJsonObject(parsed);
   const servers =
      config && "mcpServers" in config
         ? asJsonObject(config.mcpServers)
         : undefined;
   if (!config || (config.mcpServers !== undefined && !servers)) {
      // Nothing we can merge into without guessing at the user's file, and not
      // even --force overwrites it: these hold other servers' credentials, and
      // "unreadable by us" is not "empty" (a BOM used to land here). Report it
      // and hand back the block to paste, which the user can merge by eye.
      const problem = describeMcpProblem(parses, parsed, config);
      result.skipped.push(`${relPath} (${problem})`);
      result.mcpWired = false;
      result.mcpConfigProblem = problem;
      result.mcpPasteBlock = mcpConfig();
      return;
   }

   const existingEntry = servers?.malloy;
   const desired = malloyServer();
   result.mcpWired = true;
   // Counted, not inferred from "we merged into an existing file": an .mcp.json
   // of {} or {"mcpServers":{}} is an existing file with nothing in it, and the
   // caller was saying the malloy server went in "alongside the servers already
   // in it" over both.
   result.mcpOtherServers = Object.keys(servers ?? {}).filter(
      (name) => name !== "malloy",
   ).length;
   if (JSON.stringify(existingEntry) === JSON.stringify(desired)) {
      result.skipped.push(`${relPath} (malloy already wired)`);
      return;
   }
   // Only past the identical check: an entry that was already what we wanted was
   // not replaced by anything.
   result.mcpEntryReplaced = existingEntry !== undefined;
   // Even under --force this is a merge, not a rewrite: the other servers in
   // here belong to the user, and only the malloy key is ours to set.
   config.mcpServers = { ...(servers ?? {}), malloy: desired };
   result.mcpMerged = true;
   writeFile(target, JSON.stringify(config, null, 2) + "\n", cwd);
   result.written.push(
      `${relPath} (${
         existingEntry === undefined ? "added" : "updated"
      } the malloy server)`,
   );
   if (existingEntry !== undefined) {
      result.replaced.push(`${relPath} malloy server`);
   }
}

/**
 * Say why the file could not be merged into. "Could not be read as JSON" covered
 * both a syntax error and a file that parses perfectly but holds the wrong shape,
 * which sent anyone in the second case hunting for a broken quote.
 */
function describeMcpProblem(
   parses: boolean,
   parsed: unknown,
   config: Record<string, unknown> | undefined,
): string {
   if (!parses) {
      return "not valid JSON";
   }
   if (!config) {
      return `valid JSON, but ${describeJsonValue(
         parsed,
      )} rather than a JSON object`;
   }
   return `valid JSON, but its "mcpServers" is ${describeJsonValue(
      config.mcpServers,
   )} rather than a JSON object`;
}

function renderAgentsFile(
   result: ScaffoldResult,
   host: Host,
   envPackages: string[],
): string {
   const mcpNote = !result.mcpWired
      ? `\`${result.mcpConfigPath}\` could not be updated automatically, so the \`malloy\` server may still need adding to it by hand.`
      : host === "cursor"
        ? "This workspace ships a `.cursor/mcp.json` that points Cursor at the endpoint."
        : "This workspace ships a `.mcp.json`, so Claude Code offers to connect on first run.";
   const reconnectNote =
      host === "cursor"
         ? "ask the user to reload the MCP servers from Cursor's settings (the `malloy` server, then Refresh), or to restart Cursor."
         : "ask the user to run `/mcp`, select the `malloy` server, and choose Reconnect, or to restart Claude Code. That panel reports `Auth: not authenticated` and offers `Authenticate` first, which is a red herring: this endpoint has no auth, and Reconnect is the one that works.";
   // The count is the real one, not the shipped one. This file used to assert the
   // skills were there in the same run that installed none of them, and an agent
   // reading it went looking for skills that do not exist.
   const skillsCount = String(result.skillsInstalled);
   const skillsNote =
      result.skillsInstalled === 0
         ? "This run installed no skills into `.claude/skills/` (the scaffolder's output says why), so there are none here to load. Pull the same guidance as MCP prompts from the endpoint above instead."
         : host === "cursor"
           ? `\`.claude/skills/\` holds ${skillsCount} Malloy agent skills as real files. Cursor reads \`AGENTS.md\`; these skills are the same guidance broken out by task, and any MCP client can also pull them as prompts from the endpoint above.`
           : `\`.claude/skills/\` holds ${skillsCount} Malloy agent skills as real files, so Claude Code auto-discovers them. Hosts that read \`AGENTS.md\` rather than Anthropic Agent Skills can pull the same guidance as MCP prompts from the endpoint above.`;
   const startCommand = result.hasStartScript
      ? "npm start"
      : result.startCommand;
   // The port-override line in AGENTS.md has to be the same boot as the one
   // above it, with two flags added. Hardcoding `npm start -- --port ...` there
   // ran the workspace's own start script (`vite --port 4100 --mcp_port 4140`)
   // whenever that script was not ours, and npm's `--` separator only belongs on
   // the `npm start` form in the first place.
   const portOverrideCommand =
      `${startCommand}${result.hasStartScript ? " --" : ""} ` +
      `--port ${ALT_PUBLISHER_PORT} --mcp_port ${ALT_MCP_PORT}`;
   return renderTemplate("AGENTS.md", {
      title: result.packageCreated
         ? `${result.packageName}: a Malloy Publisher package`
         : "A Malloy Publisher workspace",
      startCommand,
      portOverrideCommand,
      // A second package is in the config but not in the already-persisted state,
      // so the reset command is the one that makes the server serve it.
      resetCommand: result.hasResetScript
         ? "npm run reset"
         : result.resetCommand,
      skillsCount,
      envName: result.envName,
      port: String(result.publisherPort),
      mcpPort: String(result.mcpPort),
      packageSection: packageSection(result, envPackages),
      mcpNote,
      reconnectNote,
      skillsNote,
   });
}

/**
 * The package-specific half of AGENTS.md, over every package the environment
 * holds rather than only the one this run created.
 *
 * This file is regenerated in full on every run, so a section written from this
 * run alone is a briefing that says the workspace has one package in it while
 * the config says it serves three. Scaffolding `sales` and then `orders` left
 * the only description of the workspace naming `orders` five times and `sales`
 * not once, and nothing else in the workspace described `sales` either.
 *
 * The package this run created keeps the full detail, because its model file,
 * source name and sample view are known here. The others are named and pointed
 * at, because they are not: this tool never read their models. Saying which
 * packages exist and where to look them up is the part that was missing.
 *
 * A setup-only run over an empty workspace still gets nothing: prose about "your
 * package" would be instructions to an agent about files that are not there, and
 * a REST URL it cannot call.
 */
function packageSection(result: ScaffoldResult, envPackages: string[]): string {
   const others = envPackages.filter((name) => name !== result.packageName);
   if (!result.packageCreated && others.length === 0) {
      return "";
   }
   const restBase = (name: string): string =>
      `http://localhost:${result.publisherPort}/api/v0/environments/` +
      `${result.envName}/packages/${name}`;
   // Both headings hold "## The package", so nothing that looks for the section
   // has to know which one it got.
   const lines = [
      "",
      others.length > 0 ? "## The packages" : "## The package",
      "",
   ];
   // One line per paragraph: the names are substituted, so any hard wrap here
   // would come out ragged for a name that is not the length we guessed.
   if (result.packageCreated) {
      const base = restBase(result.packageName as string);
      lines.push(
         `\`${result.packageName}/${result.modelFile}\` defines a Malloy source named \`${result.sourceName}\` over local data. Read it for the real source, field, and view names and use them verbatim; never guess them. The package's REST base is:`,
         "",
         "```",
         base,
         "```",
         "",
         "Run one of its views from a script:",
         "",
         "```bash",
         `curl -s -X POST ${base}/models/${result.modelFile}/query \\`,
         "  -H 'content-type: application/json' \\",
         `  -d '{"query":"run: ${result.sourceName} -> overview"}'`,
         "```",
         "",
      );
   }
   if (others.length > 0) {
      lines.push(otherPackagesParagraph(result, others, restBase), "");
   }
   return lines.join("\n");
}

/**
 * The packages in this environment that this run did not create. Named where the
 * names can be named: they come out of a publisher.config.json that may have been
 * cloned rather than written here, and this paragraph is read by an agent as
 * instructions, so a name outside the character set Publisher itself accepts is
 * counted rather than quoted back.
 */
function otherPackagesParagraph(
   result: ScaffoldResult,
   others: string[],
   restBase: (name: string) => string,
): string {
   const plural = others.length === 1 ? "" : "s";
   const opening = result.packageCreated
      ? `This file is regenerated on every run and details only the package that run created. \`publisher.config.json\` registers ${others.length} other package${plural} in the \`${result.envName}\` environment`
      : `This run created no package. \`publisher.config.json\` registers ${others.length} package${plural} in the \`${result.envName}\` environment`;
   const named = others.every(isNameableHere)
      ? `: ${others.map((name) => `\`${name}\``).join(", ")}`
      : `, whose names this file does not repeat because at least one of them is outside the character set Publisher accepts in a URL`;
   const shape = result.packageCreated
      ? `the one above with the package name changed`
      : `\`${restBase("<package>")}\``;
   return (
      `${opening}${named}. Nothing here has read those models, so use ` +
      `\`malloy_getContext\`, or read the \`.malloy\` file in each package's ` +
      `own directory, for its source, field, and view names. Their REST bases ` +
      `are ${shape}. The packages endpoint further down lists what the running ` +
      `server actually mounted, which is the only answer that counts.`
   );
}

/**
 * Whether a package name read out of an existing config can be written into the
 * briefing. validatePackageName is the same rule Publisher enforces on the
 * package segment of its own URLs; borrowing it (rather than restating the
 * pattern here) is what keeps the two from drifting apart.
 */
function isNameableHere(name: string): boolean {
   try {
      validatePackageName(name);
      return true;
   } catch {
      return false;
   }
}

function workspacePackageJson(cwd: string, result: ScaffoldResult): string {
   return (
      JSON.stringify(
         {
            name: toNpmName(path.basename(path.resolve(cwd))),
            version: "0.1.0",
            private: true,
            scripts: {
               start: result.startCommand,
               reset: result.resetCommand,
            },
         },
         null,
         2,
      ) + "\n"
   );
}

const GITIGNORE_HEADER = "# Malloy Publisher";

function workspaceGitignoreEntries(): string[] {
   return [
      "node_modules/",
      "publisher_data/",
      // The glob, not the file: DuckDB's .wal sidecar survives a clean shutdown.
      "publisher.db*",
      "*.log",
      ".DS_Store",
   ];
}

function workspaceGitignore(): string {
   return [GITIGNORE_HEADER, ...workspaceGitignoreEntries(), ""].join("\n");
}

/**
 * Ignore the server's generated state, without ever taking a line out of a
 * .gitignore we did not write.
 *
 * This is the one workspace file where neither half of writeIfAbsent was right.
 * Skipped (no --force), an existing .gitignore never learns about publisher_data/
 * or publisher.db*, so the next `git add -A` commits the server's database and a
 * second copy of the user's data. Replaced (--force), a file holding .env and
 * *.pem became this five-line list, which is the data-loss button that
 * package.json and .mcp.json each got merge logic to avoid, on a file that is by
 * far the easiest of the three to merge. Appending the missing lines is the whole
 * of what is needed, is idempotent, and so does not depend on --force at all.
 */
function mergeGitignore(cwd: string, result: ScaffoldResult): void {
   const relPath = ".gitignore";
   const target = path.join(cwd, relPath);
   if (!fs.existsSync(target)) {
      writeFile(target, workspaceGitignore(), cwd);
      result.written.push(relPath);
      return;
   }
   assertWithinWorkspace(target, cwd, relPath);

   let existing: string;
   try {
      existing = fs.readFileSync(target, "utf8");
   } catch (err) {
      // Unreadable is not empty. Say so rather than writing over it.
      result.skipped.push(
         `${relPath} (could not be read, so Publisher's files are not ` +
            `ignored here: ${(err as Error).message})`,
      );
      return;
   }

   const present = new Set(
      existing.split(/\r?\n/).map(normalizeIgnoreEntry).filter(Boolean),
   );
   const missing = workspaceGitignoreEntries().filter(
      (entry) => !present.has(normalizeIgnoreEntry(entry)),
   );
   if (missing.length === 0) {
      result.skipped.push(`${relPath} (already ignores what Publisher writes)`);
      return;
   }

   const prefix =
      existing === "" ? "" : existing.endsWith("\n") ? "\n" : "\n\n";
   fs.appendFileSync(
      target,
      `${prefix}${GITIGNORE_HEADER}\n${missing.join("\n")}\n`,
   );
   const entries = missing.length === 1 ? "entry" : "entries";
   result.written.push(
      `${relPath} (${missing.length} ${entries} appended, nothing removed)`,
   );
}

/**
 * A .gitignore line reduced to what it ignores, so an existing `/publisher_data`
 * counts as the same rule as our `publisher_data/` and is not appended twice.
 * Comments and blank lines reduce to nothing.
 */
function normalizeIgnoreEntry(line: string): string {
   const trimmed = line.trim();
   if (trimmed === "" || trimmed.startsWith("#")) {
      return "";
   }
   return trimmed.replace(/^\/+/, "").replace(/\/+$/, "");
}

function malloyServer(): Record<string, string> {
   return { type: "http", url: `http://localhost:${MCP_PORT}/mcp` };
}

function mcpConfig(): string {
   return (
      JSON.stringify({ mcpServers: { malloy: malloyServer() } }, null, 2) + "\n"
   );
}

/**
 * Claude Code loads this file on its own, so its one job is to point at the file
 * that really holds the Publisher briefing. Pointing at AGENTS.md when AGENTS.md
 * is the user's own project notes is a false pointer that persists on disk, and
 * it is false only on the path this tool detects itself.
 */
function claudeFile(agentsFile: string): string {
   const pointer =
      `See @${agentsFile} for how to start Publisher, connect an agent to the ` +
      `MCP endpoint, and use the bundled Malloy skills.\n`;
   if (agentsFile === "AGENTS.md") {
      return pointer;
   }
   return (
      pointer +
      `\nThis directory already had an AGENTS.md when the Malloy workspace was ` +
      `scaffolded, so that file was left exactly as it was and carries none of ` +
      `the above. ${agentsFile} is the generated briefing; read that one for ` +
      `anything about Publisher, Malloy, or the MCP endpoint.\n`
   );
}

function validateDataFile(dataFile: string): void {
   if (!fs.existsSync(dataFile) || !fs.statSync(dataFile).isFile()) {
      // printable(), because the path is echoed to a terminal and a filename can
      // hold ESC or CR: see printable() in names.ts.
      throw new ScaffoldError(`--data file not found: ${printable(dataFile)}`);
   }
   const ext = path.extname(dataFile).toLowerCase();
   if (ext !== ".csv" && ext !== ".parquet" && ext !== ".xlsx") {
      throw new ScaffoldError(
         `--data must be a .csv, .parquet, or .xlsx file (got "${printable(
            path.basename(dataFile),
         )}").`,
      );
   }
}

/** npm names are lowercase and url-safe; fall back if nothing survives. */
function toNpmName(base: string): string {
   const cleaned = base
      .toLowerCase()
      .replace(/[^a-z0-9._-]/g, "-")
      .replace(/^[._]+/, "");
   return cleaned || "malloy-workspace";
}

/** Keep a copied data filename to characters safe in a Malloy string literal. */
function sanitizeFileName(base: string): string {
   return base.replace(/[^A-Za-z0-9._-]/g, "_");
}

/** Returns true when the file was written, false when it was left in place. */
function writeIfAbsent(
   cwd: string,
   relPath: string,
   content: string,
   options: ScaffoldOptions,
   result: ScaffoldResult,
): boolean {
   const target = path.join(cwd, relPath);
   const exists = fs.existsSync(target);
   if (exists && !options.force) {
      result.skipped.push(relPath);
      return false;
   }
   writeFile(target, content, cwd);
   result.written.push(relPath);
   if (exists) {
      result.replaced.push(relPath);
   }
   return true;
}

/**
 * Write a file, having checked it is still inside the workspace. scaffold() checks
 * the fixed set of paths up front so a refusal costs nothing; this is the backstop
 * for paths built from a name, and for anything added later.
 */
function writeFile(target: string, content: string, root: string): void {
   assertWithinWorkspace(target, root, path.relative(root, target) || target);
   fs.mkdirSync(path.dirname(target), { recursive: true });
   fs.writeFileSync(target, content);
}

function copyFile(
   source: string,
   target: string,
   root: string,
   label: string,
): void {
   assertWithinWorkspace(target, root, label);
   fs.copyFileSync(source, target);
}
