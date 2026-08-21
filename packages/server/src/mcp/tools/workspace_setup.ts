import fs from "fs";
import path from "path";
import {
   MODEL_FILE_SUFFIX,
   PACKAGE_MANIFEST_NAME,
   PUBLISHER_CONFIG_NAME,
   PUBLISHER_DATA_DIR,
} from "../../constants";
import {
   processConfigValue,
   resolvePublisherConfigPath,
   substituteEnvVars,
} from "../../config";
import { resolvePackageLocation } from "../../service/environment_store";

/**
 * Why this exists, measured 2026-08-20 against a real server.
 *
 * Point a server at a directory that is not set up and it reports perfect
 * health: `{"operationalState":"serving","initialized":true,"environments":[]}`,
 * no loadErrors, `PUBLISHER_READY ... environments=0 packages=0 load_errors=0`.
 * `malloy_getContext` returns `{"results":[]}`, and a guessed name comes back
 * from `malloy_executeQuery` as "Resource not found" with advice to check
 * capitalization, so the agent hunts a typo when there is no config at all.
 *
 * `malloy_getStatus` cannot help by construction: nothing was attempted, so
 * there is no load error to report. Yet MCP_INSTRUCTIONS routes the agent here
 * ("check malloy_getStatus for load errors ... before concluding there is no
 * data"), and the server writes `.mcp.json` into an empty directory, so an
 * agent really is connected to a server serving nothing.
 *
 * This module answers "why is nothing here" in the same call.
 */

/** A configured package that is not currently being served. */
export type UnservedPackage = {
   environment: string;
   package: string;
   /**
    * The location exactly as the config declares it, ABSENT when the entry
    * declares none. Echoed as DECLARED rather than substituted, unlike the
    * name fields above: this is the text the user edits in the file, whereas
    * the names exist to be matched against loadErrors, which carries the
    * substituted form. The asymmetry is deliberate; do not "fix" it.
    */
   location?: string;
   /**
    * Absolute path of the package's files, for a LOCAL location this server
    * could resolve. The load error for a missing manifest names the server's
    * own COPY under `publisher_data/`, which is not a path a user should ever
    * edit, so the source location is reported alongside it.
    *
    * Absent for a remote location, and also absent for a local location that
    * could not be resolved, so its absence does NOT by itself mean "remote".
    * Read `location` to tell the two apart.
    */
   sourcePath?: string;
   /**
    * What sourcePath actually is. Absent when sourcePath is. A boolean would
    * collapse "points at a file" and "points at nothing", which need opposite
    * fixes and can both appear in one config.
    */
   sourcePathKind?: "directory" | "file" | "missing";
   /**
    * Whether `publisher.json` exists at sourcePath. Absent (not false) unless
    * the directory exists: reporting false for a location that points at
    * nothing would route a typo into "create a manifest there".
    */
   hasManifest?: boolean;
};

export type WorkspaceSetup = {
   /** One sentence naming what is wrong. */
   problem: string;
   /** One sentence naming the concrete next action. */
   nextAction: string;
   serverRoot: string;
   /** The config actually in force, or null when there is none. */
   configFile: string | null;
   unservedPackages?: UnservedPackage[];
   /** True when unservedPackages was capped and does not list every package. */
   unservedPackagesTruncated?: boolean;
   /** Connection names the config declares, so an unresolved name is legible. */
   configuredConnections?: string[];
   /** `.malloy` files near the server root that no package claims. */
   unclaimedModelFiles?: string[];
};

const SCAFFOLD_COMMAND = "npm create @malloy-publisher/malloy-package@latest";
/** Bounded so a large directory cannot inflate the response. */
const MAX_LISTED = 10;

/** Directories that never contain a user's authored model. */
function isSkippedDir(name: string): boolean {
   return (
      name === PUBLISHER_DATA_DIR ||
      name === "node_modules" ||
      name.startsWith(".")
   );
}

/**
 * `.malloy` files at the server root or one level below it. One level is
 * deliberate: it covers "my model is right here" and "my model is in a
 * subdirectory" without walking an arbitrary tree on a diagnostic call.
 *
 * Symlinks are skipped in both roles: a Dirent for a symlink reports neither
 * isFile nor isDirectory (verified), so a symlinked model is not listed and a
 * symlinked directory is not descended into. That errs toward listing too
 * little, which is the safe direction for a hint: a missed file costs the
 * agent nothing it cannot see with ls, while descending a link would read a
 * tree the server root does not own.
 */
async function findUnclaimedModelFiles(serverRoot: string): Promise<string[]> {
   const found: string[] = [];
   let entries: fs.Dirent[];
   try {
      entries = await fs.promises.readdir(serverRoot, { withFileTypes: true });
   } catch {
      return found;
   }
   for (const entry of entries) {
      if (found.length >= MAX_LISTED) break;
      if (entry.isFile() && entry.name.endsWith(MODEL_FILE_SUFFIX)) {
         found.push(entry.name);
      } else if (entry.isDirectory() && !isSkippedDir(entry.name)) {
         let children: string[];
         try {
            children = await fs.promises.readdir(
               path.join(serverRoot, entry.name),
            );
         } catch {
            continue;
         }
         for (const child of children) {
            if (found.length >= MAX_LISTED) break;
            if (child.endsWith(MODEL_FILE_SUFFIX)) {
               found.push(`${entry.name}/${child}`);
            }
         }
      }
   }
   return found;
}

/**
 * The loader interpolates `${VAR}` in EVERY config string (processConfigValue
 * walks the whole tree), not just in `location`. This diagnostic reads four
 * config strings: the environment name, the connection name, the package name
 * and the package location. All four must be read the same way the loader read
 * them, or a name echoed back here will not match the one in `loadErrors` and
 * `environments`, which carry the substituted values, and the agent cannot join
 * them up.
 *
 * Returns the raw text when substitution fails, because a diagnostic that
 * throws away a name is worse than one that shows the un-interpolated form.
 * (After the whole-config check above, substitution here cannot actually
 * throw; the fallback is belt and braces.)
 *
 * Applied to the three NAME fields, deliberately NOT to the location that is
 * echoed back: see the note on UnservedPackage.location. Names are matched
 * against loadErrors, so they must be substituted; the location is edited by
 * a human, so it must be shown as written.
 */
function interpolate(value: string): string {
   try {
      return substituteEnvVars(value);
   } catch {
      return value;
   }
}

function fileExists(target: string): boolean {
   try {
      return fs.statSync(target).isFile();
   } catch {
      return false;
   }
}

/**
 * Three outcomes, not two. "Points at a file" is a distinct first-timer
 * mistake (a location aimed at `./sales.malloy` rather than its directory),
 * and reporting it as "does not exist" is the same failure the --config branch
 * avoids: telling someone a path is missing that `ls` plainly shows.
 */
type PathKind = "directory" | "file" | "missing";

function probePath(target: string): PathKind {
   try {
      return fs.statSync(target).isDirectory() ? "directory" : "file";
   } catch {
      return "missing";
   }
}

/**
 * A plain restart re-reads the config only while the store has no persisted
 * environments: `initialize()` loads from the database whenever
 * `repository.listEnvironments()` is non-empty, and otherwise from the config
 * file. Measured 2026-08-20 on a package whose manifest was missing: creating
 * the manifest and restarting still did not load it, with no load error at
 * all, because the server kept serving its own copy; `--init` loaded it and
 * the query returned a row.
 *
 * The signal is whether this server already has environments, NOT whether
 * `publisher_data/` exists. That directory is created unconditionally by
 * `initialize()`, so testing it would demand `--init` in every case.
 *
 * `--init` is destructive and the advice must say so: it removes
 * `publisher_data/` outright and reloads from the config manifest alone, so an
 * environment or package created over the REST API, and absent from the config
 * file, does not come back.
 */
function restartPhrase(hasPersistedEnvironments: boolean): string {
   return hasPersistedEnvironments
      ? "restart the server with `--init` so it reloads from this config (a plain restart keeps serving the environments stored from an earlier boot). Note that `--init` deletes " +
           PUBLISHER_DATA_DIR +
           " and the stored environment list, so any environment or package created over the REST API and not written into this config is lost"
      : "restart the server (if the change does not take effect, this server is loading environments it stored earlier: restart with `--init`, which reloads from the config but deletes " +
           PUBLISHER_DATA_DIR +
           " and anything created over the REST API that is not in the config)";
}

type StatusView = {
   initialized?: boolean;
   operationalState?: string;
   /** Present only when something failed; the advice must not promise it otherwise. */
   loadErrors?: unknown[];
   environments: ReadonlyArray<{ name?: string; packages: readonly unknown[] }>;
};

/**
 * Describe why nothing is being served, or return undefined when something is.
 *
 * SECURITY: this emits the server root and package source paths, which the
 * rest of `malloy_getStatus` deliberately does not. What bounds it is that it
 * is returned ONLY when the server is fully up and serving no package at all,
 * so a healthy deployment never produces it, and that the unauthenticated
 * REST /status already returns each environment's absolute location. It does
 * NOT hold that a load error is always there to carry the path anyway: in the
 * un-set-up case nothing was attempted, so there are no load errors and this
 * block is the sole disclosure. Connection NAMES only, never attributes.
 */
export async function describeWorkspaceSetup(
   serverRoot: string,
   status: StatusView,
): Promise<WorkspaceSetup | undefined> {
   // Only diagnose a server that has finished starting. The MCP endpoint has
   // no readiness gate and getStatus deliberately skips the initialization
   // wait, so a cold start answers with zero packages and no load errors,
   // which is indistinguishable from an un-set-up directory. Telling an agent
   // to restart mid-boot would be actively wrong; waiting is the right move.
   if (status.initialized !== true || status.operationalState !== "serving") {
      return undefined;
   }

   const environments = status.environments;
   const servedPackages = environments.reduce(
      (total, environment) => total + environment.packages.length,
      0,
   );
   // A server with anything to answer with is not the case this describes.
   if (servedPackages > 0) return undefined;

   // Environments exist only once something was loaded from config or from the
   // store, which is exactly when a plain restart would reload the stored set.
   const hasPersistedEnvironments = environments.length > 0;
   const hasLoadErrors = (status.loadErrors?.length ?? 0) > 0;
   const resolvedRoot = path.resolve(serverRoot);
   // An explicit --config that cannot be read is its own mistake, and the
   // generic "no config in the server root" wording would name a directory the
   // user never mentioned while ignoring the flag that is actually wrong.
   // "Exists but is a directory" is separated from "is not there": telling
   // someone a path does not exist when `ls` shows it is worse than silence.
   const explicitConfig = process.env.PUBLISHER_CONFIG_PATH;
   if (explicitConfig && explicitConfig.length > 0) {
      const explicitProblem = fs.existsSync(explicitConfig)
         ? fileExists(explicitConfig)
            ? undefined
            : `--config names ${explicitConfig}, which is not a file`
         : `--config names ${explicitConfig}, which does not exist`;
      if (explicitProblem !== undefined) {
         return {
            problem: `${explicitProblem}, so no environments were configured and nothing is served.`,
            nextAction: `Point --config at a ${PUBLISHER_CONFIG_NAME} file (or drop the flag to use one in the server root), then ${restartPhrase(hasPersistedEnvironments)}.`,
            serverRoot: resolvedRoot,
            configFile: null,
         };
      }
   }

   let resolvedConfig: { path: string; isBundledDefault: boolean } | null;
   try {
      resolvedConfig = resolvePublisherConfigPath(resolvedRoot);
   } catch {
      resolvedConfig = null;
   }

   // Zero-arg `npx` boots the bundled sample config, whose packages are fetched
   // over the network. Its environments DO exist, so the "nothing is
   // configured" wording below would be false here, and would send the agent
   // off to scaffold when the real failure is that the samples did not load.
   if (resolvedConfig?.isBundledDefault) {
      return {
         problem: `The server is running on its bundled sample config (there is no ${PUBLISHER_CONFIG_NAME} in ${resolvedRoot}), and none of its packages loaded.`,
         nextAction: `The bundled packages are fetched over the network, so read loadErrors and check connectivity first. To serve your OWN models instead, scaffold a workspace with \`${SCAFFOLD_COMMAND}\` and start the server there, or point --server_root at a directory that has a ${PUBLISHER_CONFIG_NAME}.`,
         serverRoot: resolvedRoot,
         configFile: null,
      };
   }

   const configFile = resolvedConfig ? resolvedConfig.path : null;

   // No config in force. Two different stories: a directory nothing was ever
   // attempted in, and a server whose environments came from its stored state
   // rather than from any file. Saying "no environments are configured" in the
   // second case contradicts the non-empty environments array beside it.
   if (configFile === null) {
      if (hasPersistedEnvironments) {
         return {
            problem: `There is no ${PUBLISHER_CONFIG_NAME} in ${resolvedRoot}; the environments being served come from this server's stored state, and none of them has a package that loaded.`,
            nextAction: `Add a package to an existing environment over the REST API (POST /api/v0/environments/<environment>/packages), or read loadErrors if one is already there and failed. Do NOT restart with \`--init\`: with no config file to rebuild from, it would delete these environments and leave the server with nothing.`,
            serverRoot: resolvedRoot,
            configFile: null,
         };
      }
      const unclaimedModelFiles = await findUnclaimedModelFiles(resolvedRoot);
      const hasModels = unclaimedModelFiles.length > 0;
      return {
         problem: hasModels
            ? `No ${PUBLISHER_CONFIG_NAME} in ${resolvedRoot}, so the ${MODEL_FILE_SUFFIX} files there are not part of any package and nothing is served.`
            : `No ${PUBLISHER_CONFIG_NAME} in ${resolvedRoot}, so no environments are configured and nothing is served.`,
         nextAction: hasModels
            ? `Scaffold a workspace with \`${SCAFFOLD_COMMAND}\` and move the model into it, or hand-write ${PUBLISHER_CONFIG_NAME} in ${resolvedRoot} declaring an environment whose package location points at the model's directory, plus ${PACKAGE_MANIFEST_NAME} in that directory. Then ${restartPhrase(hasPersistedEnvironments)}.`
            : `Scaffold a workspace with \`${SCAFFOLD_COMMAND}\`, then start the server pointed at it. Do not create files under ${PUBLISHER_DATA_DIR}; that is server-managed state.`,
         serverRoot: resolvedRoot,
         configFile: null,
         ...(hasModels && { unclaimedModelFiles }),
      };
   }

   // Read the config directly rather than through getPublisherConfig, which
   // swallows a parse failure and returns an empty environment list. That is
   // indistinguishable from a valid config declaring nothing, and the two need
   // opposite advice.
   let parsed: unknown;
   try {
      parsed = JSON.parse(fs.readFileSync(configFile, "utf8"));
   } catch (error) {
      const detail = error instanceof Error ? error.message : String(error);
      return {
         problem: `${configFile} could not be read as JSON (${detail}), so no environments were configured and nothing is served.`,
         nextAction: `Fix the JSON in ${configFile}, then ${restartPhrase(hasPersistedEnvironments)}. Adding a package will not help until the file parses.`,
         serverRoot: resolvedRoot,
         configFile,
      };
   }

   // One unset ${VAR} anywhere in the file discards the WHOLE config:
   // getPublisherConfig substitutes over the entire tree, and
   // reloadEnvironmentManifest catches the throw and returns zero
   // environments. Nothing is attempted, so there are no loadErrors either.
   // Verified live: a config with one healthy package and one unset variable
   // on a connection served nothing, reported no load errors, and would
   // otherwise be described here as "see loadErrors for why each one failed"
   // beside a package listed as perfectly healthy.
   try {
      processConfigValue(parsed);
   } catch (error) {
      const detail = error instanceof Error ? error.message : String(error);
      return {
         problem: `${configFile} references an environment variable that is not set (${detail}), and one unset variable rejects the ENTIRE file, so no environment was created and nothing is served.`,
         nextAction: `Set that variable in the server's environment (or remove the reference), then ${restartPhrase(hasPersistedEnvironments)}. Every package in this file is affected, including any that is otherwise fine, so do not start by editing the packages.`,
         serverRoot: resolvedRoot,
         configFile,
      };
   }

   // Walk the RAW config: this path deliberately runs on configs the validated
   // loader may have rejected, so every field is checked before it is used.
   // `projects` is the pre-rename key that getPublisherConfig still accepts, so
   // a legacy config must not read here as declaring nothing.
   const configRoot = parsed as {
      environments?: unknown;
      projects?: unknown;
   } | null;
   const rawEnvironments = Array.isArray(configRoot?.environments)
      ? configRoot.environments
      : Array.isArray(configRoot?.projects)
        ? configRoot.projects
        : [];
   // Resolved, because a relative --config leaves a relative dirname, and
   // sourcePath is documented as absolute (see getPublisherConfigDir, which
   // resolves for exactly this reason).
   const configDir = path.resolve(path.dirname(configFile));
   const unservedPackages: UnservedPackage[] = [];
   const configuredConnections: string[] = [];
   let declaredPackageCount = 0;
   let missingManifestCount = 0;
   let missingDirectoryCount = 0;
   let notADirectoryCount = 0;
   let missingLocationCount = 0;

   for (const rawEnvironment of rawEnvironments) {
      if (!rawEnvironment || typeof rawEnvironment !== "object") continue;
      const environment = rawEnvironment as {
         name?: unknown;
         packages?: unknown;
         connections?: unknown;
      };
      const environmentName =
         typeof environment.name === "string"
            ? interpolate(environment.name)
            : "(unnamed)";

      if (Array.isArray(environment.connections)) {
         for (const connection of environment.connections) {
            const name = (connection as { name?: unknown })?.name;
            if (typeof name === "string") {
               const resolvedName = interpolate(name);
               if (!configuredConnections.includes(resolvedName)) {
                  configuredConnections.push(resolvedName);
               }
            }
         }
      }

      for (const rawPackage of Array.isArray(environment.packages)
         ? environment.packages
         : []) {
         if (!rawPackage || typeof rawPackage !== "object") continue;
         const pkg = rawPackage as { name?: unknown; location?: unknown };
         // Counted even when the location is unusable: dropping it would
         // report a config that plainly declares a package as declaring none.
         declaredPackageCount++;
         const packageName =
            typeof pkg.name === "string" ? interpolate(pkg.name) : "(unnamed)";
         // A package with no usable location is its own fault with its own
         // fix, and it is a common typo. Folding it into the generic branch
         // told the agent to "read its location" when there is none to read.
         const declaredLocation =
            typeof pkg.location === "string" ? pkg.location : undefined;
         if (declaredLocation === undefined) {
            missingLocationCount++;
            if (unservedPackages.length < MAX_LISTED) {
               unservedPackages.push({
                  environment: environmentName,
                  package: packageName,
               });
            }
            continue;
         }
         // Read the way the loader reads it: substitution over the whole tree
         // already succeeded above, so this cannot throw here.
         const location = interpolate(declaredLocation);

         // Only these three forms are local paths this server can resolve.
         // `~user/` and a bare `~` are deliberately excluded: resolvePackage-
         // Location expands only the `~/` prefix, so treating them as local
         // yields a path containing a literal `~` directory, which is the very
         // thing this field exists to avoid.
         const isLocal =
            location.startsWith("./") ||
            location.startsWith("../") ||
            location.startsWith("/") ||
            location.startsWith("~/");
         let sourcePath: string | undefined;
         let sourcePathKind: PathKind | undefined;
         let hasManifest: boolean | undefined;
         if (isLocal) {
            try {
               // The server's own resolver, so `~/` expansion and the
               // config-dir anchor cannot drift from what it actually loads.
               sourcePath = resolvePackageLocation(location, configDir);
            } catch {
               // e.g. `~/` with no home directory. Local, but unresolvable.
               sourcePath = undefined;
            }
            if (sourcePath !== undefined) {
               const kind = probePath(sourcePath);
               sourcePathKind = kind;
               if (kind === "directory") {
                  hasManifest = fileExists(
                     path.join(sourcePath, PACKAGE_MANIFEST_NAME),
                  );
                  if (!hasManifest) missingManifestCount++;
               } else if (kind === "file") {
                  // A location aimed at the model file rather than its
                  // directory. Saying "does not exist" would be false.
                  notADirectoryCount++;
               } else {
                  // A location pointing at nothing is a different fix from a
                  // directory missing its manifest, and "create publisher.json
                  // there" would just materialise the typo.
                  missingDirectoryCount++;
               }
            }
         }
         if (unservedPackages.length < MAX_LISTED) {
            unservedPackages.push({
               environment: environmentName,
               package: packageName,
               location: declaredLocation,
               ...(sourcePath !== undefined && { sourcePath }),
               ...(sourcePathKind !== undefined && { sourcePathKind }),
               ...(hasManifest !== undefined && { hasManifest }),
            });
         }
      }
   }

   const truncated = declaredPackageCount > unservedPackages.length;

   let problem: string;
   let nextAction: string;
   if (declaredPackageCount === 0) {
      problem = `${configFile} declares no packages, so nothing is served.`;
      nextAction = `Add a package to an environment in ${configFile}, pointing its location at a directory that contains a ${PACKAGE_MANIFEST_NAME} and at least one ${MODEL_FILE_SUFFIX} file, then ${restartPhrase(hasPersistedEnvironments)}. If loadErrors instead names a package added over the REST API, it is stored outside this file: fix it through the API, and do not reach for \`--init\`, which would delete it.`;
   } else if (missingLocationCount > 0) {
      problem = `No configured package is being served; ${missingLocationCount} of ${declaredPackageCount} declares no usable "location".`;
      nextAction = `Give each package below that has no location field a "location" in ${configFile}, pointing at the directory that holds its ${MODEL_FILE_SUFFIX} files and ${PACKAGE_MANIFEST_NAME} (a misspelled key such as "locaton" reads as absent). Then ${restartPhrase(hasPersistedEnvironments)}.`;
   } else if (notADirectoryCount > 0) {
      problem = `No configured package is being served; ${notADirectoryCount} of ${declaredPackageCount} names a path that exists but is not a directory.`;
      nextAction = `A package location must be the DIRECTORY holding the model files and their ${PACKAGE_MANIFEST_NAME}, not a ${MODEL_FILE_SUFFIX} file. Point each location reporting sourcePathKind "file" at its containing directory. Any entry reporting "missing" is a different fault, a location that is not there at all. Then ${restartPhrase(hasPersistedEnvironments)}.`;
   } else if (missingDirectoryCount > 0) {
      problem = `No configured package is being served; ${missingDirectoryCount} of ${declaredPackageCount} names a location that does not exist on disk.`;
      nextAction = `Check the location of each package reporting sourcePathKind "missing": nothing is at that path, so this is usually a wrong or misspelled location in ${configFile}. Correct the location (or create the package directory with a ${PACKAGE_MANIFEST_NAME} in it), then ${restartPhrase(hasPersistedEnvironments)}.`;
   } else if (missingManifestCount > 0) {
      // Counted over every declared package, not over the capped list.
      problem = `No configured package is being served; ${missingManifestCount} of ${declaredPackageCount} has no ${PACKAGE_MANIFEST_NAME} at its source path.`;
      nextAction = `Create ${PACKAGE_MANIFEST_NAME} (minimally {"name": "<package>"}) in each sourcePath below that reports hasManifest false, then ${restartPhrase(hasPersistedEnvironments)}. Edit the sourcePath, NOT ${hasLoadErrors ? `the ${PUBLISHER_DATA_DIR} path named in loadErrors` : `any path under ${PUBLISHER_DATA_DIR}`}: that is the server's own copy.`;
   } else {
      // Never point at a list that is not in the same payload. Several paths
      // reach here with nothing attempted and therefore no load errors.
      problem = hasLoadErrors
         ? `No configured package is being served; see loadErrors for why each one failed.`
         : `No configured package is being served, and the server recorded no load error, so the packages were never attempted.`;
      // Referring to configuredConnections is only useful when the key is
      // there; with none declared, say what that implies instead.
      const connectionAdvice =
         configuredConnections.length > 0
            ? `configuredConnections lists the connections this config declares.`
            : `This config declares no connections, so the only name that resolves is \`duckdb\`, the per-package sandbox.`;
      nextAction = `${hasLoadErrors ? "Read loadErrors and fix" : "Check"} each package at its sourcePath below, then ${restartPhrase(hasPersistedEnvironments)}. An entry with no sourcePath has no local file to fix: read its location, which is either a remote location Publisher fetches (so the failure is a download or access problem) or a local path this server could not resolve. If the failure is an unresolved name before \`.table(\`, it is a connection. ${connectionAdvice}`;
   }

   return {
      problem,
      nextAction,
      serverRoot: resolvedRoot,
      configFile,
      ...(unservedPackages.length > 0 && { unservedPackages }),
      ...(truncated && { unservedPackagesTruncated: true }),
      ...(configuredConnections.length > 0 && { configuredConnections }),
   };
}
