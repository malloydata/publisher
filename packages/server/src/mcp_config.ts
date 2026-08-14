/**
 * Put this server in the host's MCP config, so an agent opened here finds the
 * `malloy_*` tools without being told how.
 *
 * An MCP client only knows a server exists if something registered it: a
 * `.mcp.json` in the directory the session starts in, or a manual
 * `claude mcp add`. A running server is otherwise invisible to it, and starting
 * the server first does not help, because registration and ordering are separate
 * requirements. The repo ships a committed `.mcp.json` and the scaffolder writes
 * one, so `npx @malloy-publisher/server` was the only way in that did not
 * register itself, which is the only reason getting-started had to teach
 * `claude mcp add` before a reader could ask a question.
 *
 * Creates only, never edits. An existing REGULAR file is read once, bounded,
 * solely to compare its `mcpServers.malloy.url` against this run's endpoint,
 * because a file naming a dead port fails the NEXT agent session silently (a
 * failed boot can write the file and die; the successful retry on another port
 * then skips it). Nothing else is read or logged from it: it may hold other
 * servers and their credentials. The lstat gate keeps the old safety
 * properties: a FIFO is not a regular file so the read cannot block, a symlink
 * is never read through, and an oversized file is skipped. A boot still never
 * rewrites the file, which would churn version-controlled files; the mismatch
 * is warned about instead. The scaffolder can afford to merge because it runs
 * once at the user's request; a server boot cannot.
 *
 * A symlink named `.mcp.json` is never written through. `wx` (`O_EXCL`) refuses
 * one on POSIX whether or not its target exists, but Windows resolves the
 * reparse point first, so a DANGLING link there gets its target created. So the
 * symlink is rejected explicitly, using the same `lstat` that reports a stale
 * config, and `wx` stays as the race-free backstop for everything else.
 */
import * as fs from "fs";
import * as os from "os";
import * as path from "path";
import { parseBoolEnv } from "./config";
import { logger } from "./logger";

export const MCP_CONFIG_FILENAME = ".mcp.json";

/** What happened, so the caller can log it and tests can assert on it. */
export type McpConfigOutcome =
   | {
        action: "skipped-home";
        dir: string;
        endpoint: string;
        staleConfig: string | undefined;
     }
   | {
        action: "skipped-git";
        dir: string;
        /** The directory holding `.git`, which is often far above `dir`. */
        gitRoot: string;
        /** A `.mcp.json` already up-tree, named so the reader can go look. */
        rootConfig: string | undefined;
        endpoint: string;
        staleConfig: string | undefined;
     }
   | {
        action: "skipped-root";
        dir: string;
        endpoint: string;
        staleConfig: string | undefined;
     }
   | {
        action: "skipped-unstable-port";
        dir: string;
        endpoint: string;
        requestedPort: number;
        boundPort: number;
        staleConfig: string | undefined;
     }
   | { action: "created"; file: string }
   | {
        action: "exists";
        file: string;
        endpoint: string;
        /**
         * The malloy URL the existing file names, set ONLY when it parses and
         * differs from this run's endpoint. Presence means "the file points
         * agents somewhere this server is not", which the logger warns about.
         */
        existingUrl?: string;
     }
   | { action: "failed"; file: string; problem: string; endpoint: string };

/** Pinned rather than `Record<string, string>`, so `type` cannot drift. */
type McpServerEntry = { type: "http"; url: string };

function malloyServer(endpoint: string): McpServerEntry {
   return { type: "http", url: endpoint };
}

/**
 * The port to write, given what was asked for and what the OS handed back.
 *
 * Extracted from the listen callback so it can be tested: this is the piece
 * that makes "no ports to know" true, and a silent fallback to the requested
 * value would write a config naming a port nothing is on.
 */
export function resolveBoundPort(
   address: ReturnType<import("net").Server["address"]>,
   requestedPort: number,
): number {
   return typeof address === "object" && address ? address.port : requestedPort;
}

/**
 * The host an agent on this machine should dial to reach the bound socket. A
 * wildcard becomes the loopback literal of its own family; a specific bind
 * address is written as-is, since when the server is bound only to some address
 * that address is the only one that reaches it.
 *
 * Not `localhost`, which is ambiguous: it resolves to both `127.0.0.1` and
 * `::1`, while the server binds exactly one family (`0.0.0.0` is the IPv4
 * wildcard and does not cover IPv6). With the server on IPv4, `::1` is free, any
 * local process can bind the same port there, and clients using `fetch` prefer
 * the IPv6 answer, so that process receives the traffic. Reproduced 3 times out
 * of 3 before this was changed.
 *
 * This is unambiguous naming, not isolation. On macOS a process can still bind
 * the more specific `127.0.0.1:PORT` while this server holds the wildcard, and
 * any attacker able to do that already runs as this user and could rewrite the
 * file directly. `displayHost` in `service/environment_store.ts` answers the
 * same question with `localhost`, as do the committed root `.mcp.json` and the
 * scaffolder. Reconciling all three is its own change: one feeds a documented
 * line scripts parse, and another ships from a different package.
 */
export function resolveClientHost(
   address: ReturnType<import("net").Server["address"]>,
   fallbackHost: string,
): string {
   // fallbackHost is the one user-controlled input here (everything else is
   // OS-derived), and it is only consulted on a path no runtime reaches today.
   // Anything that is not an IP literal is discarded rather than interpolated:
   // a hostname would reintroduce the ambiguity this function exists to remove,
   // and a quote would break out of the quoting in addCommand, in a line the
   // docs tell people to paste.
   const usableFallback = /^[0-9a-fA-F:.]+$/.test(fallbackHost)
      ? fallbackHost
      : "";
   const fromSocket =
      typeof address === "object" && address ? address.address : undefined;
   // Both runtimes populate `address` for a TCP socket. This runs in a listen
   // callback where a throw kills an already-bound server, so a missing or
   // non-string value falls back rather than throwing, and both no-address
   // shapes fall back the same way.
   const raw =
      typeof fromSocket === "string" && fromSocket
         ? fromSocket
         : usableFallback;
   if (raw === "0.0.0.0" || raw === "") return "127.0.0.1";
   if (raw === "::" || raw === "::0") return "[::1]";
   if (!raw.includes(":")) return raw;
   // Bracket an IPv6 literal so it is a legal URL authority, and drop any zone
   // index first: node keeps `%en0` in address() where bun strips it, and no
   // HTTP client can dial a zone anyway, so keeping it writes a URL that fails
   // to parse rather than one that merely cannot connect.
   return `[${raw.split("%")[0]}]`;
}

/** The URL to write into the config and to print in advice. */
export function mcpEndpoint(host: string, port: number): string {
   return `http://${host}:${port}/mcp`;
}

/**
 * Is this directory inside a git working tree?
 *
 * A checkout belongs to somebody, and dropping an untracked file into one is both
 * a surprise in `git status` and a thing that gets committed by accident. A clone
 * of this repo is the sharp case: the documented `bun run start` chdirs into
 * `packages/server`, so without this the file landed there, untracked, and not at
 * the root where a session actually reads it.
 *
 * This is deliberately blunt: one `.git` anywhere above the working directory is
 * enough, so it also covers a user's own project, which is a normal place to run
 * the server. That case gets no file, so the caller logs the manual command
 * rather than leaving them with nothing.
 *
 * Walks up rather than shelling out to git, and treats `.git` as present whether
 * it is a directory or the file that worktrees and submodules use.
 */
function findGitWorkTreeRoot(dir: string): string | undefined {
   let current = path.resolve(dir);
   for (;;) {
      if (fs.existsSync(path.join(current, ".git"))) return current;
      const parent = path.dirname(current);
      if (parent === current) return undefined;
      current = parent;
   }
}

/**
 * The malloy URL an existing config names, when it differs from this run's
 * endpoint; undefined otherwise (matching, absent, unreadable, unparseable).
 *
 * Guarded so the old never-read safety properties survive: lstat first, and
 * only a REGULAR file under 64 KiB is opened — a FIFO cannot block the boot, a
 * symlink is never read through, and a surprise multi-megabyte file is
 * skipped. Only the one URL is extracted; the rest of the file (which may hold
 * other servers and their credentials) is never surfaced. Never throws.
 */
function conflictingMalloyUrl(
   file: string,
   endpoint: string,
): string | undefined {
   try {
      const stats = fs.lstatSync(file);
      if (!stats.isFile() || stats.size > 64 * 1024) return undefined;
      const parsed: unknown = JSON.parse(fs.readFileSync(file, "utf8"));
      const url = (parsed as { mcpServers?: { malloy?: { url?: unknown } } })
         ?.mcpServers?.malloy?.url;
      return typeof url === "string" && url !== endpoint ? url : undefined;
   } catch {
      return undefined;
   }
}

/**
 * Is the feature on?
 *
 * `parseBoolEnv`, not a bespoke set: `config.ts` already owns boolean env
 * parsing for this server and throws on an unrecognised value, so a typo in a
 * manifest is loud rather than silently flipping a filesystem write. An earlier
 * version of this function invented its own spellings, including stripping
 * surrounding quotes, which is a problem every flag here would share and so
 * belongs in the shared helper if it belongs anywhere.
 */
export function mcpConfigEnabled(): boolean {
   return !(parseBoolEnv("PUBLISHER_NO_MCP_CONFIG") ?? false);
}

/**
 * Create `<dir>/.mcp.json` if this directory should have one and does not.
 *
 * Never throws. A convenience file must not be able to stop a server booting, so
 * every failure path returns an outcome instead.
 */
export function ensureMcpConfig(options: {
   dir: string;
   /** The URL an agent on this machine should dial, from `mcpEndpoint`. */
   endpoint: string;
   /**
    * What was asked for, and what the OS actually gave. Required as a pair: when
    * only one was supplied the guard below silently did nothing, which is the
    * defect it exists to prevent.
    */
   requestedPort: number;
   boundPort: number;
   /**
    * The home directory to stay out of. Defaults to `os.homedir()`; injected by
    * tests so they never depend on the developer's real one, where a regression
    * in guard ordering would write the file instead of going red.
    */
   homeDir?: string;
}): McpConfigOutcome {
   const { dir, endpoint, requestedPort, boundPort, homeDir } = options;

   const file = path.join(dir, MCP_CONFIG_FILENAME);
   try {
      // Probed before the guards, not just via EEXIST on the write. Every skip
      // returns before writing, so without this a stale config sitting in this
      // very directory goes unmentioned in exactly the cases where it is the
      // most useful thing to say.
      // lstat, not existsSync: they disagree on a dangling symlink, which is
      // the case this module's header calls out, and the write path (via
      // EEXIST) counts that as present. Reporting and writing must agree.
      const existing = (():
         | { path: string; isSymlink: boolean }
         | undefined => {
         try {
            return {
               path: file,
               isSymlink: fs.lstatSync(file).isSymbolicLink(),
            };
         } catch {
            return undefined;
         }
      })();
      const staleConfig = existing?.path;

      // Only write a port that will still be this port next boot. Comparing
      // bound against requested catches every way they diverge with one
      // condition: `--mcp_port 0` asks for any free port, and a non-numeric
      // MCP_PORT becomes NaN, which bun binds ephemerally too (k8s injects
      // MCP_PORT=tcp://... into every pod in a namespace with a Service named
      // `mcp`). Create-never-edit means the first boot's file is never
      // corrected, so a file naming a port that moves is permanently wrong.
      if (boundPort !== requestedPort) {
         return {
            action: "skipped-unstable-port",
            dir,
            endpoint,
            requestedPort,
            boundPort,
            staleConfig,
         };
      }
      // Inside the try because os.homedir() throws when HOME is unset and the uid
      // has no passwd entry, which is the ordinary distroless container shape.
      // realpath, not resolve: `resolve` normalises `.` and `..` but not
      // symlinks, while process.cwd() is always fully resolved. With HOME on a
      // symlinked path (an automounted /home is the common one) the two forms
      // never matched and the file landed in the home directory this guard
      // exists to protect. Falls back to the unresolved path when it does not
      // exist, since realpathSync throws on a missing directory.
      const realish = (p: string) => {
         try {
            return fs.realpathSync(p);
         } catch {
            return path.resolve(p);
         }
      };
      if (realish(dir) === realish(homeDir ?? os.homedir())) {
         return { action: "skipped-home", dir, endpoint, staleConfig };
      }
      // The filesystem root is the other directory nobody opens an agent in,
      // and it is where a process manager puts you by default: systemd gives
      // system units a working directory of `/`.
      if (path.resolve(dir) === path.parse(path.resolve(dir)).root) {
         return { action: "skipped-root", dir, endpoint, staleConfig };
      }
      const gitRoot = findGitWorkTreeRoot(dir);
      if (gitRoot !== undefined) {
         // Existence only, never a read. Reported rather than acted on: it tells
         // the reader where to look, and cannot tell us whether that file names
         // this server or some unrelated one, so it must not decide whether to
         // stay quiet.
         const rootCandidate = path.join(gitRoot, MCP_CONFIG_FILENAME);
         return {
            action: "skipped-git",
            dir,
            gitRoot,
            rootConfig: fs.existsSync(rootCandidate)
               ? rootCandidate
               : undefined,
            endpoint,
            staleConfig,
         };
      }

      // Refuse a symlink before attempting the write, rather than relying on
      // `wx` to refuse it. `O_EXCL` does that on POSIX whether or not the
      // target exists, but Windows resolves the reparse point first, so
      // CREATE_NEW on a DANGLING link creates the target: the write-through
      // this module exists to prevent, on the one platform where `wx` does not
      // prevent it. Caught by the cross-platform CI run, not by reasoning.
      // `wx` stays as the race-free backstop for the non-symlink case.
      // (conflictingMalloyUrl lstat-gates on a regular file, so the symlink
      // itself is never read through and existingUrl stays undefined here.)
      if (existing?.isSymlink) return { action: "exists", file, endpoint };

      const body =
         JSON.stringify(
            { mcpServers: { malloy: malloyServer(endpoint) } },
            null,
            2,
         ) + "\n";
      fs.writeFileSync(file, body, { encoding: "utf8", flag: "wx" });
      return { action: "created", file };
   } catch (error) {
      const code = (error as NodeJS.ErrnoException)?.code;
      // EEXIST covers both an ordinary existing file and a dangling symlink.
      // The response to each is the same, leave it alone, but a regular file is
      // compared first: one pointing at a URL this server is not on breaks the
      // NEXT session silently (the observed shape: a boot writes the file, dies
      // on the REST port, and the retry on another port skips the rewrite).
      if (code === "EEXIST") {
         const existingUrl = conflictingMalloyUrl(file, endpoint);
         return {
            action: "exists",
            file,
            endpoint,
            ...(existingUrl !== undefined && { existingUrl }),
         };
      }
      return {
         action: "failed",
         file,
         problem: error instanceof Error ? error.message : String(error),
         endpoint,
      };
   }
}

/**
 * The command to register this server by hand, at the DEFAULT (local) scope.
 *
 * Not `-s user`. Claude Code resolves a duplicate name by precedence, local then
 * project then user, and takes the winning entry whole rather than merging. Every
 * branch that prints this either has a `.mcp.json` in the directory or may have
 * one up-tree, and that file is project scope, so a user-scoped registration is
 * silently shadowed by the very file that caused the message. Local outranks it
 * and works. `-s user` still belongs in the README's separate "register once for
 * every directory" instruction, where nothing shadows it.
 */
export function addCommand(endpoint: string): string {
   // Quoted because an IPv6 endpoint contains brackets, which zsh (the macOS
   // default) treats as a glob and refuses outright: `no matches found`. The
   // docs tell the reader to paste this line as printed.
   return `claude mcp add --transport http malloy '${endpoint}'`;
}

/**
 * Say whether an agent opened here will find this server, and what to do when it
 * will not.
 *
 * Every branch that leaves the user without a file says so at `info`, with the
 * endpoint and the command. Noise is preferred to silence on purpose: a skip
 * that says nothing is indistinguishable from a broken install, and `debug` is
 * not a middle ground, because it disappears entirely at `LOG_LEVEL=info`.
 */
/** The stale-file warning, appended to whichever skip fired. */
function staleNote(
   staleConfig: string | undefined,
   alreadyNamed?: string,
): string {
   // `alreadyNamed` is the git branch's rootConfig: when cwd is the repo root
   // the two are the same path, and naming it twice reads like two files.
   return staleConfig === undefined || staleConfig === alreadyNamed
      ? ""
      : `. Note that ${staleConfig} is already here and was not written by this run, so an agent started here uses whatever that names.`;
}

export function logMcpConfigOutcome(outcome: McpConfigOutcome): void {
   switch (outcome.action) {
      case "created":
         logger.info(
            `Wrote ${outcome.file} so an agent started in this directory finds this server. Disable with --no-mcp-config.`,
         );
         return;
      case "exists":
         // Two tones on purpose. The mismatch case is a real warning: the file
         // sends every agent opened here to a URL this server is not on, the
         // observed way this happens being a boot that wrote the file and then
         // died on its REST port, leaving the retry on another port to skip the
         // rewrite. The matching/unreadable case fires on every boot of a
         // scaffolded package, where the file is almost always correct, so it
         // stays calm info and does not speculate that the file is wrong.
         if (outcome.existingUrl !== undefined) {
            logger.warn(
               `${outcome.file} points agents at ${outcome.existingUrl}, but this server is at ${outcome.endpoint}. It was left alone (this run did not create it, and a boot never rewrites the file). An agent started here will reach whatever answers at the old URL, or nothing. To point it at this server: ${addCommand(outcome.endpoint)}`,
            );
            return;
         }
         logger.info(
            `Left the existing ${outcome.file} alone. This server is at ${outcome.endpoint}. If an agent started here reaches a different Publisher than you expect, ask it to run malloy_getContext, which names the environment and packages it is actually talking to. To point it here: ${addCommand(outcome.endpoint)}`,
         );
         return;
      case "failed":
         // info, not warn: nothing is broken. The server is serving, and a
         // convenience file was not created. On a read-only filesystem this
         // fires on every pod of every rollout, where a warning would be picked
         // up by alerting and read by someone who cannot act on it.
         logger.info(
            `Could not write ${outcome.file} (${outcome.problem}). An agent started here will not find this server on its own. To connect one, run: ${addCommand(outcome.endpoint)}`,
         );
         return;
      case "skipped-git":
         logger.info(
            outcome.rootConfig !== undefined
               ? // Named, not acted on: it may register an unrelated server, or
                 // name a port this run is not on, and we never read it to find
                 // out. So the endpoint and the command are still given.
                 `Did not write ${MCP_CONFIG_FILENAME} into ${outcome.dir} because it is inside the git working tree at ${outcome.gitRoot}, which already has ${outcome.rootConfig}. This server is at ${outcome.endpoint}; if your agent does not list malloy, run this from the directory you start it in: ${addCommand(outcome.endpoint)}${staleNote(outcome.staleConfig, outcome.rootConfig)}`
               : `Did not write ${MCP_CONFIG_FILENAME} into ${outcome.dir} because it is inside the git working tree at ${outcome.gitRoot}. To connect an agent, run this from the directory you start it in: ${addCommand(outcome.endpoint)}${staleNote(outcome.staleConfig)}`,
         );
         return;
      case "skipped-home":
         logger.info(
            `Did not write ${MCP_CONFIG_FILENAME} into your home directory (${outcome.dir}). To connect an agent, run this from the directory you start it in: ${addCommand(outcome.endpoint)}${staleNote(outcome.staleConfig)}`,
         );
         return;
      case "skipped-unstable-port":
         logger.info(
            `Did not write ${MCP_CONFIG_FILENAME}: this server asked for MCP port ${outcome.requestedPort} and got ${outcome.boundPort}, so the port changes from run to run and a saved config would be wrong next boot. To connect an agent to this run: ${addCommand(outcome.endpoint)}. That registration outlives this run, and the port will have moved by the next one, so undo it with claude mcp remove malloy${staleNote(outcome.staleConfig)}`,
         );
         return;
      case "skipped-root":
         logger.info(
            `Did not write ${MCP_CONFIG_FILENAME} into the filesystem root (${outcome.dir}). To connect an agent, run this from the directory you start it in: ${addCommand(outcome.endpoint)}${staleNote(outcome.staleConfig)}`,
         );
         return;
      default: {
         // A new variant must be handled above rather than logging nothing.
         const exhaustive: never = outcome;
         return exhaustive;
      }
   }
}
