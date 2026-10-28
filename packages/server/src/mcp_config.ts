// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * Put this server in the host's MCP config, so an agent opened here finds the
 * Malloy tools without being told how: an MCP client only sees a server that
 * a `.mcp.json` in its start directory (or `claude mcp add`) registered.
 *
 * Creates only, never edits: a boot that rewrote the file would churn
 * version-controlled files. An existing REGULAR file is read once, bounded,
 * solely to compare its `mcpServers.malloy.url` with this run's endpoint, and a
 * mismatch is warned about. Nothing else is read or logged from it: it may hold
 * other servers' credentials.
 *
 * A symlink named `.mcp.json` is never written through (see ensureMcpConfig).
 */
import * as fs from "fs";
import * as os from "os";
import * as path from "path";
import { settings } from "./settings";
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

/** The port to write: the one the OS bound, not merely the one requested. */
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
 * Not `localhost`: it resolves to both `127.0.0.1` and `::1` while the server
 * binds one family, so another local process can bind the free family on the
 * same port and `fetch`, preferring IPv6, sends it the traffic. This is
 * unambiguous naming, not isolation.
 */
export function resolveClientHost(
   address: ReturnType<import("net").Server["address"]>,
   fallbackHost: string,
): string {
   // fallbackHost is the one user-controlled input. A non-IP-literal is
   // discarded: a hostname reintroduces the ambiguity above, and a quote would
   // break out of addCommand's quoting in a line the docs say to paste.
   const usableFallback = /^[0-9a-fA-F:.]+$/.test(fallbackHost)
      ? fallbackHost
      : "";
   const fromSocket =
      typeof address === "object" && address ? address.address : undefined;
   // Falls back rather than throwing: this runs in a listen callback, where a
   // throw kills an already-bound server.
   const raw =
      typeof fromSocket === "string" && fromSocket
         ? fromSocket
         : usableFallback;
   if (raw === "0.0.0.0" || raw === "") return "127.0.0.1";
   if (raw === "::" || raw === "::0") return "[::1]";
   if (!raw.includes(":")) return raw;
   // Bracket an IPv6 literal for a legal URL authority. The zone index (node
   // keeps `%en0`, bun strips it) is dropped: it makes the URL unparseable.
   return `[${raw.split("%")[0]}]`;
}

/** The URL to write into the config and to print in advice. */
export function mcpEndpoint(host: string, port: number): string {
   return `http://${host}:${port}/mcp`;
}

/**
 * The root of the git working tree holding `dir`, if any. An untracked file
 * dropped into someone's checkout surprises `git status` and gets committed by
 * accident, so any `.git` above (a directory, or a worktree/submodule file)
 * suppresses the write.
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
 * Only a REGULAR file under 64 KiB is opened (lstat first), so a FIFO cannot
 * block the boot and a symlink is never read through. Only the one URL is
 * extracted. Never throws.
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

/** Throws on an unrecognised value, so a typo is loud rather than silent. */
export function mcpConfigEnabled(): boolean {
   return settings.get("mcp.configFileEnabled");
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
   /** Required as a pair, so the unstable-port guard cannot be bypassed. */
   requestedPort: number;
   boundPort: number;
   /** Defaults to `os.homedir()`; tests inject one so they never write there. */
   homeDir?: string;
}): McpConfigOutcome {
   const { dir, endpoint, requestedPort, boundPort, homeDir } = options;

   const file = path.join(dir, MCP_CONFIG_FILENAME);
   try {
      // Probed before the guards so every skip can mention a stale config here.
      // lstat, not existsSync: a dangling symlink must count as present, as
      // it does for the write's EEXIST.
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

      // Only write a port that will still be this port next boot, since the
      // file is never corrected. Covers `--mcp_port 0`, which binds
      // ephemerally. A non-numeric MCP_PORT, which once did too, now fails
      // startup.
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
      // Inside the try: os.homedir() throws in a distroless container (no HOME,
      // no passwd entry). realpath, not resolve, because process.cwd() is fully
      // resolved and a symlinked HOME would otherwise never match.
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
      // Nobody opens an agent in `/`, but systemd starts system units there.
      if (path.resolve(dir) === path.parse(path.resolve(dir)).root) {
         return { action: "skipped-root", dir, endpoint, staleConfig };
      }
      const gitRoot = findGitWorkTreeRoot(dir);
      if (gitRoot !== undefined) {
         // Existence only, never a read: it is named for the reader, but cannot
         // say whether it registers this server, so it decides nothing.
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

      // Refuse a symlink explicitly: `wx` (O_EXCL) refuses one on POSIX, but
      // Windows resolves the reparse point first and creates a DANGLING link's
      // target. `wx` stays as the race-free backstop for everything else.
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
      // EEXIST: a regular file or a dangling symlink, left alone either way. A
      // regular file is compared, since one naming another URL silently breaks
      // the next agent session.
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
 * Not `-s user`: Claude Code takes local over project over user, so a
 * user-scoped entry would be shadowed by the project `.mcp.json` that may be
 * the very reason this is printed.
 */
export function addCommand(endpoint: string): string {
   // Quoted because zsh globs an IPv6 endpoint's brackets.
   return `claude mcp add --transport http malloy '${endpoint}'`;
}

/** The stale-file warning, appended to whichever skip fired. */
function staleNote(
   staleConfig: string | undefined,
   alreadyNamed?: string,
): string {
   // `alreadyNamed` is the git branch's rootConfig, which may be the same path.
   return staleConfig === undefined || staleConfig === alreadyNamed
      ? ""
      : `. Note that ${staleConfig} is already here and was not written by this run, so an agent started here uses whatever that names.`;
}

/**
 * Say whether an agent opened here will find this server, and what to do when
 * it will not. Every skip logs at `info` with the command: a silent skip looks
 * like a broken install, and `debug` vanishes at `LOG_LEVEL=info`.
 */
export function logMcpConfigOutcome(outcome: McpConfigOutcome): void {
   switch (outcome.action) {
      case "created":
         logger.info(
            `Wrote ${outcome.file} so an agent started in this directory finds this server. Disable with --no-mcp-config.`,
         );
         return;
      case "exists":
         // A mismatch warns: every agent opened here goes to the wrong URL. The
         // matching case fires on every scaffolded boot, so it stays info.
         if (outcome.existingUrl !== undefined) {
            logger.warn(
               `${outcome.file} points agents at ${outcome.existingUrl}, but this server is at ${outcome.endpoint}. It was left alone (this run did not create it, and a boot never rewrites the file). An agent started here will reach whatever answers at the old URL, or nothing. To point it at this server: ${addCommand(outcome.endpoint)}`,
            );
            return;
         }
         logger.info(
            `Left the existing ${outcome.file} alone. This server is at ${outcome.endpoint}. If an agent started here reaches a different Publisher than you expect, ask it to run get_context, which names the environment and packages it is actually talking to. To point it here: ${addCommand(outcome.endpoint)}`,
         );
         return;
      case "failed":
         // info, not warn: nothing is broken, and on a read-only filesystem
         // this fires on every pod, where a warning would page for nothing.
         logger.info(
            `Could not write ${outcome.file} (${outcome.problem}). An agent started here will not find this server on its own. To connect one, run: ${addCommand(outcome.endpoint)}`,
         );
         return;
      case "skipped-git":
         logger.info(
            outcome.rootConfig !== undefined
               ? // It may name another server, so the command is still given.
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
