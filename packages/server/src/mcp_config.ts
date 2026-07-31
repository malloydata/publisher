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
 * This CREATES, and never edits. An earlier version merged into an existing
 * file the way `scaffold.ts` does, and review found that nearly every hazard in
 * this module came from that one decision: replacing a `malloy` entry destroyed
 * user fields including auth headers, reading an existing file hung the whole
 * process when that file was a FIFO, and a repoint-on-every-boot rewrote
 * version-controlled files. The scaffolder can afford to merge because it runs
 * once, deliberately, at the user's request. A server boot cannot. So: write the
 * file when there is none, otherwise say so and leave it entirely alone,
 * unread.
 *
 * `wx` rather than `existsSync` then write, because those two disagree about
 * what they are naming when `.mcp.json` is a dangling symlink: the check says
 * absent, and the write follows the link and creates a file somewhere else on
 * the machine. `O_EXCL` refuses that, and closes the gap between the two calls.
 */
import * as fs from "fs";
import * as os from "os";
import * as path from "path";
import { logger } from "./logger";

export const MCP_CONFIG_FILENAME = ".mcp.json";

/** What happened, so the caller can log it and tests can assert on it. */
export type McpConfigOutcome =
   | { action: "disabled" }
   | { action: "skipped-home"; dir: string; endpoint: string }
   | {
        action: "skipped-git";
        dir: string;
        /** The directory holding `.git`, which is often far above `dir`. */
        gitRoot: string;
        /** A `.mcp.json` already up-tree, named so the reader can go look. */
        rootConfig: string | undefined;
        endpoint: string;
     }
   | { action: "skipped-root"; dir: string; endpoint: string }
   | {
        action: "skipped-unstable-port";
        dir: string;
        endpoint: string;
        requestedPort: number;
        boundPort: number;
     }
   | { action: "created"; file: string }
   | { action: "exists"; file: string; endpoint: string }
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
 * Read that as "name the socket unambiguously", NOT as a security boundary,
 * because it is not one. Two measured limits:
 *
 *   - On macOS/BSD a second process can bind the more specific `127.0.0.1:PORT`
 *     while this server holds the wildcard `0.0.0.0:PORT`, and the specific
 *     socket wins. Also reproduced 3/3, on the default bind, after this change.
 *     Linux refuses that second bind. So on macOS the unambiguous name does not
 *     stop a squatter; only `--host 127.0.0.1`, which the README's quick start
 *     uses, is exclusive.
 *   - Every variant needs a hostile process running as this user, and such a
 *     process can already rewrite `.mcp.json`, `~/.claude.json`, or `claude` on
 *     PATH. It gains nothing from the port that it did not already have.
 *
 * So this is correctness worth having and cheap to keep, and the threat it is
 * sometimes described as closing is not closed by it. `PUBLISHER_READY`'s
 * `displayHost` in `service/environment_store.ts` still prints `localhost` for
 * the same reason it always did; that is a documented output scripts parse, so
 * changing it belongs in its own change.
 */
export function resolveClientHost(
   address: ReturnType<import("net").Server["address"]>,
   fallbackHost: string,
): string {
   const candidate =
      typeof address === "object" && address ? address.address : fallbackHost;
   // Both runtimes populate `address` for a TCP socket, but this runs in a
   // listen callback where a throw kills an already-bound server, so a missing
   // or non-string value degrades to the configured host instead.
   const raw = typeof candidate === "string" && candidate ? candidate : "";
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

/** Spellings of "no" for a variable that is itself named "no". */
const LEAVE_ON_SPELLINGS = new Set(["", "0", "false", "no", "off"]);

/**
 * Is the feature on?
 *
 * A "stop writing to my filesystem" switch should honour `=1` as readily as
 * `=true`, so any value turns it off except the spellings a reader plainly means
 * as "leave it on". Bare truthiness got that backwards:
 * `PUBLISHER_NO_MCP_CONFIG=false` disabled the very thing it names, because every
 * non-empty string is truthy.
 */
export function mcpConfigEnabled(
   env: NodeJS.ProcessEnv = process.env,
): boolean {
   const raw = env.PUBLISHER_NO_MCP_CONFIG;
   if (raw === undefined) return true;
   // One pair of surrounding quotes is stripped: some env-file and CI paths pass
   // `"false"` through with the quote characters still attached, and a variable
   // whose name is a negation is the worst place to get that subtly wrong.
   const value = raw
      .trim()
      .replace(/^(['"])(.*)\1$/, "$2")
      .trim()
      .toLowerCase();
   return LEAVE_ON_SPELLINGS.has(value);
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
   enabled: boolean;
}): McpConfigOutcome {
   const { dir, endpoint, requestedPort, boundPort, homeDir, enabled } =
      options;
   if (!enabled) return { action: "disabled" };

   // Only write a port that will still be this port next boot. Comparing bound
   // against requested catches every way they diverge with one condition:
   // `--mcp_port 0` asks for any free port, and a non-numeric MCP_PORT becomes
   // NaN, which bun binds ephemerally too (k8s injects MCP_PORT=tcp://... into
   // every pod in a namespace with a Service named `mcp`). Create-never-edit
   // means the first boot's file is never corrected, so a file naming a port
   // that moves is permanently wrong, which the module considers worse than no
   // file at all.
   if (boundPort !== requestedPort) {
      return {
         action: "skipped-unstable-port",
         dir,
         endpoint,
         requestedPort,
         boundPort,
      };
   }

   const file = path.join(dir, MCP_CONFIG_FILENAME);
   try {
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
         return { action: "skipped-home", dir, endpoint };
      }
      // The filesystem root is the other directory nobody opens an agent in,
      // and it is where a process manager puts you by default: systemd gives
      // system units a working directory of `/`.
      if (path.resolve(dir) === path.parse(path.resolve(dir)).root) {
         return { action: "skipped-root", dir, endpoint };
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
         };
      }

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
      // EEXIST covers both an ordinary existing file and a dangling symlink, and
      // the response to each is the same: leave it alone, unread.
      if (code === "EEXIST") return { action: "exists", file, endpoint };
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
function addCommand(endpoint: string): string {
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
 * endpoint and the command. Two earlier attempts to be quieter both turned into
 * the same defect: a skip that says nothing leaves someone with no tools and no
 * explanation, and the docs then send them to relaunch the agent in a directory
 * that has nothing to find. The most recent attempt used `debug`, which only
 * looks quiet because the default level happens to be `debug`: set `LOG_LEVEL=info`
 * (the production shape) and the branch went completely silent. One honest line
 * per boot is the smaller cost, so noise is preferred to silence here on purpose.
 */
export function logMcpConfigOutcome(outcome: McpConfigOutcome): void {
   switch (outcome.action) {
      case "created":
         logger.info(
            `Wrote ${outcome.file} so an agent started in this directory finds this server. Disable with --no-mcp-config.`,
         );
         return;
      case "exists":
         // States the endpoint rather than speculating that the file is wrong.
         // This fires on every boot of a scaffolded package, where the file is
         // almost always correct, so it must not read as a warning.
         logger.info(
            `Left the existing ${outcome.file} alone, unread. This server is at ${outcome.endpoint}. Check that the file names the same URL, because a stale one points an agent at whatever else is on that port. To repoint it: ${addCommand(outcome.endpoint)}`,
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
                 `Did not write ${MCP_CONFIG_FILENAME} into ${outcome.dir} because it is inside the git working tree at ${outcome.gitRoot}, which already has ${outcome.rootConfig}. This server is at ${outcome.endpoint}; if your agent does not list malloy, run this from the directory you start it in: ${addCommand(outcome.endpoint)}`
               : `Did not write ${MCP_CONFIG_FILENAME} into ${outcome.dir} because it is inside the git working tree at ${outcome.gitRoot}. To connect an agent, run this from the directory you start it in: ${addCommand(outcome.endpoint)}`,
         );
         return;
      case "skipped-home":
         logger.info(
            `Did not write ${MCP_CONFIG_FILENAME} into your home directory (${outcome.dir}). To connect an agent, run this from the directory you start it in: ${addCommand(outcome.endpoint)}`,
         );
         return;
      case "skipped-unstable-port":
         logger.info(
            `Did not write ${MCP_CONFIG_FILENAME}: this server asked for MCP port ${outcome.requestedPort} and got ${outcome.boundPort}, so the port changes from run to run and a saved config would be wrong next boot. To connect an agent to this run: ${addCommand(outcome.endpoint)}`,
         );
         return;
      case "skipped-root":
         logger.info(
            `Did not write ${MCP_CONFIG_FILENAME} into the filesystem root (${outcome.dir}). To connect an agent, run this from the directory you start it in: ${addCommand(outcome.endpoint)}`,
         );
         return;
      case "disabled":
         return;
      default: {
         // A new variant must be handled above rather than logging nothing.
         const exhaustive: never = outcome;
         return exhaustive;
      }
   }
}
