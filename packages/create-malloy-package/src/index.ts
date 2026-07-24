#!/usr/bin/env node
import * as crypto from "node:crypto";
import * as fs from "node:fs";
import * as path from "node:path";
import { fileURLToPath } from "node:url";
import { Command, Option } from "commander";
import { ScaffoldError } from "./errors";
import * as log from "./log";
import { preview, printable } from "./names";
import {
   scaffold,
   type DeclinedScript,
   type Host,
   type ScaffoldResult,
} from "./scaffold";

interface CliOptions {
   data?: string;
   client: string;
   force?: boolean;
}

const CLIENTS: Host[] = ["claude-code", "cursor"];

function version(): string {
   try {
      const pkg = JSON.parse(
         fs.readFileSync(new URL("../package.json", import.meta.url), "utf8"),
      );
      // "0.0.0" is a real version somebody could be running, so printing it for
      // a manifest we could not read answers "which version is this?" with a
      // number that was never true.
      return typeof pkg.version === "string" ? pkg.version : "unknown";
   } catch {
      return "unknown";
   }
}

/**
 * Commander's `.choices()` has already refused anything outside the list before
 * the action runs, so this only narrows the string it hands back.
 */
function asClient(value: string): Host {
   return value === "cursor" ? "cursor" : "claude-code";
}

/**
 * `--data ""` used to be indistinguishable from no `--data` at all, because an
 * empty string is falsy: the flag was dropped and the run scaffolded the sample
 * data under a green check, having been asked for something else.
 */
function resolveDataFile(cwd: string, data?: string): string | undefined {
   if (data === undefined) {
      return undefined;
   }
   if (data.trim() === "") {
      throw new ScaffoldError(
         `--data was given an empty filename. Pass the CSV, Parquet, or XLSX ` +
            `file to seed the package from, or leave --data off to use the ` +
            `sample data.`,
      );
   }
   return path.resolve(cwd, data);
}

function run(name: string | undefined, options: CliOptions): void {
   const cwd = process.cwd();
   // Taken before anything is written, so a failure part-way through can name
   // both what this run put on disk and what it rewrote, rather than leaving the
   // user to guess or, worse, telling them nothing was rewritten.
   const before = snapshotArtifacts(cwd, name);
   try {
      const result = scaffold({
         name,
         cwd,
         dataFile: resolveDataFile(cwd, options.data),
         host: asClient(options.client),
         force: Boolean(options.force),
      });
      process.stdout.write(formatSuccess(result));
   } catch (err) {
      process.stderr.write(
         formatFailure(
            err,
            changesSince(cwd, name, before),
            Boolean(options.force),
         ),
      );
      process.exitCode = 1;
   }
}

/**
 * Every path this tool can create in the workspace. Used only to describe a run
 * that failed part-way, so a path that is not ours to create does not belong here.
 */
function artifactCandidates(name: string | undefined): string[] {
   const paths = [
      "publisher.config.json",
      "package.json",
      ".mcp.json",
      path.join(".cursor", "mcp.json"),
      ".gitignore",
      "AGENTS.md",
      "CLAUDE.md",
      path.join(".claude", "skills"),
   ];
   // The name is not validated yet at snapshot time, so only take it when it is
   // a single path component; anything else could never have been created.
   if (
      name !== undefined &&
      name !== "" &&
      name !== "." &&
      name !== ".." &&
      !name.includes("/") &&
      !name.includes("\\")
   ) {
      paths.unshift(name);
   }
   return paths;
}

/** relative path -> what was there, or undefined when nothing was. */
export type ArtifactSnapshot = Map<string, string | undefined>;

/** What a run left behind, observed rather than assumed. */
export interface WorkspaceChanges {
   /** Paths that did not exist when the run started and do now. */
   created: string[];
   /** Paths that were already there and whose contents this run changed. */
   changed: string[];
}

export function snapshotArtifacts(
   cwd: string,
   name: string | undefined,
): ArtifactSnapshot {
   const snapshot: ArtifactSnapshot = new Map();
   for (const relPath of artifactCandidates(name)) {
      snapshot.set(relPath, fingerprint(cwd, relPath));
   }
   return snapshot;
}

export function changesSince(
   cwd: string,
   name: string | undefined,
   before: ArtifactSnapshot,
): WorkspaceChanges {
   const changes: WorkspaceChanges = { created: [], changed: [] };
   for (const relPath of artifactCandidates(name)) {
      const was = before.get(relPath);
      const now = fingerprint(cwd, relPath);
      if (now === undefined || now === was) {
         continue;
      }
      if (was === undefined) {
         changes.created.push(relPath);
      } else {
         changes.changed.push(relPath);
      }
   }
   return changes;
}

/**
 * A cheap identity for one artifact path: undefined when nothing is there, the
 * bytes for a file, the entry list (name, size, mtime) for a directory. Two of
 * these compared across a run are what lets a failure name the files it had
 * already rewritten. Existence alone cannot: `--force` rewrites files that
 * existed both before and after, so a run that reported only what it created
 * asserted the user's package.json, AGENTS.md and CLAUDE.md were untouched at
 * the moment it had just replaced all three.
 */
function fingerprint(cwd: string, relPath: string): string | undefined {
   const target = path.join(cwd, relPath);
   let stat: fs.Stats;
   try {
      stat = fs.lstatSync(target);
   } catch {
      return undefined;
   }
   try {
      if (stat.isDirectory()) {
         return `dir:${digest(listDirectory(target))}`;
      }
      if (stat.isFile()) {
         return `file:${digest(fs.readFileSync(target))}`;
      }
      // A symlink, a socket, a device: what it is and where it points is all
      // there is to compare without following it.
      return `other:${stat.mode}:${linkTarget(target)}`;
   } catch {
      // There, but we cannot read it. Distinct from absent, and stable across
      // the run, so it is neither created nor changed on its own.
      return "unreadable";
   }
}

/**
 * Every file under a directory as `path size mtime`, sorted. Capped, because
 * this walks .claude/skills and a package directory the user may have filled.
 */
function listDirectory(dir: string, cap = 5000): string {
   const entries: string[] = [];
   const stack = [dir];
   while (stack.length > 0 && entries.length < cap) {
      const current = stack.pop() as string;
      let children: fs.Dirent[];
      try {
         children = fs.readdirSync(current, { withFileTypes: true });
      } catch {
         entries.push(`${path.relative(dir, current)} unreadable`);
         continue;
      }
      for (const child of children) {
         const full = path.join(current, child.name);
         if (child.isDirectory()) {
            stack.push(full);
            continue;
         }
         let size = -1;
         let mtime = -1;
         try {
            const childStat = fs.lstatSync(full);
            size = childStat.size;
            mtime = childStat.mtimeMs;
         } catch {
            // The -1s stand in for a stat we could not take; a file that later
            // becomes statable still reads as a change, which is what it is.
         }
         entries.push(`${path.relative(dir, full)} ${size} ${mtime}`);
      }
   }
   entries.sort();
   return entries.join("\n");
}

function linkTarget(target: string): string {
   try {
      return fs.readlinkSync(target);
   } catch {
      return "";
   }
}

function digest(data: string | Buffer): string {
   return crypto.createHash("sha1").update(data).digest("hex");
}

/**
 * Render any failure readably. Only ScaffoldError used to be formatted, so a
 * filesystem refusal (a name that collides with a regular file, a read-only or
 * full disk, a path the filesystem thinks is too long) came out as a stack trace
 * naming line numbers inside dist/index.js, sometimes after the config, the
 * package.json and the agent files were already written.
 */
export function formatFailure(
   err: unknown,
   changes: WorkspaceChanges,
   force: boolean,
): string {
   const lines = ["", `${log.red("Error:")} ${describeFailure(err)}`];

   if (changes.created.length > 0) {
      lines.push("");
      lines.push(log.yellow("This run created these before it stopped:"));
      for (const relPath of changes.created) {
         lines.push(`  ${relPath}`);
      }
   }
   if (changes.changed.length > 0) {
      lines.push("");
      lines.push(log.yellow("It also rewrote these, which were already here:"));
      for (const relPath of changes.changed) {
         lines.push(`  ${relPath}`);
      }
   }
   if (changes.created.length > 0 || changes.changed.length > 0) {
      // Both lists are observed, so this says how they were arrived at and how
      // far they reach. The line that used to sit here, "Nothing else in this
      // directory was touched", was a claim about every other file in the
      // directory that the code had never looked at, and --force made it false.
      lines.push(
         log.dim(
            "  Both lists come from comparing the paths this tool writes\n" +
               "  against this directory as it was when the run started.",
         ),
      );
      lines.push(
         log.dim(
            force
               ? "  Fix the cause and run again: --force is already set, so the\n" +
                    "  next run writes over what is listed above."
               : "  Remove what it created before running again, or re-run with\n" +
                    "  --force once the cause is fixed.",
         ),
      );
   }

   const stack = err instanceof Error ? err.stack : undefined;
   if (stack && !(err instanceof ScaffoldError) && !isErrno(err)) {
      // An error with no errno is a bug in this tool rather than something the
      // user did, so keep the stack reachable without printing it by default.
      lines.push("");
      lines.push(
         log.dim(
            "  This looks like a bug in create-malloy-package. Re-run with " +
               "DEBUG=1 for the stack trace.",
         ),
      );
      if (process.env.DEBUG) {
         lines.push("");
         lines.push(log.dim(stack));
      }
   }

   lines.push("");
   return lines.join("\n") + "\n";
}

interface ErrnoError extends Error {
   code: string;
   syscall?: string;
   path?: string;
   dest?: string;
}

function isErrno(err: unknown): err is ErrnoError {
   return (
      err instanceof Error &&
      typeof (err as ErrnoError).code === "string" &&
      /^E[A-Z]+$/.test((err as ErrnoError).code)
   );
}

/**
 * Plain English for the filesystem refusals this tool can actually hit. The code
 * is kept in the message because it is what a search engine and an strace both
 * key on, but it is no longer the whole of what the user is told.
 */
const ERRNO_REASON: Record<string, string> = {
   EACCES: "permission denied",
   EEXIST: "it already exists",
   EISDIR: "it is a directory, not a file",
   ELOOP: "too many symbolic links to follow",
   EMFILE: "too many open files",
   ENAMETOOLONG: "the name is longer than this filesystem allows",
   ENOENT: "no such file or directory",
   ENOSPC: "there is no space left on the device",
   ENOTDIR:
      "something along that path is a regular file, so it cannot hold a directory",
   ENOTEMPTY: "the directory is not empty",
   EPERM: "the operation is not permitted",
   EROFS: "the filesystem is read-only",
};

function describeFailure(err: unknown): string {
   if (err instanceof ScaffoldError) {
      return err.message;
   }
   if (isErrno(err)) {
      const reason = ERRNO_REASON[err.code];
      const target = err.path ?? err.dest;
      const what = err.syscall ? `Could not ${err.syscall}` : "Failed";
      const where = target ? ` ${relativeToCwd(target)}` : "";
      return reason
         ? `${what}${where}: ${reason} (${err.code}).`
         : `${what}${where}: ${err.message}`;
   }
   if (err instanceof Error) {
      return err.message;
   }
   return String(err);
}

/** Show a path inside the workspace as the user typed it, not as an absolute. */
function relativeToCwd(target: string): string {
   const relative = path.relative(process.cwd(), target);
   return relative && !relative.startsWith("..") && !path.isAbsolute(relative)
      ? `./${relative}`
      : target;
}

/** The text of a workspace file, or undefined when there is none to read. */
function readWorkspaceFile(cwd: string, relPath: string): string | undefined {
   try {
      return fs.readFileSync(path.join(cwd, relPath), "utf8");
   } catch {
      return undefined;
   }
}

/**
 * What an AGENTS.md this run left alone means for whoever reads it next.
 *
 * Only one kind of file reaches here: the project's own AGENTS.md, which holds
 * none of the Publisher instructions (no start command, no ports, no MCP
 * endpoint, no skills index). scaffold() answers that by writing the briefing to
 * a file of its own and pointing CLAUDE.md there, so the only thing left to say
 * is where it went, and the only thing left to check is that the pointer really
 * goes there. Checked rather than asserted, because CLAUDE.md is loaded
 * automatically and persists on disk long after this output has scrolled away: a
 * pointer into a file with no Malloy guidance in it is a wrong answer given for
 * as long as the directory exists.
 *
 * A briefing this tool wrote on an earlier run is not that case and never
 * reaches these sentences: scaffold() recognises its own marker line and
 * regenerates the file in place, so it is reported under "Overwrote these"
 * rather than as skipped. The guard below keeps the diverted-briefing wording
 * from being printed over a file that is the briefing, which is the shape the
 * message would take if that recognition ever stopped working.
 */
function agentsFileNotes(result: ScaffoldResult): string[] {
   if (
      !result.skipped.includes("AGENTS.md") ||
      result.agentsFile === "AGENTS.md"
   ) {
      return [];
   }

   const wroteClaudeFile =
      result.written.includes("CLAUDE.md") ||
      result.replaced.includes("CLAUDE.md");
   if (!wroteClaudeFile) {
      // No CLAUDE.md of ours to be wrong: --client cursor writes none, and an
      // existing one is the user's and is reported as skipped on its own.
      return [
         log.dim(
            `  That AGENTS.md is the project's own, so it carries no Publisher\n` +
               `  instructions and none were added to it. This run's briefing ` +
               `is in\n  ${result.agentsFile} instead.`,
         ),
      ];
   }
   const claude = readWorkspaceFile(result.cwd, "CLAUDE.md") ?? "";
   if (claude.includes(`@${result.agentsFile}`)) {
      return [
         log.dim(
            `  That AGENTS.md is the project's own, so it carries no Publisher\n` +
               `  instructions and none were added to it. This run's briefing ` +
               `is in\n  ${result.agentsFile} instead, and the CLAUDE.md this ` +
               `run wrote points there.`,
         ),
      ];
   }
   return [
      log.yellow(
         `  That AGENTS.md is the project's own: no start command, no ports, no\n` +
            `  MCP endpoint, no skills index. The briefing went to ` +
            `${result.agentsFile},\n  but the CLAUDE.md this run wrote does not ` +
            `point at that file, and\n  Claude Code reads CLAUDE.md on its own. ` +
            `Point it at ${result.agentsFile}\n  by hand.`,
      ),
   ];
}

/**
 * Why `npm start`, or `npm run reset`, is not the command being offered. The
 * sentence printed here used to be "this directory's package.json has no
 * Publisher start script" in every case, which is false over a script that names
 * the server and binds 0.0.0.0, and that is the one case worth telling the user
 * about.
 */
function declinedScriptReason(
   declined: DeclinedScript | undefined,
   kind: "start" | "reset",
): string {
   const noneAdded = `no Publisher ${kind} script was added to this directory's package.json`;
   if (declined === undefined) {
      // Deliberately a statement about what this run did rather than about what
      // the manifest holds. The sentence that used to sit here, "this
      // directory's package.json has no start script", is a claim about the
      // contents of a file this branch is also reached over when the file could
      // not be opened or did not parse: the scripts come back undefined either
      // way, so the claim was printed as fact over a file nothing had read. What
      // was left alone, and why, is listed above by the code that did look.
      return noneAdded;
   }
   switch (declined.reason) {
      case "no-host-flag":
         // The joined form is worth its own sentence: that script *looks* like
         // it binds loopback, and the reason it does not is invisible from the
         // command line itself. Told "no --host" over a script whose text holds
         // `--host=127.0.0.1`, a reader concludes this tool cannot read, and
         // ignores the warning under it.
         return declined.joinedHostFlag === undefined
            ? `this directory's ${kind} script boots Publisher with no --host, ` +
                 "and Publisher binds every interface by default"
            : `this directory's ${kind} script writes ` +
                 `--host=${printable(declined.joinedHostFlag)}, which ` +
                 "Publisher's own argument parser does not accept: it reads " +
                 "--host and the address as two arguments, drops the joined " +
                 "form without a word, and binds every interface";
      case "unrecognised-shape":
         return (
            `this directory's ${kind} script names the Publisher server inside ` +
            "a longer command line, so what it runs, and what it binds, is not " +
            "something this tool established"
         );
      case "different-invocation":
         // Distinct from the case above, which this used to share. Such a script
         // IS a single Publisher invocation, and its --host was read and found
         // loopback one branch earlier, so telling the reader it sits inside a
         // longer command line contradicts what the code just did. The true
         // statement is narrower: some other argument differs, so the ports and
         // the workspace root printed below are not the ones it would serve.
         return (
            `this directory's ${kind} script boots Publisher with different ` +
            "arguments to the ones here, so the ports and the workspace root " +
            "below are not what it would serve"
         );
      case "non-loopback-host":
         // The host is a substring of a script this tool did not write, so it is
         // shown, not sent: see printable().
         return (
            `this directory's ${kind} script boots Publisher on ` +
            `${printable(String(declined.host))}, which is not a loopback address`
         );
      case "no-init-flag":
         // Only reachable for `reset`, which is the same boot plus --init. The
         // script does boot Publisher on loopback, so nothing above says a word
         // about it, and running it leaves the package this tool just created
         // unserved with nothing on screen explaining why.
         return (
            `this directory's ${kind} script boots Publisher but carries no ` +
            "--init, so it would not rebuild the package list from the config"
         );
      case "not-publisher": {
         // The commonest real shape of all: a directory whose package.json
         // already has a `start` (or `reset`) script that runs something else
         // entirely. Every other decline reason names the script and says why;
         // this one used to fall back to "no Publisher start script was added",
         // which is true but says nothing about the script that is there, so the
         // one case a reader is most likely to meet was the one case the run
         // stayed quiet about, over a `npm start` that still runs.
         //
         // The script is a string read off disk in a directory the user may have
         // cloned: preview() caps its length and shows control characters rather
         // than sending them. An entry that is empty or only whitespace has
         // nothing to name, so it keeps the original sentence.
         const script = declined.script.trim();
         return script === ""
            ? noneAdded
            : `this directory's ${kind} script runs something else: ` +
                 `\`${preview(script)}\`, so it was left alone`;
      }
      default:
         // A reason this file has no wording for, which is what a newly added
         // one looks like from here. Say only what is true of every reason
         // rather than inventing a fact about a script nothing here parsed.
         return (
            `this directory's ${kind} script is not one this tool recognises ` +
            "as its own Publisher boot"
         );
   }
}

/**
 * What a declined script would serve on. `known` carries the address when this
 * tool established one; otherwise the script names the Publisher server and the
 * binding was *not* established, which is still worth interrupting for and is
 * not the same sentence.
 */
type BindRisk = { known: true; address: string } | { known: false };

/**
 * This is the moment, and the only moment, that this tool sees an
 * unauthenticated REST API and MCP endpoint about to be served to the whole
 * network by the command a user is most likely to type.
 *
 * Note which way the unknown case falls. Every decline reason other than the two
 * below means the script names the server and this tool could not establish that
 * it binds loopback, so silence there would be the original defect: quiet at the
 * one moment the warning exists for.
 */
function bindRisk(declined: DeclinedScript | undefined): BindRisk | undefined {
   if (declined === undefined) {
      return undefined;
   }
   switch (declined.reason) {
      case "not-publisher":
         // It does not name the server, so there is no Publisher endpoint of
         // ours for it to expose.
         return undefined;
      case "no-init-flag":
      case "different-invocation":
         // The binding was established and it is loopback; what is wrong with
         // this one is said by declinedScriptReason instead. Both of these sit
         // past the non-loopback-host check, so warning that the binding is
         // unknown would contradict the flag this code just read.
         return undefined;
      case "no-host-flag":
         // True of the joined `--host=<addr>` form as well: the server drops it
         // and falls back, so the address is known and it is the default one.
         return { known: true, address: "0.0.0.0, Publisher's default" };
      case "non-loopback-host":
         return declined.host === undefined
            ? { known: false }
            : { known: true, address: printable(declined.host) };
      case "unrecognised-shape":
         return { known: false };
      default:
         return { known: false };
   }
}

/** One line naming a script that would serve this workspace to the network. */
function exposureWarning(
   command: string,
   declined: DeclinedScript | undefined,
): string[] {
   const risk = bindRisk(declined);
   if (risk === undefined) {
      return [];
   }
   return [
      log.yellow(
         risk.known
            ? `  Do not use \`${command}\` here: that script binds ${risk.address},\n` +
                 `  which serves the unauthenticated REST API and MCP endpoint ` +
                 `to your\n  whole network. The command above binds 127.0.0.1.`
            : `  Do not use \`${command}\` here: this tool could not establish ` +
                 `what\n  that script binds, and Publisher serves an ` +
                 `unauthenticated REST API\n  and MCP endpoint on whatever it ` +
                 `binds. The command above binds\n  127.0.0.1.`,
      ),
   ];
}

/**
 * True when this run put the Publisher start script into package.json itself,
 * rather than adopting one that was already there. Read off the manifest of what
 * was written, which is the same list the output prints, so it cannot claim a
 * write that did not happen: scaffold() records "package.json" for a manifest it
 * created and "package.json (start and reset scripts set)" for the --force merge
 * that added the scripts to somebody else's.
 */
function wroteStartScript(result: ScaffoldResult): boolean {
   return result.written.some(
      (item) => item === "package.json" || item.startsWith("package.json "),
   );
}

export function formatSuccess(result: ScaffoldResult): string {
   const lines: string[] = [""];

   if (result.packageCreated) {
      lines.push(
         `${log.green("✓")} Created package ${log.bold(
            result.packageName as string,
         )} in ./${result.packageName}`,
      );
   }
   if (result.dataFileRenamedFrom) {
      // The model quotes the copied name, so that is the one on disk. Leaving it
      // unsaid meant looking for a file that is not there under that name.
      lines.push(
         log.dim(
            `  ${result.dataFileRenamedFrom} was copied in as ${result.dataPath}, ` +
               `renamed to the characters a Malloy table path can carry.`,
         ),
      );
   }
   if (result.configExtended) {
      lines.push(
         `${log.green("✓")} Registered ${log.bold(
            result.packageName as string,
         )} in the existing publisher.config.json`,
      );
   }
   if (result.mcpWired) {
      lines.push(
         `${log.green("✓")} Wired the agent workspace (${result.host}) in ${
            result.cwd
         }`,
      );
   } else {
      // "Wired" was printed either way, over the one case where the agent ends
      // up with no malloy_* tools at all. The paste block is further down.
      lines.push(
         `${log.yellow("!")} Set up the agent workspace (${result.host}) in ${
            result.cwd
         }, but the MCP endpoint is not wired`,
      );
   }
   if (result.skillsInstalled > 0) {
      lines.push(
         `${log.green("✓")} Installed ${log.bold(
            String(result.skillsInstalled),
         )} Malloy skills in .claude/skills/`,
      );
   } else {
      // A green "Installed 0 skills" is a check over nothing; the skipped list
      // below says which ones and why.
      lines.push(
         log.yellow("!") +
            " No Malloy skills were installed in .claude/skills/",
      );
   }

   // The checks above are the headline; this is the manifest. scaffold() has
   // always kept it, naming each path as it wrote it and how, and nothing read
   // it, so a user who wanted to know what landed in their directory had to
   // diff for it.
   // Every list below can quote something read off disk in a directory the user
   // may have cloned rather than written, most plainly the start script this run
   // replaced. Printed raw, an ESC or a CR in that script repaints the lines
   // above it, and the line most worth repainting is the one that says what this
   // run overwrote. printable() shows those characters instead of sending them.
   if (result.written.length > 0) {
      lines.push("");
      lines.push(log.bold("Wrote these:"));
      for (const item of result.written) {
         lines.push(`  ${printable(item)}`);
      }
   }

   // What we did not do is the part the user cannot see from a green check.
   if (result.skipped.length > 0) {
      lines.push("");
      lines.push(log.yellow("Left these existing files alone:"));
      for (const item of result.skipped) {
         lines.push(`  ${printable(item)}`);
      }
      lines.push(...agentsFileNotes(result));
   }
   if (result.replaced.length > 0) {
      lines.push("");
      lines.push(log.yellow("Overwrote these:"));
      for (const item of result.replaced) {
         lines.push(`  ${printable(item)}`);
      }
   }

   const url = `http://localhost:${result.publisherPort}`;
   // Counted by scaffold() out of the config it wrote, not inferred from what
   // this run did: whether the server has anything to serve and whether this run
   // created a package are different questions, and every line below that used
   // to ask the second one was answering the first.
   const environmentIsEmpty = result.envPackageCount === 0;
   lines.push("");
   lines.push(log.bold("Next steps:"));
   if (environmentIsEmpty) {
      // A config whose environment holds no packages drops that environment
      // entirely when Publisher boots on it: it reports serving with
      // environments [], the watch mode for it fails to start, and
      // /api/v0/environments/<env>/packages answers 404. Saying so here is the
      // difference between "empty" and "broken" for whoever runs it next.
      lines.push(
         `  ${log.cyan("npx create-malloy-package <name>")}   ${log.dim(
            "add a package; the server has nothing to serve without one",
         )}`,
      );
   }
   if (result.needsReset) {
      // A package created into a workspace Publisher has already been pointed at
      // needs the boot that carries --init: the server reads
      // publisher.config.json once and after that its own persisted package list
      // wins, so a plain restart serves the old set and says nothing about the
      // package it is missing. Deliberately not `configExtended`: that is "did
      // this run write a new entry", which is false for a --force re-run and for
      // a name already registered, and both of those still need the reset.
      const served = `re-read the config, so ${
         result.packageName ?? "the new package"
      } is served`;
      if (result.hasResetScript) {
         lines.push(`  ${log.cyan("npm run reset")}   ${log.dim(served)}`);
      } else {
         // The reason the reset script was declined was computed and carried on
         // the result from the day the field existed, and printed nowhere, so a
         // reset script that boots Publisher without --init (the one thing reset
         // is for) was silently replaced by the explicit command with no sign
         // that the `npm run reset` the README tells the user to run would not
         // do it. Printed the same way the declined start script is.
         lines.push(
            `  ${log.dim(
               `${served} (${declinedScriptReason(
                  result.declinedResetScript,
                  "reset",
               )}):`,
            )}`,
         );
         lines.push(`  ${log.cyan(result.resetCommand)}`);
      }
      lines.push(
         log.dim(
            "  Restarting is not enough: Publisher reads publisher.config.json\n" +
               "  once, and after that its persisted package list wins. This boot\n" +
               "  carries --init, which rebuilds the package list from the config\n" +
               "  and drops the rest of the persisted state with it: materialized\n" +
               "  tables, saved themes, and anything set through the API rather\n" +
               "  than the config file.",
         ),
      );
      // Reproduced, and said nowhere until now: the tool's own previous
      // instruction was to start a server, and that server still holds the
      // lock when the reset boot is run.
      lines.push(
         log.dim(
            "  Stop any Publisher already serving this directory first. One\n" +
               "  server per workspace: a second one dies on the lock over\n" +
               "  publisher.db, and moving it to free ports instead leaves it\n" +
               "  at initializing forever, because the lock is on the\n" +
               "  workspace, not the port.",
         ),
      );
      if (result.hasStartScript) {
         lines.push(
            `  ${log.cyan("npm start")}   ${log.dim("for every boot after that")}`,
         );
      } else {
         // Without a Publisher start script in this directory's package.json,
         // `npm start` runs whatever else is in there (or nothing), so the only
         // command left to offer was the one carrying --init: every later boot
         // would then drop the state --init drops.
         lines.push(`  ${log.dim("for every boot after that:")}`);
         lines.push(`  ${log.cyan(result.startCommand)}`);
      }
      if (!result.hasResetScript) {
         lines.push(
            ...exposureWarning("npm run reset", result.declinedResetScript),
         );
      }
   } else if (result.hasStartScript) {
      lines.push(
         `  ${log.cyan("npm start")}   ${log.dim(
            startNote(result, environmentIsEmpty),
         )}`,
      );
   } else {
      lines.push(
         `  ${log.dim(
            `start Publisher (${declinedScriptReason(
               result.declinedStartScript,
               "start",
            )}):`,
         )}`,
      );
      lines.push(`  ${log.cyan(result.startCommand)}`);
   }
   // Printed under either boot branch, because both leave that script in
   // package.json where `npm start` will run it.
   if (!result.hasStartScript) {
      lines.push(...exposureWarning("npm start", result.declinedStartScript));
   }
   // Not `open <url>`: that command exists on macOS only, and the line was
   // printed on every platform.
   lines.push(`  ${log.cyan(url)}   ${log.dim("explore in the browser")}`);
   lines.push("");
   lines.push(log.bold("Check it is ready:"));
   lines.push(`  curl -s ${url}/api/v0/status`);
   const packagesUrl = `${url}/api/v0/environments/${result.envName}/packages`;
   if (environmentIsEmpty) {
      lines.push(
         log.dim(
            "  It reports serving with nothing to serve. There is no\n" +
               "  environment to list packages from until a package exists, so\n" +
               `  ${packagesUrl}\n` +
               "  answers 404 until then.",
         ),
      );
   } else {
      // The status endpoint cannot see this. A package whose model or data fails
      // to load is dropped, quietly, while the status stays "serving": the check
      // this tool used to print on its own went green over a server holding
      // nothing. AGENTS.md already says so, but only an agent reads that.
      lines.push(`  curl -s ${packagesUrl}`);
      lines.push(
         log.dim(
            "  The second one is the check that matters: the status says\n" +
               "  serving even when nothing loaded. A package the server could\n" +
               "  not mount is simply missing from that list, so " +
               (result.packageCreated
                  ? `an empty []\n  means it could not load ${result.packageName}, and the reason is in the\n  server's own log.`
                  : "a package\n  missing from it is one the server could not load, and the reason is\n  in the server's own log."),
         ),
      );
   }

   lines.push("");
   lines.push(log.bold("Connect an agent:"));
   lines.push(`  MCP endpoint http://localhost:${result.mcpPort}/mcp`);
   if (result.mcpMerged) {
      // Both halves of this used to be assumed from mcpMerged alone.
      // "Alongside the servers already in it" was printed over an .mcp.json that
      // held none, and "was added" over a malloy entry that was replaced, which
      // is a state AGENTS.md creates itself when it tells the reader to change
      // the url after moving the ports. Both are now counted while the merge
      // happens.
      const added = !result.mcpEntryReplaced;
      const others = result.mcpOtherServers;
      const alongside =
         others > 0
            ? `, alongside the ${others} server${
                 others === 1 ? "" : "s"
              } already in it`
            : "";
      lines.push(
         log.dim(
            added
               ? `  the malloy server was added to your ${result.mcpConfigPath}${alongside}.`
               : `  your ${result.mcpConfigPath} already had a malloy server pointing ` +
                    `somewhere else; it now points at the endpoint above${alongside}.`,
         ),
      );
   } else if (result.mcpWired) {
      lines.push(
         log.dim(
            `  ${result.mcpConfigPath} points at it${
               result.host === "cursor" ? "" : ", so Claude Code offers it"
            }.`,
         ),
      );
   } else {
      // The claim used to be printed either way, so an unwritable MCP config
      // meant an agent with no malloy_* tools and no sign anything was wrong.
      const problem = result.mcpConfigProblem ?? "not usable";
      const paste = (result.mcpPasteBlock ?? "").trimEnd();
      if (paste === "") {
         // "Add this server to it by hand:" followed by nothing is the same
         // defect one level down: an instruction the output does not carry out.
         lines.push(
            log.yellow(
               `  ${result.mcpConfigPath} is ${problem}, so it was left alone. ` +
                  `Add an MCP server named "malloy" pointing at the endpoint ` +
                  `above by hand; ${result.agentsFile} has the block.`,
            ),
         );
      } else {
         lines.push(
            log.yellow(
               `  ${result.mcpConfigPath} is ${problem}, so it was left alone. ` +
                  `Add this server to it by hand:`,
            ),
         );
         for (const line of paste.split("\n")) {
            lines.push(`    ${line}`);
         }
      }
   }
   lines.push(
      log.dim(
         "  An agent that starts the server itself can't reconnect MCP in that\n" +
            `  session; ask the user to reconnect it. See ${result.agentsFile}.`,
      ),
   );
   lines.push("");

   return lines.join("\n") + "\n";
}

/**
 * What `npm start` will do, which is not the same sentence in all three cases. A
 * setup-only run into an empty directory deliberately writes `"packages": []`,
 * so promising "with the package in watch mode" described a package the run had
 * just decided not to create. The same run over a workspace that already has
 * packages is the opposite case, and telling that user there is nothing to
 * browse is a reason to disbelieve a healthy server. (The fourth case, a package
 * that needs the --init boot to mount, does not start with `npm start` at all;
 * see the caller.)
 *
 * The fifth case is a `start` script that was already in this directory's
 * package.json and was adopted. Whatever was established about that script, this
 * tool did not write it, so describing it as "Publisher (web UI + MCP) with the
 * package in watch mode" is a claim about a string read off disk that nothing
 * verified: a wrapped or chained script commonly does other things as well, and
 * `npm start` is the line an agent following the generated AGENTS.md runs.
 */
function startNote(
   result: ScaffoldResult,
   environmentIsEmpty: boolean,
): string {
   if (!wroteStartScript(result)) {
      return environmentIsEmpty
         ? "run this directory's own start script; it boots Publisher on a loopback address, with no environment to browse"
         : "run this directory's own start script, which boots Publisher on a loopback address";
   }
   if (environmentIsEmpty) {
      return "start Publisher anyway; it reports serving, with no environment to browse";
   }
   if (result.packageCreated) {
      return "start Publisher (web UI + MCP) with the package in watch mode";
   }
   return "start Publisher (web UI + MCP) with this workspace's packages in watch mode";
}

/**
 * Every extra argument this tool ever sees comes from the same place. `npm create`
 * runs the command line through npm's own config parser before this bin is
 * started, so `--data mydata.csv --client cursor` arrives as the bare arguments
 * `mydata.csv cursor`, and options need a `--` in front of them there. `npx` is
 * the opposite: it forwards the flags untouched, and a `--` is passed through
 * literally and counted as another argument. So the fix has to name which
 * invocation it is for, and npm says which in npm_command ("init" for
 * `npm create`, "exec" for `npx`).
 */
function excessArgumentsHint(): string {
   const npmCreateFix = `  ${log.cyan(
      "npm create malloy-package sales -- --data mydata.csv",
   )}`;
   if (process.env.npm_command === "init") {
      return [
         "",
         "npm parsed this command line before the scaffolder saw it, so the",
         "options came through as bare arguments. Put a -- in front of them:",
         npmCreateFix,
         "",
      ].join("\n");
   }
   return [
      "",
      "If you ran this through `npm create`, npm parsed the command line before",
      "the scaffolder saw it and the options came through as bare arguments.",
      "Put a -- in front of them:",
      npmCreateFix,
      "",
      "With npx there is no separator: it forwards the flags as they are, and a",
      "-- would be passed through and counted as another argument.",
      `  ${log.cyan("npx create-malloy-package sales --data mydata.csv")}`,
      "",
   ].join("\n");
}

const program = new Command();
program
   .name("create-malloy-package")
   .description(
      "Scaffold a Malloy Publisher package and a local agent workspace.",
   )
   .version(version(), "-v, --version")
   .argument(
      "[name]",
      "package name to create; omit to only wire the agent workspace into the current directory",
   )
   .option(
      "--data <file>",
      "seed the package from a CSV, Parquet, or XLSX file",
   )
   // Not --host: Publisher's own server takes `--host <address>` for the
   // interface it binds to, and the start command this tool writes now passes
   // one. Two meanings of --host in a single generated workspace is a trap, and
   // this package has not shipped, so the name is still free to fix.
   .addOption(
      new Option("--client <client>", "agent client to wire up")
         .choices(CLIENTS)
         .default("claude-code"),
   )
   .option(
      "--force",
      "overwrite existing workspace files instead of keeping them",
   )
   .allowExcessArguments(false)
   // Commander prints the failure and exits; the exit is the only hook we get to
   // say where these arguments came from.
   .exitOverride((err) => {
      if (err.code === "commander.excessArguments") {
         process.stderr.write(excessArgumentsHint());
      }
      if (
         err.code === "commander.unknownOption" &&
         err.message.includes("--host")
      ) {
         // No hidden alias, but the rename is recent enough to name outright.
         process.stderr.write(
            `\nThe flag that picks the agent client is ${log.cyan(
               "--client",
            )} (claude-code or cursor).\n` +
               `--host is Publisher's own flag for the address its server binds to.\n\n`,
         );
      }
      process.exit(err.exitCode);
   })
   .action((name: string | undefined, options: CliOptions) => {
      run(name, options);
   });

/**
 * True when this file was started as the program, false when something imported
 * it. index.spec.ts imports it to render the real output for states a scaffold
 * cannot easily be driven into, and an unconditional parse() there sees the test
 * runner's argv: no name, no options, which is a valid command line that would
 * scaffold into whatever directory the tests run from.
 *
 * Anything that cannot be resolved counts as "started", so an unfamiliar
 * launcher still gets a CLI rather than a silent exit 0. npm and npx both put
 * the .bin symlink in argv[1], which realpath resolves to this same file.
 */
function startedAsProgram(): boolean {
   const entry = process.argv[1];
   if (entry === undefined) {
      return true;
   }
   try {
      return (
         fs.realpathSync(entry) ===
         fs.realpathSync(fileURLToPath(import.meta.url))
      );
   } catch {
      return true;
   }
}

if (startedAsProgram()) {
   program.parse();
}
