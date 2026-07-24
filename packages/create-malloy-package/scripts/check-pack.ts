/**
 * Verify what `npm pack` actually publishes, by packing and reading the tarball.
 *
 * The failure this guards against is silent: if the build did not run there is no
 * dist/, or a templates/ file gets dropped, and the tarball ships a bin that can
 * scaffold nothing, with exit 0 and no warning. Nothing else would notice.
 *
 * Template expectations are derived from the source tree, never hardcoded, so a
 * new template file cannot be silently left out of the package.
 *
 * Every assertion reads the tarball, not the working tree, because the publish
 * path edits the manifest between the two.
 *
 * On the publish path it also asks the registry whether the tarball's pinned
 * dependency ranges resolve, since nothing else on that path does: npm publish
 * ships a pin that points at nothing without a word, and npm versions are
 * immutable.
 */
import { execFileSync } from "node:child_process";
import * as fs from "node:fs";
import * as os from "node:os";
import * as path from "node:path";
import { fileURLToPath } from "node:url";

const packageDir = path.dirname(path.dirname(fileURLToPath(import.meta.url)));

interface PackedManifest {
   name: string;
   version: string;
   bin?: Record<string, string>;
   dependencies?: Record<string, string>;
   peerDependencies?: Record<string, string>;
   optionalDependencies?: Record<string, string>;
}

/**
 * True when this run is about to hand the tarball to the registry, either as
 * npm's prepublishOnly hook or from the publish workflow, which passes the flag
 * after rewriting the manifest. Publishing is the one moment a workspace: range
 * stops being survivable.
 *
 * Both env vars are checked because `bun run` resets npm_command to "run-script"
 * on its way through, so under the prepublishOnly script only the lifecycle name
 * survives; npm_command is what a direct node invocation would leave behind.
 */
const publishing =
   process.env["npm_command"] === "publish" ||
   process.env["npm_lifecycle_event"] === "prepublishOnly" ||
   process.argv.slice(2).includes("--publish-ready");

/**
 * npm drops these no matter what the files field says, so the source-tree walk
 * has to drop them too. Without this, one Finder visit to templates/ leaves a
 * .DS_Store that the walk reads as a template and the tarball cannot contain,
 * and the publish is blocked by "the tarball is missing templates/.DS_Store",
 * which sends the contributor hunting for a file they never wrote. The package
 * .gitignore lists .DS_Store, so git status will not show it either.
 *
 * These are npm's own always-excluded entries (npm-packlist defaults), minus
 * the root-anchored ones, which cannot match inside templates/. Two lists,
 * because the two halves need opposite handling: junk is skipped in silence,
 * exactly as npm skips it, while content npm refuses to carry is a real hole
 * in the package and gets reported rather than quietly dropped.
 */
const NPM_JUNK_DIRS = new Set([".git", ".svn", ".hg", "CVS", "node_modules"]);
const isNpmJunkFile = (name: string): boolean =>
   name === ".DS_Store" ||
   name === "npm-debug.log" ||
   name.startsWith("._") ||
   name.endsWith(".orig") ||
   (name.startsWith(".") && name.endsWith(".swp"));
/** Real content npm strips by name, whatever the files field says. */
const NPM_STRIPPED_NAMES = new Set([".gitignore", ".npmignore", ".npmrc"]);

interface TemplateWalk {
   /** Files npm will carry, as forward-slash paths relative to the package. */
   files: string[];
   /** Files that exist but that npm will never ship, so nothing can use them. */
   stripped: string[];
}

/** Every file under templates/ that npm would actually put in the tarball. */
function templateFiles(): TemplateWalk {
   const files: string[] = [];
   const stripped: string[] = [];
   const walk = (dir: string, prefix: string): void => {
      for (const entry of fs.readdirSync(dir, { withFileTypes: true })) {
         const relative = prefix ? `${prefix}/${entry.name}` : entry.name;
         if (entry.isDirectory()) {
            if (NPM_JUNK_DIRS.has(entry.name)) {
               continue;
            }
            walk(path.join(dir, entry.name), relative);
         } else if (isNpmJunkFile(entry.name)) {
            continue;
         } else if (NPM_STRIPPED_NAMES.has(entry.name)) {
            stripped.push(`templates/${relative}`);
         } else {
            files.push(`templates/${relative}`);
         }
      }
   };
   walk(path.join(packageDir, "templates"), "");
   return { files, stripped };
}

type RegistryAnswer =
   | { kind: "resolved"; version: string }
   | { kind: "no-match"; detail: string }
   | { kind: "unreachable"; detail: string };

/**
 * Ask npm whether a dependency range can actually be installed by a user.
 *
 * `npm publish --dry-run` never asks this, and neither does anything else on
 * the manual publish path, so a typo'd or not-yet-published pin publishes
 * clean and then fails at every install. npm versions are immutable, so the
 * question has to be asked while the answer can still change something.
 */
function resolveOnNpm(spec: string): RegistryAnswer {
   try {
      const out = execFileSync("npm", ["view", spec, "version"], {
         cwd: packageDir,
         encoding: "utf8",
         stdio: ["ignore", "pipe", "pipe"],
         timeout: 120_000,
      });
      const lines = out
         .split("\n")
         .map((line) => line.trim())
         .filter(Boolean);
      if (lines.length === 0) {
         // Some npm versions answer a non-matching range this way instead of
         // erroring, so an empty stdout is a no, not a yes.
         return { kind: "no-match", detail: "npm view printed no version" };
      }
      // With more than one match npm prints "name@version 'version'" lines;
      // the last one is the highest version in the range.
      const last = lines[lines.length - 1] ?? "";
      const version = last.includes(" ")
         ? (last.split(" ")[0] ?? "").replace(/^.*@/, "")
         : last;
      return { kind: "resolved", version };
   } catch (error) {
      const failed = error as { stderr?: Buffer | string; message?: string };
      const detail =
         String(failed.stderr ?? "").trim() || String(failed.message ?? error);
      const firstLine = detail
         .split("\n")
         .map((line) => line.trim())
         .filter(Boolean)
         .slice(0, 2)
         .join(" ");
      // A 404 is the registry answering "no such package or version". Anything
      // else (offline, proxy, auth) means the question never reached it, and
      // guessing either way would be the failure this script exists to prevent.
      if (/E404|404 Not Found|No match found/.test(detail)) {
         return { kind: "no-match", detail: firstLine };
      }
      return { kind: "unreachable", detail: firstLine };
   }
}

/** Pack for real, then read back both the file list and the manifest npm shipped. */
function pack(): { files: Set<string>; manifest: PackedManifest } {
   const outDir = fs.mkdtempSync(path.join(os.tmpdir(), "cmp-pack-"));
   try {
      execFileSync("npm", ["pack", "--pack-destination", outDir], {
         cwd: packageDir,
         stdio: ["ignore", "inherit", "inherit"],
         // Clear an inherited dry-run so pack actually writes the tarball.
         env: { ...process.env, npm_config_dry_run: "false" },
      });
      const tarball = fs.readdirSync(outDir).find((f) => f.endsWith(".tgz"));
      if (!tarball) {
         throw new Error(`npm pack produced no tarball in ${outDir}`);
      }
      const tarballPath = path.join(outDir, tarball);
      const files = new Set(
         execFileSync("tar", ["-tzf", tarballPath], {
            encoding: "utf8",
         })
            .split("\n")
            .filter(Boolean)
            // Entries are prefixed with "package/", and some tars add "./".
            .map((entry) => entry.replace(/^\.?\/?package\//, ""))
            .filter((entry) => !entry.endsWith("/")),
      );
      // Extract rather than match a tar member pattern, so a "./" prefix cannot
      // turn a missing manifest into a silent pass.
      const extractDir = path.join(outDir, "extracted");
      fs.mkdirSync(extractDir);
      execFileSync("tar", ["-xzf", tarballPath, "-C", extractDir], {
         stdio: ["ignore", "inherit", "inherit"],
      });
      const manifest = JSON.parse(
         fs.readFileSync(
            path.join(extractDir, "package", "package.json"),
            "utf8",
         ),
      ) as PackedManifest;
      return { files, manifest };
   } finally {
      fs.rmSync(outDir, { recursive: true, force: true });
   }
}

const { files: packed, manifest } = pack();
const failures: string[] = [];

const templates = templateFiles();
const required = [
   "dist/index.js",
   "package.json",
   "README.md",
   "LICENSE",
   ...templates.files,
];
for (const file of required) {
   if (!packed.has(file)) {
      failures.push(`the tarball is missing ${file}`);
   }
}

// Reported rather than skipped: these are files someone wrote on purpose, and
// npm strips them by name, so the scaffolder would ship without them. The
// convention is to store the template under a name npm will carry (gitignore,
// npmrc) and rename it when writing the scaffolded package.
for (const file of templates.stripped) {
   failures.push(
      `${file} can never be published: npm strips ${path.basename(file)} from ` +
         `every tarball regardless of the files field. Store it under a name ` +
         `npm will carry and rename it when scaffolding.`,
   );
}

const bin = manifest.bin?.["create-malloy-package"];
if (!bin) {
   failures.push('package.json has no bin named "create-malloy-package"');
} else if (!packed.has(bin.replace(/^\.?\/?/, ""))) {
   failures.push(`the bin ${bin} is not in the tarball`);
}

// npm ships a workspace: range to the registry verbatim; rewriting it at pack
// time is pnpm and yarn behaviour, not npm's. A published tarball carrying one
// fails every `npm create @malloy-publisher/malloy-package` with EUNSUPPORTEDPROTOCOL, and npm
// versions are immutable, so the only repair is burning the version. The publish
// path rewrites the range to a real one first; this is what proves it happened.
const workspaceDeps = Object.entries({
   ...manifest.dependencies,
   ...manifest.peerDependencies,
   ...manifest.optionalDependencies,
}).filter(([, range]) => String(range).startsWith("workspace:"));
const described = workspaceDeps
   .map(([name, range]) => `${name}@${range}`)
   .join(", ");

if (publishing && workspaceDeps.length > 0) {
   failures.push(
      `the tarball still declares workspace: ranges no install can resolve: ` +
         `${described}. Pin them to a published version in package.json first, ` +
         `and leave that edit uncommitted.`,
   );
} else if (workspaceDeps.length > 0) {
   // Expected in a plain checkout, where bun resolves these from the monorepo.
   console.warn(
      `note: ${described} still uses the workspace protocol; the publish path ` +
         `must rewrite it before this tarball can be published.`,
   );
}

// A pin that points at nothing is the same failure as a workspace: range, one
// typo further along: the manifest looks published-ready, npm publish (and
// --dry-run) exits 0 without ever asking the registry, and every user's
// install then dies on a version that does not exist. CI has a step for this,
// but the first publish of a new package name is manual and skips CI entirely,
// which is exactly the run that cannot be taken back. So ask here too, on the
// publish path, where it cannot be skipped by reading past it.
//
// Only on the publish path: a plain `bun run check-pack` must not need a
// network. And when the registry cannot be reached, this warns instead of
// failing, because "we could not ask" is not "the answer was no", and a
// publish with no network is going to fail on its own anyway.
let registryChecked = "not checked against npm (runs on the publish path only)";
if (publishing) {
   const pinned = Object.entries(manifest.dependencies ?? {}).filter(
      // Anything with a protocol (workspace:, file:, git+, npm:, http) is not
      // a registry range; workspace: is already a failure above.
      ([, range]) => !/^[a-z+]+:/i.test(String(range)),
   );
   const unreachable: string[] = [];
   for (const [name, range] of pinned) {
      const spec = `${name}@${range}`;
      const answer = resolveOnNpm(spec);
      if (answer.kind === "resolved") {
         console.log(`ok: ${spec} resolves to ${answer.version} on npm`);
      } else if (answer.kind === "no-match") {
         failures.push(
            `no published version satisfies ${spec}, so every install of this ` +
               `tarball will fail (${answer.detail}). Publish that dependency ` +
               `first, or pin a version that exists.`,
         );
      } else {
         unreachable.push(spec);
         console.warn(
            `warning: could not ask npm whether ${spec} resolves ` +
               `(${answer.detail}); this tarball's dependencies are unverified.`,
         );
      }
   }
   registryChecked =
      unreachable.length > 0
         ? `UNVERIFIED on npm (${unreachable.join(", ")}): the registry could not be reached`
         : `${pinned.length} verified to resolve on npm`;
}

if (failures.length > 0) {
   console.error(`npm pack check failed for ${manifest.name}:`);
   for (const failure of failures) {
      console.error(`  - ${failure}`);
   }
   process.exit(1);
}

console.log(
   `npm pack check passed: ${required.length} required files present in ` +
      `${packed.size} entries (${manifest.name}@${manifest.version}); ` +
      `dependency ranges ${registryChecked}`,
);
