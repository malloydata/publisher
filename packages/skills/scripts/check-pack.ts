/**
 * Verify what `npm pack` really publishes, by packing and reading the tarball.
 *
 * The failure this exists to catch is silent: a symlinked or uncopied skills/
 * dir packs to package.json alone, with exit 0, no warning, and every other
 * test still green. Nothing else in the repo would notice an empty tarball.
 *
 * It reads the real tarball rather than `npm pack --json`, whose stdout is
 * shared with whatever the prepack script prints, and which reports what npm
 * intends to pack rather than what it packed.
 *
 * Expectations are derived from the source skills/ tree, never hardcoded. The
 * MCP bundle's own "at least 24 skills" floor sat 4 behind reality, which is
 * what a hardcoded count buys you.
 */
import { execFileSync } from "node:child_process";
import * as fs from "node:fs";
import * as os from "node:os";
import * as path from "node:path";
import { fileURLToPath } from "node:url";
import { isExcluded } from "./exclusions";

const packageDir = path.dirname(path.dirname(fileURLToPath(import.meta.url)));
const source = path.join(packageDir, "..", "..", "skills");

/** Every file under skills/, as forward-slash paths relative to skills/. */
function sourceFiles(dir: string = source, prefix = ""): string[] {
   const files: string[] = [];
   for (const entry of fs.readdirSync(dir, { withFileTypes: true })) {
      const relative = prefix ? `${prefix}/${entry.name}` : entry.name;
      if (entry.isDirectory()) {
         files.push(...sourceFiles(path.join(dir, entry.name), relative));
      } else {
         files.push(relative);
      }
   }
   return files;
}

/** Pack for real and list the files that came out. */
function pack(): Set<string> {
   const outDir = fs.mkdtempSync(path.join(os.tmpdir(), "skills-pack-"));
   try {
      execFileSync("npm", ["pack", "--pack-destination", outDir], {
         cwd: packageDir,
         stdio: ["ignore", "inherit", "inherit"],
         // Clear an inherited dry-run: under `npm publish --dry-run` this pack
         // would write no tarball, and the audit would report that as the
         // catastrophic empty-pack it exists to catch.
         env: { ...process.env, npm_config_dry_run: "false" },
      });
      const tarball = fs.readdirSync(outDir).find((f) => f.endsWith(".tgz"));
      if (!tarball)
         throw new Error(`npm pack produced no tarball in ${outDir}`);
      return new Set(
         execFileSync("tar", ["-tzf", path.join(outDir, tarball)], {
            encoding: "utf8",
         })
            .split("\n")
            .filter(Boolean)
            // Entries are prefixed with "package/", and some tars add "./".
            .map((entry) => entry.replace(/^\.?\/?package\//, ""))
            .filter((entry) => !entry.endsWith("/")),
      );
   } finally {
      fs.rmSync(outDir, { recursive: true, force: true });
   }
}

const expected = sourceFiles()
   .filter((file) => !isExcluded(file))
   .map((file) => `skills/${file}`);

const packedPaths = pack();
const manifest = JSON.parse(
   fs.readFileSync(path.join(packageDir, "package.json"), "utf8"),
);
const failures: string[] = [];

const missing = expected.filter((file) => !packedPaths.has(file));
if (missing.length > 0) {
   failures.push(
      `${missing.length} of ${expected.length} skill files are missing from ` +
         `the tarball, starting with: ${missing.slice(0, 5).join(", ")}`,
   );
}

for (const required of ["dist/index.js", "dist/index.d.ts", "README.md"]) {
   if (!packedPaths.has(required)) {
      failures.push(`the tarball is missing ${required}`);
   }
}

// Same predicate as the copy, so the two cannot disagree about a given file.
const leaked = [...packedPaths]
   .filter((file) => file.startsWith("skills/"))
   .filter((file) => isExcluded(file.slice("skills/".length)));
if (leaked.length > 0) {
   failures.push(`the tarball must not contain: ${leaked.join(", ")}`);
}

/**
 * npm ships a `workspace:*` range verbatim, with exit 0 and no warning, and the
 * consumer's install then dies with EUNSUPPORTEDPROTOCOL. Only packages/app is
 * de-workspaced before publishing, by a sed naming one file and one key, so a
 * workspace dep added here would reach users unnoticed.
 */
const workspaceDeps = Object.entries({
   ...manifest.dependencies,
   ...manifest.peerDependencies,
}).filter(([, range]) => String(range).startsWith("workspace:"));
if (workspaceDeps.length > 0) {
   failures.push(
      `a published package cannot depend on workspace:*: ` +
         workspaceDeps.map(([name]) => name).join(", "),
   );
}

if (failures.length > 0) {
   console.error(`npm pack check failed for ${manifest.name}:`);
   for (const failure of failures) console.error(`  - ${failure}`);
   process.exit(1);
}

console.log(
   `npm pack check passed: ${expected.length} skill files and dist/ present ` +
      `in ${packedPaths.size} entries (${manifest.name}@${manifest.version})`,
);
