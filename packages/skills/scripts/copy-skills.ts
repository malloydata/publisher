/**
 * Copy the repo's top-level skills/ into this package so npm can pack it.
 *
 * npm cannot pack above the package directory, and a symlink named in "files"
 * packs as nothing at all (you get a tarball holding package.json, exit 0, no
 * warning), so the files have to physically exist here at pack time. Runs from
 * prepack; the copy is gitignored.
 */
import * as fs from "node:fs";
import * as path from "node:path";
import { fileURLToPath } from "node:url";
import { isExcluded } from "./exclusions";

const packageDir = path.dirname(path.dirname(fileURLToPath(import.meta.url)));
const source = path.join(packageDir, "..", "..", "skills");
const destination = path.join(packageDir, "skills");

fs.rmSync(destination, { recursive: true, force: true });
fs.cpSync(source, destination, {
   recursive: true,
   filter: (from: string) => {
      const relative = path.relative(source, from);
      return relative === "" || !isExcluded(relative);
   },
});

const copied = fs
   .readdirSync(destination, { withFileTypes: true })
   .filter((entry) =>
      fs.existsSync(path.join(destination, entry.name, "SKILL.md")),
   );

if (copied.length === 0) {
   console.error(`No skills found in ${source}`);
   process.exit(1);
}

// Stamp each SKILL.md's frontmatter with the package version at pack time, so
// a copy installed months ago is identifiable on disk. Without this, a stale
// install and a current one are byte-identical in every way that matters to a
// reader, and a QA session lost real time to skills that named tools the
// server no longer serves with nothing saying how old they were (field notes
// F3). Pack-time only: the repo's source skills stay unstamped so the
// upstream-sync copies remain byte-identical in both directions.
const version = (
   JSON.parse(
      fs.readFileSync(path.join(packageDir, "package.json"), "utf8"),
   ) as { version: string }
).version;
let stamped = 0;
for (const entry of copied) {
   const skillFile = path.join(destination, entry.name, "SKILL.md");
   const text = fs.readFileSync(skillFile, "utf8");
   // Insert into the existing frontmatter block rather than appending a new
   // one; a SKILL.md without frontmatter is left alone rather than guessed at.
   if (!text.startsWith("---\n")) continue;
   const close = text.indexOf("\n---", 4);
   if (close === -1) continue;
   fs.writeFileSync(
      skillFile,
      `${text.slice(0, close)}\nversion: ${version}${text.slice(close)}`,
   );
   stamped += 1;
}

console.log(
   `Copied ${copied.length} skills to ${destination} (stamped ${stamped} with version ${version})`,
);
