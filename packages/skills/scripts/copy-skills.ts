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

console.log(`Copied ${copied.length} skills to ${destination}`);
