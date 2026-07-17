import * as fs from "node:fs";
import * as path from "node:path";
import { fileURLToPath } from "node:url";

/**
 * The template files ship in the package's templates/ directory, one level under
 * the package root. This module lives one level under the package root too (src/
 * before build, dist/ after), so ../templates resolves the same way whether we run
 * from source in a test or from the bundled bin an npm user installed.
 */
export const templatesDir = path.resolve(
   path.dirname(fileURLToPath(import.meta.url)),
   "..",
   "templates",
);

/**
 * Read a template and substitute every {{key}} with vars[key]. Both halves of the
 * mismatch are a bug in the caller, not the user, so both throw rather than
 * emitting a file that is half rendered or quietly out of date:
 *
 * - a {{key}} with no matching var would render as a literal `{{key}}`;
 * - a var the template never references means the caller computed a value the
 *   file does not use. That direction used to be silent, and it is how AGENTS.md
 *   came to hardcode `npm run reset` while a computed `resetCommand` was passed in
 *   and dropped. The rendered file then contradicted the command the CLI printed,
 *   in the same run, with nothing to catch it.
 */
export function renderTemplate(
   name: string,
   vars: Record<string, string>,
): string {
   const raw = fs.readFileSync(path.join(templatesDir, name), "utf8");
   const referenced = new Set<string>();
   const rendered = raw.replace(/\{\{(\w+)\}\}/g, (_match, key: string) => {
      if (!(key in vars)) {
         throw new Error(
            `Template ${name} references unknown variable {{${key}}}`,
         );
      }
      referenced.add(key);
      return vars[key];
   });
   const unused = Object.keys(vars).filter((key) => !referenced.has(key));
   if (unused.length > 0) {
      const names = unused.map((key) => `{{${key}}}`).join(", ");
      const one = unused.length === 1;
      throw new Error(
         `Template ${name} never references ${names}, so ` +
            `${one ? "that value is" : "those values are"} computed and then ` +
            `dropped. Either use ${one ? "it" : "them"} in the template or ` +
            `stop passing ${one ? "it" : "them"}.`,
      );
   }
   return rendered;
}
