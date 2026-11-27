// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * The drive-less file URL check: `publisher/no-driveless-file-url`
 * (eslint.config.mjs) refuses a drive-less `file:///…` in a spec's or test
 * helper's string, at its start or inside it (a source ID, `m@file:///…`),
 * because on Windows it names no path, and points at
 * src/test_helpers/test_file_url.ts. The files that spelled one when the rule
 * was added are listed in driveless_file_url_baseline.mjs, and the list only
 * shrinks: an entry whose file no longer spells one fails here (delete it),
 * and so does a file that spells one and is not listed (use the helper).
 */

import { beforeAll, describe, expect, it } from "bun:test";
import * as fs from "fs";
import * as path from "path";
import * as ts from "typescript";
import { fileURLToPath } from "url";
import {
   DRIVELESS_FILE_URL_BASELINE,
   isDrivelessFileURL,
} from "../driveless_file_url_baseline.mjs";
import {
   testFilePath,
   testFileURL,
   testSourceID,
} from "./test_helpers/test_file_url";

const SERVER_DIR = path.resolve(__dirname, "..");
const REPO_DIR = path.resolve(SERVER_DIR, "../..");
const RULE = "publisher/no-driveless-file-url";

const posix = (p: string) => p.split(path.sep).join("/");
/** The bare scheme, which the rule allows, so this spec can spell the URLs it refuses. */
const FILE = "file:///";

/** The `.ts`/`.tsx` files under `dir` (repo-relative) that `keep` accepts. */
function filesUnder(dir: string, keep: (rel: string) => boolean): string[] {
   return fs
      .readdirSync(path.join(REPO_DIR, dir), {
         recursive: true,
         encoding: "utf8",
      })
      .map((f) => posix(path.join(dir, f)))
      .filter((f) => /\.tsx?$/.test(f) && !f.includes("node_modules"))
      .filter(keep);
}

/** The files the rule covers, as eslint.config.mjs scopes it. */
function coveredFiles(): string[] {
   return [
      ...filesUnder(
         "packages/server/src",
         (f) =>
            /\.spec\.ts$/.test(f) ||
            f.startsWith("packages/server/src/test_helpers/"),
      ),
      ...filesUnder("packages/server/tests", (f) => f.endsWith(".ts")),
      ...filesUnder("packages/sdk/src", (f) => /\.spec\.tsx?$/.test(f)),
      ...filesUnder("packages/sdk/test", () => true),
   ];
}

/** How many drive-less file URL literals `text` spells, by the rule's test. */
function drivelessLiterals(file: string, text: string): number {
   if (!text.includes("file:///")) return 0;
   const sf = ts.createSourceFile(file, text, ts.ScriptTarget.Latest, true);
   let count = 0;
   const visit = (node: ts.Node): void => {
      if (
         ts.isStringLiteral(node) ||
         ts.isNoSubstitutionTemplateLiteral(node)
      ) {
         if (isDrivelessFileURL(node.text, false)) count++;
      } else if (ts.isTemplateExpression(node)) {
         const pieces = [
            node.head.text,
            ...node.templateSpans.map((s) => s.literal.text),
         ];
         if (
            pieces.some((t, i) => isDrivelessFileURL(t, i < pieces.length - 1))
         )
            count++;
      }
      ts.forEachChild(node, visit);
   };
   visit(sf);
   return count;
}

describe("testFileURL", () => {
   it("is an absolute file URL that reads back as its path", () => {
      const url = testFileURL("pkg", "m.malloy");
      expect(fileURLToPath(url)).toBe(testFilePath("pkg", "m.malloy"));
      expect(path.isAbsolute(testFilePath("pkg", "m.malloy"))).toBe(true);
      expect(isDrivelessFileURL(url, false)).toBe(process.platform !== "win32");
      if (process.platform === "win32") {
         expect(url).toMatch(/^file:\/\/\/[A-Za-z]:\/pkg\/m\.malloy$/);
      } else {
         expect(url).toBe(`${FILE}pkg/m.malloy`);
      }
   });

   it("keeps a trailing slash, so it can be a root to extend", () => {
      const root = testFileURL("pkg/");
      expect(root.endsWith("/pkg/")).toBe(true);
      expect(new URL("m.malloy", root).href).toBe(
         testFileURL("pkg", "m.malloy"),
      );
      expect(testFilePath("pkg/").endsWith(path.sep)).toBe(true);
   });

   it("names a source declared in that file", () => {
      expect(testSourceID("m", "pkg", "m.malloy")).toBe(
         `m@${testFileURL("pkg", "m.malloy")}`,
      );
   });
});

describe("isDrivelessFileURL", () => {
   it("reads a file URL with no drive letter as drive-less", () => {
      expect(isDrivelessFileURL(`${FILE}m.malloy`, false)).toBe(true);
      expect(isDrivelessFileURL(`${FILE}pkg/`, false)).toBe(true);
      expect(isDrivelessFileURL("file:///", true)).toBe(true);
   });

   it("finds one inside the text, as in a source ID", () => {
      expect(isDrivelessFileURL(`m@${FILE}pkg/m.malloy`, false)).toBe(true);
      expect(isDrivelessFileURL(`x file:///C:/a ${FILE}b`, false)).toBe(true);
      expect(isDrivelessFileURL("m@file:///", true)).toBe(true);
   });

   it("leaves a drive letter, a bare prefix and anything else alone", () => {
      expect(isDrivelessFileURL("file:///C:/pkg/m.malloy", false)).toBe(false);
      expect(isDrivelessFileURL("file:///", false)).toBe(false);
      expect(isDrivelessFileURL("m@file:///C:/pkg/m.malloy", false)).toBe(
         false,
      );
      expect(isDrivelessFileURL("m@file:///", false)).toBe(false);
      expect(isDrivelessFileURL("https://x/", true)).toBe(false);
   });
});

type LintMessage = { ruleId: string | null; message: string };
type ESLintInstance = {
   lintText(
      code: string,
      options: { filePath: string },
   ): Promise<{ messages: LintMessage[] }[]>;
};
// eslint is CommonJS; require keeps tsc from asking for its declarations.
// eslint-disable-next-line @typescript-eslint/no-require-imports
const eslintApi = require("eslint") as {
   loadESLint(opts: {
      useFlatConfig: boolean;
      cwd: string;
   }): Promise<new (opts: { cwd: string }) => ESLintInstance>;
};

describe("the lint rule", () => {
   let server: ESLintInstance;
   let repo: ESLintInstance;
   beforeAll(async () => {
      const ESLint = await eslintApi.loadESLint({
         useFlatConfig: true,
         cwd: SERVER_DIR,
      });
      server = new ESLint({ cwd: SERVER_DIR });
      repo = new ESLint({ cwd: REPO_DIR });
   });

   const flagged = async (
      eslint: ESLintInstance,
      filePath: string,
      code: string,
   ) => {
      const [result] = await eslint.lintText(code, { filePath });
      return result.messages.filter((m) => m.ruleId === RULE);
   };
   const PLANTED = [
      `export const a = "${FILE}pkg/m.malloy";`,
      "export const b = (n: string) => `" + FILE + "${n}`;",
      `export const c = "m@${FILE}pkg/m.malloy";`,
      "export const d = (n: string) => `${n}@" + FILE + "pkg/${n}.malloy`;",
      'export const e = "file:///C:/pkg/m.malloy";',
      `export const f = "${FILE}";`,
      "",
   ].join("\n");

   it("flags a drive-less literal in a spec, a test helper and tests/, pointing at the helper", async () => {
      for (const file of [
         "src/service/__planted__.spec.ts",
         "src/test_helpers/__planted__.ts",
         "tests/component/__planted__.component.spec.ts",
      ]) {
         const messages = await flagged(
            server,
            path.join(SERVER_DIR, file),
            PLANTED,
         );
         expect({ file, count: messages.length }).toEqual({ file, count: 4 });
         expect(messages[0].message).toContain("testFileURL");
         expect(messages[0].message).toContain("testSourceID");
      }
   });

   it("leaves production code and a baselined file alone", async () => {
      expect(
         await flagged(
            server,
            path.join(SERVER_DIR, "src/service/__planted__.ts"),
            PLANTED,
         ),
      ).toEqual([]);
      expect(
         await flagged(
            server,
            path.join(REPO_DIR, DRIVELESS_FILE_URL_BASELINE[0]),
            PLANTED,
         ),
      ).toEqual([]);
   });

   it("flags one in an SDK spec, linted from the repository root", async () => {
      const messages = await flagged(
         repo,
         path.join(REPO_DIR, "packages/sdk/src/__planted__.spec.ts"),
         PLANTED,
      );
      expect(messages).toHaveLength(4);
   });
});

describe("the baseline only shrinks", () => {
   it("names exactly the files that still spell a drive-less file URL", () => {
      const offending = coveredFiles().filter(
         (f) =>
            drivelessLiterals(
               f,
               fs.readFileSync(path.join(REPO_DIR, f), "utf8"),
            ) > 0,
      );
      // A glob that matched nothing would pass vacuously.
      expect(coveredFiles().length).toBeGreaterThan(100);
      const listed = new Set(DRIVELESS_FILE_URL_BASELINE);
      // Unlisted: a new drive-less URL. Use testFileURL; do not extend the list.
      expect(offending.filter((f) => !listed.has(f))).toEqual([]);
      // Stale: the file no longer spells one. Delete its entry.
      const found = new Set(offending);
      expect(DRIVELESS_FILE_URL_BASELINE.filter((f) => !found.has(f))).toEqual(
         [],
      );
   });

   it("is sorted and has no duplicates", () => {
      expect(DRIVELESS_FILE_URL_BASELINE).toEqual(
         [...new Set(DRIVELESS_FILE_URL_BASELINE)].sort(),
      );
   });
});
