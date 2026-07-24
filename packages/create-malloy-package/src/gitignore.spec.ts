import { afterEach, beforeEach, describe, expect, test } from "bun:test";
import * as fs from "node:fs";
import * as os from "node:os";
import * as path from "node:path";
import { scaffold, type ScaffoldOptions } from "./scaffold";

/**
 * .gitignore is the one workspace file that is merged rather than written or
 * skipped, so both of the failures it replaces have to stay reproduced here: a
 * run that leaves publisher_data/ and publisher.db* uncommitted-but-untracked
 * (the next `git add -A` commits the server's database and a second copy of the
 * user's data), and a --force run that replaced a file holding .env and *.pem.
 */
let tmp: string;

beforeEach(() => {
   tmp = fs.mkdtempSync(path.join(os.tmpdir(), "cmp-gitignore-"));
});

afterEach(() => {
   fs.rmSync(tmp, { recursive: true, force: true });
});

function run(overrides: Partial<ScaffoldOptions> = {}) {
   return scaffold({
      name: "sales",
      cwd: tmp,
      host: "claude-code",
      force: false,
      ...overrides,
   });
}

function ignoreFile(): string {
   return fs.readFileSync(path.join(tmp, ".gitignore"), "utf8");
}

/** The rules the file carries, ignoring comments, blanks and slash styling. */
function rules(): string[] {
   return ignoreFile()
      .split("\n")
      .map((line) => line.trim())
      .filter((line) => line !== "" && !line.startsWith("#"))
      .map((line) => line.replace(/^\/+/, "").replace(/\/+$/, ""));
}

const PUBLISHER_RULES = ["publisher_data", "publisher.db*"];

describe(".gitignore: no file yet", () => {
   test("is written with everything the server generates", () => {
      const result = run();
      for (const rule of PUBLISHER_RULES) {
         expect(rules()).toContain(rule);
      }
      expect(result.written).toContain(".gitignore");
   });
});

describe(".gitignore: a file the user wrote", () => {
   const userFile = ".env\n*.pem\n/node_modules\n";

   test("gains what is missing and loses nothing, without --force", () => {
      fs.writeFileSync(path.join(tmp, ".gitignore"), userFile);
      const result = run();
      expect(ignoreFile().startsWith(userFile)).toBe(true);
      expect(rules()).toContain(".env");
      expect(rules()).toContain("*.pem");
      for (const rule of PUBLISHER_RULES) {
         expect(rules()).toContain(rule);
      }
      expect(result.skipped).not.toContain(".gitignore");
      expect(
         result.written.some((entry) => entry.startsWith(".gitignore")),
      ).toBe(true);
   });

   test("--force appends too, and still replaces nothing", () => {
      fs.writeFileSync(path.join(tmp, ".gitignore"), userFile);
      const result = run({ force: true });
      expect(ignoreFile().startsWith(userFile)).toBe(true);
      expect(rules()).toContain(".env");
      expect(rules()).toContain("*.pem");
      for (const rule of PUBLISHER_RULES) {
         expect(rules()).toContain(rule);
      }
      // The file is only ever appended to, so --force cannot report it lost.
      expect(result.replaced).not.toContain(".gitignore");
   });

   test("a rule already there in another slash style is not duplicated", () => {
      fs.writeFileSync(
         path.join(tmp, ".gitignore"),
         "/node_modules\n/publisher_data/\n",
      );
      run();
      expect(rules().filter((rule) => rule === "node_modules")).toHaveLength(1);
      expect(rules().filter((rule) => rule === "publisher_data")).toHaveLength(
         1,
      );
      expect(rules()).toContain("publisher.db*");
   });

   test("a second run changes nothing", () => {
      fs.writeFileSync(path.join(tmp, ".gitignore"), userFile);
      run({ name: "sales" });
      const afterFirst = ignoreFile();
      const result = run({ name: "marketing" });
      expect(ignoreFile()).toBe(afterFirst);
      expect(result.skipped).toContain(
         ".gitignore (already ignores what Publisher writes)",
      );
   });

   test("a file with no trailing newline is not run onto its last line", () => {
      fs.writeFileSync(path.join(tmp, ".gitignore"), "dist");
      run();
      expect(ignoreFile().split("\n")[0]).toBe("dist");
      expect(rules()).toContain("dist");
      expect(rules()).toContain("publisher_data");
   });
});
