// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * Holds the `test:tz` script to every spec that handles a date.
 *
 * CI runs the whole suite once under UTC, then runs `test:tz` again east and
 * west of it, because a local-versus-UTC date bug only shows outside UTC. That
 * second pass covers only the files `test:tz` names. A spec that starts
 * handling dates without being added would never run outside UTC, and nothing
 * else would notice; this spec is what notices.
 *
 * "Handles a date" is a text match on the APIs that read or write one. It errs
 * wide on purpose: an extra file in `test:tz` costs a few seconds per run,
 * while a missing one silently drops the coverage the runs exist for.
 */

import { describe, expect, it } from "bun:test";
import { existsSync, readdirSync, readFileSync } from "fs";
import path from "path";

const PACKAGE_DIR = path.resolve(import.meta.dir, "..");
const SRC_DIR = path.join(PACKAGE_DIR, "src");

const DATE_API =
   /dayjs|new Date\(|Date\.UTC|toLocale(?:Date|Time)?String|getTimezoneOffset|Intl\.DateTimeFormat|timeZone|getHours|getDate\(|setHours|startOf\(|toISOString/;

function specFiles(dir: string): string[] {
   return readdirSync(dir, { withFileTypes: true }).flatMap((entry) => {
      const full = path.join(dir, entry.name);
      if (entry.isDirectory()) return specFiles(full);
      return /\.spec\.tsx?$/.test(entry.name) ? [full] : [];
   });
}

/** The package-relative paths `test:tz` passes to `bun test`. */
function timezoneRunFiles(): string[] {
   const pkg = JSON.parse(
      readFileSync(path.join(PACKAGE_DIR, "package.json"), "utf8"),
   ) as { scripts: Record<string, string> };
   return pkg.scripts["test:tz"]
      .split(/\s+/)
      .filter((token) => token.startsWith("./"))
      .map((token) => path.normalize(token.slice(2)));
}

describe("the timezone runs", () => {
   it("include every spec that handles a date", () => {
      const listed = new Set(timezoneRunFiles());
      const missing = specFiles(SRC_DIR)
         .filter((file) => file !== import.meta.path)
         .filter((file) => DATE_API.test(readFileSync(file, "utf8")))
         .map((file) => path.relative(PACKAGE_DIR, file))
         .filter((file) => !listed.has(file));
      expect(missing).toEqual([]);
   });

   it("name only spec files that exist", () => {
      const absent = timezoneRunFiles().filter(
         (file) => !existsSync(path.join(PACKAGE_DIR, file)),
      );
      expect(absent).toEqual([]);
   });
});
