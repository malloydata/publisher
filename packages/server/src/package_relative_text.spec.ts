// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { afterAll, describe, expect, it } from "bun:test";
import * as fs from "fs";
import * as os from "os";
import * as path from "path";
import { pathToFileURL } from "url";
import { packageRelativeText } from "./package_relative_text";

const dirs: string[] = [];
afterAll(() => {
   for (const d of dirs) fs.rmSync(d, { recursive: true, force: true });
});

function packageDir(name = "pkg"): string {
   const parent = fs.mkdtempSync(path.join(os.tmpdir(), "pkg-relative-"));
   dirs.push(parent);
   const dir = path.join(parent, name);
   fs.mkdirSync(path.join(dir, "sub"), { recursive: true });
   return dir;
}

describe("packageRelativeText", () => {
   it("names a file under the package by its package-relative path, as a path or a URL", () => {
      const dir = packageDir();
      const file = path.join(dir, "sub", "m.malloy");
      const text =
         `FILE: ${file}\nline 3: 'x' is not defined\n` +
         `at ${pathToFileURL(file).href}:3:4`;
      const out = packageRelativeText(text, dir);
      expect(out).toBe(
         "FILE: sub/m.malloy\nline 3: 'x' is not defined\nat sub/m.malloy:3:4",
      );
      expect(out).not.toContain(path.dirname(dir));
   });

   it("finds the package through the symlink the compiler resolved", () => {
      const dir = packageDir();
      const real = fs.realpathSync(dir);
      const link = path.join(path.dirname(dir), "link");
      fs.symlinkSync(dir, link);
      expect(packageRelativeText(`FILE: ${real}/m.malloy`, link)).toBe(
         "FILE: m.malloy",
      );
   });

   it("decodes a URL a reader printed with a space in it", () => {
      const dir = packageDir("my pkg");
      const url = pathToFileURL(path.join(dir, "m.malloy")).href;
      expect(url).toContain("%20");
      expect(packageRelativeText(`a ${url} b ${decodeURI(url)}`, dir)).toBe(
         "a m.malloy b m.malloy",
      );
   });

   it("leaves a sibling directory sharing the package's prefix alone", () => {
      const dir = packageDir();
      const sibling = `${dir}2/m.malloy`;
      expect(packageRelativeText(`FILE: ${sibling}`, dir)).toBe(
         `FILE: ${sibling}`,
      );
      expect(packageRelativeText(`in ${dir}, nothing`, dir)).toBe(
         "in ., nothing",
      );
   });

   it("returns text that names no part of the package as it is", () => {
      expect(packageRelativeText("Binder Error: x", packageDir())).toBe(
         "Binder Error: x",
      );
   });
});
