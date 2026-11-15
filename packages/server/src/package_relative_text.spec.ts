// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { afterAll, describe, expect, it } from "bun:test";
import * as fs from "fs";
import * as os from "os";
import * as path from "path";
import { pathToFileURL } from "url";
import {
   callerVisibleText,
   OUTSIDE_PACKAGE,
   packageRelativeText,
   rewriteErrorMessage,
   withoutAbsolutePaths,
} from "./package_relative_text";

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
      const file = path.join(real, "m.malloy");
      expect(packageRelativeText(`FILE: ${file}`, link)).toBe("FILE: m.malloy");
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
      const sibling = path.join(`${dir}2`, "m.malloy");
      expect(packageRelativeText(`FILE: ${sibling}`, dir)).toBe(
         `FILE: ${sibling}`,
      );
      expect(packageRelativeText(`in ${dir}, nothing`, dir)).toBe(
         "in pkg, nothing",
      );
   });

   it("leaves a longer path that ends in the package's path alone", () => {
      const dir = packageDir();
      const longer = path.join(path.parse(dir).root, "elsewhere") + dir;
      const text = `at ${path.join(longer, "m.malloy")}`;
      expect(packageRelativeText(text, dir)).toBe(text);
   });

   it("returns text that names no part of the package as it is", () => {
      expect(packageRelativeText("Binder Error: x", packageDir())).toBe(
         "Binder Error: x",
      );
   });
});

describe("packageRelativeText on a Windows server's paths", () => {
   const win = path.win32;
   const root = "C:\\srv\\Publisher Data\\env\\pkg";
   const rel = (text: string) => packageRelativeText(text, root, win);

   it("names a file under the package package-relative, `/`-separated", () => {
      expect(rel(`FILE: ${win.join(root, "sub", "m.malloy")}:3:4`)).toBe(
         "FILE: sub/m.malloy:3:4",
      );
   });

   it("finds the package spelled with forward slashes, or another case", () => {
      expect(rel("at C:/srv/Publisher Data/env/pkg/m.malloy")).toBe(
         "at m.malloy",
      );
      expect(rel("at c:\\SRV\\publisher data\\ENV\\pkg\\m.malloy")).toBe(
         "at m.malloy",
      );
   });

   it("finds the package as a file URL, encoded or not, with the drive's colon plain or encoded", () => {
      expect(
         rel("at file:///C:/srv/Publisher%20Data/env/pkg/sub/m.malloy:2"),
      ).toBe("at sub/m.malloy:2");
      expect(rel("at file:///C:/srv/Publisher Data/env/pkg/m.malloy")).toBe(
         "at m.malloy",
      );
      expect(rel("at file:///c%3A/srv/Publisher%20Data/env/pkg/m.malloy")).toBe(
         "at m.malloy",
      );
   });

   it("names the directory itself by its name, and leaves a sibling alone", () => {
      expect(rel(`manifest for ${root} does not exist`)).toBe(
         "manifest for pkg does not exist",
      );
      const sibling = `${root}2\\m.malloy`;
      expect(rel(`at ${sibling}`)).toBe(`at ${sibling}`);
   });
});

describe("callerVisibleText", () => {
   const pkg = "/srv/data/env/pkg";

   it("names the package's files package-relative and any other path as outside it", () => {
      expect(
         callerVisibleText("FILE: file:///srv/data/env/pkg/m.malloy", pkg),
      ).toBe("FILE: m.malloy");
      expect(
         callerVisibleText(
            "FILE: file:///srv/data/env/q/x.malloy\nline 1",
            pkg,
         ),
      ).toBe(`FILE: ${OUTSIDE_PACKAGE}\nline 1`);
      expect(
         callerVisibleText("ENOENT: open '/etc/conf/other/x.json'", pkg),
      ).toBe(`ENOENT: open '${OUTSIDE_PACKAGE}'`);
      expect(callerVisibleText("at C:\\Users\\me\\x.malloy", pkg)).toBe(
         `at ${OUTSIDE_PACKAGE}`,
      );
   });

   it("leaves relative paths, URLs and arithmetic as they are", () => {
      const text =
         "a / b, x/y/z, ../q/x.malloy, 1/2/3, https://h.com/a/b, @2024-01-01";
      expect(callerVisibleText(text, pkg)).toBe(text);
   });
});

describe("rewriteErrorMessage", () => {
   it("rewrites a writable message", () => {
      const error = new Error("at /srv/a/b");
      rewriteErrorMessage(error, withoutAbsolutePaths);
      expect(error.message).toBe(`at ${OUTSIDE_PACKAGE}`);
   });

   it("leaves an error whose message cannot be written, rather than throwing", () => {
      const error = Object.freeze(new Error("at /srv/a/b"));
      expect(() =>
         rewriteErrorMessage(error, withoutAbsolutePaths),
      ).not.toThrow();
      expect(error.message).toBe("at /srv/a/b");
   });
});
