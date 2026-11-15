// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { afterAll, beforeAll, describe, expect, it } from "bun:test";
import * as fs from "fs";
import * as os from "os";
import * as path from "path";
import { pathToFileURL } from "url";
import { IMPORT_OUTSIDE_PACKAGE_MESSAGE, readPackageURL } from "./utils";

/**
 * The package URL reader: a model reads its own package's files and nothing
 * else, and what it is told about a file outside is the same whether or not
 * that file exists.
 *
 *   <tmp>/env/pkg/              the package
 *   <tmp>/env/pkg/models/a.malloy, models/sub/b.malloy, top.malloy
 *   <tmp>/env/other/x.malloy    a sibling package
 *   <tmp>/env/pkg/out -> ../other          a link leading out
 *   <tmp>/env/pkg/gone -> ../missing.malloy  a dangling link leading out
 *   <tmp>/env/pkg/alias -> models          a link that stays inside
 *   <tmp>/mount -> env/pkg                 the package mounted as a link (watch mode)
 */
let tmp = "";
let pkg = "";
const url = (p: string) => pathToFileURL(p);

beforeAll(() => {
   tmp = fs.mkdtempSync(path.join(os.tmpdir(), "package-url-reader-"));
   pkg = path.join(tmp, "env", "pkg");
   fs.mkdirSync(path.join(pkg, "models", "sub"), { recursive: true });
   fs.mkdirSync(path.join(tmp, "env", "other"), { recursive: true });
   fs.writeFileSync(path.join(pkg, "top.malloy"), "top");
   fs.writeFileSync(path.join(pkg, "models", "a.malloy"), "a");
   fs.writeFileSync(path.join(pkg, "models", "sub", "b.malloy"), "b");
   fs.writeFileSync(path.join(tmp, "env", "other", "x.malloy"), "secret");
   fs.symlinkSync(path.join("..", "other"), path.join(pkg, "out"));
   fs.symlinkSync(path.join("..", "missing.malloy"), path.join(pkg, "gone"));
   fs.symlinkSync("models", path.join(pkg, "alias"));
   fs.symlinkSync(path.join("env", "pkg"), path.join(tmp, "mount"));
});

afterAll(() => {
   fs.rmSync(tmp, { recursive: true, force: true });
});

/** The message `readPackageURL` rejects with, or "READ:<text>" when it reads. */
async function outcome(root: string, target: URL): Promise<string> {
   try {
      return `READ:${await readPackageURL(root, target)}`;
   } catch (error) {
      return (error as Error).message;
   }
}

describe("readPackageURL", () => {
   it("reads in-package files, including a ../ that stays inside", async () => {
      expect(await outcome(pkg, url(path.join(pkg, "top.malloy")))).toBe(
         "READ:top",
      );
      // models/sub/b.malloy importing "../a.malloy"
      const fromSub = new URL(
         "../a.malloy",
         url(path.join(pkg, "models/sub/b.malloy")),
      );
      expect(await outcome(pkg, fromSub)).toBe("READ:a");
      const toRoot = new URL(
         "../../top.malloy",
         url(path.join(pkg, "models/sub/b.malloy")),
      );
      expect(await outcome(pkg, toRoot)).toBe("READ:top");
   });

   it("reads through a symlink inside the package that stays inside", async () => {
      expect(await outcome(pkg, url(path.join(pkg, "alias", "a.malloy")))).toBe(
         "READ:a",
      );
   });

   it("reads a package mounted as a symlink, as watch mode mounts it", async () => {
      const mount = path.join(tmp, "mount");
      expect(
         await outcome(mount, url(path.join(mount, "models", "a.malloy"))),
      ).toBe("READ:a");
      // Either spelling of the root, against either spelling of the file.
      expect(await outcome(mount, url(path.join(pkg, "top.malloy")))).toBe(
         "READ:top",
      );
      expect(await outcome(pkg, url(path.join(mount, "top.malloy")))).toBe(
         "READ:top",
      );
   });

   it("refuses a ../ that leaves the package, for a sibling package's file", async () => {
      const escape = new URL(
         "../other/x.malloy",
         url(path.join(pkg, "top.malloy")),
      );
      expect(await outcome(pkg, escape)).toBe(IMPORT_OUTSIDE_PACKAGE_MESSAGE);
      // From inside a mount too: <tmp>/mount/../env/other/x.malloy exists.
      const mount = path.join(tmp, "mount");
      const fromMount = new URL(
         "../env/other/x.malloy",
         url(path.join(mount, "top.malloy")),
      );
      expect(await outcome(mount, fromMount)).toBe(
         IMPORT_OUTSIDE_PACKAGE_MESSAGE,
      );
   });

   it("refuses an absolute path the same way whether or not it exists", async () => {
      const there = await outcome(
         pkg,
         url(path.join(tmp, "env", "other", "x.malloy")),
      );
      const absent = await outcome(
         pkg,
         url(path.join(tmp, "env", "other", "nope.malloy")),
      );
      const noDir = await outcome(pkg, url("/definitely/not/here.malloy"));
      const etc = await outcome(pkg, url("/etc/hosts"));
      for (const message of [there, absent, noDir, etc]) {
         expect(message).toBe(IMPORT_OUTSIDE_PACKAGE_MESSAGE);
      }
   });

   it("refuses a symlink inside the package that leads out, dangling or not", async () => {
      expect(await outcome(pkg, url(path.join(pkg, "out", "x.malloy")))).toBe(
         IMPORT_OUTSIDE_PACKAGE_MESSAGE,
      );
      expect(
         await outcome(pkg, url(path.join(pkg, "out", "nope.malloy"))),
      ).toBe(IMPORT_OUTSIDE_PACKAGE_MESSAGE);
      expect(await outcome(pkg, url(path.join(pkg, "gone")))).toBe(
         IMPORT_OUTSIDE_PACKAGE_MESSAGE,
      );
   });

   it("refuses any scheme but file:, even where it would read as a path inside the package", async () => {
      expect(await outcome(pkg, new URL("https://example.com/x.malloy"))).toBe(
         IMPORT_OUTSIDE_PACKAGE_MESSAGE,
      );
      // Read as a path, a non-file URL resolves against the working
      // directory; with the package there, it would land inside it.
      expect(
         await outcome(process.cwd(), new URL("https://example.com/x.malloy")),
      ).toBe(IMPORT_OUTSIDE_PACKAGE_MESSAGE);
   });

   it("refuses everything when the package root is gone", async () => {
      expect(
         await outcome(
            path.join(tmp, "env", "vanished"),
            url(path.join(pkg, "top.malloy")),
         ),
      ).toBe(IMPORT_OUTSIDE_PACKAGE_MESSAGE);
   });

   it("fails a missing in-package file as missing, not as outside", async () => {
      const message = await outcome(
         pkg,
         url(path.join(pkg, "models", "nope.malloy")),
      );
      expect(message).toContain("ENOENT");
      expect(message).not.toBe(IMPORT_OUTSIDE_PACKAGE_MESSAGE);
   });
});
