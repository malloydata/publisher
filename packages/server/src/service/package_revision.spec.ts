import { afterEach, describe, expect, it } from "bun:test";
import fs from "fs";
import os from "os";
import path from "path";
import { computeSourceContentSha } from "./package_revision";

describe("computeSourceContentSha", () => {
   const dirs: string[] = [];

   afterEach(() => {
      for (const dir of dirs) {
         fs.rmSync(dir, { recursive: true, force: true });
      }
      dirs.length = 0;
   });

   it("is stable across path order and changes when bytes change", () => {
      const dir = fs.mkdtempSync(path.join(os.tmpdir(), "pkg-rev-"));
      dirs.push(dir);
      fs.writeFileSync(path.join(dir, "a.malloy"), "source: orders is table('t')");
      fs.writeFileSync(path.join(dir, "b.malloy"), "source: items is table('i')");

      const first = computeSourceContentSha(dir, ["b.malloy", "a.malloy"]);
      const second = computeSourceContentSha(dir, ["a.malloy", "b.malloy"]);
      expect(first).toBe(second);

      fs.writeFileSync(path.join(dir, "a.malloy"), "source: orders is table('u')");
      expect(computeSourceContentSha(dir, ["a.malloy", "b.malloy"])).not.toBe(
         first,
      );
   });
});
