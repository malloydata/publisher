// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { afterEach, beforeEach, describe, expect, it } from "bun:test";
import {
   CacheManager,
   InMemoryModelCache,
   type ModelDef,
} from "@malloydata/malloy";
import * as fs from "fs";
import * as os from "os";
import * as path from "path";
import { pathToFileURL } from "url";
import {
   compileLevels,
   dataStylesForModel,
   importReadOrder,
   jobFiles,
   ModelCacheView,
} from "./job_files";

describe("importReadOrder", () => {
   it("lists a dependency tree level by level, in import order, each URL once", () => {
      // root imports a then b; a imports c; b imports c and a.
      const tree = { a: { c: {} }, b: { c: {}, a: { c: {} } } };
      expect(importReadOrder(tree)).toEqual(["a", "b", "c"]);
   });

   it("reads nothing from a model with no imports", () => {
      expect(importReadOrder({})).toEqual([]);
      expect(importReadOrder(undefined)).toEqual([]);
   });
});

describe("dataStylesForModel", () => {
   function filesWithStyles(styles: Record<string, object>) {
      const files = jobFiles({ readURL: async () => "" });
      for (const [url, s] of Object.entries(styles)) {
         files.stylesByUrl.set(url, s);
      }
      return files;
   }

   it("lets a file read later win, as a reader accumulating its reads did", () => {
      // root imports a then b, a imports c. Malloy reads a, b, then c, so
      // c wins over b although a depth-first walk would visit c first.
      const files = filesWithStyles({
         root: { shared: { renderer: "root" }, own: { renderer: "root" } },
         b: { shared: { renderer: "b" } },
         c: { shared: { renderer: "c" } },
      });
      expect(
         dataStylesForModel(files, "root", { a: { c: {} }, b: {} }),
      ).toEqual({
         shared: { renderer: "c" },
         own: { renderer: "root" },
      });
   });

   it("applies a file imported twice where it was first read", () => {
      // leaf imports base then child, and child imports base again.
      const files = filesWithStyles({
         base: { shared: { renderer: "base" } },
         child: { shared: { renderer: "child" } },
      });
      expect(
         dataStylesForModel(files, "leaf", { base: {}, child: { base: {} } }),
      ).toEqual({ shared: { renderer: "child" } });
   });
});

describe("jobFiles", () => {
   it("reads a URL once for any number of concurrent askers, keeping its text and styles", async () => {
      const reads: string[] = [];
      const files = jobFiles({
         readURL: async (url: URL) => {
            reads.push(url.toString());
            if (url.pathname.endsWith(".json")) {
               return '{"x":{"renderer":"bar_chart"}}';
            }
            return "--! styles s.json\nsource: s is duckdb.sql('select 1')\n";
         },
      });
      const url = new URL("file:///pkg/m.malloy");
      const texts = await Promise.all([
         files.reader.readURL(url),
         files.reader.readURL(url),
         files.reader.readURL(url),
      ]);
      expect(new Set(texts).size).toBe(1);
      expect(reads.filter((r) => r === url.toString())).toHaveLength(1);
      expect(files.texts.get(url.toString())).toBe(texts[0]);
      expect(files.stylesByUrl.get(url.toString())).toEqual({
         x: { renderer: "bar_chart" },
      });
   });

   it("does not keep a failed read, so the next asker retries it", async () => {
      let calls = 0;
      const files = jobFiles({
         readURL: async () => {
            calls += 1;
            if (calls === 1) throw new Error("transient");
            return "source: s is duckdb.sql('select 1')";
         },
      });
      const url = new URL("file:///pkg/m.malloy");
      await expect(files.reader.readURL(url)).rejects.toThrow("transient");
      expect(await files.reader.readURL(url)).toContain("source: s");
      expect(calls).toBe(2);
   });
});

describe("ModelCacheView", () => {
   const modelDef = (name: string) =>
      ({
         name,
         exports: [],
         contents: {},
         queryList: [],
         dependencies: {},
      }) as unknown as ModelDef;
   // A fixed invalidation key, so a cached entry stays valid.
   const reader = {
      readURL: async () => "",
      getInvalidationKey: async () => "k",
   };
   const keyed = (url: string) => ({ [url]: "k" });

   it("serves every import from the shared cache but never the model's own file", async () => {
      const shared = new CacheManager(new InMemoryModelCache());
      await shared.setCachedModelDef("file:///pkg/own.malloy", {
         modelDef: modelDef("own"),
         invalidationKeys: keyed("file:///pkg/own.malloy"),
      });
      await shared.setCachedModelDef("file:///pkg/dep.malloy", {
         modelDef: modelDef("dep"),
         invalidationKeys: keyed("file:///pkg/dep.malloy"),
      });
      const view = new ModelCacheView(shared, "file:///pkg/own.malloy");
      expect(
         await view.getCachedModelDef(reader, "file:///pkg/own.malloy"),
      ).toBeUndefined();
      expect(
         (await view.getCachedModelDef(reader, "file:///pkg/dep.malloy"))
            ?.modelDef.name,
      ).toBe("dep");
   });

   it("writes what it translates to the shared cache", async () => {
      const shared = new CacheManager(new InMemoryModelCache());
      const view = new ModelCacheView(shared, "file:///pkg/own.malloy");
      await view.setCachedModelDef("file:///pkg/own.malloy", {
         modelDef: modelDef("own"),
         invalidationKeys: keyed("file:///pkg/own.malloy"),
      });
      expect(
         (await shared.getCachedModelDef(reader, "file:///pkg/own.malloy"))
            ?.modelDef.name,
      ).toBe("own");
   });
});

describe("compileLevels", () => {
   let dir: string;

   beforeEach(() => {
      dir = fs.mkdtempSync(path.join(os.tmpdir(), "publisher-levels-"));
   });

   afterEach(() => {
      fs.rmSync(dir, { recursive: true, force: true });
   });

   function write(rel: string, text: string): void {
      fs.mkdirSync(path.dirname(path.join(dir, rel)), { recursive: true });
      fs.writeFileSync(path.join(dir, rel), text);
   }

   const reader = {
      readURL: async (url: URL) => fs.promises.readFile(url.pathname, "utf8"),
   };

   it("puts every file a model imports in an earlier level, and notebooks last", async () => {
      write("base.malloy", "source: base is duckdb.sql('select 1 as a')\n");
      write("a.malloy", 'import "base.malloy"\nsource: a is base\n');
      write(
         "sub/b.malloy",
         "import { a } from '../a.malloy'\nimport \"../base.malloy\"\nsource: b is a\n",
      );
      write(
         "index.malloy",
         '// import "nothing.malloy"\nimport "a.malloy"\nimport "sub/b.malloy"\nimport "https://example.com/x.malloy"\nimport "missing.malloy"\n',
      );
      write("nb.malloynb", '>>>malloy\nimport "index.malloy"\n');
      const modelPaths = [
         "index.malloy",
         "nb.malloynb",
         "sub/b.malloy",
         "a.malloy",
         "base.malloy",
      ];
      const levels = await compileLevels(dir, modelPaths, reader);
      expect(levels).toEqual([
         ["base.malloy"],
         ["a.malloy"],
         ["sub/b.malloy"],
         ["index.malloy"],
         ["nb.malloynb"],
      ]);
   });

   it("compiles every model of an import cycle or an unreadable file once", async () => {
      write("c1.malloy", 'import "c2.malloy"\n');
      write("c2.malloy", 'import "c1.malloy"\n');
      write("solo.malloy", "source: s is duckdb.sql('select 1')\n");
      const modelPaths = [
         "c1.malloy",
         "c2.malloy",
         "solo.malloy",
         "gone.malloy",
      ];
      const levels = await compileLevels(dir, modelPaths, reader);
      expect(levels.flat().sort()).toEqual([...modelPaths].sort());
   });

   it("keeps an unreadable file's place for its compile to report", async () => {
      const url = pathToFileURL(path.join(dir, "gone.malloy"));
      await expect(reader.readURL(url)).rejects.toThrow();
      expect(await compileLevels(dir, ["gone.malloy"], reader)).toEqual([
         ["gone.malloy"],
      ]);
   });
});
