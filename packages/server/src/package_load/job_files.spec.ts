// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { afterEach, beforeEach, describe, expect, it } from "bun:test";
import {
   CacheManager,
   InMemoryModelCache,
   type ModelDef,
} from "@malloydata/malloy";
import type { DataStyles } from "@malloydata/render";
import * as fs from "fs";
import * as os from "os";
import * as path from "path";
import { fileURLToPath, pathToFileURL } from "url";
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
   function filesWithStyles(styles: Record<string, DataStyles>) {
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
         root: { shared: { renderer: "list" }, own: { renderer: "list" } },
         b: { shared: { renderer: "bar_chart" } },
         c: { shared: { renderer: "line_chart" } },
      });
      expect(
         dataStylesForModel(files, "root", { a: { c: {} }, b: {} }),
      ).toEqual({
         shared: { renderer: "line_chart" },
         own: { renderer: "list" },
      });
   });

   it("applies a file imported twice where it was first read", () => {
      // leaf imports base then child, and child imports base again.
      const files = filesWithStyles({
         base: { shared: { renderer: "table" } },
         child: { shared: { renderer: "percent" } },
      });
      expect(
         dataStylesForModel(files, "leaf", { base: {}, child: { base: {} } }),
      ).toEqual({ shared: { renderer: "percent" } });
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

   it("shares no object between compiles: it keeps a copy and hands out copies", async () => {
      const shared = new CacheManager(new InMemoryModelCache());
      const writer = new ModelCacheView(shared, "file:///pkg/dep.malloy");
      const own = modelDef("dep");
      await writer.setCachedModelDef("file:///pkg/dep.malloy", {
         modelDef: own,
         invalidationKeys: keyed("file:///pkg/dep.malloy"),
      });
      // The model's own result is worked on after its compile.
      own.name = "changed after the compile";

      const first = new ModelCacheView(shared, "file:///pkg/a.malloy");
      const second = new ModelCacheView(shared, "file:///pkg/b.malloy");
      const seen = await first.getCachedModelDef(
         reader,
         "file:///pkg/dep.malloy",
      );
      expect(seen?.modelDef.name).toBe("dep");
      // An importer writes into what it imports.
      seen!.modelDef.name = "written by an importer";
      expect(
         (await second.getCachedModelDef(reader, "file:///pkg/dep.malloy"))
            ?.modelDef.name,
      ).toBe("dep");
   });

   it("does not keep a model no package file imports, but keeps what it imports", async () => {
      const shared = new CacheManager(new InMemoryModelCache());
      const root = new ModelCacheView(
         shared,
         "file:///pkg/index.malloy",
         false,
      );
      expect(
         await root.setCachedModelDef("file:///pkg/index.malloy", {
            modelDef: modelDef("index"),
            invalidationKeys: keyed("file:///pkg/index.malloy"),
         }),
      ).toBe(false);
      await root.setCachedModelDef("file:///pkg/dep.malloy", {
         modelDef: modelDef("dep"),
         invalidationKeys: keyed("file:///pkg/dep.malloy"),
      });
      expect(
         await shared.getCachedModelDef(reader, "file:///pkg/index.malloy"),
      ).toBeUndefined();
      expect(
         (await shared.getCachedModelDef(reader, "file:///pkg/dep.malloy"))
            ?.modelDef.name,
      ).toBe("dep");
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
      readURL: async (url: URL) =>
         fs.promises.readFile(fileURLToPath(url), "utf8"),
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

   it("orders a longer cycle, a self-import and a model importing into the cycle without looping", async () => {
      write("a.malloy", 'import "b.malloy"\n');
      write("b.malloy", 'import "c.malloy"\n');
      write("c.malloy", 'import "a.malloy"\n');
      write("into.malloy", 'import "a.malloy"\n');
      write("self.malloy", 'import "self.malloy"\n');
      const modelPaths = [
         "a.malloy",
         "b.malloy",
         "c.malloy",
         "into.malloy",
         "self.malloy",
      ];
      const levels = await compileLevels(dir, modelPaths, reader);
      // Every model once, and the one importing into the cycle after it.
      expect(levels.flat().sort()).toEqual([...modelPaths].sort());
      const levelOf = (p: string) => levels.findIndex((l) => l.includes(p));
      expect(levelOf("into.malloy")).toBeGreaterThan(levelOf("a.malloy"));
   });

   it("puts a file imported directly and through another import before both importers", async () => {
      // a imports b and base; b imports base.
      write("base.malloy", "source: base is duckdb.sql('select 1 as x')\n");
      write("b.malloy", 'import "base.malloy"\nsource: b is base\n');
      write(
         "a.malloy",
         'import "b.malloy"\nimport "base.malloy"\nsource: a is b\n',
      );
      const imported = new Set<string>();
      expect(
         await compileLevels(
            dir,
            ["a.malloy", "b.malloy", "base.malloy"],
            reader,
            imported,
         ),
      ).toEqual([["base.malloy"], ["b.malloy"], ["a.malloy"]]);
      // a is imported by nothing, so its translation is not kept.
      expect([...imported].map((u) => path.basename(u)).sort()).toEqual([
         "b.malloy",
         "base.malloy",
      ]);
   });

   it("keeps an unreadable file's place for its compile to report", async () => {
      const url = pathToFileURL(path.join(dir, "gone.malloy"));
      await expect(reader.readURL(url)).rejects.toThrow();
      expect(await compileLevels(dir, ["gone.malloy"], reader)).toEqual([
         ["gone.malloy"],
      ]);
   });
});
