// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { afterEach, beforeEach, describe, expect, it } from "bun:test";
import type { ModelDef } from "@malloydata/malloy";
import type { DataStyles } from "@malloydata/render";
import * as fs from "fs";
import * as os from "os";
import * as path from "path";
import { fileURLToPath, pathToFileURL } from "url";
import {
   compileOrder,
   dataStylesForModel,
   importReadOrder,
   jobFiles,
   ModelCacheView,
   modelFileTexts,
   urlKey,
} from "./job_files";

describe("importReadOrder", () => {
   it("lists a file's imports before descending into them, each URL once", () => {
      // root imports a then b; a imports c; b imports c and a.
      const tree = { a: { c: {} }, b: { c: {}, a: { c: {} } } };
      expect(importReadOrder(tree)).toEqual(["a", "b", "c"]);
   });

   it("reads one import's whole subtree before the next import", () => {
      // root imports b then c; b imports d, which imports e; c imports f.
      const tree = { b: { d: { e: {} } }, c: { f: {} } };
      expect(importReadOrder(tree)).toEqual(["b", "c", "d", "e", "f"]);
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
      // root imports a then b, a imports c. Malloy asks for a and b together,
      // then reads a's import c, so c wins over b.
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
   it("finds a file's styles whether its URL is spelled encoded or decoded", () => {
      // Files are read, so stored, under the encoded URL; `dependencies`
      // names them decoded.
      const files = filesWithStyles({
         "file:///pkg/my%20models/b%C3%A4se.malloy": {
            k: { renderer: "bar_chart" },
         },
      });
      expect(
         dataStylesForModel(files, "file:///pkg/root.malloy", {
            "file:///pkg/my models/bäse.malloy": {},
         }),
      ).toEqual({ k: { renderer: "bar_chart" } });
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
   const reader = { readURL: async () => "" };
   const cached = (name: string, url: string) => ({
      modelDef: modelDef(name),
      invalidationKeys: { [url]: "k" },
   });
   /** A load's files whose import scan found `imported`. */
   const filesImporting = (...imported: string[]) => {
      const files = jobFiles({ readURL: async () => "" });
      for (const url of imported) files.imported.add(urlKey(url));
      return files;
   };

   it("serves every stored import but never the model's own file", async () => {
      const own = "file:///pkg/own.malloy";
      const dep = "file:///pkg/dep.malloy";
      const files = filesImporting(own, dep);
      const writer = new ModelCacheView(files, "file:///pkg/other.malloy");
      await writer.setCachedModelDef(own, cached("own", own));
      await writer.setCachedModelDef(dep, cached("dep", dep));
      const view = new ModelCacheView(files, own);
      expect(await view.getCachedModelDef(reader, own)).toBeUndefined();
      expect((await view.getCachedModelDef(reader, dep))?.modelDef.name).toBe(
         "dep",
      );
      expect(files.stats).toEqual({ stored: 2, reused: 1 });
   });

   it("keeps only package models another package file imports", async () => {
      const dep = "file:///pkg/dep.malloy";
      const files = filesImporting(dep);
      const view = new ModelCacheView(files, "file:///pkg/index.malloy");
      // A root nothing imports, and a file that is not a package model, are
      // not kept: the first is never read back, and each importer translates
      // the second itself, so each reports its problems.
      expect(
         await view.setCachedModelDef(
            "file:///pkg/index.malloy",
            cached("index", "file:///pkg/index.malloy"),
         ),
      ).toBe(false);
      expect(
         await view.setCachedModelDef(
            "file:///shared/x.malloy",
            cached("x", "file:///shared/x.malloy"),
         ),
      ).toBe(false);
      expect(await view.setCachedModelDef(dep, cached("dep", dep))).toBe(true);
      expect([...files.models.keys()]).toEqual([dep]);
   });

   it("keeps each translation under the spelling Malloy gave it", async () => {
      // A model compiled as itself is translated, and stored, under its
      // encoded URL; an importer translates and asks for it decoded. The two
      // translations carry different source IDs and locations, so the first
      // must not be served for the second.
      const encoded = "file:///pkg/my%20models/b%C3%A4se.malloy";
      const decoded = "file:///pkg/my models/bäse.malloy";
      const files = filesImporting(decoded);
      const owner = new ModelCacheView(files, encoded);
      expect(
         await owner.setCachedModelDef(encoded, cached("as itself", encoded)),
      ).toBe(true);
      // Its own file stays its own in either spelling.
      expect(await owner.getCachedModelDef(reader, decoded)).toBeUndefined();
      const first = new ModelCacheView(files, "file:///pkg/a.malloy");
      expect(await first.getCachedModelDef(reader, decoded)).toBeUndefined();
      // The first importer's translation of the import is stored decoded, and
      // that is what a later importer gets.
      await first.setCachedModelDef(decoded, cached("as an import", decoded));
      const later = new ModelCacheView(files, "file:///pkg/b.malloy");
      expect(
         (await later.getCachedModelDef(reader, decoded))?.modelDef.name,
      ).toBe("as an import");
      expect(files.stats).toEqual({ stored: 2, reused: 1 });
   });

   it("shares no object between compiles: it keeps a copy and hands out copies", async () => {
      const dep = "file:///pkg/dep.malloy";
      const files = filesImporting(dep);
      const writer = new ModelCacheView(files, dep);
      const own = modelDef("dep");
      await writer.setCachedModelDef(dep, {
         modelDef: own,
         invalidationKeys: { [dep]: "k" },
      });
      // The model's own result is worked on after its compile.
      own.name = "changed after the compile";

      const first = new ModelCacheView(files, "file:///pkg/a.malloy");
      const second = new ModelCacheView(files, "file:///pkg/b.malloy");
      const seen = await first.getCachedModelDef(reader, dep);
      expect(seen?.modelDef.name).toBe("dep");
      // An importer writes into what it imports.
      seen!.modelDef.name = "written by an importer";
      expect((await second.getCachedModelDef(reader, dep))?.modelDef.name).toBe(
         "dep",
      );
   });
});

describe("modelFileTexts", () => {
   it("gives a model only its own file and the files it imports, in either URL spelling", () => {
      const files = jobFiles({ readURL: async () => "" });
      const root = pathToFileURL("/pkg/root.malloy").href;
      const dep = pathToFileURL("/pkg/my models/bäse.malloy").href;
      files.texts.set(root, "root text");
      files.texts.set(dep, "dep text");
      files.texts.set(pathToFileURL("/pkg/other.malloy").href, "unrelated");
      // `dependencies` names the file decoded.
      const texts = modelFileTexts(files, root, { [decodeURI(dep)]: {} });
      expect(
         [...texts.entries()].map(([p, t]) => [path.basename(p), t]),
      ).toEqual([
         ["root.malloy", "root text"],
         ["bäse.malloy", "dep text"],
      ]);
   });
});

describe("a load's translations, through a real compile", () => {
   const UUID = /[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}/g;

   it("reuses a translation whose path has a space and a non-ASCII letter, and compiles each model as without the cache", async () => {
      const { DuckDBConnection } = await import("@malloydata/db-duckdb");
      const { FixedConnectionMap, MalloyConfig, Runtime } = await import(
         "@malloydata/malloy"
      );
      const dir = fs.mkdtempSync(path.join(os.tmpdir(), "publisher-reuse-"));
      const duckdb = new DuckDBConnection("duckdb", ":memory:", dir);
      try {
         fs.mkdirSync(path.join(dir, "my models"));
         fs.writeFileSync(
            path.join(dir, "my models", "bäse.malloy"),
            `source: base is duckdb.sql("select 1 as x")\n`,
         );
         for (const name of ["a", "b"]) {
            fs.writeFileSync(
               path.join(dir, `${name}.malloy`),
               `import "my models/bäse.malloy"\nsource: ${name} is base\n`,
            );
         }
         const malloyConfig = new MalloyConfig({ connections: {} });
         malloyConfig.wrapConnections(
            () =>
               new FixedConnectionMap(new Map([["duckdb", duckdb]]), "duckdb"),
         );
         const disk = {
            readURL: async (url: URL) =>
               fs.promises.readFile(fileURLToPath(url), "utf8"),
         };
         const files = jobFiles(disk);
         const modelPaths = ["a.malloy", "b.malloy", "my models/bäse.malloy"];
         await compileOrder(dir, modelPaths, files.reader, files.imported);
         const compile = async (rel: string, cached: boolean) => {
            const url = pathToFileURL(path.join(dir, rel));
            const runtime = new Runtime({
               urlReader: cached ? files.reader : disk,
               config: malloyConfig,
               cacheManager: cached
                  ? new ModelCacheView(files, url.toString())
                  : undefined,
            });
            const model = await runtime.loadModel(url).getModel();
            return JSON.stringify(model._modelDef).replace(UUID, "<uuid>");
         };
         // The imported file first, as compileOrder schedules it.
         await compile("my models/bäse.malloy", true);
         const a = await compile("a.malloy", true);
         const b = await compile("b.malloy", true);
         // base as itself (encoded), then a's translation of it as an import
         // (decoded), which b reuses.
         expect(files.stats).toEqual({ stored: 2, reused: 1 });
         expect(a).toBe(await compile("a.malloy", false));
         expect(b).toBe(await compile("b.malloy", false));
      } finally {
         await duckdb.close();
         fs.rmSync(dir, { recursive: true, force: true });
      }
   });
});

describe("compileOrder", () => {
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

   /** Following the waits from any model ends: no model waits on itself. */
   function expectNoWaitLoop(waitsOn: Map<string, string[]>): void {
      const reaches = (
         from: string,
         to: string,
         seen = new Set<string>(),
      ): boolean =>
         (waitsOn.get(from) ?? []).some(
            (d) =>
               d === to ||
               (!seen.has(d) && (seen.add(d), reaches(d, to, seen))),
         );
      for (const model of waitsOn.keys()) {
         expect(reaches(model, model)).toBe(false);
      }
   }

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
      const { levels, waitsOn } = await compileOrder(dir, modelPaths, reader);
      expect(levels).toEqual([
         ["base.malloy"],
         ["a.malloy"],
         ["sub/b.malloy"],
         ["index.malloy"],
         ["nb.malloynb"],
      ]);
      expect(waitsOn.get("index.malloy")).toEqual(["a.malloy", "sub/b.malloy"]);
      expect(waitsOn.get("sub/b.malloy")).toEqual(["a.malloy", "base.malloy"]);
      // A notebook compiles without the cache, so it waits for nothing.
      expect(waitsOn.get("nb.malloynb")).toEqual([]);
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
      const { levels, waitsOn } = await compileOrder(dir, modelPaths, reader);
      expect(levels.flat().sort()).toEqual([...modelPaths].sort());
      expectNoWaitLoop(waitsOn);
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
      const { levels, waitsOn } = await compileOrder(dir, modelPaths, reader);
      // Every model once, and the one importing into the cycle after it.
      expect(levels.flat().sort()).toEqual([...modelPaths].sort());
      const levelOf = (p: string) => levels.findIndex((l) => l.includes(p));
      expect(levelOf("into.malloy")).toBeGreaterThan(levelOf("a.malloy"));
      expect(waitsOn.get("into.malloy")).toEqual(["a.malloy"]);
      expect(waitsOn.get("self.malloy")).toEqual([]);
      expectNoWaitLoop(waitsOn);
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
      const { levels, waitsOn } = await compileOrder(
         dir,
         ["a.malloy", "b.malloy", "base.malloy"],
         reader,
         imported,
      );
      expect(levels).toEqual([["base.malloy"], ["b.malloy"], ["a.malloy"]]);
      // Each waits for its own imports only.
      expect(waitsOn.get("a.malloy")).toEqual(["b.malloy", "base.malloy"]);
      expect(waitsOn.get("b.malloy")).toEqual(["base.malloy"]);
      expect(waitsOn.get("base.malloy")).toEqual([]);
      // a is imported by nothing, so its translation is not kept.
      expect([...imported].map((u) => path.basename(u)).sort()).toEqual([
         "b.malloy",
         "base.malloy",
      ]);
   });

   it("reads an import keyword in any case, as Malloy does", async () => {
      write("base.malloy", "source: base is duckdb.sql('select 1 as a')\n");
      write("a.malloy", 'IMPORT "base.malloy"\nsource: a is base\n');
      const { waitsOn } = await compileOrder(
         dir,
         ["a.malloy", "base.malloy"],
         reader,
      );
      expect(waitsOn.get("a.malloy")).toEqual(["base.malloy"]);
   });

   it("keeps an unreadable file's place for its compile to report", async () => {
      const url = pathToFileURL(path.join(dir, "gone.malloy"));
      await expect(reader.readURL(url)).rejects.toThrow();
      expect((await compileOrder(dir, ["gone.malloy"], reader)).levels).toEqual(
         [["gone.malloy"]],
      );
   });
});
