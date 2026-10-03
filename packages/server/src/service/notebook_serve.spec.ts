// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * Identity, discovery and listing of served notebooks (`notebooks/*.malloy`
 * with a model-level `## artifact` note), driven through `Package.create` on
 * the package-load worker pool, the way a real load and reload run.
 */
import {
   afterAll,
   afterEach,
   beforeAll,
   beforeEach,
   describe,
   expect,
   it,
} from "bun:test";
import * as fs from "fs";
import * as os from "os";
import * as path from "path";
import {
   PackageLoadPool,
   __setPackageLoadPoolForTests,
} from "../package_load/package_load_pool";
import { Model } from "./model";
import { Package } from "./package";
import {
   ANY_ARTIFACT_NOTE,
   artifactKindInText,
   artifactNoteLine,
   claimsToBeANotebook,
   docNotesAboveArtifact,
   documentKind,
   hasArtifactLineOutsideBlocks,
   isArtifactNoteText,
   isDocumentModelPath,
   isNotebookModelPath,
} from "./notebook";

const ORIGINAL_ENV = process.env.PACKAGE_LOAD_WORKERS;

const note = (text: string, line: number) => ({
   text,
   at: {
      url: "file:///x.malloy",
      range: {
         start: { line, character: 0 },
         end: { line, character: text.length },
      },
   },
});

describe("notebook predicates", () => {
   it("admits only a .malloy directly under notebooks/", () => {
      expect(isNotebookModelPath("notebooks/a.malloy")).toBe(true);
      expect(isNotebookModelPath("notebooks/a.malloynb")).toBe(false);
      expect(isNotebookModelPath("notebooks/sub/a.malloy")).toBe(false);
      expect(isNotebookModelPath("a/notebooks/a.malloy")).toBe(false);
      expect(isNotebookModelPath("dashboards/a.malloy")).toBe(false);
   });

   it("takes the kind from the tag and falls back to the folder", () => {
      expect(documentKind("dashboards/a.malloy", "notebook")).toBe("notebook");
      expect(documentKind("notebooks/a.malloy", "dashboard")).toBe("dashboard");
      expect(documentKind("notebooks/a.malloy", undefined)).toBe("notebook");
      expect(documentKind("dashboards/a.malloy", "text")).toBe("dashboard");
      expect(isDocumentModelPath("dashboards/a.malloy")).toBe(true);
      expect(isDocumentModelPath("notebooks/a.malloy")).toBe(true);
      expect(isDocumentModelPath("models/a.malloy")).toBe(false);
      expect(isDocumentModelPath("notebooks/sub/a.malloy")).toBe(false);
   });

   it("reads the top-level kind off the artifact line, not a tile entry's", () => {
      const text = (tag: string) => `${tag}\nrun: x`;
      expect(
         artifactKindInText(
            text("## artifact { kind=notebook tiles=[a { kind=text }] }"),
         ),
      ).toBe("notebook");
      expect(
         artifactKindInText(text("## artifact { tiles=[a { kind=text }] }")),
      ).toBeUndefined();
      expect(artifactKindInText("run: x")).toBeUndefined();
      expect(
         artifactKindInText('##|"\n## artifact { kind=notebook }\n|##\nrun: x'),
      ).toBeUndefined();
   });

   it("reads the artifact tag written as a ##| block", () => {
      const block =
         '##| artifact { kind=notebook\n  tiles=[\n    a { kind=text },\n    "s -> v"\n  ]\n}\n|##\nrun: x';
      expect(artifactKindInText(block)).toBe("notebook");
      expect(claimsToBeANotebook(block)).toBe(true);
      // The lexer takes the opener's whole line, so a same-line `|##` is tag text the tag cannot parse.
      const sameLine = "##| artifact { kind=dashboard } |##\n}\n|##\nrun: x";
      expect(claimsToBeANotebook(sameLine)).toBe(true);
      expect(artifactKindInText(sameLine)).toBeUndefined();
      expect(claimsToBeANotebook("##| artifacts\nprose\n|##\nrun: x")).toBe(
         false,
      );
      expect(isArtifactNoteText("##| artifact { kind=notebook }")).toBe(true);
   });

   it("reads an artifact property anywhere among the tag's properties", () => {
      const second = "## dashboard { columns=2 } artifact { kind=notebook }";
      expect(isArtifactNoteText(second)).toBe(true);
      expect(claimsToBeANotebook(`${second}\nrun: x`)).toBe(true);
      expect(artifactKindInText(`${second}\nrun: x`)).toBe("notebook");
      expect(
         artifactKindInText(
            "##| dashboard { columns=2 }\n  artifact { kind=notebook }\n|##\nrun: x",
         ),
      ).toBe("notebook");
      expect(isArtifactNoteText('## dashboard { title="artifact" }')).toBe(
         false,
      );
      expect(isArtifactNoteText("## dashboard { artifact {} }")).toBe(false);
      expect(isArtifactNoteText("## title=artifact")).toBe(false);
   });

   it("scans a megabyte of unclosed block openers in linear time", () => {
      const MB = 1024 * 1024;
      const flat = "##| x\n".repeat(MB / 6);
      let indented = "";
      for (let i = 0; indented.length < MB; i++)
         indented += `${" ".repeat(i % 64)}##| x\n`;
      for (const source of [flat, indented]) {
         const started = performance.now();
         expect(claimsToBeANotebook(source)).toBe(false);
         expect(hasArtifactLineOutsideBlocks(source, ANY_ARTIFACT_NOTE)).toBe(
            false,
         );
         expect(artifactKindInText(source)).toBeUndefined();
         expect(performance.now() - started).toBeLessThan(2000);
      }
   });

   it("locates the artifact note by its line, and only the ## form", () => {
      const notes = [
         note('##" above\n', 0),
         note("## artifact { kind=notebook }\n", 1),
         note('##" below\n', 3),
      ];
      expect(artifactNoteLine(notes)).toBe(1);
      expect(artifactNoteLine([note("## artifactual\n", 0)])).toBeUndefined();
      expect(artifactNoteLine([note("# artifact {}\n", 0)])).toBeUndefined();
   });

   it("keeps only the notes above the artifact line, and all when there is none", () => {
      const notes = [
         note('##" above\n', 0),
         note("## artifact {}\n", 1),
         note('##" below\n', 3),
      ];
      expect(docNotesAboveArtifact(notes)).toEqual(['##" above\n']);
      expect(docNotesAboveArtifact([notes[0], notes[2]])).toEqual([
         '##" above\n',
         '##" below\n',
      ]);
   });

   it('ignores an artifact line inside a ##|" block body', () => {
      expect(claimsToBeANotebook("## artifact {}\nrun: x")).toBe(true);
      expect(claimsToBeANotebook('##|"\n## artifact kinds\n|##\nrun: x')).toBe(
         false,
      );
      expect(
         claimsToBeANotebook('##|"\nprose\n|##\n## artifact {}\nrun: x'),
      ).toBe(true);
   });
});

describe("hasArtifactLineOutsideBlocks", () => {
   const TAG = /^##[ \t]*artifact\b/;

   it("splits on a bare CR and CRLF as well as LF", () => {
      expect(hasArtifactLineOutsideBlocks("a\r## artifact {}\rb", TAG)).toBe(
         true,
      );
      expect(
         hasArtifactLineOutsideBlocks("a\r\n## artifact {}\r\nb", TAG),
      ).toBe(true);
      expect(
         hasArtifactLineOutsideBlocks(
            '##|"\r## artifact kinds\r|##\rrun: x',
            TAG,
         ),
      ).toBe(false);
   });

   it("does not let an unterminated block hide a later artifact line", () => {
      expect(
         hasArtifactLineOutsideBlocks('##|"\nprose\n## artifact {}', TAG),
      ).toBe(true);
      expect(hasArtifactLineOutsideBlocks('##|"\nprose\nmore', TAG)).toBe(
         false,
      );
   });

   it("closes a block only where the lexer does: at the opener's column, and `|#` not as `|##`", () => {
      // A `|##` body line does not close a `#|` block, so the tag-like line after it stays prose.
      expect(
         hasArtifactLineOutsideBlocks(
            "#|(markdown)\n|## x\n## artifact {}\n|#\nrun: x",
            TAG,
         ),
      ).toBe(false);
      // A closer indented past the opener's column does not close it either.
      expect(
         hasArtifactLineOutsideBlocks(
            "##|(markdown)\n  |##\n## artifact {}\n|##\nrun: x",
            TAG,
         ),
      ).toBe(false);
      // Positive control: the closer at the opener's column ends the block.
      expect(
         hasArtifactLineOutsideBlocks(
            "#|(markdown)\nprose\n|#\n## artifact {}\nrun: x",
            TAG,
         ),
      ).toBe(true);
   });
});

describe("served notebooks (worker path)", () => {
   let tempDir: string;
   let pool: PackageLoadPool;

   beforeAll(async () => {
      process.env.PACKAGE_LOAD_WORKERS = "1";
      pool = new PackageLoadPool(1);
      await __setPackageLoadPoolForTests(pool);
   });

   afterAll(async () => {
      await __setPackageLoadPoolForTests(null);
      if (ORIGINAL_ENV === undefined) delete process.env.PACKAGE_LOAD_WORKERS;
      else process.env.PACKAGE_LOAD_WORKERS = ORIGINAL_ENV;
   });

   beforeEach(() => {
      tempDir = fs.mkdtempSync(path.join(os.tmpdir(), "publisher-nb-serve-"));
      fs.mkdirSync(path.join(tempDir, "notebooks"));
      fs.mkdirSync(path.join(tempDir, "dashboards"));
   });

   afterEach(() => {
      fs.rmSync(tempDir, { recursive: true, force: true });
   });

   async function makeMalloyConfig() {
      const { MalloyConfig, FixedConnectionMap } = await import(
         "@malloydata/malloy"
      );
      const { DuckDBConnection } = await import("@malloydata/db-duckdb");
      const duckdb = new DuckDBConnection("duckdb", ":memory:");
      const connections = new FixedConnectionMap(
         new Map([["duckdb", duckdb]]),
         "duckdb",
      );
      const malloyConfig = new MalloyConfig({ connections: {} });
      malloyConfig.wrapConnections(() => connections);
      return { malloyConfig, duckdb };
   }

   const write = (rel: string, text: string) =>
      fs.writeFileSync(path.join(tempDir, rel), text);
   const manifest = (extra: Record<string, unknown> = {}) =>
      write("publisher.json", JSON.stringify({ name: "pkg", ...extra }));

   const BASE = `source: base is duckdb.sql("select 1 as id, 'x' as label")\n`;

   async function withPackage(
      run: (
         pkg: Package,
         cfg: Awaited<ReturnType<typeof makeMalloyConfig>>,
      ) => Promise<void>,
   ) {
      const cfg = await makeMalloyConfig();
      try {
         const pkg = await Package.create(
            "env",
            "pkg",
            tempDir,
            cfg.malloyConfig,
         );
         await run(pkg, cfg);
      } finally {
         await cfg.duckdb.close();
      }
   }

   it("is a served notebook only with an artifact note, and stays a model", async () => {
      manifest();
      write(
         "notebooks/tagged.malloy",
         `## artifact { kind=notebook }\n${BASE}`,
      );
      write("notebooks/untagged.malloy", BASE);
      write("notebooks/nested.malloynb", ">>>markdown\n# x");
      await withPackage(async (pkg) => {
         expect(pkg.isServedNotebook("notebooks/tagged.malloy")).toBe(true);
         expect(pkg.isServedNotebook("notebooks/untagged.malloy")).toBe(false);
         const model = pkg.getModel("notebooks/tagged.malloy")!;
         expect(model.isNotebook()).toBe(true);
         expect(model.getModelType()).toBe("model");
         expect(pkg.getModel("notebooks/untagged.malloy")!.isNotebook()).toBe(
            false,
         );
      });
   });

   it("serves a notebook whose artifact property follows another property", async () => {
      manifest();
      write(
         "notebooks/second.malloy",
         `## dashboard { columns=2 } artifact { kind=notebook }\n${BASE}`,
      );
      write(
         "notebooks/titled.malloy",
         `## dashboard { title="artifact" }\n${BASE}`,
      );
      await withPackage(async (pkg) => {
         expect(pkg.isServedNotebook("notebooks/second.malloy")).toBe(true);
         expect(pkg.getModel("notebooks/second.malloy")!.isNotebook()).toBe(
            true,
         );
         expect(pkg.isServedNotebook("notebooks/titled.malloy")).toBe(false);
      });
   });

   it("excludes a served notebook from listModels with and without a surface", async () => {
      manifest();
      write("notebooks/nb.malloy", `## artifact {}\n${BASE}`);
      write("notebooks/plain.malloy", BASE);
      write("other.malloy", BASE);
      await withPackage(async (pkg) => {
         expect((await pkg.listModels()).map((m) => m.path).sort()).toEqual([
            "notebooks/plain.malloy",
            "other.malloy",
         ]);
         write("index.malloy", `import "other.malloy"\nexport { base }\n`);
         await pkg.reloadAllModels({});
         expect((await pkg.listModels()).map((m) => m.path)).toEqual([
            "index.malloy",
         ]);
      });
   });

   it("re-discovers on reload: gaining or losing the artifact note flips the flag", async () => {
      manifest();
      write("notebooks/gains.malloy", BASE);
      write("notebooks/loses.malloy", `## artifact {}\n${BASE}`);
      await withPackage(async (pkg) => {
         expect(pkg.getModel("notebooks/gains.malloy")!.isNotebook()).toBe(
            false,
         );
         expect(pkg.getModel("notebooks/loses.malloy")!.isNotebook()).toBe(
            true,
         );
         write("notebooks/gains.malloy", `## artifact {}\n${BASE}`);
         write("notebooks/loses.malloy", BASE);
         await pkg.reloadAllModels({});
         expect(pkg.getModel("notebooks/gains.malloy")!.isNotebook()).toBe(
            true,
         );
         expect(pkg.getModel("notebooks/loses.malloy")!.isNotebook()).toBe(
            false,
         );
      });
   });

   it("answers the notebook GET for a served notebook whose only statement is a definition", async () => {
      manifest();
      write("notebooks/nb.malloy", `## artifact {}\n${BASE}`);
      await withPackage(async (pkg) => {
         const raw = await pkg.getModel("notebooks/nb.malloy")!.getNotebook();
         expect(raw).toMatchObject({
            type: "notebook",
            format: "malloy",
            notebookCells: [
               { type: "code", kind: "definition", text: BASE.trimEnd() },
            ],
         });
      });
   });

   it("does not take an artifact line inside a block body for a dashboard", async () => {
      manifest();
      write(
         "dashboards/x.malloy",
         `## artifact { tiles=["base -> v"] }\n` +
            `source: base is duckdb.sql("select 1 as id") extend { view: v is { group_by: id } }\n`,
      );
      await withPackage(async (pkg) => {
         expect(pkg.listDashboards().map((d) => d.path)).toEqual([
            "dashboards/x.malloy",
         ]);
         write(
            "dashboards/x.malloy",
            `##|"\n## artifact kinds\n|##\nsource: oops is\n`,
         );
         await pkg.reloadAllModels({});
         expect(pkg.listDashboards()).toEqual([]);
      });
   });

   it("lists a notebook titled by each link of the chain, in order", async () => {
      manifest();
      write(
         "notebooks/a_titled.malloy",
         `##" Prose above\n## artifact { title="Explicit" }\n${BASE}`,
      );
      write(
         "notebooks/b_described.malloy",
         `##" First line\n##" second line\n## artifact {}\n${BASE}`,
      );
      write("notebooks/c_heading.malloy", `## artifact {}\n${BASE}`);
      write("notebooks/d_bare.malloy", `## artifact {}\n${BASE}`);
      await withPackage(async (pkg) => {
         pkg.getModel("notebooks/c_heading.malloy")!.setNotebookCells([
            { type: "markdown", text: "# Heading title\nbody" },
         ]);
         const listed = await pkg.listNotebooks();
         const by = (p: string) => listed.find((n) => n.path === p)!;
         expect(by("notebooks/a_titled.malloy")).toMatchObject({
            title: "Explicit",
            description: "Prose above",
         });
         expect(by("notebooks/b_described.malloy")).toMatchObject({
            title: "First line",
            description: "second line",
         });
         expect(by("notebooks/c_heading.malloy").title).toBe("Heading title");
         expect(by("notebooks/d_bare.malloy").title).toBeUndefined();
         expect(by("notebooks/d_bare.malloy").description).toBeUndefined();
      });
   });

   it("does not let prose below the artifact tag become the description", async () => {
      manifest();
      write(
         "notebooks/nb.malloy",
         `## artifact { title="T" }\n##(markdown) a markdown cell, not a description\n${BASE}`,
      );
      await withPackage(async (pkg) => {
         const [nb] = await pkg.listNotebooks();
         expect(nb.title).toBe("T");
         expect(nb.description).toBeUndefined();
      });
   });

   it("scopes a dashboard's description to the notes above its artifact tag", async () => {
      manifest();
      write(
         "dashboards/d.malloy",
         `##" Above the tag\n## artifact { tiles=["base -> v"] title="D" }\n##" Below the tag\n` +
            `source: base is duckdb.sql("select 1 as id") extend { view: v is { group_by: id } }\n`,
      );
      await withPackage(async (pkg) => {
         const dashboards = await pkg.listDashboards();
         expect(dashboards.map((d) => d.description)).toEqual([
            "Above the tag",
         ]);
      });
   });

   it("reads a dashboard's description from below the tag only when nothing is above it", async () => {
      manifest();
      const model = `source: base is duckdb.sql("select 1 as id") extend { view: v is { group_by: id } }\n`;
      write(
         "dashboards/legacy.malloy",
         `## artifact { tiles=["base -> v"] }\n##" Legacy title\n##" Legacy body\n${model}`,
      );
      write(
         "dashboards/blank_above.malloy",
         `##"\n## artifact { tiles=["base -> v"] title="T" }\n##" Below body\n${model}`,
      );
      write(
         "dashboards/current.malloy",
         `##" Current title\n##" Current body\n## artifact { tiles=["base -> v"] }\n##" Ignored below\n${model}`,
      );
      await withPackage(async (pkg) => {
         const by = (name: string) => pkg.getDashboard(name)!;
         expect(by("legacy")).toMatchObject({
            title: "Legacy title",
            description: "Legacy body",
         });
         expect(by("blank_above")).toMatchObject({
            title: "T",
            description: "Below body",
         });
         expect(by("current")).toMatchObject({
            title: "Current title",
            description: "Current body",
         });
      });
   });

   it("never reads a served notebook's description from below the tag, however little is above it", async () => {
      manifest();
      write(
         "notebooks/nb.malloy",
         `## artifact {}\n##(markdown) a markdown cell\n${BASE}`,
      );
      await withPackage(async (pkg) => {
         const [nb] = await pkg.listNotebooks();
         expect(nb.description).toBeUndefined();
         expect(nb.title).toBeUndefined();
      });
   });

   it("flags a served notebook listed in explores, as it flags a .malloynb", async () => {
      manifest({ explores: ["notebooks/nb.malloy"] });
      write("notebooks/nb.malloy", `## artifact {}\n${BASE}`);
      await withPackage(async (pkg) => {
         expect(pkg.getInvalidExplores().map((p) => p.entry)).toEqual([
            "notebooks/nb.malloy",
         ]);
      });
   });

   it("lists a notebook that failed to compile with its error, after a reload", async () => {
      manifest();
      write("notebooks/nb.malloy", `## artifact {}\n${BASE}`);
      await withPackage(async (pkg) => {
         write(
            "notebooks/nb.malloy",
            `## artifact {}\n##|"\n## artifact kinds\n|##\nsource: oops is\n`,
         );
         await pkg.reloadAllModels({});
         const listed = await pkg.listNotebooks();
         expect(listed.map((n) => n.path)).toEqual(["notebooks/nb.malloy"]);
         expect(listed[0].error).toBeTruthy();
         expect(pkg.isServedNotebook("notebooks/nb.malloy")).toBe(true);
      });
   });

   it("does not take a broken file for a notebook on a block body's prose alone", async () => {
      manifest();
      write("notebooks/nb.malloy", BASE);
      await withPackage(async (pkg) => {
         write(
            "notebooks/nb.malloy",
            `##|"\n## artifact kinds\n|##\nsource: oops is\n`,
         );
         await pkg.reloadAllModels({});
         expect(await pkg.listNotebooks()).toEqual([]);
      });
   });

   it("records the compiled text on the in-process load path", async () => {
      manifest();
      const text = `## artifact {}\n${BASE}`;
      write("notebooks/nb.malloy", text);
      const { malloyConfig, duckdb } = await makeMalloyConfig();
      try {
         const model = await Model.create(
            "pkg",
            tempDir,
            "notebooks/nb.malloy",
            malloyConfig,
         );
         expect(model.getCompiledSourceText()).toBe(text);
         model.setQueryBoundary({
            mode: "all",
            exploresDeclared: false,
            isQueryEntryPoint: true,
            notebook: true,
         });
         expect(await model.getNotebook()).toMatchObject({
            format: "malloy",
            notebookCells: [],
         });
      } finally {
         await duckdb.close();
      }
   });
});
