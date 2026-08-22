// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { afterEach, beforeEach, describe, expect, it } from "bun:test";
import fs from "fs/promises";
import os from "os";
import path from "path";
import { Environment } from "./environment";

// The compile scopes exist to close the gap between /compile and an LSP: the
// historical append-only behavior could validate NEW definitions but not an
// EDIT (resubmitting an existing definition collides with "Cannot redefine"),
// so the only way to validate the most common authoring action was to save
// first and learn from the reload. scope "file" validates the edit pre-save at
// true coordinates; scope "package" is a dry-run with reload's reach (imports
// across files) and none of its effects on the served model.

const BASE_MODEL = `source: base_source is duckdb.sql("select 1 as id, 5 as n") extend {
  measure: c is count()
}`;

const TRACKS_MODEL = `import "base.malloy"
source: tracks is base_source extend {
  measure: total is n.sum()
  view: v is { aggregate: total }
}`;

describe("compile scopes (compileSource)", () => {
   let rootDir: string;
   let env: Environment;

   beforeEach(async () => {
      rootDir = await fs.mkdtemp(path.join(os.tmpdir(), "publisher-scopes-"));
      const envPath = path.join(rootDir, "env");
      await fs.mkdir(envPath, { recursive: true });
      env = await Environment.create("testEnv", envPath, []);
      await env.installPackage("pkg", async (stagingPath) => {
         await fs.mkdir(stagingPath, { recursive: true });
         await fs.writeFile(
            path.join(stagingPath, "publisher.json"),
            '{"name":"pkg"}',
         );
         await fs.writeFile(path.join(stagingPath, "base.malloy"), BASE_MODEL);
         await fs.writeFile(
            path.join(stagingPath, "tracks.malloy"),
            TRACKS_MODEL,
         );
      });
   });

   afterEach(async () => {
      await fs.rm(rootDir, { recursive: true, force: true }).catch(() => {});
   });

   const compile = (
      modelPath: string,
      source: string | undefined,
      scope: "append" | "file" | "package",
      includeSql = false,
   ) =>
      env.compileSource("pkg", modelPath, source, includeSql, undefined, scope);

   // -- scope "file": the edit validator ----------------------------------

   it("file: validates an EDIT to an existing definition, which append cannot", async () => {
      const edited = TRACKS_MODEL.replace("n.sum()", "n.avg()");
      // The append behavior this scope exists to escape: the edit collides
      // with the model's own copy of every definition it touches.
      const appended = await compile("tracks.malloy", edited, "append");
      expect(
         appended.problems.some((p) => p.message.includes("Cannot redefine")),
      ).toBe(true);
      // At file scope the submitted text IS the file, so the edit just checks.
      const { problems } = await compile("tracks.malloy", edited, "file");
      expect(problems.filter((p) => p.severity === "error")).toEqual([]);
   });

   it("file: diagnostics land at true coordinates in the submitted text", async () => {
      // Error deliberately on 0-based line 2 of the submitted text. Append
      // scope would offset this by the on-disk file's line count; file scope
      // must not.
      const broken = `import "base.malloy"
source: tracks is base_source extend {
  measure: total is nope.sum()
}`;
      const { problems } = await compile("tracks.malloy", broken, "file");
      const error = problems.find((p) => p.severity === "error");
      expect(error?.message).toContain("nope");
      expect(
         (error as { at?: { range?: { start?: { line?: number } } } }).at?.range
            ?.start?.line,
      ).toBe(2);
      expect(error?.model).toBe("tracks.malloy");
   });

   it("file: a brand-new file compiles with package imports in scope", async () => {
      const { problems } = await compile(
         "analysis.malloy",
         `import "base.malloy"\nsource: analysis is base_source extend { measure: m is n.sum() }`,
         "file",
      );
      expect(problems.filter((p) => p.severity === "error")).toEqual([]);
   });

   // -- scope "package": the dry-run --------------------------------------

   it("package: catches cross-file breakage that file scope cannot see", async () => {
      // Rename base_source in the edit. The edited file is self-consistent, so
      // file scope is clean — but tracks.malloy imports the OLD name, so the
      // package dry-run (with the edit substituted for base.malloy) must
      // surface the importer's breakage. This is the import-graph reach that
      // previously required saving and reloading to discover.
      const renamed = BASE_MODEL.replace(/base_source/g, "base_renamed");
      const fileScoped = await compile("base.malloy", renamed, "file");
      expect(fileScoped.problems.filter((p) => p.severity === "error")).toEqual(
         [],
      );
      const { problems } = await compile("base.malloy", renamed, "package");
      const errors = problems.filter((p) => p.severity === "error");
      expect(errors.length).toBeGreaterThan(0);
      expect(errors.some((p) => p.model === "tracks.malloy")).toBe(true);
   });

   it("package: with no source, validates the files as saved", async () => {
      const clean = await compile("base.malloy", undefined, "package");
      expect(clean.problems.filter((p) => p.severity === "error")).toEqual([]);

      // Break a file on disk; the dry-run reports it, tagged with the file.
      await fs.writeFile(
         path.join(rootDir, "env", "pkg", "tracks.malloy"),
         TRACKS_MODEL.replace("n.sum()", "nope.sum()"),
      );
      const broken = await compile("base.malloy", undefined, "package");
      const errors = broken.problems.filter((p) => p.severity === "error");
      expect(errors.some((p) => p.model === "tracks.malloy")).toBe(true);
   });

   it("package: uses reload file selection for notebooks and dotfiles", async () => {
      await fs.mkdir(path.join(rootDir, "env", "pkg", ".git"), {
         recursive: true,
      });
      await fs.writeFile(
         path.join(rootDir, "env", "pkg", ".git", "ignored.malloy"),
         "this is not malloy",
      );
      await fs.writeFile(
         path.join(rootDir, "env", "pkg", "broken.malloynb"),
         `>>>malloy
##! experimental.givens
given:
  ROLE :: string is 'x'

#(authorize) id = 1 and $ROLE = 'x'
source: broken is duckdb.sql("select 1 as id") extend {
}`,
      );
      const { problems } = await compile(
         "unused.malloynb",
         undefined,
         "package",
      );
      expect(problems.some((p) => p.model === "broken.malloynb")).toBe(true);
      expect(problems.some((p) => p.model?.includes(".git"))).toBe(false);
   });

   it("package: warns when a replacement path is treated as new", async () => {
      const { problems } = await compile("Base.malloy", BASE_MODEL, "package");
      expect(
         problems.some(
            (p) =>
               p.severity === "warn" &&
               p.model === "Base.malloy" &&
               p.message.includes("did not replace another model"),
         ),
      ).toBe(true);
   });

   it("package: does not touch the served model", async () => {
      // The whole point vs. reload: after a dry-run over a broken what-if, the
      // served model still answers, and no reload happened (the package would
      // otherwise be marked stale by a failing one).
      const pkg = await env.getPackage("pkg");
      const servedBefore = pkg.getModel("tracks.malloy");
      await compile(
         "base.malloy",
         BASE_MODEL.replace(/base_source/g, "base_renamed"),
         "package",
      );
      expect(pkg.getModel("tracks.malloy")).toBe(servedBefore);
      const { result } = await pkg
         .getModel("tracks.malloy")!
         .getQueryResults("tracks", "v");
      expect(result.data).toBeDefined();
      expect(env.getFailedPackages().size).toBe(0);
   });

   it("package: reports identical problems once, not once per importing entry", async () => {
      // base.malloy is compiled directly AND through tracks.malloy's import;
      // its defect must appear once.
      await fs.writeFile(
         path.join(rootDir, "env", "pkg", "base.malloy"),
         BASE_MODEL.replace("n, 5", "n; 5"), // parse error in base
      );
      const { problems } = await compile("tracks.malloy", undefined, "package");
      const inBase = problems.filter(
         (p) => p.severity === "error" && p.model === "base.malloy",
      );
      const keys = new Set(
         inBase.map(
            (p) =>
               `${(p as { at?: { range?: { start?: { line?: number } } } }).at?.range?.start?.line}|${p.message}`,
         ),
      );
      expect(inBase.length).toBe(keys.size);
   });

   // -- validation ---------------------------------------------------------

   it("rejects a missing source at append and file scope, naming the fix", async () => {
      for (const scope of ["append", "file"] as const) {
         await expect(
            compile("tracks.malloy", undefined, scope),
         ).rejects.toThrow(
            `Compile scope "${scope}" requires a source to compile`,
         );
      }
   });

   it("rejects includeSql at package scope", async () => {
      await expect(
         compile("tracks.malloy", undefined, "package", true),
      ).rejects.toThrow(`includeSql is not available at scope "package"`);
   });

   it("rejects an unknown scope instead of consuming it", async () => {
      await expect(
         env.compileSource(
            "pkg",
            "tracks.malloy",
            "run: tracks -> v",
            false,
            undefined,
            "buffer" as never,
         ),
      ).rejects.toThrow(
         'Invalid compile scope "buffer": expected one of "append", "file", "package".',
      );
   });

   it("rejects a caller #(authorize) annotation at file scope too", async () => {
      // At file scope the submitted text replaces the file, so a caller gate
      // would displace the author's — same rejection as append.
      await expect(
         compile("tracks.malloy", `#(authorize) true\n${TRACKS_MODEL}`, "file"),
      ).rejects.toThrow(/authorize` annotation is not permitted/);
   });

   // -- append scope unchanged ---------------------------------------------

   it("append: still validates a NEW definition against the model", async () => {
      const { problems } = await compile(
         "tracks.malloy",
         "run: tracks -> { aggregate: c2 is total }",
         "append",
      );
      expect(problems.filter((p) => p.severity === "error")).toEqual([]);
   });
});
