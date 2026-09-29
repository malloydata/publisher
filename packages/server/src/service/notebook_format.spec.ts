// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * The Malloyyo notebook format needs no Malloy language change: prose is a doc
 * string on Malloy's built-in `"` route, as a `##"` line or a `##|"` … `|##`
 * block, and both are legal between any two statements. This spec pins that
 * claim against the compiler, so a Malloy upgrade that breaks it fails here
 * rather than in the notebook reader built on it.
 *
 * Fixtures: `tests/fixtures/notebooks-malloyyo/`. Each is compiled through
 * `Model.create`, the server's in-process compile path. The two rewritten
 * variants compile on a bare `Runtime` instead, because they assert problem
 * codes and `Model.create` wraps a compile failure in an error that keeps the
 * message but drops them.
 */
import { DuckDBConnection } from "@malloydata/db-duckdb";
import {
   Annotations,
   FixedConnectionMap,
   InMemoryURLReader,
   MalloyError,
   Runtime,
   type Connection,
   type ModelDef,
} from "@malloydata/malloy";
import { parseTag } from "@malloydata/malloy-tag";
import { afterAll, beforeAll, describe, expect, it } from "bun:test";
import * as fs from "fs";
import * as path from "path";
import { modelAnnotations, ownModelAnnotations } from "./annotations";
import { Model } from "./model";

const FIXTURE_DIR = path.resolve(
   __dirname,
   "../../tests/fixtures/notebooks-malloyyo",
);

/** A `"`-route note as the spec states it: 1-based file line, text prefix. */
interface ExpectedNote {
   line: number;
   textStartsWith: string;
}

/**
 * Every fixture and the `"`-route notes it owns, in file order. Written out
 * literally so a moved or reclassified note fails with a readable diff.
 */
const EXPECTED_OWN_NOTES: Record<string, ExpectedNote[]> = {
   "models/orders.malloy": [
      { line: 1, textStartsWith: '##" Shared orders model' },
   ],
   "notebooks/revenue_review.malloy": [
      { line: 5, textStartsWith: '##|"\n# Where revenue came from\n' },
      { line: 13, textStartsWith: '##" A single line of prose is a cell too.' },
   ],
   "notebooks/definitions_only.malloy": [],
   "notebooks/adjacent_blocks.malloy": [
      { line: 5, textStartsWith: '##|"\n## A heading\n' },
      { line: 9, textStartsWith: '##|"\nThe second block' },
   ],
   "notebooks/tagged_runs.malloy": [
      { line: 12, textStartsWith: '##" Trailing prose is a model note' },
   ],
   "notebooks/imported_prose.malloy": [
      { line: 2, textStartsWith: '##" Order totals, described above' },
      { line: 6, textStartsWith: '##" The only prose cell' },
   ],
   "dashboards/text_tiles.malloy": [
      { line: 1, textStartsWith: '##" A dashboard whose first tile is prose.' },
      { line: 5, textStartsWith: '##|" intro\n## How to read this page' },
   ],
};

/** `"`-route notes on one annotation bundle, sorted by where they start. */
function docStringNotes(
   annote: ReturnType<typeof ownModelAnnotations>,
): { line: number; text: string }[] {
   return new Annotations(annote)
      .forRoute('"')
      .map((note) => ({ line: note.at.range.start.line + 1, text: note.text }))
      .sort((a, b) => a.line - b.line);
}

describe("Malloyyo notebook format (compiler contract)", () => {
   let duckdb: DuckDBConnection;
   const models = new Map<string, Model>();

   beforeAll(async () => {
      // The include's `duckdb.table('data/orders.csv')` is package-root relative, like every fixture's.
      duckdb = new DuckDBConnection("duckdb", ":memory:", FIXTURE_DIR);
      for (const modelPath of Object.keys(EXPECTED_OWN_NOTES)) {
         const model = await Model.create(
            "notebooks-malloyyo",
            FIXTURE_DIR,
            modelPath,
            new Map<string, Connection>([["duckdb", duckdb]]),
         );
         models.set(modelPath, model);
      }
   });

   afterAll(async () => {
      await duckdb.close();
   });

   const defOf = (modelPath: string): ModelDef => {
      const def = models.get(modelPath)?.getModelDef();
      if (!def) throw new Error(`${modelPath} did not compile`);
      return def;
   };

   for (const [modelPath, expected] of Object.entries(EXPECTED_OWN_NOTES)) {
      describe(modelPath, () => {
         it("compiles with no errors", () => {
            // `Model.create` throws on a problem of error severity and keeps it here.
            expect(
               models.get(modelPath)?.getCompilationError(),
            ).toBeUndefined();
            expect(defOf(modelPath).modelID).toEndWith(modelPath);
         });

         it('puts each ##" line and ##|" block on the doc-string route at its own line, in file order', () => {
            const actual = docStringNotes(
               ownModelAnnotations(defOf(modelPath)),
            );
            expect(actual.map((note) => note.line)).toEqual(
               expected.map((note) => note.line),
            );
            actual.forEach((note, index) => {
               expect(note.text).toStartWith(expected[index].textStartsWith);
            });
         });
      });
   }

   it("keeps an imported include's prose out of a notebook's own notes", () => {
      const def = defOf("notebooks/imported_prose.malloy");
      const isSharedNote = (text: string) =>
         text.startsWith('##" Shared orders model');

      expect(
         docStringNotes(ownModelAnnotations(def)).filter((note) =>
            isSharedNote(note.text),
         ),
      ).toEqual([]);
      // Positive control: the folded lineage does reach the include's note, so
      // the exclusion above is the own-notes reader's doing.
      expect(
         new Annotations(modelAnnotations(def)).texts('"').some(isSharedNote),
      ).toBe(true);
   });

   it('attaches a #" caption and its render tags to the run below, not to the model', () => {
      const def = defOf("notebooks/tagged_runs.malloy");
      expect(
         new Annotations(ownModelAnnotations(def))
            .texts('"')
            .some((text) => text.includes("Revenue for each month")),
      ).toBe(false);

      const [captioned, bare] = def.queryList;
      const own = new Annotations(captioned.annotations);
      expect(own.texts('"')).toEqual(['#" Revenue for each month.\n']);
      expect(own.texts("").map((text) => text.trim())).toEqual([
         "# bar_chart",
         '# label="Revenue by month"',
      ]);
      expect(new Annotations(bare.annotations).texts()).toEqual([]);
   });

   it("keeps a named block's name on its opener line, where a reader can strip it", () => {
      const [, block] = docStringNotes(
         ownModelAnnotations(defOf("dashboards/text_tiles.malloy")),
      );
      expect(block.text).toStartWith('##|" intro\n');
   });

   it("parses a text tile entry and a quoted tile side by side in tiles=", () => {
      const [artifact] = new Annotations(
         ownModelAnnotations(defOf("dashboards/text_tiles.malloy")),
      ).forRoute("");
      const { tag, log } = parseTag(artifact.content);
      expect(log).toEqual([]);

      expect(tag.textArray("artifact", "tiles")).toEqual([
         "intro",
         "orders -> kpis",
      ]);
      const [intro] = tag.array("artifact", "tiles") ?? [];
      expect(intro.text("kind")).toBe("text");
      expect(intro.numeric("colspan")).toBe(12);
   });

   /** Compiles one fixture, rewritten, beside the real include on a bare `Runtime`. */
   const compileVariant = (
      fixture: string,
      rewrite: (text: string) => string,
   ) => {
      const root = "file:///nb/";
      const read = (file: string) =>
         fs.readFileSync(path.join(FIXTURE_DIR, file), "utf8");
      const text = read(fixture);
      const rewritten = rewrite(text);
      expect(rewritten).not.toBe(text);
      const runtime = new Runtime({
         urlReader: new InMemoryURLReader(
            new Map([
               [`${root}models/orders.malloy`, read("models/orders.malloy")],
               [`${root}${fixture}`, rewritten],
            ]),
         ),
         connections: new FixedConnectionMap(
            new Map([["duckdb", duckdb]]),
            "duckdb",
         ),
      });
      return runtime.loadModel(new URL(`${root}${fixture}`)).getModel();
   };

   it('refuses a trailing #" with nothing after it, which is why trailing prose is ##"', async () => {
      let error: unknown;
      try {
         await compileVariant("notebooks/tagged_runs.malloy", (text) =>
            text.replace('##" Trailing prose', '#" Trailing prose'),
         );
      } catch (caught) {
         error = caught;
      }
      expect(error).toBeInstanceOf(MalloyError);
      const errors = (error as MalloyError).problems.filter(
         (problem) => problem.severity === "error",
      );
      expect(errors.map((problem) => problem.code)).toContain(
         "orphaned-object-annotation",
      );
   });

   it("accepts a block opener of more than one word without complaint, so only lint can catch it", async () => {
      const model = await compileVariant(
         "dashboards/text_tiles.malloy",
         (text) => text.replace('##|" intro\n', '##|" intro extra\n'),
      );
      expect(model.problems).toEqual([]);
      const [, block] = docStringNotes(ownModelAnnotations(model._modelDef));
      expect(block.text).toStartWith('##|" intro extra\n');
   });
});
