// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * The server's one report tag reader, applied the three ways the server
 * applies it, over one case table the SDK's reader shares
 * (`reportLayoutParity.json`): to a compiled file's own notes (discovery), to
 * the same text uncompiled (a file that failed to compile, the write path, the
 * compile check), and as the layout the lint lints a file as. Every case has
 * one answer, and all three must give it.
 */
import { DuckDBConnection } from "@malloydata/db-duckdb";
import {
   FixedConnectionMap,
   InMemoryURLReader,
   Runtime,
   type ModelDef,
} from "@malloydata/malloy";
import { afterAll, beforeAll, describe, expect, it } from "bun:test";
import * as fs from "fs";
import * as path from "path";
import { factsReportTag, readDashboardModelFacts } from "./dashboard";
import { lintNotebookText } from "./notebook_lint";
import { readReportTagOfText, type ReportTag } from "./report";

type Layout = "grid" | "linear" | null;

const { cases } = JSON.parse(
   fs.readFileSync(
      path.resolve(
         __dirname,
         "../../../sdk/src/components/DashboardBuilder/testing/reportLayoutParity.json",
      ),
      "utf8",
   ),
) as {
   cases: { name: string; source: string; expected: Layout }[];
};

const MODEL_PATH = "report.malloy";
const URL_OF = `file:///parity/${MODEL_PATH}`;

let duckdb: DuckDBConnection;
let connections: FixedConnectionMap;

beforeAll(() => {
   duckdb = new DuckDBConnection("duckdb", ":memory:");
   connections = new FixedConnectionMap(
      new Map([["duckdb", duckdb]]),
      "duckdb",
   );
});

afterAll(async () => {
   await duckdb.close();
});

const layoutOf = (tag: ReportTag): Layout =>
   tag.status === "report" ? tag.layout : null;

/** The reader over the compiled file's own notes, as discovery applies it. */
async function compiledLayout(source: string): Promise<Layout> {
   const runtime = new Runtime({
      urlReader: new InMemoryURLReader(new Map([[URL_OF, source]])),
      connections,
   });
   const model = await runtime.loadModel(new URL(URL_OF)).getModel();
   const modelDef = (model as unknown as { _modelDef: ModelDef })._modelDef;
   return layoutOf(
      factsReportTag(readDashboardModelFacts(MODEL_PATH, modelDef, [])),
   );
}

/**
 * The layout the lint lints a file as, observed from outside: two probe lines
 * whose findings differ by layout. A cell-format (linear, no tiles) report
 * suggests the `##(markdown) …` spelling; any other names its page, a
 * dashboard or a notebook. A file the lint does not read as a report has no
 * findings for them at all.
 */
function lintLayout(source: string): Layout {
   const findings = lintNotebookText(
      MODEL_PATH,
      `${source}\n##(markdown)probe\n##(markdown) probe two\n`,
   );
   const spacing = findings.find(
      (f) =>
         f.code === "notebook-block-opener-spacing" &&
         f.message.includes("probe"),
   );
   if (!spacing) return null;
   if (spacing.message.includes("Did you mean")) return "linear";
   const unnamed = findings.find(
      (f) =>
         f.code === "notebook-markdown-block-unnamed" &&
         f.message.includes("probe two"),
   );
   if (unnamed?.message.includes("a dashboard does not show")) return "grid";
   if (unnamed?.message.includes("a notebook does not show")) return "linear";
   throw new Error(`the lint probe read nothing: ${JSON.stringify(findings)}`);
}

describe("report layout parity: one reader, applied every way the server reads a file", () => {
   for (const testCase of cases) {
      it(testCase.name, async () => {
         expect({
            compiled: await compiledLayout(testCase.source),
            text: layoutOf(readReportTagOfText(testCase.source)),
            lint: lintLayout(testCase.source),
         }).toEqual({
            compiled: testCase.expected,
            text: testCase.expected,
            lint: testCase.expected,
         });
      });
   }
});
