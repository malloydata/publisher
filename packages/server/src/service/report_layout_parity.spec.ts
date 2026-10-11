// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * The server's three readers of a report's layout, and its three detectors of
 * a query-level `# artifact`, run over one case table the SDK's reader shares
 * (`reportLayoutParity.json`). A case whose readers disagree must say so in
 * `knownDifference`, so a refactor that unifies them cannot change an answer
 * unnoticed, and a new disagreement cannot creep in.
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
import {
   factsCarryArtifactTag,
   factsLayout,
   ownQueries,
   readDashboardModelFacts,
} from "./dashboard";
import { lintNotebookText } from "./notebook_lint";
import {
   layoutFactsOfText,
   layoutOfText,
   queryNotesCarryArtifact,
   textClaimsToBeAReport,
} from "./report";

type Layout = "grid" | "linear" | null;

const { layoutCases, queryTagCases } = JSON.parse(
   fs.readFileSync(
      path.resolve(
         __dirname,
         "../../../sdk/src/components/DashboardBuilder/testing/reportLayoutParity.json",
      ),
      "utf8",
   ),
) as {
   layoutCases: {
      name: string;
      source: string;
      compiled: Layout;
      text: Layout;
      lint: Layout;
      sdk: Layout | "refused";
      knownDifference?: string;
   }[];
   queryTagCases: {
      name: string;
      source: string;
      compiled: boolean;
      text: boolean;
      lint: boolean;
      knownDifference?: string;
   }[];
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

async function compiledFacts(source: string) {
   const runtime = new Runtime({
      urlReader: new InMemoryURLReader(new Map([[URL_OF, source]])),
      connections,
   });
   const model = await runtime.loadModel(new URL(URL_OF)).getModel();
   const modelDef = (model as unknown as { _modelDef: ModelDef })._modelDef;
   return readDashboardModelFacts(MODEL_PATH, modelDef, []);
}

/** The compiled-notes reader, as discovery applies it. */
async function compiledLayout(source: string): Promise<Layout> {
   const facts = await compiledFacts(source);
   return factsCarryArtifactTag(facts) ? factsLayout(facts) : null;
}

/** The raw-text reader, as discovery applies it to a file with no facts. */
const textLayout = (source: string): Layout =>
   textClaimsToBeAReport(source) ? layoutOfText(source) : null;

/**
 * The layout the lint lints a file as, observed from outside: two probe lines
 * whose findings differ by layout. A cell-format (linear, no tiles) report
 * suggests the `##(markdown) …` spelling; any other names its page, a
 * dashboard or a notebook. A file the lint does not read as a report has no
 * findings at all.
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

describe("report layout parity: the server's readers", () => {
   for (const testCase of layoutCases) {
      it(testCase.name, async () => {
         const answers = {
            compiled: await compiledLayout(testCase.source),
            text: textLayout(testCase.source),
            lint: lintLayout(testCase.source),
         };
         expect(answers).toEqual({
            compiled: testCase.compiled,
            text: testCase.text,
            lint: testCase.lint,
         });
         const all = [
            testCase.compiled,
            testCase.text,
            testCase.lint,
            testCase.sdk,
         ];
         // A disagreement is only ever a documented one.
         expect(new Set(all).size > 1).toBe(
            testCase.knownDifference !== undefined,
         );
      });
   }
});

describe("report layout parity: the query-level `# artifact` detectors", () => {
   for (const testCase of queryTagCases) {
      it(testCase.name, async () => {
         const facts = await compiledFacts(testCase.source);
         const answers = {
            compiled: ownQueries(facts).some((query) =>
               queryNotesCarryArtifact(query.annotations),
            ),
            text: layoutFactsOfText(testCase.source).queryTag,
            // With no model-level tag, the lint reads the file only for a query-level one.
            lint: lintLayout(testCase.source) !== null,
         };
         expect(answers).toEqual({
            compiled: testCase.compiled,
            text: testCase.text,
            lint: testCase.lint,
         });
         const all = [testCase.compiled, testCase.text, testCase.lint];
         expect(new Set(all).size > 1).toBe(
            testCase.knownDifference !== undefined,
         );
      });
   }

   it("is read only where no model-level tag is (the lint's probe needs it)", () => {
      for (const { source } of queryTagCases)
         expect(layoutFactsOfText(source).modelTag).toBe(false);
   });
});
