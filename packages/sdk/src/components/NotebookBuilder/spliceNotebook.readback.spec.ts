// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { afterEach, describe, expect, it, mock } from "bun:test";
import * as splicer from "../DashboardBuilder/spliceDocument";
import * as locator from "../DashboardBuilder/legacyNotebook";

// No edit the writer accepts reaches these gates, so the checks are made to disagree; disarmed, both wrappers are the real thing.
const realLocator = { ...locator };
const realSplicer = { ...splicer };
let tamperReadBack:
   | ((result: locator.NotebookSourceResult) => locator.NotebookSourceResult)
   | undefined;
let breakSyntax = false;
let reads = 0;
mock.module("../DashboardBuilder/legacyNotebook", () => ({
   ...realLocator,
   readNotebookSource: async (text: string) => {
      const result = await realLocator.readNotebookSource(text);
      return tamperReadBack && ++reads === 2 ? tamperReadBack(result) : result;
   },
}));
mock.module("../DashboardBuilder/spliceDocument", () => ({
   ...realSplicer,
   syntaxErrors: async (text: string) =>
      breakSyntax ? ["a planted syntax error"] : realSplicer.syntaxErrors(text),
}));

afterEach(() => {
   tamperReadBack = undefined;
   breakSyntax = false;
   reads = 0;
});

const TEXT =
   '## artifact { kind=notebook }\nsource: a is duckdb.sql("select 1 as x")\n\n##(markdown) Prose.\n\nrun: a -> { select: x }\n';
const EDIT = {
   cells: [
      { id: "0", kind: "definition" as const },
      {
         id: "1",
         kind: "markdown" as const,
         markdown: "Edited.",
      },
      { id: "2", kind: "query" as const },
   ],
};

describe("spliceNotebookDocument: the verification gates", () => {
   it("refuses a result with a syntax error", async () => {
      const { spliceNotebookDocument } = await import("./spliceNotebook");
      breakSyntax = true;
      expect(await spliceNotebookDocument(TEXT, EDIT)).toEqual({
         ok: false,
         reason: expect.stringContaining(
            "a notebook Malloy cannot parse, so it was not written (a planted syntax error)",
         ),
      });
   });

   it("refuses a result the locator cannot read back", async () => {
      const { spliceNotebookDocument } = await import("./spliceNotebook");
      tamperReadBack = () => ({ ok: false, refused: "Line 1: planted." });
      expect(await spliceNotebookDocument(TEXT, EDIT)).toEqual({
         ok: false,
         reason: expect.stringContaining(
            "cannot be read back, so it was not written. Line 1: planted.",
         ),
      });
   });

   it("refuses a result that does not read back as the cells asked for", async () => {
      const { spliceNotebookDocument } = await import("./spliceNotebook");
      tamperReadBack = (result) => {
         if (!result.ok) return result;
         result.source.cells[1] = {
            ...result.source.cells[1],
            markdown: "Not what was asked for.",
         };
         return result;
      };
      expect(await spliceNotebookDocument(TEXT, EDIT)).toEqual({
         ok: false,
         reason: expect.stringContaining(
            "did not read back as the cells asked for",
         ),
      });
   });
});
