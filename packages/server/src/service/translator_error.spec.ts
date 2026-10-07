// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * The translator's plain Error on an author's text (`d ~ @2025`) is a compile
 * problem on every path that compiles, not a server fault. This file covers
 * the classifier and `Model.create`, the in-process load path; the worker
 * load path is in package_worker_path.spec.ts and `/compile` at each scope is
 * in curated_compile_errors.integration.spec.ts.
 */
import { DuckDBConnection } from "@malloydata/db-duckdb";
import { type Connection, MalloyError } from "@malloydata/malloy";
import { afterAll, beforeAll, describe, expect, it } from "bun:test";
import * as fs from "fs";
import * as os from "os";
import * as path from "path";

import { ModelCompilationError } from "../errors";
import { Model } from "./model";
import { translatorMalloyError } from "./translator_error";

const HINT = "Use `=` to match the whole year, month or day";

describe("translator errors", () => {
   let dir: string;
   let duckdb: DuckDBConnection;

   beforeAll(() => {
      dir = fs.mkdtempSync(path.join(os.tmpdir(), "translator-error-"));
      duckdb = new DuckDBConnection("duckdb", ":memory:");
   });

   afterAll(async () => {
      await duckdb.close();
      fs.rmSync(dir, { recursive: true, force: true });
   });

   it("loads a model whose query compares a date to @2025 with ~ as a compile error carrying the hint", async () => {
      fs.writeFileSync(
         path.join(dir, "tilde.malloy"),
         `source: s is duckdb.sql("select DATE '2025-03-01' as d") extend {
  measure: n is count()
}
run: s -> { where: d ~ @2025 aggregate: n }
`,
      );
      const model = await Model.create(
         "pkg",
         dir,
         "tilde.malloy",
         new Map<string, Connection>([["duckdb", duckdb]]),
      );
      const error = model.getCompilationError();
      expect(error).toBeInstanceOf(ModelCompilationError);
      expect(error?.message).toBe(
         "Malloy could not compile this query: mysterious error in range " +
            "computation. This comes from comparing a date or timestamp to a " +
            "date literal such as @2025 with `~`, which Malloy cannot compile. " +
            "Use `=` to match the whole year, month or day " +
            "(`order_date = @2025`), or an explicit range " +
            "(`order_date ? @2025-01-01 to @2026-01-01`).",
      );
   });

   it("leaves an Error that did not come from the translator alone", () => {
      const refused = new Error("connect ECONNREFUSED 127.0.0.1:5432");
      expect(translatorMalloyError(refused)).toBeUndefined();
      expect(translatorMalloyError("not an error")).toBeUndefined();
      const compileError = new MalloyError("x", []);
      expect(translatorMalloyError(compileError)).toBeUndefined();
   });

   it("reads an Error thrown from the translator as one problem", () => {
      const thrown = new Error("mysterious error in range computation");
      thrown.stack =
         "Error: mysterious error in range computation\n" +
         "    at apply (/app/node_modules/@malloydata/malloy/dist/lang/ast/expressions/range.js:54:19)";
      const error = translatorMalloyError(thrown);
      expect(error).toBeInstanceOf(MalloyError);
      expect(error?.problems).toHaveLength(1);
      expect(error?.problems[0].code).toBe("translator-error");
      expect(error?.problems[0].message).toContain(HINT);
   });
});
