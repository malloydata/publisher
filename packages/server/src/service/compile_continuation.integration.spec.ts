// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * The append-scope gate against a REAL SHIPPED PACKAGE, end to end.
 *
 * The gate compiles caller text; the compile that follows runs
 * `${modelContent}\n${source}`. While those were different units, a fragment
 * that is a syntax error ON ITS OWN -- but a continuation of the model's last
 * statement once concatenated -- was classified by nothing, passed the gate,
 * and ran unrestricted. That reopened the whole point of the gate at the
 * DEFAULT scope: a compile-time `DESCRIBE` against a caller-named file, whose
 * error text distinguishes a file that exists from one that does not and names
 * the columns of whatever it reads.
 *
 * This suite is deliberately not written against a synthetic fixture. The
 * precondition the attack needs is "the model's last statement can be
 * continued", and whether that holds is a property of the models we actually
 * ship: `examples/storefront/storefront.malloy` ends in a `query:` block and
 * `examples/governed-analytics/internal.malloy` ends in a bare
 * `duckdb.table(...)`. A fixture written here would be one the author chose to
 * be vulnerable; these are the files customers run.
 *
 * Both halves of the contract are asserted, because a gate that refuses
 * everything would pass a refusal-only suite: the hostile fragments are
 * refused, and an ordinary fragment against the same package still compiles
 * AND returns real rows from DuckDB.
 */

import { afterAll, beforeAll, describe, expect, it } from "bun:test";
import fs from "fs/promises";
import os from "os";
import path from "path";
import { CompileRefusedError } from "../errors";
import { Environment } from "./environment";

/** The tracked example packages, not the gitignored `publisher_data/` copy. */
const EXAMPLES = path.resolve(import.meta.dir, "../../../../examples");
const STOREFRONT = path.join(EXAMPLES, "storefront");
const GOVERNED = path.join(EXAMPLES, "governed-analytics");

let rootDir: string;
let env: Environment;
/** A file outside the package. Learning whether it exists is the leak. */
let secretPath: string;
let absentPath: string;

/** Copies a shipped example package into the environment under test. */
async function installExample(
   name: string,
   sourceDir: string,
): Promise<void> {
   await env.installPackage(name, async (stagingPath) => {
      await fs.cp(sourceDir, stagingPath, { recursive: true });
   });
}

beforeAll(async () => {
   rootDir = await fs.mkdtemp(path.join(os.tmpdir(), "publisher-continuation-"));
   secretPath = path.join(rootDir, "secret.csv");
   absentPath = path.join(rootDir, "no-such-file.csv");
   await fs.writeFile(secretPath, "ssn,holder\n111-11-1111,alice\n");
   const envPath = path.join(rootDir, "env");
   await fs.mkdir(envPath, { recursive: true });
   env = await Environment.create("examples", envPath, []);
   await installExample("storefront", STOREFRONT);
   await installExample("governed-analytics", GOVERNED);
});

afterAll(async () => {
   await fs.rm(rootDir, { recursive: true, force: true }).catch(() => {});
});

const compileAppend = (
   pkg: string,
   modelPath: string,
   source: string,
): Promise<{ problems: unknown[] }> =>
   env.compileSource(pkg, modelPath, source, false, undefined, "append") as
      Promise<{ problems: unknown[] }>;

/** The error a compile threw, or a failure when it unexpectedly succeeded. */
async function refusalFor(
   pkg: string,
   modelPath: string,
   source: string,
): Promise<Error> {
   try {
      await compileAppend(pkg, modelPath, source);
   } catch (error) {
      return error as Error;
   }
   throw new Error(
      `Expected the compile to be refused, but it succeeded: ${source}`,
   );
}

describe("append scope on a shipped package refuses a continuation fragment", () => {
   /**
    * The payload. It opens with ` extend {`, which is a syntax error standing
    * alone -- which is exactly why the gate used to see nothing to classify --
    * and continues `storefront.malloy`'s final statement once concatenated.
    */
   const continuationReading = (file: string) =>
      ` extend { join_cross: s is duckdb.sql("SELECT * FROM read_csv('${file}')") }\n` +
      `run: order_items -> { group_by: s.ssn }`;

   it("refuses a raw-SQL read smuggled in as a continuation", async () => {
      const error = await refusalFor(
         "storefront",
         "storefront.malloy",
         continuationReading(secretPath),
      );
      expect(error).toBeInstanceOf(CompileRefusedError);
   });

   /**
    * The oracle, and the reason refusing is not enough on its own: the two
    * answers have to be INDISTINGUISHABLE. Unrestricted, the present file
    * compiles clean and the absent one reports DuckDB's "No files found that
    * match the pattern", which answers "does this path exist" for any path.
    */
   it("cannot distinguish an existing file from a missing one", async () => {
      const present = await refusalFor(
         "storefront",
         "storefront.malloy",
         continuationReading(secretPath),
      );
      const absent = await refusalFor(
         "storefront",
         "storefront.malloy",
         continuationReading(absentPath),
      );
      expect(present.message).toBe(absent.message);
      expect(present.message).not.toContain("No files found");
      expect(present.message).not.toContain(secretPath);
   });

   it("does not name the columns of a file it was pointed at", async () => {
      const error = await refusalFor(
         "storefront",
         "storefront.malloy",
         continuationReading(secretPath),
      );
      expect(error.message).not.toContain("ssn");
      expect(error.message).not.toContain("holder");
   });

   /**
    * `internal.malloy` ends in a bare `duckdb.table(...)`, so its last
    * statement is continuable in a different shape from storefront's. The
    * precondition is a property of the shipped models, so more than one is
    * worth pinning.
    */
   it("refuses the same shape against governed-analytics", async () => {
      const error = await refusalFor(
         "governed-analytics",
         "internal.malloy",
         ` extend { join_cross: s is duckdb.sql("SELECT * FROM read_csv('${secretPath}')") }\n` +
            `run: orders_base -> { group_by: s.ssn }`,
      );
      expect(error).toBeInstanceOf(CompileRefusedError);
   });

   /**
    * A standalone raw-SQL fragment was already refused before this change.
    * Kept as the control: it shows the continuation cases above are testing the
    * new parse-unit behaviour rather than re-testing the original gate.
    */
   it("still refuses the standalone form the gate always caught", async () => {
      const error = await refusalFor(
         "storefront",
         "storefront.malloy",
         `run: duckdb.sql("SELECT * FROM read_csv('${secretPath}')") -> { group_by: ssn }`,
      );
      expect(error).toBeInstanceOf(CompileRefusedError);
   });
});

describe("append scope on a shipped package still serves ordinary work", () => {
   /**
    * The half a refusal-only suite cannot see. If the gate simply refused every
    * fragment it would pass every test above, so an ordinary fragment has to
    * compile clean against the same real package.
    */
   it("compiles a fragment over the package's own published source", async () => {
      const { problems } = await compileAppend(
         "storefront",
         "storefront.malloy",
         "run: order_items -> { aggregate: n is count() }",
      );
      expect(problems).toEqual([]);
   });

   /**
    * And the query the fragment describes actually runs, against the package's
    * real DuckDB data. A compile that is clean but unrunnable would be a
    * regression this suite should catch rather than report as success.
    */
   it("runs that query against the package's real data", async () => {
      const pkg = await env.getPackage("storefront", false);
      const model = await pkg.getModel("storefront.malloy");
      expect(model).toBeDefined();
      const result = await model!.getQueryResults(
         undefined,
         undefined,
         "run: order_items -> { aggregate: n is count() }",
         {},
         true,
      );
      const rows = result.compactResult as unknown as { n: number }[];
      expect(rows.length).toBeGreaterThan(0);
      expect(Number(rows[0].n)).toBeGreaterThan(0);
   });

   /**
    * An ordinary semantic error is still the CALLER'S diagnostic, not a
    * refusal. The parse-failure branch must not swallow the everyday authoring
    * loop: a misspelled field has a parsed tree, so its constructs were
    * classified and the absence of a rejection is real evidence.
    */
   it("reports a misspelled field as a diagnostic rather than refusing", async () => {
      const { problems } = await compileAppend(
         "storefront",
         "storefront.malloy",
         "run: order_items -> { group_by: no_such_column }",
      );
      expect(problems.length).toBeGreaterThan(0);
   });

   /**
    * Positions stay relative to the CONCATENATED file. Four doc surfaces state
    * this ("a line in your text lands after the model's own line count"), so a
    * change to what the gate parses must not quietly move them.
    */
   it("keeps diagnostic positions past the model's own line count", async () => {
      const modelText = await fs.readFile(
         path.join(rootDir, "env", "storefront", "storefront.malloy"),
         "utf8",
      );
      const modelLines = modelText.split("\n").length;
      const { problems } = await compileAppend(
         "storefront",
         "storefront.malloy",
         "run: order_items -> { group_by: no_such_column }",
      );
      const withPosition = (problems as { at?: { range?: { start?: { line?: number } } } }[])
         .map((p) => p.at?.range?.start?.line)
         .filter((line): line is number => typeof line === "number");
      expect(withPosition.length).toBeGreaterThan(0);
      for (const line of withPosition) {
         expect(line).toBeGreaterThanOrEqual(modelLines - 1);
      }
   });
});
