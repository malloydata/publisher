// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * The conformance fixture against real warehouses: every shape of
 * conformance.component.spec.ts that is plain Malloy, plus warehouse-typed
 * shapes of its own, built and served on BigQuery and Snowflake, each in the
 * warehouse (`database`, `materialize_database`: the database tier) and in
 * a DuckLake destination that reads from the warehouse (`storage`,
 * `materialize`: the `storage=` tier). Every served answer must equal live,
 * rows and result metadata, at the tier the shape names, as on DuckDB.
 *
 * Local engines cannot find what only a warehouse does: how its driver
 * returns a big integer, a NUMERIC or a zoned timestamp, how it folds a
 * quoted name, and what its passthrough hands DuckDB. So this runs the
 * production path end to end (conformance/warehouse/warehouse_runner.ts):
 * a real Environment and MaterializationService, no stand-ins.
 *
 * Gated on credentials (conformance/warehouse/targets.ts names them): a
 * warehouse with none is skipped, and says why in the skipped test's name.
 * The `storage=` tier also needs `PUBLISHER_TEST_POSTGRES_URL` for its
 * DuckLake catalog; without it those cases are skipped, saying so.
 *
 * Each run creates its own schema or dataset (`pub_whc_<UTC stamp>_<run>`),
 * fills it with the deterministic fixture (conformance/warehouse/
 * fixture_data.ts), and drops it in `afterAll`, failures included. A run
 * that never reached `afterAll` is swept by the next run once its scratch is
 * a day old, and by the workflow's own cleanup step at once.
 *
 * A case that fails on a warehouse is a finding: declare it a gap on the
 * shape or query, keyed `<dialect>:<variant>` or `<dialect>:*`, with its
 * reason (conformance/warehouse/warehouse_shapes.ts). Switches, as on DuckDB:
 *
 *  - CONFORMANCE_GAPS=run asserts the gaps too;
 *  - WAREHOUSE_CONFORMANCE_SHAPES (a regex over shape names) and
 *    WAREHOUSE_CONFORMANCE_VARIANTS / _DIALECTS (comma lists) narrow a run;
 *  - WAREHOUSE_CONFORMANCE_OUT names the directory for `<dialect>.json` (every
 *    case's outcome) and `<dialect>.md` (the summary the workflow posts).
 */

import type { Connection } from "@malloydata/malloy";
import { afterAll, beforeAll, describe, expect, it } from "bun:test";
import * as fs from "fs";
import * as os from "os";
import * as path from "path";
import { Environment } from "../../../src/service/environment";
import {
   expectedSkipFailure,
   judge,
   type Verdict,
} from "./conformance/warehouse/compare";
import { createFixtureTables } from "./conformance/warehouse/fixture_data";
import {
   createScratch,
   type Dialect,
   discoverTargets,
   dropScratch,
   type Scratch,
   scratchName,
   sweepScratch,
   WAREHOUSE_CONNECTION,
} from "./conformance/warehouse/targets";
import {
   expectedTier,
   renderWarehouseFiles,
   staleWarehouseExpects,
   tierOf,
   warehouseGapFor,
   warehouseShapes,
   warehouseVariants,
} from "./conformance/warehouse/warehouse_shapes";
import {
   type CaseRun,
   WarehouseHarness,
} from "./conformance/warehouse/warehouse_runner";
import type { Variant } from "./conformance/shapes";

const RUN_GAPS = process.env.CONFORMANCE_GAPS === "run";
const CATALOG_URL = process.env.PUBLISHER_TEST_POSTGRES_URL;
const OUT =
   process.env.WAREHOUSE_CONFORMANCE_OUT ??
   path.join(os.tmpdir(), "warehouse-conformance");

/** Hooks wait on a warehouse: setup, one case's whole run, teardown. */
const SETUP_MS = 10 * 60_000;
const CASE_MS = 20 * 60_000;

const VARIANT_CODE: Record<Variant, string> = {
   storage: "s",
   database: "v",
   materialize: "m",
   materialize_database: "mc",
};

/** One query's outcome on one warehouse, as the artifact records it. */
interface ResultRow {
   dialect: Dialect;
   shape: string;
   variant: Variant;
   tier: "database" | "storage";
   query: string;
   expect: string;
   /** Why this warehouse expects another tier than the DuckDB fixture does. */
   expectBecause?: string;
   /** `pass`, `fail`, `gap` (declared, still failing), `gap-closed` (declared, now passing), `skipped`. */
   status: "pass" | "fail" | "gap" | "gap-closed" | "skipped";
   answeredBy?: string;
   detail?: string;
   gap?: string;
   /** The metadata tolerances the served answer needed to match live. */
   tolerated?: string[];
}

const { targets, skipped } = discoverTargets();

// Runs with no credentials too: a warehouse's own expectation that names a
// shape or query no longer there would otherwise surface only on the nightly.
describe("warehouse conformance: its own expectations", () => {
   it("name shapes and queries that exist", () => {
      expect(staleWarehouseExpects()).toEqual([]);
   });
});

for (const s of skipped) {
   console.log(`warehouse conformance: ${s.dialect} skipped, ${s.reason}`);
   describe(`warehouse conformance: ${s.dialect}`, () => {
      it.skip(`every shape [skipped: ${s.reason}]`, () => {});
   });
}

for (const target of targets) {
   const dialect = target.dialect;
   describe(`warehouse conformance: ${dialect}`, () => {
      let setupRoot: string | undefined;
      let setupEnv: Environment | undefined;
      let connection: Connection | undefined;
      let scratch: Scratch | undefined;
      let harness: WarehouseHarness | undefined;
      let setupError: string | undefined;
      const results: ResultRow[] = [];
      const cases: {
         shape: string;
         variant: Variant;
         tag: string;
         run: CaseRun;
      }[] = [];
      const notes: string[] = [];

      beforeAll(async () => {
         try {
            setupRoot = fs.mkdtempSync(
               path.join(os.tmpdir(), `whconf-setup-${dialect}-`),
            );
            setupEnv = await Environment.create(
               `whc_setup_${dialect}`,
               setupRoot,
               [target.connection()],
            );
            connection = (await setupEnv.getMalloyConnection(
               WAREHOUSE_CONNECTION,
            )) as Connection;
            const swept = await sweepScratch(connection, dialect).catch(
               (error) => ({ dropped: [], failed: [String(error)] }),
            );
            notes.push(
               `swept ${swept.dropped.length} stale scratch area(s)${
                  swept.failed.length
                     ? `; could not sweep: ${swept.failed.join("; ")}`
                     : ""
               }`,
            );
            const name = scratchName();
            scratch = await createScratch(dialect, connection, name);
            notes.push(
               scratch.ownsContainer
                  ? `scratch ${scratch.container}`
                  : `scratch: tables prefixed ${scratch.prefix} in ${scratch.container} (no grant to create a schema or dataset)`,
            );
            await createFixtureTables(connection, scratch);
            harness = new WarehouseHarness(
               target,
               name,
               CATALOG_URL,
               scratch.ownsContainer ? scratch.container : undefined,
            );
            await harness.open();
         } catch (error) {
            setupError = error instanceof Error ? error.message : String(error);
            notes.push(`setup failed: ${setupError}`);
         }
      }, SETUP_MS);

      afterAll(async () => {
         let dropError: unknown;
         try {
            if (connection && scratch) {
               await dropScratch(connection, scratch).catch((error) => {
                  dropError = error;
               });
            }
            await harness?.close();
            await setupEnv?.closeAllConnections().catch(() => undefined);
            if (setupRoot)
               fs.rmSync(setupRoot, { recursive: true, force: true });
         } finally {
            writeReport(dialect, results, cases, notes, dropError);
         }
         // A scratch area left behind costs money until the sweeper finds it.
         if (dropError) throw dropError;
      }, SETUP_MS);

      it("creates its scratch area and fixture", () => {
         expect(setupError).toBeUndefined();
      });

      warehouseShapes().forEach((shape, index) => {
         for (const variant of warehouseVariants(shape)) {
            const tag = `c${index}${VARIANT_CODE[variant]}`;
            const tier = tierOf(variant);
            const noLake = tier === "storage" && !CATALOG_URL;
            describe(`${shape.name} [${variant}]`, () => {
               let run: CaseRun | undefined;
               const verdicts: (Verdict | undefined)[] = [];

               beforeAll(async () => {
                  if (setupError || noLake || !harness || !scratch) return;
                  run = await harness.run({
                     tag,
                     files: renderWarehouseFiles(shape, variant, scratch, tag),
                     queries: shape.queries.map((q) => ({
                        model: q.model ?? Object.keys(shape.files)[0],
                        malloy: q.malloy,
                        givens: q.givens,
                     })),
                     env: shape.env,
                  });
                  cases.push({ shape: shape.name, variant, tag, run });
                  shape.queries.forEach((q, i) => {
                     const { want, because, skip } = expectedTier(
                        shape,
                        q,
                        variant,
                        dialect,
                     );
                     const verdict: Verdict = run!.loadError
                        ? {
                             failure: `load: ${run!.loadError}`,
                             answeredBy: "-",
                             tolerated: [],
                          }
                        : judge(run!.live[i], run!.served[i], want, variant);
                     // A warehouse's own expected-live case: live's answer
                     // alone proves nothing, so the copy must have been built
                     // and declined for the reason named.
                     if (skip && !verdict.failure) {
                        verdict.failure = expectedSkipFailure(
                           run!.served[i],
                           skip,
                           run!.build?.status,
                        );
                     }
                     verdicts[i] = verdict;
                     const gap = warehouseGapFor(shape, q, variant, dialect);
                     results.push({
                        dialect,
                        shape: shape.name,
                        variant,
                        tier,
                        query: q.name,
                        expect: want,
                        ...(because ? { expectBecause: because } : {}),
                        status: gap
                           ? verdict.failure
                              ? "gap"
                              : "gap-closed"
                           : verdict.failure
                             ? "fail"
                             : "pass",
                        answeredBy: verdict.answeredBy,
                        ...(verdict.tolerated.length
                           ? { tolerated: [...verdict.tolerated] }
                           : {}),
                        ...(verdict.failure ? { detail: verdict.failure } : {}),
                        ...(gap ? { gap: `${gap.kind}: ${gap.reason}` } : {}),
                     });
                  });
               }, CASE_MS);

               shape.queries.forEach((q, i) => {
                  const { want } = expectedTier(shape, q, variant, dialect);
                  const title = `${q.name}: ${want}, equal to live`;
                  if (noLake) {
                     results.push({
                        dialect,
                        shape: shape.name,
                        variant,
                        tier,
                        query: q.name,
                        expect: want,
                        status: "skipped",
                        detail:
                           "no PUBLISHER_TEST_POSTGRES_URL for the DuckLake catalog",
                     });
                     it.skip(`${title} [skipped: the storage= tier needs PUBLISHER_TEST_POSTGRES_URL for its DuckLake catalog]`, () => {});
                     return;
                  }
                  const gap = warehouseGapFor(shape, q, variant, dialect);
                  const test = () => {
                     expect(setupError).toBeUndefined();
                     expect(verdicts[i]?.failure).toBeUndefined();
                  };
                  if (gap && !RUN_GAPS) {
                     it.skip(
                        `${title} [gap ${gap.kind}${gap.issue ? ` ${gap.issue}` : ""}: ${gap.reason}]`,
                        test,
                     );
                  } else {
                     it(title, test);
                  }
               });
            });
         }
      });

      // Every case above can skip (a filter that matches nothing, a tier with
      // no lake), and every skip reads green: a run that answered no query
      // proves nothing, so it fails.
      it("answered at least one query", () => {
         expect(setupError).toBeUndefined();
         expect(
            results.filter((r) => r.status !== "skipped").length,
         ).toBeGreaterThan(0);
      });
   });
}

/**
 * Key material and the Snowflake account, wherever an error message or a
 * warning might have carried them: the account in any case form, a PEM
 * block, and a key JSON's `private_key` field.
 */
function scrubSecrets(text: string): string {
   const account = accountPattern();
   return (
      (account ? text.replace(account, "[redacted account]") : text)
         .replace(/^.*::add-mask::.*$/gm, "[redacted mask command]")
         // A key body without its PEM lines: no hash or id this suite
         // writes runs this long.
         .replace(/[A-Za-z0-9+/]{200,}={0,2}/g, "[redacted base64]")
         .replace(
            /-----BEGIN [A-Z ]*PRIVATE KEY-----[\s\S]*?-----END [A-Z ]*PRIVATE KEY-----/g,
            "[redacted private key]",
         )
         .replace(
            /("?private_key(?:_id)?"?\s*[:=]\s*)"(?:[^"\\]|\\.)*"/gi,
            '$1"[redacted]"',
         )
         // The same field inside a JSON string (an error message quoting a key file).
         .replace(
            /(private_key(?:_id)?\\"\s*:\s*\\")(?:[^"\\]|\\[^"])*/gi,
            "$1[redacted]",
         )
   );
}

/**
 * The Snowflake account as a connection error may spell it, in any case: as
 * configured, and with `_` and `-` swapped (its URL form). Undefined when no
 * account is set.
 */
function accountPattern(): RegExp | undefined {
   const account = process.env.SNOWFLAKE_ACCOUNT?.trim();
   if (!account) return undefined;
   const escape = (t: string) => t.replace(/[.*+?^${}()|[\]\\]/g, "\\$&");
   const forms = [
      ...new Set([
         account,
         account.replace(/_/g, "-"),
         account.replace(/-/g, "_"),
      ]),
   ].sort((x, y) => y.length - x.length);
   return new RegExp(forms.map(escape).join("|"), "gi");
}

/** Text as a Markdown table cell: backslashes first, then the pipes. */
function tableCell(text: string): string {
   return text.replace(/\\/g, "\\\\").replace(/\|/g, "\\|");
}

/** One cell of the summary: how a shape fared on one tier. */
function cell(rows: ResultRow[]): string {
   if (rows.length === 0) return "-";
   const count = (s: ResultRow["status"]) =>
      rows.filter((r) => r.status === s).length;
   const ran = rows.length - count("skipped");
   if (ran === 0) return "skipped";
   const parts = [`${count("pass") + count("gap-closed")}/${ran}`];
   if (count("fail")) parts.unshift("FAIL");
   if (count("gap")) parts.push(`${count("gap")} gap`);
   if (count("gap-closed")) parts.push(`${count("gap-closed")} gap closed`);
   return parts.join(" ");
}

function writeReport(
   dialect: Dialect,
   results: ResultRow[],
   cases: { shape: string; variant: Variant; tag: string; run: CaseRun }[],
   notes: string[],
   dropError: unknown,
): void {
   fs.mkdirSync(OUT, { recursive: true });
   const ran = results.filter((r) => r.status !== "skipped").length;
   if (ran === 0 && !notes.some((n) => n.startsWith("setup failed"))) {
      notes.push(
         "**no query was answered**: every case was filtered out or skipped",
      );
   }
   fs.writeFileSync(
      path.join(OUT, `${dialect}.json`),
      scrubSecrets(
         JSON.stringify(
            {
               dialect,
               notes,
               ...(dropError ? { dropError: String(dropError) } : {}),
               results,
               cases: cases.map((c) => ({
                  shape: c.shape,
                  variant: c.variant,
                  tag: c.tag,
                  loadError: c.run.loadError,
                  build: c.run.build,
                  verdicts: c.run.verdicts,
                  warnings: c.run.warnings,
               })),
            },
            null,
            2,
         ),
      ),
   );
   const shapes = [...new Set(results.map((r) => r.shape))];
   const count = (s: ResultRow["status"]) =>
      results.filter((r) => r.status === s).length;
   const md = [
      `### Warehouse conformance: ${dialect}`,
      "",
      `${count("pass")} pass, ${count("fail")} fail, ${count("gap")} declared gap, ${count("gap-closed")} gap closed, ${count("skipped")} skipped.`,
      "",
      ...notes.map((n) => `- ${n}`),
      ...(dropError ? [`- **scratch drop failed**: ${String(dropError)}`] : []),
      "",
      "| shape | database tier | storage= tier |",
      "| --- | --- | --- |",
      ...shapes.map((s) => {
         const of = (tier: string) =>
            results.filter((r) => r.shape === s && r.tier === tier);
         return `| ${tableCell(s)} | ${cell(of("database"))} | ${cell(of("storage"))} |`;
      }),
      "",
   ];
   const tolerated = results.filter((r) => r.tolerated?.length);
   if (tolerated.length) {
      md.push(
         `<details><summary>Tolerances used (${tolerated.length} answers)</summary>`,
         "",
         ...tolerated.map(
            (r) =>
               `- **${r.shape}** [${r.variant}] ${r.query}: ${r.tolerated!.join(", ")}`,
         ),
         "",
         "</details>",
         "",
      );
   }
   const failures = results.filter((r) => r.status === "fail");
   if (failures.length) {
      md.push(
         "<details><summary>Failures</summary>",
         "",
         ...failures.map(
            (r) =>
               `- **${r.shape}** [${r.variant}] ${r.query}: ${(r.detail ?? "")
                  .replace(/\s+/g, " ")
                  .slice(0, 400)}`,
         ),
         "",
         "</details>",
         "",
      );
   }
   fs.writeFileSync(
      path.join(OUT, `${dialect}.md`),
      scrubSecrets(md.join("\n")),
   );
}
