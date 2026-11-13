// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * The storage tier's half of the mixed-fleet guarantee
 * (`run/v1_mixed_fleet_manifest.spec.ts` pins the colocated half): a v1
 * `#@ preaggregate storage=` rollup is `__preagg__` with
 * `PUBLISHER_V1_SERVED_COPY` off and `__rollup__` with it on, at different
 * addresses. A worker handed the other side's storage entry must not read
 * that table, though the storage tier falls back to matching an entry by
 * name: the two sides' names are never equal. Each side's table is tampered
 * with its own offset, and both tables are present in each side's lake, so
 * an answer read from either is told apart from live and from the other.
 */

import { afterEach, describe, expect, it } from "bun:test";
import * as fs from "fs";
import * as os from "os";
import * as path from "path";
import type { DuckDBConnection } from "@malloydata/db-duckdb";
import { DATA_FILES } from "./conformance/data";
import {
   freshLoadPool,
   loadFixture,
   withSwitch,
   type Row,
} from "./conformance/package_harness";
import { renderFiles, SHAPES } from "./conformance/shapes";

const SHAPE = SHAPES.find((s) => s.name === "v1 rollup on annotated measures");
const QUERY =
   "run: orders -> { group_by: region; aggregate: total, n; order_by: region }";

type Fixture = Awaited<ReturnType<typeof loadFixture>>;

async function rows(lake: DuckDBConnection, sql: string): Promise<Row[]> {
   return (await lake.runSQL(sql, { rowLimit: 10_000 })).rows as Row[];
}

async function tableLike(lake: DuckDBConnection, part: string) {
   const found = (
      await rows(
         lake,
         `SELECT table_name FROM duckdb_tables() WHERE table_name LIKE '%${part}%'`,
      )
   ).map((r) => String(r.table_name));
   expect(found).toHaveLength(1);
   return found[0];
}

/** Copy `table` from one lake into another, through Parquet. */
async function copyTable(
   from: DuckDBConnection,
   to: DuckDBConnection,
   table: string,
): Promise<void> {
   const file = path.join(
      fs.mkdtempSync(path.join(os.tmpdir(), "mixed-fleet-")),
      "t.parquet",
   );
   await from.runSQL(`COPY "${table}" TO '${file}' (FORMAT PARQUET)`);
   await to.runSQL(
      `CREATE TABLE "${table}" AS SELECT * FROM read_parquet('${file}')`,
   );
}

/** Add `offset` to every numeric column of `table`. */
async function tamper(
   lake: DuckDBConnection,
   table: string,
   offset: number,
): Promise<void> {
   const numeric = (await rows(lake, `DESCRIBE "${table}"`))
      .filter((c) =>
         /INT|DOUBLE|DECIMAL|FLOAT|NUMERIC/i.test(String(c.column_type)),
      )
      .map((c) => String(c.column_name));
   expect(numeric.length).toBeGreaterThan(0);
   await lake.runSQL(
      `UPDATE "${table}" SET ${numeric.map((c) => `"${c}" = "${c}" + ${offset}`).join(", ")}`,
   );
}

const shifted = (live: Row[], offset: number) =>
   live.map((row) =>
      Object.fromEntries(
         Object.entries(row).map(([k, v]) => [
            k,
            typeof v === "number" ? v + offset : v,
         ]),
      ),
   );

/** Bind `entries` as the package's storage manifest, as a host does. */
async function bindStored(
   fixture: Fixture,
   entries: Record<string, Record<string, unknown>>,
) {
   fixture.pkg.bindColocatedServeManifest({});
   fixture.pkg.bindStorageServeBindings(entries as never);
   for (const modelPath of fixture.pkg.getModelPaths()) {
      await fixture.pkg.getModel(modelPath)?.prepareServedCopy();
   }
}

describe("a host storage manifest from the other side of the v1 cutover (mixed fleet)", () => {
   const disposers: (() => Promise<void>)[] = [];
   let restore: (() => void) | undefined;
   afterEach(async () => {
      restore?.();
      for (const d of disposers.splice(0)) await d();
      await freshLoadPool();
   });

   it("ignores a foreign storage entry in both directions, and reads only its own beside one", async () => {
      expect(SHAPE).toBeDefined();
      const files = { ...DATA_FILES, ...renderFiles(SHAPE!, "storage") };

      // Off: the v1 composite's __preagg__ storage table.
      restore = withSwitch("on");
      await freshLoadPool();
      const off = await loadFixture(files);
      disposers.push(off.dispose);
      const live = await off.run("m.malloy", QUERY);
      if ("error" in live) throw new Error(live.error);
      const LIVE = live.answer.rows;
      const offBound = await off.bind({ includeRollups: true });
      const offEntries = Object.fromEntries(
         Object.entries(offBound.stored).filter(([, e]) =>
            String(e.physicalTableName).includes("__preagg__"),
         ),
      );
      expect(Object.keys(offEntries)).toHaveLength(1);

      // On: the rollup leg's __rollup__ storage table.
      restore();
      restore = withSwitch("cutover");
      await freshLoadPool();
      const on = await loadFixture(files);
      disposers.push(on.dispose);
      const onBound = await on.bind({ includeRollups: true });
      const onEntries = Object.fromEntries(
         Object.entries(onBound.stored).filter(([, e]) =>
            String(e.physicalTableName).includes("__rollup__"),
         ),
      );
      expect(Object.keys(onEntries)).toHaveLength(1);
      expect(Object.keys(offEntries).filter((id) => id in onEntries)).toEqual(
         [],
      );

      // Each side's lake holds both tables, each tampered with its side's offset.
      const preagg = await tableLike(off.lake, "__preagg__");
      const rollup = await tableLike(on.lake, "__rollup__");
      await copyTable(off.lake, on.lake, preagg);
      await copyTable(on.lake, off.lake, rollup);
      for (const lake of [off.lake, on.lake]) {
         await tamper(lake, preagg, 1000);
         await tamper(lake, rollup, 2000);
      }

      const ask = async (fixture: Fixture) => {
         const out = await fixture.run("m.malloy", QUERY);
         if ("error" in out) throw new Error(out.error);
         return { rows: out.answer.rows, servedBy: out.answer.servedBy };
      };

      // An on worker, handed only the off side's entry: live.
      await bindStored(on, offEntries);
      expect(await ask(on)).toEqual({ rows: LIVE, servedBy: "live" });
      // Both sides' entries: its own table, never the foreign one.
      await bindStored(on, { ...offEntries, ...onEntries });
      expect(await ask(on)).toEqual({
         rows: shifted(LIVE, 2000),
         servedBy: "rollup",
      });

      // An off worker (a rollback to off), handed only the on side's entry: live.
      restore();
      restore = withSwitch("on");
      await bindStored(off, onEntries);
      expect(await ask(off)).toEqual({ rows: LIVE, servedBy: "live" });
      await bindStored(off, { ...onEntries, ...offEntries });
      expect(await ask(off)).toEqual({
         rows: shifted(LIVE, 1000),
         servedBy: "rollup",
      });
   }, 300_000);
});
