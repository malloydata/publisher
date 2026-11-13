// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * The storage tier's half of the mixed-fleet guarantee
 * (`run/v1_mixed_fleet_manifest.spec.ts` pins the colocated half): during the
 * rollout of the v1 cutover's retirement (6g), an older worker built a v1
 * `#@ preaggregate storage=` rollup as `__preagg__`, and this one builds it as
 * `__rollup__`, at another address. Handed the older worker's storage entry,
 * this worker must not read that table: its address is not this worker's
 * (the storage tier matches an entry by name AND address), nor is its
 * name. (The older
 * worker's half was pinned while the switch existed; it ships in that
 * worker.)
 *
 * The older entry is this worker's own entry under an address and a table no
 * plan of this worker names, since this server can no longer build one. Each
 * table is tampered with its own offset, so an answer read from either is
 * told apart from live and from the other.
 */

import { afterEach, describe, expect, it } from "bun:test";
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

describe("a host storage manifest from an older worker of the v1 cutover (mixed fleet)", () => {
   const disposers: (() => Promise<void>)[] = [];
   let restore: (() => void) | undefined;
   afterEach(async () => {
      restore?.();
      for (const d of disposers.splice(0)) await d();
      await freshLoadPool();
   });

   it("ignores an older worker's __preagg__ storage entry, and reads only its own beside one", async () => {
      expect(SHAPE).toBeDefined();
      const files = { ...DATA_FILES, ...renderFiles(SHAPE!, "storage") };

      restore = withSwitch("cutover");
      await freshLoadPool();
      const fixture = await loadFixture(files);
      disposers.push(fixture.dispose);
      const live = await fixture.run("m.malloy", QUERY);
      if ("error" in live) throw new Error(live.error);
      const LIVE = live.answer.rows;
      const bound = await fixture.bind({ includeRollups: true });
      const ownEntries = Object.fromEntries(
         Object.entries(bound.stored).filter(([, e]) =>
            String(e.physicalTableName).includes("__rollup__"),
         ),
      );
      const [[ownID, own]] = Object.entries(ownEntries);

      // The older worker's table and entry: the composite's name, at an
      // address no plan of this worker names.
      const rollup = await tableLike(fixture.lake, "__rollup__");
      const preagg = "orders__preagg__region__0123abcd";
      await fixture.lake.runSQL(
         `CREATE TABLE "${preagg}" AS SELECT * FROM "${rollup}"`,
      );
      const olderID = "f".repeat(ownID.length);
      expect(olderID).not.toBe(ownID);
      const olderEntries = {
         [olderID]: {
            ...own,
            sourceEntityId: olderID,
            sourceName: preagg,
            physicalTableName: preagg,
         },
      };
      await tamper(fixture.lake, preagg, 1000);
      await tamper(fixture.lake, rollup, 2000);

      const ask = async () => {
         const out = await fixture.run("m.malloy", QUERY);
         if ("error" in out) throw new Error(out.error);
         return { rows: out.answer.rows, servedBy: out.answer.servedBy };
      };

      // Handed only the older entry: live.
      await bindStored(fixture, olderEntries);
      expect(await ask()).toEqual({ rows: LIVE, servedBy: "live" });
      // Live too when the older entry carries this worker's name: an entry
      // is matched by its address, never by its name alone.
      await bindStored(fixture, {
         [olderID]: { ...olderEntries[olderID], sourceName: own.sourceName },
      });
      expect(await ask()).toEqual({ rows: LIVE, servedBy: "live" });
      // Both: its own table, never the older one.
      await bindStored(fixture, { ...olderEntries, ...ownEntries });
      expect(await ask()).toEqual({
         rows: shifted(LIVE, 2000),
         servedBy: "rollup",
      });
   }, 300_000);
});
