// Address stability for the covered_through ledger, against the real compiler.
//
// The ledger is keyed by a source's CONTENT ADDRESS (computeSourceEntityId), and
// every incremental guarantee rests on that key behaving in exactly two ways:
//
//   - It must NOT move between a seed and the deltas that follow it. If it did,
//     each run would look up a boundary that isn't there, seed again, and never
//     advance — the table rebuilt forever while the logs claim incremental.
//   - It MUST move when the source's computation changes. If it didn't, a delta
//     would be applied into a table built from different SQL, silently mixing
//     two definitions' rows in one table.
//
// The dangerous direction is the first one, because the run itself is what could
// break it: a delta derives its SELECT from the same `PersistSource.getSQL()` the
// address is computed over, so anything that leaked the range into the source's
// own SQL would re-address the table on every refresh. These tests build real
// deltas at real ranges and assert the address does not budge.
//
// Same real-compile harness as incremental_compiler_contract.spec.ts (in-memory
// DuckDB is the compile target only; the delta is never run here).
import type { FixedConnectionMap, PersistSource } from "@malloydata/malloy";
import { beforeAll, describe, expect, it } from "bun:test";
import { computeSourceEntityId } from "./build_plan";
import { deltaSelect, type WatermarkBound } from "./incremental_apply";
import {
   compilePersistSources,
   duckdbTestConnections,
} from "./incremental_test_harness";

const DIGESTS = { duckdb: "digest-1" };
let connections: FixedConnectionMap;

beforeAll(() => {
   ({ connections } = duckdbTestConnections());
});

async function compile(
   model: string,
): Promise<{ sources: Record<string, PersistSource> }> {
   return compilePersistSources(connections, model);
}

const RAW = `##! experimental.persistence
source: raw is duckdb.sql("""
  SELECT 1 AS amount, DATE '2024-01-01' AS order_date, 'US' AS region
""")
`;

const MODEL =
   `${RAW}#@ persist name="t" refresh="incremental" watermark="order_date"\n` +
   `source: mz is raw -> {
  group_by: order_date, region
  aggregate: revenue is amount.sum()
}`;

function date(value: string): WatermarkBound {
   return { malloyType: "date", value };
}

/** The address the ledger would be keyed by, for source `mz` of `model`. */
async function address(model: string): Promise<string> {
   const { sources } = await compile(model);
   return computeSourceEntityId(sources.mz, DIGESTS);
}

describe("incremental refresh: content-address stability", () => {
   it("survives a seed and two deltas at different ranges", async () => {
      // The whole lifecycle in one compile session: address it as the seed
      // would, build two deltas at different ranges as two later runs would,
      // then address it again. The ledger key must be byte-identical throughout.
      const { sources } = await compile(MODEL);
      const seedAddress = computeSourceEntityId(sources.mz, DIGESTS);

      const deltaFor = (start: WatermarkBound, end: WatermarkBound) =>
         deltaSelect({
            dialect: "duckdb",
            // The seed's own SQL, which is also what the address is computed over.
            sourceSQL: sources.mz.getSQL({ buildManifest: { entries: {} } }),
            watermarkName: "order_date",
            start,
            end,
         });

      const first = deltaFor(date("2024-01-01"), date("2024-02-01"));
      expect(computeSourceEntityId(sources.mz, DIGESTS)).toBe(seedAddress);

      const second = deltaFor(date("2024-02-01"), date("2024-03-01"));
      expect(computeSourceEntityId(sources.mz, DIGESTS)).toBe(seedAddress);

      // Not vacuous: the ranges did reach the delta SQL, they just did not reach
      // the address.
      expect(first).not.toBe(second);
      expect(first).toContain("2024-01-01");
      expect(second).toContain("2024-02-01");
   });

   it("is identical across a fresh compile of the same model", async () => {
      // A delta run after a restart is a NEW compile of the same text; it has to
      // land on the boundary the previous process recorded.
      expect(await address(MODEL)).toBe(await address(MODEL));
   });

   it("does not move when only the declaration changes", async () => {
      // Adopting incremental refresh — or tightening it with a merge key — must
      // not orphan the table that is already built.
      const full = await address(MODEL.replace(' refresh="incremental"', ""));
      const incremental = await address(MODEL);
      const keyed = await address(
         MODEL.replace(
            'watermark="order_date"',
            'watermark="order_date" merge_key="region"',
         ),
      );
      expect(incremental).toBe(full);
      expect(keyed).toBe(full);
   });

   it("MOVES when the source's computation changes", async () => {
      // The complement, and the reason the address is a content address: a model
      // edit must re-seed rather than apply a delta into a table built from
      // different SQL.
      const base = await address(MODEL);
      const extraColumn = await address(
         MODEL.replace(
            "aggregate: revenue",
            "aggregate: orders is count()\n  aggregate: revenue",
         ),
      );
      const filtered = await address(
         MODEL.replace(
            "group_by: order_date, region",
            "where: region = 'US'\n  group_by: order_date, region",
         ),
      );
      expect(extraColumn).not.toBe(base);
      expect(filtered).not.toBe(base);
   });

   it("MOVES when the connection digest changes", async () => {
      // The digest stands for the connection the table lives in, so the same SQL
      // against a different warehouse is a different table — and must not read
      // the other one's boundary.
      const { sources } = await compile(MODEL);
      expect(
         computeSourceEntityId(sources.mz, { duckdb: "digest-2" }),
      ).not.toBe(computeSourceEntityId(sources.mz, DIGESTS));
   });
});
