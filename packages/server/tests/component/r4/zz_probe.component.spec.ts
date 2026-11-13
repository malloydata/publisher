import { afterAll, beforeAll, expect, it } from "bun:test";
import { installSettings } from "../../../src/test_helpers/settings_env";
import { buildScenario, disposeScenarios, rowDifference, type R4Fixture } from "./harness";
const seedSql = [
  `CREATE TABLE flights AS SELECT * FROM (VALUES (1,'AA',100.0),(2,'AA',250.0),(3,'UA',300.0),(4,'DL',50.0),(8,'AA',410.0)) AS t(id, carrier, distance)`,
  `CREATE TABLE notes AS SELECT * FROM (VALUES (1,'AA','gate',5.0),(2,'AA','crew',7.0),(3,'UA','gate',11.0),(4,'ZZ','none',13.0)) AS t(note_id, carrier, topic, w)`,
];
const model = `source: notes is duckdb.table('notes') extend { primary_key: note_id }
#(materialize) storage="lake" inline=["notes"]
source: fn is duckdb.table('flights') extend {
  primary_key: id
  join_many: notes on notes.carrier = carrier
  measure: flight_count is count(), note_n is notes.count(), wsum is notes.w.sum(), wavg is notes.w.avg(), dsum is distance.sum()
}`;
let restore: () => void; let fx: R4Fixture;
beforeAll(async () => { restore = installSettings({ env: { PERSIST_STORAGE_MODE: "on", PUBLISHER_MATERIALIZE: "true" } }).restore; fx = await buildScenario({ files: { "m.malloy": model }, seedSql, materialize: true }); });
afterAll(async () => { restore(); await disposeScenarios(); });
for (const q of [
  "run: fn -> { group_by: carrier; aggregate: note_n, wsum, wavg }",
  "run: fn -> { aggregate: note_n, wsum, wavg }",
  "run: fn -> { group_by: notes.topic; aggregate: wsum, note_n }",
  "run: fn -> { aggregate: flight_count, wsum, x is notes.w.sum(), c is count(notes.note_id) }",
  "run: fn -> { group_by: notes.topic; aggregate: flight_count, note_n }",
  "run: fn -> { group_by: notes.* }",
]) it(q, async () => {
  const r = await fx.run({ name: q, malloy: q });
  console.log("PROBE", q, r.servedVia, JSON.stringify(r.live), JSON.stringify(r.served));
  expect(rowDifference(r.live, r.served)).toBeUndefined();
});
