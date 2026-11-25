// Timing: live vs the pair table served flat. Not committed.
import { SingleConnectionRuntime } from "@malloydata/malloy";
import { DuckDBConnection } from "@malloydata/db-duckdb";
import {
   rerootDeclaration,
   typeDeclaration,
} from "../src/service/materialization/serve/served_copy";
import {
   pairMeasureLevels,
   flatPairQueryVerdict,
} from "../src/service/governance/ir/pair_flat_query";

const FLIGHTS = Number(process.argv[2] ?? 2_000_000);
const MAXFAN = Number(process.argv[3] ?? 5);
const conn = new DuckDBConnection("duckdb", ":memory:");
await conn.runSQL(`SET threads=4`);
await conn.runSQL(
   `CREATE TABLE flights AS SELECT i AS id, ['AA','UA','DL','WN','B6','AS','NK','F9','HA','G4','SY','XE'][(i % 12)+1] AS carrier, (i*7919 % 3000)::DOUBLE AS distance, (i % 50000) AS route FROM range(${FLIGHTS}) t(i)`,
);
await conn.runSQL(
   `CREATE TABLE segments AS SELECT row_number() OVER () AS seg_id, f.id AS flight_id, ((f.id*31 + k*17) % 900)::DOUBLE AS miles FROM flights f, LATERAL (SELECT unnest(range(CASE WHEN f.id % 5 = 0 THEN 0 ELSE 1 + (f.id % ${MAXFAN}) END)) AS k)`,
);
const n = (await conn.runSQL("select (select count(*) from segments) s"))
   .rows[0];
const flDecl = `flights is duckdb.table('flights') extend {
  primary_key: id
  join_many: segments on segments.flight_id = id
  measure: flight_count is count(), total_distance is distance.sum()
  measure: leg_miles is segments.miles.sum(), leg_count is segments.count()
}`;
const author = `source: segments is duckdb.table('segments') extend {
  primary_key: seg_id
  dimension: is_long_leg is miles > 450
  measure: seg_miles is miles.sum()
}
source: ${flDecl}`;
const rt = new SingleConnectionRuntime({ connection: conn });
const build = `${author}
query: pair is flights -> {
  group_by: id, carrier, distance, route, segments__seg_id is segments.seg_id, segments__flight_id is segments.flight_id, segments__miles is segments.miles, segments__is_long_leg is segments.is_long_leg
  calculate: __parent_row is row_number() { partition_by: id } = 1
  order_by: id
}`;
let t = Date.now();
const lm = rt.loadModel(author);
await conn.runSQL(
   `CREATE TABLE pair AS ${await rt.loadModel(build).loadQueryByName("pair").getSQL()}`,
);
console.log(
   `flights ${FLIGHTS}, segments ${n.s}, pair build ${Date.now() - t} ms`,
);
const own = ["id", "carrier", "distance", "route"].map((name) => ({
   name,
   type:
      name === "carrier"
         ? "VARCHAR"
         : name === "distance"
           ? "DOUBLE"
           : "BIGINT",
}));
const authorDef: any = (await lm.getModel())._modelDef.contents.flights;
const levels = pairMeasureLevels(authorDef.fields, "segments");
const parentMeasures = [...levels]
   .filter(([, l]) => l.size === 1 && l.has("parent"))
   .map(([name]) => ({ name, internalName: `${name}__pair` }));
const pair = {
   join: "segments",
   parentRowColumn: { name: "__parent_row", type: "BOOLEAN" },
   parentRowField: "__parent_row__pair",
   childColumns: [
      ["seg_id", "BIGINT"],
      ["flight_id", "BIGINT"],
      ["miles", "DOUBLE"],
      ["is_long_leg", "BOOLEAN"],
   ].map(([name, type]) => ({
      name,
      type,
      column: `segments__${name}`,
      internalName: `segments__${name}__pair`,
   })),
   parentMeasures,
   childStoredDimensions: ["is_long_leg"],
};
const r = {
   sourceName: "flights",
   connectionAlias: "duckdb",
   virtualHandle: "pair",
   typeName: "parent_t",
   columns: own,
   storedDimensions: [],
   pair,
};
const served = `##! experimental { virtual_source }
${typeDeclaration({ typeName: "parent_t", columns: [...own, pair.parentRowColumn, ...pair.childColumns.map((c) => ({ name: c.column, type: c.type }))] })}
source: ${rerootDeclaration(flDecl, r)}`;
const virtualMap = new Map([["duckdb", new Map([["pair", "pair"]])]]);
const queries: [string, string][] = [
   [
      "parent only: flight_count, distance by carrier",
      "run: flights -> { group_by: carrier; aggregate: flight_count, total_distance }",
   ],
   [
      "parent only: flight_count by route",
      "run: flights -> { group_by: route; aggregate: flight_count }",
   ],
   [
      "child only: leg_miles by carrier",
      "run: flights -> { group_by: carrier; aggregate: leg_miles }",
   ],
   [
      "child only: leg_count by carrier",
      "run: flights -> { group_by: carrier; aggregate: leg_count }",
   ],
   [
      "mixed: parent and child by carrier",
      "run: flights -> { group_by: carrier; aggregate: flight_count, total_distance, leg_miles, leg_count }",
   ],
   [
      "child grouped: by segments.is_long_leg",
      "run: flights -> { group_by: segments.is_long_leg; aggregate: leg_miles, leg_count }",
   ],
   [
      "nest: carrier > long leg",
      "run: flights -> { group_by: carrier; aggregate: flight_count; nest: legs is { group_by: segments.is_long_leg; aggregate: leg_miles } }",
   ],
   [
      "parent only: avg/max/distinct by carrier",
      "run: flights -> { group_by: carrier; aggregate: a is distance.avg(), flight_count }",
   ],
   [
      "parent only: count() written in query",
      "run: flights -> { group_by: carrier; aggregate: c is count() }",
   ],
   [
      "mixed: flight_count, leg_count by route",
      "run: flights -> { group_by: route; aggregate: flight_count, leg_count }",
   ],
   [
      "declined: child-source measure",
      "run: flights -> { group_by: carrier; aggregate: segments.seg_miles }",
   ],
   [
      "declined: parent by child dim",
      "run: flights -> { group_by: segments.is_long_leg; aggregate: flight_count }",
   ],
];
const sm = rt.loadModel(served);
const median = (xs: number[]) =>
   xs.sort((a, b) => a - b)[Math.floor(xs.length / 2)];
const time = async (sql: string) => {
   const out: number[] = [];
   let rows;
   for (let i = 0; i < 7; i++) {
      const s = performance.now();
      rows = (await conn.runSQL(sql, { rowLimit: 100000 })).rows;
      out.push(performance.now() - s);
   }
   return { ms: median(out.slice(2)), rows };
};
const norm = (rs: any[]) =>
   JSON.stringify(
      rs
         .map((x) =>
            JSON.stringify(x, (_k, v) =>
               typeof v === "number" ? Math.round(v * 1e6) / 1e6 : v,
            ),
         )
         .sort(),
   );
console.log("| shape | live ms | served ms | served/live | result |");
console.log("|---|---|---|---|---|");
for (const [name, q] of queries) {
   const lsql = await lm.loadQuery(q).getSQL();
   const L = await time(lsql);
   let refusal: string | undefined;
   let ssql: string | undefined;
   try {
      const sq = sm.loadQuery(q);
      ssql = await sq.getSQL({ virtualMap } as never);
      const pq: any = await sq.getPreparedQuery();
      const v = flatPairQueryVerdict(pq._query, pq._modelDef.contents.flights, {
         join: "segments",
         parentRowField: "__parent_row__pair",
      });
      refusal =
         v.kind === "served"
            ? undefined
            : v.kind === "parent_only"
              ? "parent only: next candidate"
              : v.reason;
   } catch (e) {
      refusal = `does not compile: ${(e as Error).message.split("\n")[0]}`;
   }
   if (refusal) {
      console.log(
         `| ${name} | ${L.ms.toFixed(1)} | live (declined) | 1.00 | ${refusal} |`,
      );
      continue;
   }
   const S = await time(ssql!);
   console.log(
      `| ${name} | ${L.ms.toFixed(1)} | ${S.ms.toFixed(1)} | ${(S.ms / L.ms).toFixed(2)} | ${norm(L.rows) === norm(S.rows) ? "same" : "DIFF"} |`,
   );
}
console.log((await conn.runSQL("select version() v")).rows);
