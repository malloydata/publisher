// Probe: does creating and closing DuckDB instances (as the suites do ~2,300
// times) or package-load workers grow the process's committed memory?
import { DuckDBInstance } from "@duckdb/node-api";
import { spawnSync } from "child_process";
const MODE = process.env.MODE ?? "instances";
const N = Number(process.env.N ?? 2000);
function commitMB(): string {
   const m = process.memoryUsage();
   if (process.platform !== "win32") return `rss=${(m.rss / 1048576) | 0}`;
   const r = spawnSync("powershell", ["-NoProfile", "-Command", `(Get-Process -Id ${process.pid}).PrivateMemorySize64`], { encoding: "utf8" });
   return `private=${(Number(r.stdout.trim()) / 1048576) | 0} ws=${(m.rss / 1048576) | 0} heap=${(m.heapUsed / 1048576) | 0}`;
}
console.log(`start ${commitMB()}`);
if (MODE === "instances" || MODE === "instances-4t") {
   for (let i = 1; i <= N; i++) {
      const inst = await DuckDBInstance.create(":memory:", { threads: MODE === "instances-4t" ? "4" : "2" });
      const c = await inst.connect();
      await c.run("SELECT sum(range) FROM range(10000)");
      c.disconnectSync();
      inst.closeSync();
      if (i % 250 === 0) { Bun.gc(true); console.log(`${MODE} ${i} ${commitMB()}`); }
   }
} else if (MODE === "connections") {
   const inst = await DuckDBInstance.create(":memory:", { threads: "2" });
   for (let i = 1; i <= N; i++) {
      const c = await inst.connect();
      await c.run("SELECT sum(range) FROM range(10000)");
      c.disconnectSync();
      if (i % 250 === 0) { Bun.gc(true); console.log(`${MODE} ${i} ${commitMB()}`); }
   }
} else if (MODE === "workers") {
   const { PackageLoadPool } = await import("./src/package_load/package_load_pool");
   const { duckdbMalloyConfig, writeTempPackage } = await import("./src/test_helpers/temp_package");
   const { dir } = writeTempPackage({ "m.malloy": `source: nums is duckdb.sql("select 1 as a") extend { measure: t is a.sum() }` });
   for (let i = 1; i <= N; i++) {
      const pool = new PackageLoadPool(1);
      const { malloyConfig, duckdb } = duckdbMalloyConfig();
      await pool.loadPackage({ packagePath: dir, packageName: "pkg", environmentName: "env", malloyConfig, defaultConnectionName: "duckdb" });
      await duckdb.close();
      await pool.shutdown();
      if (i % 25 === 0) { Bun.gc(true); console.log(`${MODE} ${i} ${commitMB()}`); }
   }
}
console.log(`end ${commitMB()}`);
