import { afterEach, beforeEach, describe, expect, it } from "bun:test";
import * as fs from "fs/promises";
import * as os from "os";
import * as path from "path";

import { Environment } from "./environment";
import type { Package } from "./package";

/**
 * The publish gate for the package-level materialization cron. At Phase B the
 * package-level cron (`materialization.schedule`) is replaced outright by
 * `materialization.freshness.window` (persistence.md §9.4), so ANY declared cron
 * is rejected regardless of the sources' sharing. The `sharing=private`
 * carve-out arrives whole with Phase A (persistence-phase-b-design.md §3.4);
 * until then there is no private artifact for a cron to own, so the B→A rule is
 * reject-all. These tests run a real `Environment` + `Package.create` over temp
 * dirs, so they also prove end-to-end that the pinned compiler ACCEPTS
 * `sharing=` / `refresh=` keys in the `#@ persist` annotation and that the
 * values survive to the wire build plan verbatim (unset stays null — never
 * defaulted to "shared").
 */
describe("materialization cron gate", () => {
   let rootDir: string;
   let envPath: string;

   async function loadPackage(model: string, schedule?: string) {
      const dir = path.join(envPath, "pkg");
      await fs.mkdir(dir, { recursive: true });
      await fs.writeFile(
         path.join(dir, "publisher.json"),
         JSON.stringify({
            name: "pkg",
            description: "fixture",
            ...(schedule ? { materialization: { schedule } } : {}),
         }),
      );
      await fs.writeFile(path.join(dir, "model.malloy"), model);
      const env = await Environment.create("testEnv", envPath, []);
      await env.addPackage("pkg");
      return env.getPackage("pkg", false);
   }

   function sourceByName(pkg: Package, name: string) {
      const sources = pkg.getBuildPlan()?.sources ?? {};
      const found = Object.values(sources).find((s) => s.name === name);
      if (!found) throw new Error(`persist source '${name}' not in build plan`);
      return found;
   }

   beforeEach(async () => {
      rootDir = await fs.mkdtemp(path.join(os.tmpdir(), "publisher-cron-"));
      envPath = path.join(rootDir, "env");
      await fs.mkdir(envPath, { recursive: true });
   });

   afterEach(async () => {
      await fs.rm(rootDir, { recursive: true, force: true }).catch(() => {});
   });

   const MIXED_MODEL = `##! experimental.persistence

#@ persist name="priv_table" sharing=private refresh=incremental
source: priv is duckdb.sql("SELECT 1 as x")

#@ persist name="open_table" sharing=shared
source: open is duckdb.sql("SELECT 2 as x")

#@ persist name="unset_table"
source: unspecified is duckdb.sql("SELECT 3 as x")
`;

   const ALL_PRIVATE_MODEL = `##! experimental.persistence

#@ persist name="a_table" sharing=private
source: a is duckdb.sql("SELECT 1 as x")

#@ persist name="b_table" sharing=private refresh=full
source: b is duckdb.sql("SELECT 2 as x")
`;

   it(
      "surfaces declared sharing/refresh verbatim on the build plan (null when unset)",
      async () => {
         const pkg = await loadPackage(MIXED_MODEL);

         const priv = sourceByName(pkg, "priv");
         expect(priv.sharing).toBe("private");
         expect(priv.refresh).toBe("incremental");

         const open = sourceByName(pkg, "open");
         expect(open.sharing).toBe("shared");
         expect(open.refresh).toBeNull();

         // Unset must be reported as null — distinguishable from an explicit
         // "shared" — because the control plane applies the platform default
         // (unset => shared) itself.
         const unspecified = sourceByName(pkg, "unspecified");
         expect(unspecified.sharing).toBeNull();
         expect(unspecified.refresh).toBeNull();
      },
      { timeout: 30000 },
   );

   it(
      "rejects any declared cron, pointing at freshness.window",
      async () => {
         const pkg = await loadPackage(MIXED_MODEL, "0 6 * * *");

         // One actionable message for the whole package — the B→A rule is
         // reject-all, not per-source.
         const warnings = pkg.scheduleWarnings();
         expect(warnings).toHaveLength(1);
         const joined = pkg.formatInvalidSchedule();
         expect(joined).toContain("materialization.schedule");
         expect(joined).toContain("materialization.freshness.window");
      },
      { timeout: 30000 },
   );

   it(
      "rejects a declared cron even when every persist source is private",
      async () => {
         // The sharing=private carve-out arrives with Phase A; during B→A even
         // an all-private package's cron is rejected outright.
         const pkg = await loadPackage(ALL_PRIVATE_MODEL, "0 6 * * *");
         expect(pkg.scheduleWarnings()).toHaveLength(1);
         expect(pkg.formatInvalidSchedule()).toContain(
            "materialization.freshness.window",
         );
      },
      { timeout: 30000 },
   );

   it(
      "is inert when no cron is declared, whatever the sources' sharing",
      async () => {
         const pkg = await loadPackage(MIXED_MODEL);
         expect(pkg.scheduleWarnings()).toEqual([]);
      },
      { timeout: 30000 },
   );
});
