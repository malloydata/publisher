import { afterEach, beforeEach, describe, expect, it } from "bun:test";
import * as fs from "fs/promises";
import * as os from "os";
import * as path from "path";

import { Environment } from "./environment";
import type { Package } from "./package";

/**
 * The publish gate for materialization crons (Phase A carve-out,
 * persistence.md §9.4). A cron is the power tier of artifact-anchored
 * scheduling and is valid only over `sharing="private"` artifacts:
 *
 *  - a package-level cron (`materialization.schedule`) is valid only when
 *    EVERY governed persist source resolves to explicit `sharing="private"`;
 *  - a per-source cron (`#@ persist ... schedule=`) is valid only when THAT
 *    source resolves to explicit `sharing="private"`;
 *  - a cron on a shared/unset artifact is rejected, pointing at
 *    `freshness.window` as the shared-tier replacement.
 *
 * These tests run a real `Environment` + `Package.create` over temp dirs, so
 * they also prove end-to-end that the pinned compiler ACCEPTS `sharing=` /
 * `refresh=` / `schedule=` / `freshness.*` keys in the `#@ persist` annotation
 * and that the values survive to the wire build plan verbatim (unset stays null
 * — never defaulted to "shared").
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

   // Per-source crons: valid on the private source, rejected on the shared and
   // the unset (⇒ shared) sources.
   const PER_SOURCE_CRON_MODEL = `##! experimental.persistence

#@ persist name="p_table" sharing=private schedule="0 */6 * * *"
source: p is duckdb.sql("SELECT 1 as x")

#@ persist name="s_table" sharing=shared schedule="0 0 * * *"
source: s is duckdb.sql("SELECT 2 as x")

#@ persist name="u_table" schedule="0 0 * * *"
source: u is duckdb.sql("SELECT 3 as x")
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
      "rejects a package cron on a mixed package, pointing at freshness.window",
      async () => {
         // Phase A: a package cron governs every persist source, so it is valid
         // only when all resolve to explicit private. MIXED has a shared and an
         // unset source, so the package cron is rejected.
         const pkg = await loadPackage(MIXED_MODEL, "0 6 * * *");

         const warnings = pkg.scheduleWarnings();
         expect(warnings).toHaveLength(1);
         const joined = pkg.formatInvalidSchedule();
         expect(joined).toContain("materialization.schedule");
         expect(joined).toContain("materialization.freshness.window");
      },
      { timeout: 30000 },
   );

   it(
      "accepts a package cron when every persist source is private",
      async () => {
         // Phase A carve-out: an all-private package legitimately carries a
         // package-level cron (the power tier for its own private artifacts).
         const pkg = await loadPackage(ALL_PRIVATE_MODEL, "0 6 * * *");
         expect(pkg.scheduleWarnings()).toEqual([]);
      },
      { timeout: 30000 },
   );

   it(
      "rejects a per-source cron unless that source resolves to private",
      async () => {
         // No package cron here — only per-source `schedule=` declarations. The
         // private source's cron is accepted; the shared and unset sources' are
         // rejected, each pointing at freshness.window.
         const pkg = await loadPackage(PER_SOURCE_CRON_MODEL);

         const warnings = pkg.scheduleWarnings();
         expect(warnings).toHaveLength(2);
         const joined = warnings.join("\n");
         expect(joined).toContain('"s"');
         expect(joined).toContain('"u"');
         expect(joined).not.toContain('"p"');
         expect(joined).toContain("freshness.window");

         // The resolved per-source schedule is surfaced verbatim on the plan.
         expect(sourceByName(pkg, "p").schedule).toBe("0 */6 * * *");
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
