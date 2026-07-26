import { afterEach, beforeEach, describe, expect, it } from "bun:test";
import * as fs from "fs/promises";
import * as os from "os";
import * as path from "path";

import { Environment } from "./environment";
import type { Package } from "./package";

/**
 * The publish gate for the package's Malloy Persistence policy (persistence.md
 * §3.1, §9.2–§9.5). Scope is a single package-level mode (`Package.scope`) and a
 * materialization cron is package-root-only + version-scope-only:
 *
 *  - per-source `#@ persist ... sharing=`/`schedule=` are retired (declaring
 *    either on a source is a manifest error);
 *  - `materialization.schedule` is legal only when `scope: version`;
 *  - `materialization.schedule` and freshness (package or source) are mutually
 *    exclusive.
 *
 * These tests run a real `Environment` + `Package.create` over temp dirs, so
 * they also prove end-to-end that the pinned compiler still ACCEPTS the
 * `#@ persist` annotation keys (`sharing`/`schedule`/`freshness.*`) at compile
 * — the rejection is the publisher's manifest-level gate, not a compile error —
 * and that `scope` is parsed from the manifest root and surfaced on the package.
 */
describe("persistence policy gate", () => {
   let rootDir: string;
   let envPath: string;

   async function loadPackage(
      model: string,
      manifest: Record<string, unknown> = {},
   ): Promise<Package> {
      const dir = path.join(envPath, "pkg");
      await fs.mkdir(dir, { recursive: true });
      await fs.writeFile(
         path.join(dir, "publisher.json"),
         JSON.stringify({ name: "pkg", description: "fixture", ...manifest }),
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
      rootDir = await fs.mkdtemp(path.join(os.tmpdir(), "publisher-policy-"));
      envPath = path.join(rootDir, "env");
      await fs.mkdir(envPath, { recursive: true });
   });

   afterEach(async () => {
      await fs.rm(rootDir, { recursive: true, force: true }).catch(() => {});
   });

   const PLAIN_MODEL = `##! experimental.persistence

#@ persist name="a_table"
source: a is duckdb.sql("SELECT 1 as x")

#@ persist name="b_table" refresh=full
source: b is duckdb.sql("SELECT 2 as x")
`;

   const SHARING_SOURCE_MODEL = `##! experimental.persistence

#@ persist name="s_table" sharing=private
source: s is duckdb.sql("SELECT 1 as x")
`;

   const SCHEDULE_SOURCE_MODEL = `##! experimental.persistence

#@ persist name="s_table" schedule="0 */6 * * *"
source: s is duckdb.sql("SELECT 1 as x")
`;

   const FRESHNESS_SOURCE_MODEL = `##! experimental.persistence

#@ persist name="f_table" freshness.window="1h"
source: f is duckdb.sql("SELECT 1 as x")
`;

   // ── scope parsing + surfacing ─────────────────────────────────────────

   it(
      "defaults scope to 'package' when the manifest omits it",
      async () => {
         const pkg = await loadPackage(PLAIN_MODEL);
         expect(pkg.getPackageMetadata().scope).toBe("package");
      },
      { timeout: 30000 },
   );

   it(
      "surfaces an explicit scope: version from the manifest root",
      async () => {
         const pkg = await loadPackage(PLAIN_MODEL, { scope: "version" });
         expect(pkg.getPackageMetadata().scope).toBe("version");
      },
      { timeout: 30000 },
   );

   it(
      "fails the load on an invalid scope value",
      async () => {
         await expect(
            loadPackage(PLAIN_MODEL, { scope: "shared" }),
         ).rejects.toThrow(/scope/i);
      },
      { timeout: 30000 },
   );

   // ── Rule 1: per-source sharing/schedule retired ───────────────────────

   it(
      "rejects a per-source sharing= annotation, pointing at root scope",
      async () => {
         const pkg = await loadPackage(SHARING_SOURCE_MODEL, {
            scope: "version",
         });
         const joined = pkg.formatInvalidPersistencePolicy();
         expect(joined).toContain('"s"');
         expect(joined).toContain("sharing");
         expect(joined).toContain('"scope"');
      },
      { timeout: 30000 },
   );

   it(
      "rejects a per-source schedule= annotation, pointing at the root schedule",
      async () => {
         const pkg = await loadPackage(SCHEDULE_SOURCE_MODEL, {
            scope: "version",
         });
         const joined = pkg.formatInvalidPersistencePolicy();
         expect(joined).toContain('"s"');
         expect(joined).toContain("schedule");
         expect(joined).toContain("materialization.schedule");
         // The retired field is not emitted on the wire plan.
         const source = sourceByName(pkg, "s") as Record<string, unknown>;
         expect(source.schedule).toBeUndefined();
         expect(source.sharing).toBeUndefined();
      },
      { timeout: 30000 },
   );

   // ── Rule 2: schedule requires scope: version ──────────────────────────

   it(
      "rejects a package schedule when scope is package (the default)",
      async () => {
         const pkg = await loadPackage(PLAIN_MODEL, {
            materialization: { schedule: "0 6 * * *" },
         });
         const joined = pkg.formatInvalidPersistencePolicy();
         expect(joined).toContain("materialization.schedule");
         expect(joined).toContain('"scope": "version"');
      },
      { timeout: 30000 },
   );

   it(
      "accepts a package schedule when scope is version and no freshness is set",
      async () => {
         const pkg = await loadPackage(PLAIN_MODEL, {
            scope: "version",
            materialization: { schedule: "0 6 * * *" },
         });
         expect(pkg.persistencePolicyWarnings()).toEqual([]);
      },
      { timeout: 30000 },
   );

   // ── Rule 3: schedule XOR freshness ────────────────────────────────────

   it(
      "rejects a schedule declared alongside package-level freshness",
      async () => {
         const pkg = await loadPackage(PLAIN_MODEL, {
            scope: "version",
            materialization: {
               schedule: "0 6 * * *",
               freshness: { window: "24h" },
            },
         });
         const joined = pkg.formatInvalidPersistencePolicy();
         expect(joined).toContain("mutually exclusive");
      },
      { timeout: 30000 },
   );

   it(
      "rejects a schedule declared alongside a source-level freshness",
      async () => {
         const pkg = await loadPackage(FRESHNESS_SOURCE_MODEL, {
            scope: "version",
            materialization: { schedule: "0 6 * * *" },
         });
         const joined = pkg.formatInvalidPersistencePolicy();
         expect(joined).toContain("mutually exclusive");
      },
      { timeout: 30000 },
   );

   // ── Rule 4: the cron must be a valid 5-field UNIX expression ──────────

   it(
      "rejects an unparseable schedule cron",
      async () => {
         const pkg = await loadPackage(PLAIN_MODEL, {
            scope: "version",
            materialization: { schedule: "not a cron" },
         });
         const joined = pkg.formatInvalidPersistencePolicy();
         expect(joined).toContain("valid");
         expect(joined).toContain("UNIX cron");
      },
      { timeout: 30000 },
   );

   it(
      "rejects a cron-parser extension the UNIX grammar lacks (L)",
      async () => {
         // Guards config-parity: cron-parser accepts `0 6 L * *` but the control
         // plane's UNIX parser does not, so publish must reject it too rather
         // than let it silently never arm in production.
         const pkg = await loadPackage(PLAIN_MODEL, {
            scope: "version",
            materialization: { schedule: "0 6 L * *" },
         });
         expect(pkg.formatInvalidPersistencePolicy()).toContain("UNIX cron");
      },
      { timeout: 30000 },
   );

   // ── inert when nothing is declared ────────────────────────────────────

   it(
      "is inert for a plain package with no schedule and no forbidden knobs",
      async () => {
         const pkg = await loadPackage(PLAIN_MODEL, { scope: "package" });
         expect(pkg.persistencePolicyWarnings()).toEqual([]);
      },
      { timeout: 30000 },
   );

   it(
      "accepts source-level freshness without a schedule in either scope",
      async () => {
         const pkg = await loadPackage(FRESHNESS_SOURCE_MODEL, {
            scope: "package",
         });
         expect(pkg.persistencePolicyWarnings()).toEqual([]);
         expect(sourceByName(pkg, "f").freshness).toEqual({ window: "1h" });
      },
      { timeout: 30000 },
   );

   // ── within-package persist-target collisions ──────────────────────────

   // Two DISTINCT sources both resolving to name="dup" in the same destination
   // (here their own duckdb warehouse — colocated) — a self-assign clobber.
   const COLLIDING_MODEL = `##! experimental.persistence

#@ persist name="dup"
source: a is duckdb.sql("SELECT 1 as x")

#@ persist name="dup"
source: b is duckdb.sql("SELECT 2 as y")
`;

   // Same name= but DIFFERENT destinations — no collision (different tables).
   const SAME_NAME_DIFF_DEST_MODEL = `##! experimental.persistence

#@ persist name="dup" storage=lake
source: a is duckdb.sql("SELECT 1 as x")

#@ persist name="dup"
source: b is duckdb.sql("SELECT 2 as y")
`;

   it(
      "warns when two distinct sources resolve to the same name= + destination",
      async () => {
         const pkg = await loadPackage(COLLIDING_MODEL);
         const warnings = pkg.persistenceCollisionWarnings();
         expect(warnings).toHaveLength(1);
         expect(warnings[0]).toContain('"a"');
         expect(warnings[0]).toContain('"b"');
         expect(warnings[0]).toContain('"dup"');
      },
      { timeout: 30000 },
   );

   it(
      "surfaces the collision on the package status warnings (not just the log)",
      async () => {
         const pkg = await loadPackage(COLLIDING_MODEL);
         const warnings = pkg.getPackageMetadata().warnings ?? [];
         const collision = warnings.find((w) =>
            w.message?.includes("resolve to the same materialized table"),
         );
         expect(collision).toBeDefined();
         expect(collision!.message).toContain('"dup"');
      },
      { timeout: 30000 },
   );

   it(
      "does not warn when the same name= targets different destinations",
      async () => {
         const pkg = await loadPackage(SAME_NAME_DIFF_DEST_MODEL);
         expect(pkg.persistenceCollisionWarnings()).toEqual([]);
         const warnings = pkg.getPackageMetadata().warnings ?? [];
         expect(
            warnings.some((w) =>
               w.message?.includes("resolve to the same materialized table"),
            ),
         ).toBe(false);
      },
      { timeout: 30000 },
   );

   it(
      "does not warn for a plain package with distinct persist names",
      async () => {
         const pkg = await loadPackage(PLAIN_MODEL);
         expect(pkg.persistenceCollisionWarnings()).toEqual([]);
      },
      { timeout: 30000 },
   );

   it(
      "collisions never enter persistencePolicyWarnings (the strict-publish set)",
      async () => {
         const pkg = await loadPackage(COLLIDING_MODEL);
         // The collision is real…
         expect(pkg.persistenceCollisionWarnings()).toHaveLength(1);
         // …but it must not ride the always-strict-at-publish policy set.
         expect(pkg.persistencePolicyWarnings()).toEqual([]);
      },
      { timeout: 30000 },
   );

   it(
      "gates the publish rejection behind PERSIST_COLLISION_ENFORCE",
      async () => {
         const pkg = await loadPackage(COLLIDING_MODEL);
         const prev = process.env.PERSIST_COLLISION_ENFORCE;
         try {
            delete process.env.PERSIST_COLLISION_ENFORCE;
            // Default: warn-only, so the publish gate string is empty.
            expect(pkg.formatPersistenceCollisionRejections()).toBe("");
            process.env.PERSIST_COLLISION_ENFORCE = "true";
            // Enforced: the collision becomes a publish rejection.
            expect(pkg.formatPersistenceCollisionRejections()).toContain(
               '"dup"',
            );
         } finally {
            if (prev === undefined)
               delete process.env.PERSIST_COLLISION_ENFORCE;
            else process.env.PERSIST_COLLISION_ENFORCE = prev;
         }
      },
      { timeout: 30000 },
   );
});
