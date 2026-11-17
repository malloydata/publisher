// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { afterEach, beforeEach, describe, expect, it } from "bun:test";
import * as fs from "fs/promises";
import * as os from "os";
import * as path from "path";

import { Environment } from "./environment";
import type { Package } from "./package";
import { installSettings } from "../test_helpers/settings_env";
import { BadRequestError } from "../errors";
import { writePackageFiles } from "../test_helpers/temp_package";
import { parsePackageName } from "./identity";

/**
 * The publish gate for the package's Malloy Persistence policy (persistence.md
 * §3.1, §9.2–§9.5). Scope is a single package-level mode (`Package.scope`) and a
 * materialization cron is package-root-only + version-scope-only:
 *
 *  - per-source `#@ persist ... sharing=`/`schedule=` are retired keys
 *    (`persist-retired-key`, which fails the load);
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
      writePackageFiles(
         dir,
         {
            "model.malloy": model,
         },
         { manifest: { name: "pkg", description: "fixture", ...manifest } },
      );
      const env = await Environment.create("testEnv", envPath, []);
      await env.addPackage("pkg");
      return env.getPackage(parsePackageName("pkg"), false);
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

   it.each([
      ["sharing=", SHARING_SOURCE_MODEL, "materialization.scope"],
      ["schedule=", SCHEDULE_SOURCE_MODEL, "materialization.schedule"],
   ])(
      "fails the load on a per-source %s annotation, pointing at the root setting",
      async (_key, model, root) => {
         let error: unknown;
         try {
            await loadPackage(model, { scope: "version" });
         } catch (e) {
            error = e;
         }
         expect(error).toBeInstanceOf(BadRequestError);
         const message = (error as Error).message;
         expect(message).toContain('#@ persist source "s"');
         expect(message).toContain(root);
         expect(message).toContain("[persist-retired-key]");
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

   // ── refused persist sources reach the author ──────────────────────────

   // A given in the persisted query, with no `storage=`: refused by the
   // colocated gate, which is the gate a plain `#@ persist` is evaluated
   // against. Chosen because it needs no storage destination to resolve, so the
   // refusal is the only thing under test.
   const GIVEN_IN_QUERY_MODEL = `##! experimental.persistence
##! experimental.givens

given: tenant :: string is 'acme'
source: base is duckdb.sql("SELECT 1 AS x, 'acme' AS tenant")

#@ persist name="p_table"
source: p is base -> { where: tenant = $tenant; aggregate: c is count() }
`;

   it(
      "surfaces a refused persist source on the package warnings",
      async () => {
         const pkg = await loadPackage(GIVEN_IN_QUERY_MODEL);
         const warnings = pkg.getPackageMetadata().warnings ?? [];
         const refusal = warnings.find((w) =>
            w.message?.includes("persisted query references a given"),
         );
         expect(refusal).toBeDefined();
         // The source the author named, so the finding is attributable to a line
         // they can open — not the refusal's map key.
         expect(refusal!.subject).toBe("p");
         expect(refusal!.model).toBe("model.malloy");
         // The remedy the gate authored, carried rather than re-worded.
         expect(refusal!.message).toContain("Keep the given OUT");
      },
      { timeout: 30000 },
   );

   it(
      "warns about a refused source even though the build plan omits it",
      async () => {
         const pkg = await loadPackage(GIVEN_IN_QUERY_MODEL);
         const plan = pkg.getBuildPlan();
         // A storage/colocated refusal means ABSENCE from `sources` — so the plan
         // itself carries no trace of the annotation, and the warning is the only
         // thing standing between the author and a silent no-op.
         expect(
            Object.values(plan?.sources ?? {}).some((s) => s.name === "p"),
         ).toBe(false);
         expect(
            Object.values(plan?.refusedSources ?? {}).some(
               (r) => r.name === "p",
            ),
         ).toBe(true);
         expect(
            (pkg.getPackageMetadata().warnings ?? []).some((w) =>
               w.message?.includes("persisted query references a given"),
            ),
         ).toBe(true);
      },
      { timeout: 30000 },
   );

   it(
      "raises no refusal warning for a package whose persist sources are eligible",
      async () => {
         const pkg = await loadPackage(PLAIN_MODEL);
         expect(pkg.getBuildPlan()?.refusedSources ?? {}).toEqual({});
         expect(
            (pkg.getPackageMetadata().warnings ?? []).some((w) =>
               w.message?.includes("cannot be materialized"),
            ),
         ).toBe(false);
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

   /** The load's failure message for `model`, which must not load. */
   async function loadError(model: string): Promise<string> {
      let error: unknown;
      try {
         await loadPackage(model);
      } catch (e) {
         error = e;
      }
      expect(error).toBeInstanceOf(BadRequestError);
      return (error as Error).message;
   }

   it(
      "fails the load when two distinct sources resolve to the same name= + destination",
      async () => {
         const message = await loadError(COLLIDING_MODEL);
         expect(message).toContain('"a"');
         expect(message).toContain('"b"');
         expect(message).toContain('"dup"');
         expect(message).toContain("[materialization-table-collision]");
         // Naming the operator's opt-out.
         expect(message).toContain("PERSIST_COLLISION_ENFORCE=false");
      },
      { timeout: 30000 },
   );

   it.each([
      ["main.t, then t", "main.t", "t"],
      ["t, then main.t", "t", "main.t"],
   ])(
      "fails the load when a name= with the default schema and one without it are one table: %s",
      async (_order, first, second) => {
         const message = await loadError(`##! experimental.persistence

#@ persist name="${first}"
source: a is duckdb.sql("SELECT 1 as x")

#@ persist name="${second}"
source: b is duckdb.sql("SELECT 2 as y")
`);
         expect(message).toContain("[materialization-table-collision]");
      },
      { timeout: 30000 },
   );

   it(
      "fails the load on a collision even when one of the two is an incremental source DuckDB cannot build",
      async () => {
         const message = await loadError(`##! experimental.persistence

source: raw is duckdb.sql("SELECT 1 as x, DATE '2024-01-01' as d")

#@ persist name="t" refresh="incremental" watermark="d"
source: a is raw -> { group_by: x, d }

#@ persist name="t"
source: b is raw -> { group_by: x }
`);
         expect(message).toContain("[materialization-table-collision]");
      },
      { timeout: 30000 },
   );

   it(
      'fails the load on name="t" beside name="other.t": a bare name may land in that schema, so it is judged one table',
      async () => {
         // A known false positive when the connection's default schema is
         // not `other`: a bare name's schema cannot be told at load, and a
         // missed collision would serve one source the other's rows.
         const message = await loadError(`##! experimental.persistence

#@ persist name="t"
source: a is duckdb.sql("SELECT 1 as x")

#@ persist name="other.t"
source: b is duckdb.sql("SELECT 2 as y")
`);
         expect(message).toContain("[materialization-table-collision]");
      },
      { timeout: 30000 },
   );

   it(
      "does not collide two name=s in two named schemas",
      async () => {
         const pkg = await loadPackage(`##! experimental.persistence

#@ persist name="s1.t"
source: a is duckdb.sql("SELECT 1 as x")

#@ persist name="s2.t"
source: b is duckdb.sql("SELECT 2 as y")
`);
         expect(pkg.materialization().persistenceCollisionWarnings()).toEqual(
            [],
         );
      },
      { timeout: 30000 },
   );

   it(
      "with PERSIST_COLLISION_ENFORCE=false, loads and refuses the colliding sources alone, on the package's warnings",
      async () => {
         const { restore } = installSettings({
            env: { PERSIST_COLLISION_ENFORCE: "false" },
         });
         try {
            const pkg = await loadPackage(COLLIDING_MODEL);
            expect(
               pkg.materialization().persistenceCollisionWarnings(),
            ).toHaveLength(1);
            const collision = (pkg.getPackageMetadata().warnings ?? []).find(
               (w) =>
                  w.message?.includes("resolve to the same materialized table"),
            );
            expect(collision).toMatchObject({
               code: "persist-not-built",
               severity: "warn",
            });
            expect(collision!.message).toContain('"dup"');
            expect(pkg.formatInvalidAnnotations()).toBe("");
            // The manifest policy set never carries it.
            expect(pkg.persistencePolicyWarnings()).toEqual([]);
         } finally {
            restore();
         }
      },
      { timeout: 30000 },
   );

   it(
      "does not warn when the same name= targets different destinations",
      async () => {
         const pkg = await loadPackage(SAME_NAME_DIFF_DEST_MODEL);
         expect(pkg.materialization().persistenceCollisionWarnings()).toEqual(
            [],
         );
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
         expect(pkg.materialization().persistenceCollisionWarnings()).toEqual(
            [],
         );
      },
      { timeout: 30000 },
   );
});
