import { afterEach, beforeEach, describe, expect, it } from "bun:test";
import * as fs from "fs/promises";
import * as os from "os";
import * as path from "path";

import { Environment } from "./environment";

/**
 * The publisher must surface a non-null `materialization` object on a loaded
 * package's metadata so the control plane can treat object-present as the
 * authoritative manifest policy ("this is what the manifest says") and
 * object-absent as "metadata not loaded this request" — never as a schedule
 * removal. A metadata PATCH must not drop that policy from the in-memory
 * metadata. Regression coverage for the re-materialization schedule self-wipe
 * (docs/bugs/materialization-schedule-self-wipe.md).
 *
 * Runs against a real `Environment` + real `Package.create` over temp dirs.
 */
describe("materialization schedule surfacing", () => {
   const MODEL = `source: ones is duckdb.sql("SELECT 1 as x")\n`;
   let rootDir: string;
   let envPath: string;

   async function writePackageDir(
      manifest: Record<string, unknown>,
   ): Promise<void> {
      const dir = path.join(envPath, "pkg");
      await fs.mkdir(dir, { recursive: true });
      await fs.writeFile(
         path.join(dir, "publisher.json"),
         JSON.stringify({ name: "pkg", description: "fixture", ...manifest }),
      );
      await fs.writeFile(path.join(dir, "model.malloy"), MODEL);
   }

   beforeEach(async () => {
      rootDir = await fs.mkdtemp(path.join(os.tmpdir(), "publisher-mat-"));
      envPath = path.join(rootDir, "env");
      await fs.mkdir(envPath, { recursive: true });
   });

   afterEach(async () => {
      await fs.rm(rootDir, { recursive: true, force: true }).catch(() => {});
   });

   it(
      "surfaces the manifest's materialization.schedule on load",
      async () => {
         const env = await Environment.create("testEnv", envPath, []);
         await writePackageDir({ materialization: { schedule: "0 6 * * *" } });
         await env.addPackage("pkg");

         const pkg = await env.getPackage("pkg", false);
         expect(pkg.getPackageMetadata().materialization).toEqual({
            schedule: "0 6 * * *",
            freshness: null,
         });
      },
      { timeout: 20000 },
   );

   it(
      "surfaces the manifest's materialization.freshness on load",
      async () => {
         const env = await Environment.create("testEnv", envPath, []);
         await writePackageDir({
            materialization: {
               freshness: { window: "24h", fallback: "stale_ok" },
            },
         });
         await env.addPackage("pkg");

         // Freshness rides the same channel as schedule: present-and-verbatim
         // when declared, null when unset — the control plane owns the
         // scheduling/gating logic and only needs the values surfaced.
         const pkg = await env.getPackage("pkg", false);
         expect(pkg.getPackageMetadata().materialization).toEqual({
            schedule: null,
            freshness: { window: "24h", fallback: "stale_ok" },
         });
      },
      { timeout: 20000 },
   );

   it(
      "surfaces a non-null materialization object even when the manifest declares none",
      async () => {
         const env = await Environment.create("testEnv", envPath, []);
         await writePackageDir({});
         await env.addPackage("pkg");

         // Object present with a null schedule — NOT a null object — so the
         // control plane reads it as an authoritative "no schedule declared",
         // not as "metadata unavailable".
         const pkg = await env.getPackage("pkg", false);
         expect(pkg.getPackageMetadata().materialization).toEqual({
            schedule: null,
            freshness: null,
         });
      },
      { timeout: 20000 },
   );

   it(
      "preserves the materialization policy across a metadata PATCH",
      async () => {
         const env = await Environment.create("testEnv", envPath, []);
         await writePackageDir({ materialization: { schedule: "0 6 * * *" } });
         await env.addPackage("pkg");

         // A description-only PATCH must not wipe the schedule from the
         // in-memory metadata. The bug: setPackageMetadata replaces the whole
         // object, so a later getPackage reported no schedule and the control
         // plane misread the gap as a removal and cleared the cadence.
         const updated = await env.updatePackage("pkg", {
            name: "pkg",
            description: "updated",
         });
         expect(updated.materialization).toEqual({
            schedule: "0 6 * * *",
            freshness: null,
         });

         const pkg = await env.getPackage("pkg", false);
         expect(pkg.getPackageMetadata().materialization).toEqual({
            schedule: "0 6 * * *",
            freshness: null,
         });
      },
      { timeout: 20000 },
   );
});
