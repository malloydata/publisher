// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

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
 * removal. Regression coverage for the re-materialization schedule self-wipe
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
            queryMetadata: null,
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
            queryMetadata: null,
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
      "surfaces a manifest's queryMetadata in both wire homes",
      async () => {
         // The canonical field is the one a client should read; the block is
         // populated alongside it only so an un-migrated client keeps working.
         const env = await Environment.create("testEnv", envPath, []);
         await writePackageDir({ queryMetadata: { team: "fin" } });
         await env.addPackage("pkg");

         const pkg = await env.getPackage("pkg", false);
         const metadata = pkg.getPackageMetadata();
         expect(metadata.queryMetadata).toEqual({ team: "fin" });
         expect(metadata.materialization).toMatchObject({
            queryMetadata: { team: "fin" },
         });
         expect(pkg.getDeclaredQueryMetadata()).toEqual({ team: "fin" });
      },
      { timeout: 20000 },
   );

   it(
      "does not untag the package when an edit adds only a schedule",
      async () => {
         // The block is what the BUILD path reads, and the bag is declared at
         // the root. Resolving the build's tags from the block alone would
         // drop it, so the next build's statements go out untagged for a
         // package that asked to be tagged.
         const env = await Environment.create("testEnv", envPath, []);
         await writePackageDir({
            scope: "version",
            queryMetadata: { team: "finance" },
         });
         await env.addPackage("pkg");

         // The author adds a schedule and nothing else, then reloads.
         await writePackageDir({
            scope: "version",
            queryMetadata: { team: "finance" },
            materialization: { schedule: "0 6 * * *" },
         });
         await env.getPackage("pkg", true);

         const pkg = await env.getPackage("pkg", false);
         expect(pkg.getDeclaredQueryMetadata()).toEqual({ team: "finance" });
         expect(pkg.getMaterializationConfig()).toMatchObject({
            schedule: "0 6 * * *",
            queryMetadata: { team: "finance" },
         });
      },
      { timeout: 20000 },
   );

   it(
      "warns about a package+model tag overflow with no source declaring one",
      async () => {
         // End-to-end over a real Environment: the floor has to be SIZED, not
         // just checkable. Sizing only the sources that declare their own bag
         // meant this package published clean while every query it serves shed
         // context properties.
         const env = await Environment.create("testEnv", envPath, []);
         const six = (prefix: string) =>
            Object.fromEntries(
               Array.from({ length: 6 }, (_, i) => [`${prefix}${i}`, "v"]),
            );
         const dir = path.join(envPath, "pkg");
         await fs.mkdir(dir, { recursive: true });
         await fs.writeFile(
            path.join(dir, "publisher.json"),
            JSON.stringify({
               name: "pkg",
               description: "fixture",
               queryMetadata: six("p"),
            }),
         );
         // Six more at the model file, and no `#@` on the source at all.
         await fs.writeFile(
            path.join(dir, "model.malloy"),
            Object.entries(six("m"))
               .map(([name, value]) => `## queryMetadata.${name}="${value}"`)
               .join("\n") +
               "\n" +
               MODEL,
         );
         await env.addPackage("pkg");

         const warnings =
            (await env.getPackage("pkg", false)).getPackageMetadata()
               .warnings ?? [];
         const overflow = warnings.filter((w) =>
            w.message?.includes("package + model file, merged"),
         );
         expect(overflow).toHaveLength(1);
         expect(overflow[0].model).toBe("model.malloy");
         expect(overflow[0].subject).toBeUndefined();
      },
      { timeout: 20000 },
   );
});
