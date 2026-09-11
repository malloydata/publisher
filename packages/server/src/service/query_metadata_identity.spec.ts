// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { afterEach, beforeEach, describe, expect, it } from "bun:test";
import * as fs from "fs/promises";
import * as os from "os";
import * as path from "path";

import { Environment } from "./environment";

/**
 * Query metadata is observability, and it must never be identity.
 *
 * `sourceEntityId` is a content address: `makeBuildId(connectionDigest,
 * getSQL())`. If a tag ever reached either input, declaring one would
 * re-address every persist source in the package — the manifest lookup would
 * miss, every source would rebuild into a new physical table, and the tables
 * built before the change would be orphaned at rest. Silent, expensive, and
 * discovered from a warehouse bill.
 *
 * It holds by construction today: `queryMetadata` exists only on Malloy's
 * `RunSQLOptions` (the dispatch path), never on the compile options `getSQL`
 * takes, and every connector's `getDigest()` enumerates connection-shape fields.
 * Nothing pins it, which is what this file is for — the invariant is cheap to
 * assert and its failure mode is the worst on this surface.
 *
 * Deliberately end-to-end over a real `Environment`/`Package` rather than the
 * fake source fixture: `fakeSource.makeBuildId` returns a constant, so a
 * fixture-based version of this test would pass no matter what leaked. The
 * three layers exercised here are the ones that ride in the model text, where a
 * leak is actually plausible — the manifest block, the model-file `##`
 * envelope, and the source's own `#@ persist`.
 */
describe("query metadata is excluded from persist identity", () => {
   let rootDir: string;
   let envPath: string;

   /** The one persist source both variants declare, byte-identical but for the tags. */
   const model = (tagged: boolean) =>
      [
         "##! experimental.persistence",
         ...(tagged
            ? ['## materialization.queryMetadata.surface="marts"']
            : []),
         "",
         `source: raw is duckdb.sql("SELECT 1 as id, 'a' as category")`,
         "",
         tagged
            ? '#@ persist name="rollup" queryMetadata.tier="platinum"'
            : '#@ persist name="rollup"',
         "source: rollup is raw -> {",
         "  group_by: category",
         "  aggregate: n is count()",
         "}",
         "",
      ].join("\n");

   const manifest = (tagged: boolean) => ({
      name: "pkg",
      description: "fixture",
      materialization: {
         scope: "version",
         ...(tagged
            ? { queryMetadata: { team: "finance", tier: "bronze" } }
            : {}),
      },
   });

   /**
    * Load a fresh package from a fresh environment and return the entity id its
    * build plan assigned to `rollup`. A new environment per variant so nothing
    * is served from the previous compile.
    */
   async function entityIdFor(tagged: boolean): Promise<string> {
      const dir = path.join(envPath, "pkg");
      await fs.mkdir(dir, { recursive: true });
      await fs.writeFile(
         path.join(dir, "publisher.json"),
         JSON.stringify(manifest(tagged)),
      );
      await fs.writeFile(path.join(dir, "model.malloy"), model(tagged));

      const env = await Environment.create("testEnv", envPath, []);
      await env.addPackage("pkg");
      const pkg = await env.getPackage("pkg", false);
      const plan = pkg.getBuildPlan();
      const sources = Object.values(plan?.sources ?? {});
      // Guard the guard: an empty plan would make the comparison below vacuous,
      // and a `#@ persist` Malloy declines to treat as a build root yields
      // exactly that.
      expect(sources).toHaveLength(1);
      return sources[0].sourceEntityId as string;
   }

   beforeEach(async () => {
      rootDir = await fs.mkdtemp(path.join(os.tmpdir(), "publisher-qm-id-"));
      envPath = path.join(rootDir, "env");
      await fs.mkdir(envPath, { recursive: true });
   });

   afterEach(async () => {
      await fs.rm(rootDir, { recursive: true, force: true }).catch(() => {});
   });

   it(
      "declaring metadata at every model-side layer does not re-address the source",
      async () => {
         const bare = await entityIdFor(false);
         await fs.rm(path.join(envPath, "pkg"), {
            recursive: true,
            force: true,
         });
         const tagged = await entityIdFor(true);

         // Byte-identical, not merely "both defined": this is a content address,
         // and a differing id means every table built under the old one is
         // orphaned the moment someone adds a tag.
         expect(tagged).toBe(bare);
      },
      { timeout: 30000 },
   );

   it(
      "the resolved metadata really did reach the plan",
      async () => {
         // Without this the test above passes just as well when the tags are
         // being dropped on the floor — the failure mode that would make it
         // look like the invariant holds while nothing is being tagged at all.
         const dir = path.join(envPath, "pkg");
         await fs.mkdir(dir, { recursive: true });
         await fs.writeFile(
            path.join(dir, "publisher.json"),
            JSON.stringify(manifest(true)),
         );
         await fs.writeFile(path.join(dir, "model.malloy"), model(true));

         const env = await Environment.create("testEnv", envPath, []);
         await env.addPackage("pkg");
         const pkg = await env.getPackage("pkg", false);
         const source = Object.values(pkg.getBuildPlan()?.sources ?? {})[0];

         expect(source.queryMetadata).toMatchObject({
            team: "finance",
            surface: "marts",
            // Most specific wins: the source's own `#@ persist` over the
            // manifest's `tier`.
            tier: "platinum",
         });
      },
      { timeout: 30000 },
   );
});
