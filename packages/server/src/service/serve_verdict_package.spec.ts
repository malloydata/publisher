// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * The serve verdict on a real package load (materialization v2, D2;
 * ms2data/service#6277): a curated surface that reaches one persisted source
 * and not another, the storage mode, a bind, and the publish warning that
 * names a materialized source nothing can query.
 */
import { afterEach, describe, expect, it, spyOn } from "bun:test";
import { logger } from "../logger";
import { installSettings } from "../test_helpers/settings_env";
import {
   loadTempPackage,
   usePackageLoadPool,
   type LoadedTempPackage,
} from "../test_helpers/temp_package";
import {
   PackageMaterialization,
   PERSIST_SOURCE_REFUSED_CODE,
   type PackageMaterializationOwner,
} from "./materialization/package_materialization";
import { MATERIALIZATION_UNREACHABLE_CODE } from "./materialization/serve/serve_verdict";

const ROWS = `duckdb.sql("select * from (values (1, 'east', 10), (2, 'west', 20)) as t(id, region, amount)")`;

const ORDERS = `##! experimental.persistence
#@ persist name="orders_t"
source: orders is ${ROWS} -> { select: * }

#@ persist name="staging_t"
source: staging is ${ROWS} -> { group_by: region; aggregate: total is amount.sum() }

source: orders_public is orders extend {
   measure: total is amount.sum()
   view: by_region is { group_by: region; aggregate: total }
}
`;

let loaded: LoadedTempPackage | undefined;
let restoreSettings: (() => void) | undefined;

afterEach(async () => {
   await loaded?.cleanup();
   loaded = undefined;
   restoreSettings?.();
   restoreSettings = undefined;
});

function byName<T extends { sourceName: string }>(
   verdicts: T[],
): Record<string, T> {
   return Object.fromEntries(verdicts.map((v) => [v.sourceName, v]));
}

describe("a package's serve verdicts", () => {
   usePackageLoadPool();

   it("names the persisted source a curated surface never reaches, and warns once", async () => {
      loaded = await loadTempPackage({
         "orders.malloy": ORDERS,
         "index.malloy": `import "orders.malloy"\nexport { orders_public }\n`,
      });
      const verdicts = byName(loaded.pkg.getServeVerdicts());
      expect(verdicts.orders).toMatchObject({
         tier: "colocated",
         verdict: "not_built",
      });
      expect(verdicts.staging).toMatchObject({
         verdict: "live",
         reason: "unreachable",
      });
      const warnings = (loaded.pkg.getPackageMetadata().warnings ?? []).filter(
         (w) => w.code === MATERIALIZATION_UNREACHABLE_CODE,
      );
      expect(warnings.map((w) => w.subject)).toEqual(["staging"]);
      expect(warnings[0].message).toContain("index.malloy");
      // Not an annotation finding: the surface curates it out, the #@ is fine.
      expect(
         loaded.pkg
            .annotationFindingWarnings()
            .some((w) => w.code === MATERIALIZATION_UNREACHABLE_CODE),
      ).toBe(false);
   });

   it("finds every source reached when nothing is curated", async () => {
      loaded = await loadTempPackage({ "orders.malloy": ORDERS });
      const verdicts = loaded.pkg.getServeVerdicts();
      // orders_public extends a persisted source, so it inherits #@ persist.
      expect(verdicts.map((v) => [v.sourceName, v.verdict])).toEqual([
         ["orders", "not_built"],
         ["orders_public", "not_built"],
         ["staging", "not_built"],
      ]);
      expect(
         (loaded.pkg.getPackageMetadata().warnings ?? []).some(
            (w) => w.code === MATERIALIZATION_UNREACHABLE_CODE,
         ),
      ).toBe(false);
   });

   it("reaches a source only an exported query-source or join reads", async () => {
      loaded = await loadTempPackage({
         "orders.malloy": ORDERS,
         "index.malloy": `import "orders.malloy"
source: by_region is staging -> { group_by: region; aggregate: t is total.sum() }
source: joined is ${ROWS} extend {
   join_one: o is orders on o.id = id
}
export { by_region, joined }
`,
      });
      const verdicts = byName(loaded.pkg.getServeVerdicts());
      expect(verdicts.staging.verdict).toBe("not_built");
      expect(verdicts.orders.verdict).toBe("not_built");
   });

   it("reaches a source only a published named query reads", async () => {
      loaded = await loadTempPackage({
         "orders.malloy": ORDERS,
         "index.malloy": `import "orders.malloy"
query: top_regions is staging -> { group_by: region; aggregate: t is total.sum() }
export { orders_public, top_regions }
`,
      });
      const verdicts = byName(loaded.pkg.getServeVerdicts());
      expect(verdicts.staging.verdict).toBe("not_built");
      expect(
         (loaded.pkg.getPackageMetadata().warnings ?? []).some(
            (w) => w.code === MATERIALIZATION_UNREACHABLE_CODE,
         ),
      ).toBe(false);
   });

   describe("under a row-level gate, as routing judges a query's entry point", () => {
      const GATED = `##! experimental { persistence givens }
given: ROLE :: string[]

#@ persist name="orders_t" storage="lake"
source: orders is ${ROWS} -> { select: * }

#(authorize) 'finance' in $ROLE
source: secure_orders is orders extend {}
`;

      it("sends a stored source live when every published entry point over it is gated", async () => {
         restoreSettings = installSettings({
            env: { PERSIST_STORAGE_MODE: "on" },
         }).restore;
         loaded = await loadTempPackage({
            "orders.malloy": GATED,
            "index.malloy": `import "orders.malloy"\nexport { secure_orders }\n`,
         });
         expect(byName(loaded.pkg.getServeVerdicts()).orders).toMatchObject({
            tier: "storage",
            verdict: "live",
            reason: "row_level_gate",
         });
      });

      it("serves it when an ungated entry point reaches it too", async () => {
         restoreSettings = installSettings({
            env: { PERSIST_STORAGE_MODE: "on" },
         }).restore;
         // Nothing curated: `orders` itself is queryable, with no gate.
         loaded = await loadTempPackage({ "orders.malloy": GATED });
         expect(byName(loaded.pkg.getServeVerdicts()).orders.verdict).toBe(
            "not_built",
         );
      });
   });

   it("codes the warning for a persisted source a compile-time gate refuses", async () => {
      // A given read inside the persisted query: the gate refuses the source.
      loaded = await loadTempPackage({
         "orders.malloy": `##! experimental { persistence givens }
given: REGION :: string

#@ persist name="orders_t"
source: orders is ${ROWS} -> { select: * ; where: region = $REGION }
`,
      });
      const refused = (loaded.pkg.getPackageMetadata().warnings ?? []).filter(
         (w) => w.code === PERSIST_SOURCE_REFUSED_CODE,
      );
      expect(refused).toHaveLength(1);
      expect(refused[0]).toMatchObject({ subject: "orders", severity: "warn" });
   });

   // GET …/status reads the verdicts on every poll: the entry-point walk is
   // done once per load, not once per read.
   it("walks the entry points once per load, however often the verdicts are read", async () => {
      loaded = await loadTempPackage({
         "orders.malloy": ORDERS,
         "index.malloy": `import "orders.malloy"\nexport { orders_public }\n`,
      });
      const first = loaded.pkg.getServeVerdicts();
      const models = [
         ...(
            loaded.pkg as unknown as { models: Map<string, object> }
         ).models.values(),
      ];
      const spies = models.map((model) =>
         spyOn(model as { getModelDef(): unknown }, "getModelDef"),
      );
      try {
         for (let i = 0; i < 3; i++) {
            expect(loaded.pkg.getServeVerdicts()).toEqual(first);
         }
         for (const spy of spies) expect(spy).not.toHaveBeenCalled();
      } finally {
         for (const spy of spies) spy.mockRestore();
      }
   });

   it("follows a bind: a colocated source serves once its table is bound", async () => {
      loaded = await loadTempPackage({ "orders.malloy": ORDERS });
      const plan = loaded.pkg.getBuildPlan();
      const orders = Object.values(plan?.sources ?? {}).find(
         (s) => s.name === "orders",
      );
      expect(orders).toBeDefined();
      loaded.pkg.bindColocatedServeManifest({
         [orders!.sourceEntityId]: { tableName: "orders_t" },
      });
      const verdicts = byName(loaded.pkg.getServeVerdicts());
      expect(verdicts.orders.verdict).toBe("serves");
      expect(verdicts.staging.verdict).toBe("not_built");
   });

   it("serves nothing from storage while PERSIST_STORAGE_MODE is off", async () => {
      restoreSettings = installSettings({
         env: { PERSIST_STORAGE_MODE: "off" },
      }).restore;
      loaded = await loadTempPackage({
         "orders.malloy": ORDERS.replace(
            '#@ persist name="orders_t"',
            '#@ persist name="orders_t" storage="lake"',
         ),
      });
      const verdicts = byName(loaded.pkg.getServeVerdicts());
      expect(verdicts.orders).toMatchObject({
         tier: "storage",
         verdict: "live",
         reason: "storage_mode",
      });
      expect(verdicts.staging.tier).toBe("colocated");
   });
});

describe("a serve verdict that cannot be computed", () => {
   // Reporting only: the dry run, the package read and the status poll all
   // carry verdicts, and none of them may fail because one could not be made.
   it("degrades to none instead of throwing", async () => {
      const broken = {
         servedStorageSourceNames: () => {
            throw new Error("boom");
         },
      };
      const materialization = new PackageMaterialization({
         models: () => new Map([["m.malloy", broken]]),
         packageName: () => "pkg",
      } as unknown as PackageMaterializationOwner);
      (materialization as unknown as { buildPlan: unknown }).buildPlan = {
         graphs: [],
         sources: {},
      };
      const warn = spyOn(logger, "warn");
      try {
         expect(materialization.serveVerdicts()).toEqual([]);
         // The computation ran and threw, rather than returning early.
         expect(warn).toHaveBeenCalledWith(
            "Could not compute serve verdicts",
            expect.anything(),
         );
      } finally {
         warn.mockRestore();
      }
      expect(materialization.verdictWarnings()).toEqual([]);
      expect(await materialization.previewServeVerdicts()).toEqual([]);
   });
});

describe("the deployment facts (P13) on the memoized path-report verdicts", () => {
   usePackageLoadPool();

   it("re-reads a fact that changes under a loaded package, with no reload", async () => {
      loaded = await loadTempPackage({ "orders.malloy": ORDERS });
      let canHold: boolean | undefined = true;
      loaded.pkg.setDeploymentFacts({
         destinationNames: () => [],
         connectionCanHoldTable: () => canHold,
      });
      const materialization = (
         loaded.pkg as unknown as { materialization: PackageMaterialization }
      ).materialization;
      const reasons = () =>
         [...(materialization.materializedTableCatalog().sourceVerdicts ?? [])]
            .filter(([, v]) => v.sourceName === "orders")
            .map(([, v]) => v.reason);

      expect(reasons()).toEqual([]);
      // The connection's type changes under the package: the memo must not
      // keep answering from before.
      canHold = false;
      expect(reasons()).toEqual(["connection_cannot_hold_table"]);
      canHold = true;
      expect(reasons()).toEqual([]);
   });
});
