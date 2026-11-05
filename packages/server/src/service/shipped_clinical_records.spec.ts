// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * `examples/clinical-records` is the worked example for governing a model
 * against an access policy. This pins its two halves:
 *
 *   - The clean package loads with no warning at all, every field on every
 *     published surface is classified, every gate reads only `#(secure)`
 *     givens, and each surface's gates decide the way the policy's workforce
 *     table says.
 *   - Each seeded violation under `violations/` has the outcome its README
 *     promises: the three mechanical ones refuse the load with their reason,
 *     and the two that only a review can catch load, and leak.
 *
 * Runs through `Package.create`, because the refusals and the query boundary
 * are package-level.
 */
import { unrecordedAudit } from "./access_record";
import { DuckDBConnection } from "@malloydata/db-duckdb";
import type { GivenValue } from "@malloydata/malloy";
import { afterAll, beforeAll, describe, expect, it } from "bun:test";
import * as fs from "fs";
import * as os from "os";
import * as path from "path";
import {
   AccessDeniedError,
   BadRequestError,
   NotQueryableError,
} from "../errors";
import { Package } from "./package";

const EXAMPLE = path.resolve(
   import.meta.dir,
   "../../../../examples/clinical-records",
);
const ORG = "e002090d-4e92-300e-b41e-7d1f21dee4c6";

async function load(dir: string): Promise<{
   pkg: Package;
   close: () => Promise<void>;
}> {
   const { MalloyConfig, FixedConnectionMap } = await import(
      "@malloydata/malloy"
   );
   const duckdb = new DuckDBConnection("duckdb", ":memory:", dir);
   const malloyConfig = new MalloyConfig({ connections: {} });
   malloyConfig.wrapConnections(
      () => new FixedConnectionMap(new Map([["duckdb", duckdb]]), "duckdb"),
   );
   try {
      const pkg = await Package.create(
         "examples",
         "clinical-records",
         dir,
         malloyConfig,
      );
      return { pkg, close: () => duckdb.close() };
   } catch (error) {
      await duckdb.close();
      throw error;
   }
}

/** A copy of the package with one seeded violation in place of the file its variant names. */
function withViolation(name: string): { dir: string; cleanup: () => void } {
   const variant = fs
      .readdirSync(path.join(EXAMPLE, "violations", name))
      .find((f) => f.endsWith(".malloy.txt"))!;
   const target = variant.replace(/\.txt$/, "");
   const dir = fs.mkdtempSync(path.join(os.tmpdir(), "clinical-violation-"));
   for (const file of [
      "base.malloy",
      "core.malloy",
      "deidentify.malloy",
      "domains.malloy",
      "index.malloy",
      "publisher.json",
   ].filter((f) => f !== target)) {
      fs.copyFileSync(path.join(EXAMPLE, file), path.join(dir, file));
   }
   fs.symlinkSync(path.join(EXAMPLE, "data"), path.join(dir, "data"));
   fs.copyFileSync(
      path.join(EXAMPLE, "violations", name, variant),
      path.join(dir, target),
   );
   return {
      dir,
      cleanup: () => fs.rmSync(dir, { recursive: true, force: true }),
   };
}

describe("examples/clinical-records, clean", () => {
   let pkg: Package;
   let close: () => Promise<void>;
   beforeAll(async () => {
      ({ pkg, close } = await load(EXAMPLE));
   });
   afterAll(async () => close());

   const run = (query: string, givens: Record<string, unknown>) =>
      pkg
         .getModel("index.malloy")!
         .getQueryResults(
            unrecordedAudit(),
            undefined,
            undefined,
            query,
            {},
            true,
            givens as Record<string, GivenValue>,
         );

   it("loads with no warning", () => {
      expect(pkg.getPackageMetadata().warnings ?? []).toEqual([]);
   });

   it("classifies every published field and gates every domain on secure givens only", () => {
      const summary = pkg.getClassificationSummary();
      expect(summary).toBeDefined();
      const domains = summary!.sources;
      expect(domains.map((d) => d.name).sort()).toEqual([
         "billing",
         "partner_lds",
         "patient_chart",
         "population_health",
         "research",
      ]);
      for (const d of domains) {
         expect(d.unclassified).toBe(0);
         expect(d.carries).toBeDefined();
         // What a domain serves stays within what it declares.
         for (const c of Object.keys(d.classCounts)) {
            expect(d.carries).toContain(c);
         }
      }
      for (const name of ["research", "population_health", "partner_lds"]) {
         const d = domains.find((x) => x.name === name)!;
         expect(d.classCounts["phi.direct_identifier"]).toBeUndefined();
      }
      // Every gate on a published domain reads only #(secure) givens.
      const sources = pkg.getModel("index.malloy")!.getSources() ?? [];
      for (const name of domains.map((d) => d.name)) {
         const src = sources.find((x) => x.name === name)!;
         const secure = new Set(
            (src.givens ?? []).filter((g) => g.secure).map((g) => g.name),
         );
         const gates = [...(src.authorize ?? []), ...(src.accessFilter ?? [])];
         expect(gates.length).toBeGreaterThan(0);
         for (const gate of gates) {
            const read = [...gate.matchAll(/\$([A-Z_]+)/g)].map((m) => m[1]);
            expect(read.length).toBeGreaterThan(0);
            for (const g of read) expect(secure.has(g)).toBe(true);
         }
      }
   });

   it("scopes a clinician to their organizations and refuses the wrong workforce class", async () => {
      const own = await run(
         "run: patient_chart -> { aggregate: n is count(organization.organization_name) }",
         {
            ROLES: ["clinical"],
            ORGANIZATIONS: [ORG],
         },
      );
      expect(own.compactResult).toEqual([{ n: 1 }]);
      await expect(
         run("run: patient_chart -> { aggregate: encounter_count }", {
            ROLES: ["billing"],
            ORGANIZATIONS: [ORG],
         }),
      ).rejects.toBeInstanceOf(AccessDeniedError);
      await expect(
         run("run: partner_lds -> { aggregate: encounter_count }", {
            ROLES: ["partner"],
            ATTRIBUTES: [],
         }),
      ).rejects.toBeInstanceOf(AccessDeniedError);
   });

   it("refuses the bases and cores as query targets", async () => {
      await expect(
         run("run: patients_core -> { aggregate: patient_count }", {
            ROLES: ["clinical"],
         }),
      ).rejects.toBeInstanceOf(NotQueryableError);
   });

   it("caps ages at 90 on the research surface", async () => {
      const r = await run(
         "run: research -> { aggregate: oldest is max(patient.age) }",
         { ROLES: ["researcher"] },
      );
      expect(r.compactResult).toEqual([{ oldest: 90 }]);
   });

   it("withholds the year of birth past age 89 on the research surface", async () => {
      const r = await run(
         "run: research -> { aggregate: earliest is min(patient.birth_year) }",
         { ROLES: ["researcher"] },
      );
      expect(Number(r.compactResult[0].earliest)).toBeGreaterThanOrEqual(1930);
   });
});

describe("examples/clinical-records, seeded violations", () => {
   it.each([
      [
         "01-research-reidentifies",
         "source research (index.malloy) carries [none, phi.clinical, phi.financial, phi.quasi_identifier] but exposes person.ssn",
      ],
      [
         "04-requester-asserted-scope",
         "source patient_chart (index.malloy) is gated on $ORGANIZATIONS, which is not declared #(secure)",
      ],
      [
         "05-billing-reopened",
         "source billing (index.malloy) is open to every caller and joins patient, which is locked",
      ],
   ])("refuses %s", async (name, reason) => {
      const { dir, cleanup } = withViolation(name);
      try {
         let caught: unknown;
         try {
            const { close } = await load(dir);
            await close();
         } catch (error) {
            caught = error;
         }
         expect(caught).toBeInstanceOf(BadRequestError);
         expect((caught as Error).message).toContain(reason);
      } finally {
         cleanup();
      }
   });

   it("loads 06-birth-year-over-89, and a researcher reads a birth year below 1930", async () => {
      const { dir, cleanup } = withViolation("06-birth-year-over-89");
      try {
         const { pkg, close } = await load(dir);
         try {
            expect(pkg.getPackageMetadata().warnings ?? []).toEqual([]);
            const r = await pkg
               .getModel("index.malloy")!
               .getQueryResults(
                  unrecordedAudit(),
                  undefined,
                  undefined,
                  "run: research -> { aggregate: earliest is min(patient.birth_year) }",
                  {},
                  true,
                  { ROLES: ["researcher"] } as Record<string, GivenValue>,
               );
            expect(Number(r.compactResult[0].earliest)).toBeLessThan(1930);
         } finally {
            await close();
         }
      } finally {
         cleanup();
      }
   });

   it.each([["02-mislabeled-identifier"], ["03-billing-all-organizations"]])(
      "loads %s, which only a review can catch",
      async (name) => {
         const { dir, cleanup } = withViolation(name);
         try {
            const { pkg, close } = await load(dir);
            try {
               expect(pkg.getPackageMetadata().warnings ?? []).toEqual([]);
            } finally {
               await close();
            }
         } finally {
            cleanup();
         }
      },
   );
});
