// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { beforeAll, describe, expect, it } from "bun:test";
import {
   compileFcjFixture,
   type FcjFixture,
} from "../../test_helpers/fcj_fixture";
import { DEFAULT_PACKAGE_RETRIEVAL } from "../../service/package_retrieval";
import { buildCatalog, catalogInputOf, type Catalog } from "./catalog";
import { catalogReportLine } from "./catalog_store";

/**
 * A served root (`trips`) reaches an unpublished chain four joins deep:
 * trips > vehicle > licence > driver > convictions. `convictions` joins
 * `courts`, which is served. `fines` (served) reaches `drivers` in one join.
 */
const FILES: Record<string, string> = {
   "courts.malloy": `
#(doc) One row per court.
source: courts is duckdb.sql("select 1 as id, 'Leeds Crown' as nm") extend {
  #(doc) Name of the court.
  dimension: court_name is nm
}
`,
   "records.malloy": `
import "courts.malloy"

#(doc) One row per conviction, every conviction on record.
source: convictions is duckdb.sql("select 1 as id, 1 as driver_id, 1 as court_id, 'speeding' as offence") extend {
  #(doc) The offence the driver was convicted of.
  dimension: offence_name is offence
  #(doc) Number of convictions.
  measure: conviction_count is count()
  join_one: court is courts on court.id = court_id
}

#(doc) One row per driver.
source: drivers is duckdb.sql("select 1 as id, 'Ann' as nm") extend {
  #(doc) Name of the driver.
  dimension: driver_name is nm
  join_many: convictions on convictions.driver_id = id
}

#(doc) One row per licence.
source: licences is duckdb.sql("select 1 as id, 1 as driver_id") extend {
  join_one: driver is drivers on driver.id = driver_id
}

#(doc) One row per vehicle.
source: vehicles is duckdb.sql("select 1 as id, 1 as licence_id") extend {
  join_one: licence is licences on licence.id = licence_id
}
`,
   "fleet.malloy": `
import "records.malloy"

#(doc) One row per trip.
source: trips is duckdb.sql("select 1 as id, 1 as vehicle_id") extend {
  join_one: vehicle is vehicles on vehicle.id = vehicle_id
  #(doc) Number of trips.
  measure: trip_count is count()
}

#(doc) One row per fine.
source: fines is duckdb.sql("select 1 as id, 1 as driver_id") extend {
  join_one: driver is drivers on driver.id = driver_id
}
`,
};
const SERVED = ["courts.malloy", "fleet.malloy"];
/** trips' fields with no unservedCopies: its own, and the shipped copies two joins deep. */
const SHIPPED_TRIPS = [
   "dimension:trips:id",
   "dimension:trips:vehicle.id",
   "dimension:trips:vehicle.licence.driver_id",
   "dimension:trips:vehicle.licence.id",
   "dimension:trips:vehicle.licence_id",
   "dimension:trips:vehicle_id",
   "measure:trips:trip_count",
];
const FINES = [
   "dimension:fines:driver.convictions.court_id",
   "dimension:fines:driver.convictions.driver_id",
   "dimension:fines:driver.convictions.id",
   "dimension:fines:driver.convictions.offence",
   "dimension:fines:driver.convictions.offence_name",
   "dimension:fines:driver.driver_name",
   "dimension:fines:driver.id",
   "dimension:fines:driver.nm",
   "dimension:fines:driver_id",
   "dimension:fines:id",
   "measure:fines:driver.convictions.conviction_count",
];

let f: FcjFixture;
const catalogs = new Map<number, Catalog>();
const catalogAt = async (cap: number) => {
   if (!catalogs.has(cap)) {
      catalogs.set(
         cap,
         buildCatalog({
            ...(await catalogInputOf(f.pkgIndex)),
            unservedCopies: cap,
         }),
      );
   }
   return catalogs.get(cap)!;
};
const fieldIds = (c: Catalog, source: string) =>
   [...c.nodes.values()]
      .filter(
         (n) =>
            n.source === source &&
            (n.kind === "dimension" || n.kind === "measure"),
      )
      .map((n) => n.id)
      .sort();

beforeAll(async () => {
   f = await compileFcjFixture(FILES, {}, DEFAULT_PACKAGE_RETRIEVAL, SERVED);
});

describe("unservedCopies", () => {
   it("serves only the listed files: courts, trips and fines", async () => {
      const c = await catalogAt(0);
      expect([...c.sources.keys()].sort()).toEqual([
         "courts@courts.malloy",
         "fines@fleet.malloy",
         "trips@fleet.malloy",
      ]);
   });

   it("at 0 (the default), the unpublished chain is indexed two joins deep and no deeper", async () => {
      const c = await catalogAt(0);
      expect(fieldIds(c, "trips")).toEqual(SHIPPED_TRIPS);
      expect(fieldIds(c, "fines")).toEqual(FINES);
      expect(c.report.unservedCopiesAdded).toBe(0);
      expect(c.report.unservedCopiesCapped).toBe(0);
   });

   it("at 3, adds trips' copies three and four joins deep, with the doc, type, path and widest relationship", async () => {
      const c = await catalogAt(3);
      expect(fieldIds(c, "trips")).toEqual(
         [
            ...SHIPPED_TRIPS,
            "dimension:trips:vehicle.licence.driver.convictions.court_id",
            "dimension:trips:vehicle.licence.driver.convictions.driver_id",
            "dimension:trips:vehicle.licence.driver.convictions.id",
            "dimension:trips:vehicle.licence.driver.convictions.offence",
            "dimension:trips:vehicle.licence.driver.convictions.offence_name",
            "dimension:trips:vehicle.licence.driver.driver_name",
            "dimension:trips:vehicle.licence.driver.id",
            "dimension:trips:vehicle.licence.driver.nm",
            "measure:trips:vehicle.licence.driver.convictions.conviction_count",
         ].sort(),
      );
      // fines already had every copy, two joins deep.
      expect(fieldIds(c, "fines")).toEqual(FINES);
      const n = c.nodes.get(
         "measure:trips@fleet.malloy:vehicle.licence.driver.convictions.conviction_count",
      )!;
      expect({
         doc: n.doc,
         dataType: n.dataType,
         joinPath: n.joinPath,
         relationship: n.relationship,
         source: n.source,
         modelPaths: n.modelPaths,
      }).toEqual({
         doc: "Number of convictions.",
         dataType: "number",
         joinPath: "vehicle.licence.driver.convictions",
         relationship: "many",
         source: "trips",
         modelPaths: ["fleet.malloy"],
      });
      expect(c.report.unservedCopiesAdded).toBe(9);
      expect(c.report.unservedCopiesCapped).toBe(0);
      expect(c.report.unservedWalkCapped).toBe(0);
   });

   it("gives a field of a served source no copy, however deep: courts.court_name five joins from trips", async () => {
      const c = await catalogAt(3);
      expect(
         [...c.nodes.values()]
            .filter(
               (n) => n.name.endsWith("court_name") || n.name.endsWith(".nm"),
            )
            .map((n) => n.id)
            .sort(),
      ).toEqual([
         "dimension:courts:court_name",
         "dimension:fines:driver.nm",
         "dimension:trips:vehicle.licence.driver.nm",
      ]);
   });

   it("caps the copies per field, shortest first, counting the shipped ones: at 1, fines' two-join copy keeps convictions' fields off trips", async () => {
      const c = await catalogAt(1);
      expect(fieldIds(c, "trips")).toEqual(SHIPPED_TRIPS);
      expect(c.report.unservedCopiesAdded).toBe(0);
      expect(c.report.unservedCopiesCapped).toBe(9);
      expect(catalogReportLine(c.report)).toContain(
         "; 0 joined copies of unserved fields added beyond the entity index, 9 left out by the per-field cap, 0 join paths stopped at the guard",
      );
   });
});
