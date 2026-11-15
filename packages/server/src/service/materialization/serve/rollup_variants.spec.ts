// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { describe, expect, it } from "bun:test";
import { LegDecline } from "../domain/leg_decline";
import {
   coverageDecline,
   grainDecline,
   joinCoverageDecline,
   joinGrainDecline,
   joinLegDecline,
} from "./rollup_variants";

const GRAIN = new Set(["region", "month"]);
const MEASURES = new Set(["total"]);

describe("grainDecline", () => {
   it("passes a query that groups and filters by grain dimensions", () => {
      expect(
         grainDecline(
            { groupedBy: ["region"], filteredBy: ["month"] },
            GRAIN,
            MEASURES,
         ),
      ).toBeUndefined();
   });

   it("passes a filter on a stored measure", () => {
      expect(
         grainDecline(
            { groupedBy: [], filteredBy: ["total"] },
            GRAIN,
            MEASURES,
         ),
      ).toBeUndefined();
   });

   it("declines the first field the grain does not keep", () => {
      const decline = grainDecline(
         { groupedBy: ["region", "city"], filteredBy: ["day"] },
         GRAIN,
         MEASURES,
      );
      expect(decline).toBeInstanceOf(LegDecline);
      expect(decline?.code).toBe("rollup_grain_too_coarse");
      expect(decline?.shapeCheck).toBe(true);
      expect(decline?.callerDetail).toContain("`city`");
   });
});

describe("coverageDecline", () => {
   it("passes a query that aggregates stored measures", () => {
      expect(
         coverageDecline({ measures: ["total"] }, MEASURES),
      ).toBeUndefined();
   });

   it("declines a measure the rollup does not store", () => {
      const decline = coverageDecline({ measures: ["total", "avg"] }, MEASURES);
      expect(decline?.code).toBe("rollup_missing_field");
      expect(decline?.shapeCheck).toBe(true);
      expect(decline?.callerDetail).toContain("`avg`");
   });

   it("does not count a grain dimension as a stored measure", () => {
      expect(coverageDecline({ measures: ["region"] }, MEASURES)?.code).toBe(
         "rollup_missing_field",
      );
   });
});

describe("joinLegDecline", () => {
   it("tries a leg whose join the query reads", () => {
      expect(
         joinLegDecline("segments", [["segments", "region"]], undefined),
      ).toBeUndefined();
   });

   it("tries a leg when the query's reads are unreadable", () => {
      expect(joinLegDecline("segments", undefined, undefined)).toBeUndefined();
   });

   it("skips a leg whose join the query does not read, without a decline", () => {
      const skipped = joinLegDecline("segments", [["orders", "id"]], {
         code: "rollup_contract",
         callerDetail: "never reported",
      });
      expect(skipped).toBeInstanceOf(Error);
      expect(skipped).not.toBeInstanceOf(LegDecline);
   });

   it("declines a read leg with its refusal, as a shape check", () => {
      const decline = joinLegDecline("segments", [["segments", "region"]], {
         code: "rollup_count_semantics",
         callerDetail: "the join fans out",
      });
      expect(decline).toBeInstanceOf(LegDecline);
      expect((decline as LegDecline).code).toBe("rollup_count_semantics");
      expect((decline as LegDecline).callerDetail).toBe("the join fans out");
      expect((decline as LegDecline).shapeCheck).toBe(true);
   });
});

describe("joinGrainDecline", () => {
   it("reads only the fields through the join, by their names in it", () => {
      expect(
         joinGrainDecline(
            "segments",
            {
               groupedBy: ["segments.region", "city"],
               filteredBy: ["segments.month"],
            },
            GRAIN,
            MEASURES,
         ),
      ).toBeUndefined();
   });

   it("declines a field through the join the grain does not keep", () => {
      const decline = joinGrainDecline(
         "segments",
         { groupedBy: [], filteredBy: ["segments.day"] },
         GRAIN,
         MEASURES,
      );
      expect(decline?.code).toBe("rollup_grain_too_coarse");
      expect(decline?.callerDetail).toContain("`segments.day`");
   });

   it("does not read a join whose name only prefixes the field's", () => {
      expect(
         joinGrainDecline(
            "seg",
            { groupedBy: ["segments.day"], filteredBy: [] },
            GRAIN,
            MEASURES,
         ),
      ).toBeUndefined();
   });
});

describe("joinCoverageDecline", () => {
   it("passes a stored measure or a grain dimension through the join", () => {
      expect(
         joinCoverageDecline(
            "segments",
            { measures: ["segments.total", "segments.region", "other.avg"] },
            GRAIN,
            MEASURES,
         ),
      ).toBeUndefined();
   });

   it("declines a measure through the join the rollup does not store", () => {
      const decline = joinCoverageDecline(
         "segments",
         { measures: ["segments.avg"] },
         GRAIN,
         MEASURES,
      );
      expect(decline?.code).toBe("rollup_missing_field");
      expect(decline?.shapeCheck).toBe(true);
      expect(decline?.callerDetail).toContain("`segments.avg`");
   });
});
