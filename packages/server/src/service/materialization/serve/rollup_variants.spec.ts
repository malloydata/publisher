// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { describe, expect, it } from "bun:test";
import { LegDecline } from "../domain/leg_decline";
import {
   coverageDecline,
   grainDecline,
   joinCoverageDecline,
   joinGrainDecline,
   joinBranchDecline,
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

describe("joinBranchDecline", () => {
   it("tries a branch whose join the query reads", () => {
      expect(
         joinBranchDecline("segments", [["segments", "region"]], undefined),
      ).toBeUndefined();
   });

   it("tries a branch when the query's reads are unreadable", () => {
      expect(
         joinBranchDecline("segments", undefined, undefined),
      ).toBeUndefined();
   });

   it("skips a branch whose join the query does not read, without a decline", () => {
      const skipped = joinBranchDecline("segments", [["orders", "id"]], {
         code: "rollup_contract",
         callerDetail: "never reported",
      });
      expect(skipped).toBeInstanceOf(Error);
      expect(skipped).not.toBeInstanceOf(LegDecline);
   });

   it("declines a read branch with its refusal, as a shape check", () => {
      const decline = joinBranchDecline("segments", [["segments", "region"]], {
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

describe("the rollup branch's decisions at their edges", () => {
   it("passes a query that groups, filters and aggregates nothing", () => {
      const empty = { groupedBy: [], filteredBy: [], measures: [] };
      expect(grainDecline(empty, GRAIN, MEASURES)).toBeUndefined();
      expect(coverageDecline(empty, MEASURES)).toBeUndefined();
      expect(
         joinGrainDecline("segments", empty, GRAIN, MEASURES),
      ).toBeUndefined();
      expect(
         joinCoverageDecline("segments", empty, GRAIN, MEASURES),
      ).toBeUndefined();
   });

   it("declines a grouped field before a filtered one, naming the first in order", () => {
      const decline = grainDecline(
         { groupedBy: ["city"], filteredBy: ["day"] },
         GRAIN,
         MEASURES,
      );
      expect(decline?.callerDetail).toBe(
         "the query reads `city`, which the rollup's grain does not keep",
      );
   });

   it("declines every field against an empty grain, a stored measure still kept", () => {
      const none = new Set<string>();
      expect(
         grainDecline({ groupedBy: ["region"], filteredBy: [] }, none, MEASURES)
            ?.code,
      ).toBe("rollup_grain_too_coarse");
      expect(
         grainDecline({ groupedBy: [], filteredBy: ["total"] }, none, MEASURES),
      ).toBeUndefined();
   });

   it("reads a join branch's filter on a stored measure through the join as in the grain", () => {
      expect(
         joinGrainDecline(
            "segments",
            { groupedBy: [], filteredBy: ["segments.total"] },
            GRAIN,
            MEASURES,
         ),
      ).toBeUndefined();
   });

   it("skips a branch whose join a query that reads nothing does not read", () => {
      const skipped = joinBranchDecline("segments", [], undefined);
      expect(skipped).toBeInstanceOf(Error);
      expect(skipped).not.toBeInstanceOf(LegDecline);
   });

   it("tries a branch the query reads through on any path, not only the first", () => {
      expect(
         joinBranchDecline(
            "segments",
            [
               ["orders", "id"],
               ["segments", "region"],
            ],
            undefined,
         ),
      ).toBeUndefined();
   });

   it("names the join and the field it reads in a join branch's decline", () => {
      expect(
         joinCoverageDecline(
            "segments",
            { measures: ["segments.avg"] },
            GRAIN,
            MEASURES,
         )?.callerDetail,
      ).toBe(
         "the query aggregates `segments.avg`, which the rollup does not store",
      );
   });
});
