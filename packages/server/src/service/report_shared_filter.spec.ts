import { describe, expect, it } from "bun:test";
import {
   buildReportManifest,
   lintMissingSharedFilter,
   lintReport,
   type ReportModelFacts,
} from "./report_manifest";

/**
 * A report written as queries of its own over served views binds each filter
 * on each tile's query. A tile left out is not filtered while the rest are.
 */
const report = (tiles: Array<[string, string[]]>): ReportModelFacts => ({
   modelPath: "dashboards/overview.malloy",
   modelAnnotations: [
      `## artifact { tiles=[${tiles.map(([name]) => `"${name}"`).join(", ")}] }\n`,
   ],
   queries: tiles.map(([name, givens]) => ({
      name,
      annotations: [],
      givens,
      sourceName: name.startsWith("w_") ? "weather" : "flights",
   })),
   givens: new Map(
      ["CARRIER", "ORIGIN"].map((name) => [
         name,
         { name, type: "filter<string>", annotations: [] },
      ]),
   ),
   viewGivens: new Map(),
   viewAnnotations: new Map(),
   sourceFields: new Map(),
   drills: [],
   suggestGivens: { forSource: () => undefined, forQuery: () => undefined },
});

const lint = (f: ReportModelFacts) => {
   const manifest = buildReportManifest(f);
   if (!manifest) throw new Error("expected a report");
   return lintMissingSharedFilter(f, manifest);
};

describe("lintMissingSharedFilter", () => {
   it("names the tile and the filter most of its source's tiles apply", () => {
      expect(
         lint(
            report([
               ["a_tile", ["CARRIER"]],
               ["b_tile", ["CARRIER", "ORIGIN"]],
               ["c_tile", []],
            ]),
         ),
      ).toEqual([
         {
            subject: "overview",
            severity: "warn",
            message:
               '"c_tile" does not filter on $CARRIER, which 2 of this ' +
               'report\'s 3 tiles over "flights" do, so that control leaves ' +
               "it unfiltered. If it should follow the control, bind it in " +
               "its query: `+ { where: <field> ~ $CARRIER }`.",
         },
      ]);
   });

   it("says nothing where the filter is on half the tiles or fewer, or on every tile", () => {
      expect(
         lint(
            report([
               ["a_tile", ["CARRIER"]],
               ["b_tile", []],
            ]),
         ),
      ).toEqual([]);
      expect(
         lint(
            report([
               ["a_tile", ["CARRIER"]],
               ["b_tile", ["CARRIER"]],
            ]),
         ),
      ).toEqual([]);
   });

   it("compares a tile only with tiles over the same source", () => {
      expect(
         lint(
            report([
               ["a_tile", ["CARRIER"]],
               ["b_tile", ["CARRIER"]],
               ["w_tile", []],
            ]),
         ),
      ).toEqual([]);
   });

   it("is one of the report's lints", () => {
      const f = report([
         ["a_tile", ["CARRIER"]],
         ["b_tile", ["CARRIER"]],
         ["c_tile", []],
      ]);
      const manifest = buildReportManifest(f);
      if (!manifest) throw new Error("expected a report");
      expect(lintReport(f, manifest).map((x) => x.message)).toContain(
         lintMissingSharedFilter(f, manifest)[0]?.message as string,
      );
   });
});
