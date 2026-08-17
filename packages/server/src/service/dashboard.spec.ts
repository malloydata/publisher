import { describe, expect, it } from "bun:test";
import {
   buildDashboardManifest,
   normalizeTileExpression,
   dashboardSlug,
   docCommentText,
   docCommentTitleAndDescription,
   filterPublisherOwnedRenderLogs,
   isDashboardModelPath,
   matchesDocumentedDashboardName,
   lintDashboard,
   lintDrillTargets,
   lintGivenTags,
   lintSelfDrills,
   lintUndiscoveredDashboard,
   motlyAnnotations,
   quoteFilterLiterals,
   unwrapFilterLiteral,
   type DashboardGivenDeclaration,
   type DashboardModelFacts,
} from "./dashboard";

/** A facts bundle with everything empty, for tests to override a slice of. */
function facts(
   overrides: Partial<DashboardModelFacts> = {},
): DashboardModelFacts {
   return {
      modelPath: "dashboards/overview.malloy",
      modelAnnotations: [],
      queries: [],
      givens: new Map(),
      viewGivens: new Map(),
      sourceFields: new Map(),
      drills: [],
      ...overrides,
   };
}

function given(
   name: string,
   type: string,
   annotations: string[],
   defaultText?: string,
): [string, DashboardGivenDeclaration] {
   return [name, { name, type, annotations, default: defaultText }];
}

const build = (f: DashboardModelFacts) => buildDashboardManifest(f);

describe("service/dashboard path helpers", () => {
   it("accepts a .malloy directly inside dashboards/", () => {
      expect(isDashboardModelPath("dashboards/overview.malloy")).toBe(true);
   });

   it("rejects nested subdirectories, so slugs cannot collide", () => {
      expect(isDashboardModelPath("dashboards/sales/overview.malloy")).toBe(
         false,
      );
   });

   it("rejects notebooks, other directories, and the package root", () => {
      expect(isDashboardModelPath("dashboards/overview.malloynb")).toBe(false);
      expect(isDashboardModelPath("reports/overview.malloy")).toBe(false);
      expect(isDashboardModelPath("overview.malloy")).toBe(false);
   });

   it("derives the slug from the basename", () => {
      expect(dashboardSlug("dashboards/overview.malloy")).toBe("overview");
   });
});

describe("service/dashboard annotation routing", () => {
   it("keeps MOTLY (plain # and ##) annotations only", () => {
      expect(
         motlyAnnotations([
            '# artifact { title="X" }\n',
            "## artifact { tiles=[] }\n",
            "#(doc) Prose about the query.\n",
            '#" A doc comment.\n',
            "##! experimental.givens\n",
            "#@ persist name='t'\n",
         ]),
      ).toEqual(['# artifact { title="X" }\n', "## artifact { tiles=[] }\n"]);
   });

   it('reads #" doc comments, joining multiple lines', () => {
      // Newline, not space. Core defines this route as doc-string markdown and
      // authors write one `#"` per source line, so space-joining would merge a
      // heading into the paragraph below it. #935 expected a space here; the
      // reader it calls now lives in `motly.ts` and joins with a newline.
      expect(
         docCommentText([
            '#" Business health at a glance.\n',
            '#" Updated nightly.\n',
            "# artifact\n",
         ]),
      ).toBe("Business health at a glance.\nUpdated nightly.");
   });

   it("has no doc comment when none is present", () => {
      expect(docCommentText(["# artifact\n", "#(doc) not this\n"])).toBe(
         undefined,
      );
   });
});

describe("service/dashboard title and description together", () => {
   // The defect this exists to prevent: `description` was the whole doc comment
   // while `title` fell back to its first line, so a one-line comment with no
   // `title=` published the same words twice and the page rendered a heading
   // with an identical subtitle under it.
   it("does not repeat the title line in the description", () => {
      expect(
         docCommentTitleAndDescription(['#" Orders by region\n'], undefined),
      ).toEqual({ title: "Orders by region", description: undefined });
   });

   it("keeps the prose after the title line as the description", () => {
      expect(
         docCommentTitleAndDescription(
            ['#" Orders by region\n', '#"\n', '#" Updated nightly.\n'],
            undefined,
         ),
      ).toEqual({
         title: "Orders by region",
         description: "Updated nightly.",
      });
   });

   // An explicit title consumed none of the comment, so the comment stays whole.
   it("keeps the whole comment when the title was written out", () => {
      expect(
         docCommentTitleAndDescription(
            ['#" Orders by region\n'],
            "Regional sales",
         ),
      ).toEqual({
         title: "Regional sales",
         description: "Orders by region",
      });
   });

   it("has neither when there is no comment and no title", () => {
      expect(
         docCommentTitleAndDescription(["# artifact\n"], undefined),
      ).toEqual({ title: undefined, description: undefined });
   });

   // A blank line between the title and the body separates them and belongs to
   // neither, but a blank line INSIDE the body is a paragraph break and stays.
   it("keeps paragraph breaks inside the description", () => {
      expect(
         docCommentTitleAndDescription(
            ['#" Title\n', '#" First.\n', '#"\n', '#" Second.\n'],
            undefined,
         ).description,
      ).toBe("First.\n\nSecond.");
   });
});

describe("service/dashboard render-log filtering", () => {
   // Publisher's tags share the `#` namespace with the renderer's, which reports
   // anything it didn't consume — so without this filter every dashboard query
   // would answer with a spurious warning about its own artifact tag.
   it("drops unknown-tag complaints about tags Publisher owns", () => {
      expect(
         filterPublisherOwnedRenderLogs([
            { message: "Unknown render tag 'artifact' on field 'root'" },
            { message: "Unknown render tag 'artifact.title' on field 'root'" },
            { message: "Unknown render tag 'drill' on field 'brand_name'" },
         ]),
      ).toEqual([]);
   });

   it("keeps every other render finding, including real unknown tags", () => {
      const kept = [
         { message: "Unknown render tag 'bar_charts' on field 'root'" },
         { message: "Invalid number suffix 'nope' on field 'amount'" },
         { message: undefined },
      ];
      expect(filterPublisherOwnedRenderLogs(kept)).toEqual(kept);
   });
});

describe("service/dashboard filter literals", () => {
   // MOTLY has no filter-literal value form, and the parse failure discards the
   // whole tag — so this rewrite is what makes Malloyyo's documented starting
   // values discoverable at all.
   it("quotes a bare filter literal so MOTLY can parse the tag", () => {
      expect(
         quoteFilterLiterals(
            "# artifact { givens { M=f'Ford Motor Company' } }",
         ),
      ).toBe("# artifact { givens { M=\"f'Ford Motor Company'\" } }");
   });

   it("escapes quotes and backslashes it introduces into the string", () => {
      expect(
         quoteFilterLiterals(`# artifact { givens { M=f'say "hi"' } }`),
      ).toBe(`# artifact { givens { M="f'say \\"hi\\"'" } }`);
   });

   it("leaves an already-quoted value alone", () => {
      const already = '# artifact { givens { M="7 days" } }';
      expect(quoteFilterLiterals(already)).toBe(already);
   });

   it("unwraps a filter literal to the body the query endpoint takes", () => {
      expect(unwrapFilterLiteral("f'us-east, us-west'")).toBe(
         "us-east, us-west",
      );
      expect(unwrapFilterLiteral("f''")).toBe("");
      expect(unwrapFilterLiteral("2024-01-01")).toBe("2024-01-01");
   });
});

describe("service/dashboard manifest (single-query form)", () => {
   it("reads the artifact tag and the sibling dashboard grid tag", () => {
      const manifest = build(
         facts({
            queries: [
               {
                  name: "overview",
                  annotations: [
                     '# artifact { title="Business Overview" } dashboard {columns=6}\n',
                  ],
                  givens: [],
               },
            ],
         }),
      );
      expect(manifest).toMatchObject({
         name: "overview",
         title: "Business Overview",
         query: "overview",
         dashboardColumns: 6,
         autorun: true,
         entryFile: "dashboards/overview.malloy",
      });
   });

   it('falls back to the #" doc comment, then to the slug, for the title', () => {
      const withDoc = build(
         facts({
            queries: [
               {
                  name: "overview",
                  annotations: [
                     '#" Business health at a glance.\n',
                     "# artifact\n",
                  ],
                  givens: [],
               },
            ],
         }),
      );
      expect(withDoc?.title).toBe("Business health at a glance.");
      // And NOT also the description. The title took the only line there was,
      // so repeating it would print the same words twice on the page.
      expect(withDoc?.description).toBe(undefined);

      const bare = build(
         facts({
            queries: [
               { name: "overview", annotations: ["# artifact\n"], givens: [] },
            ],
         }),
      );
      expect(bare?.title).toBe("overview");
      expect(bare?.description).toBe(undefined);
   });

   it("treats a file with no artifact tag as a shared include", () => {
      expect(
         build(
            facts({
               queries: [
                  {
                     name: "helper",
                     annotations: ["# dashboard {columns=2}\n"],
                     givens: [],
                  },
               ],
            }),
         ),
      ).toBe(undefined);
      expect(build(facts())).toBe(undefined);
   });

   it("honors autorun=false and per-dashboard starting values", () => {
      const manifest = build(
         facts({
            queries: [
               {
                  name: "recalls",
                  annotations: [
                     `# artifact { autorun=false givens { MANUFACTURER=f'Ford Motor Company' PERIOD="30 days" } }\n`,
                  ],
                  givens: [],
               },
            ],
            // Declared, because the unwrap is keyed on the type: MANUFACTURER is
            // a filter so `f'…'` is a wrapper, PERIOD is a string so its value
            // is whatever it says. Without the declarations neither is unwrapped.
            givens: new Map([
               given("MANUFACTURER", "filter<string>", []),
               given("PERIOD", "string", []),
            ]),
         }),
      );
      expect(manifest?.autorun).toBe(false);
      // Starting values arrive in run shape: the filter body, not the literal,
      // and the string given untouched.
      expect(manifest?.startingGivens).toEqual({
         MANUFACTURER: "Ford Motor Company",
         PERIOD: "30 days",
      });
   });
});

describe("service/dashboard given specs (the control contract)", () => {
   /**
    * `default` must be usable AS a default, so a filter given publishes the BODY
    * the query endpoint takes rather than the `f'…'` literal it is declared as.
    * Publishing the literal made this a field that silently matched zero rows.
    *
    * Type-conditional, and this pins that half too: only a filter given carries
    * the wrapper, so a plain string whose default merely READS like one must
    * survive untouched.
    */
   it("unwraps a filter default and leaves a look-alike plain default alone", () => {
      const manifest = build(
         facts({
            queries: [
               {
                  name: "overview",
                  annotations: ["# artifact\n"],
                  givens: ["FILTERED", "PLAIN"],
               },
            ],
            givens: new Map([
               given("FILTERED", "filter<string>", [], "f'Nike'"),
               given("PLAIN", "string", [], "f'Nike'"),
            ]),
         }),
      );
      const byName = Object.fromEntries(
         (manifest?.givens ?? []).map((g) => [g.name, g.default]),
      );
      expect(byName).toEqual({ FILTERED: "Nike", PLAIN: "f'Nike'" });
   });

   it("builds one control per given the query references, from its tags", () => {
      const manifest = build(
         facts({
            queries: [
               {
                  name: "overview",
                  annotations: ["# artifact\n"],
                  givens: ["BRAND", "MIN_PRICE"],
               },
            ],
            givens: new Map([
               given(
                  "BRAND",
                  "filter<string>",
                  [
                     '# label="Brand" control=select suggest { source=order_items dimension=brand }\n',
                  ],
                  "f''",
               ),
               given(
                  "MIN_PRICE",
                  "filter<number>",
                  ['# label="Min price" range_min=0 range_max=500\n'],
                  "f''",
               ),
               // Declared but not referenced by this dashboard's query.
               given("UNUSED", "filter<string>", []),
            ]),
         }),
      );

      expect(manifest?.givens).toEqual([
         {
            name: "BRAND",
            type: "filter<string>",
            default: "",
            annotations: [],
            label: "Brand",
            control: "select",
            suggest: {
               source: "order_items",
               dimension: "brand",
            },
         },
         {
            name: "MIN_PRICE",
            type: "filter<number>",
            default: "",
            annotations: [],
            label: "Min price",
            rangeMin: 0,
            rangeMax: 500,
         },
      ]);
   });

   it("carries an untagged given through as a bare control", () => {
      const manifest = build(
         facts({
            queries: [
               {
                  name: "overview",
                  annotations: ["# artifact\n"],
                  givens: ["REGION"],
               },
            ],
            givens: new Map([given("REGION", "string", [], "'US'")]),
         }),
      );
      expect(manifest?.givens).toEqual([
         { name: "REGION", type: "string", default: "'US'", annotations: [] },
      ]);
   });

   // The dashboard control row is the ordinary API `Given`, so it must carry the
   // same `#(description="…")` the model and notebook surfaces carry: the
   // shipped `GivenInput` renders its helper text from `annotations`. Dropping
   // it made a dashboard control silently plainer than the same given elsewhere.
   it("carries app-route annotations through, and drops reserved ones", () => {
      const manifest = build(
         facts({
            queries: [
               {
                  name: "overview",
                  annotations: ["# artifact\n"],
                  givens: ["REGION"],
               },
            ],
            givens: new Map([
               given("REGION", "filter<string>", [
                  '#(description="Which region") \n',
                  '# label="Region"\n',
                  '#" a doc comment\n',
               ]),
            ]),
         }),
      );
      expect(manifest?.givens[0].annotations).toEqual([
         '#(description="Which region") \n',
      ]);
      // The reserved routes still drive the control contract rather than
      // appearing as annotations.
      expect(manifest?.givens[0].label).toBe("Region");
   });

   it("ignores a control kind it does not recognize", () => {
      const manifest = build(
         facts({
            queries: [
               {
                  name: "overview",
                  annotations: ["# artifact\n"],
                  givens: ["BRAND"],
               },
            ],
            givens: new Map([
               given("BRAND", "filter<string>", ["# control=radio\n"]),
            ]),
         }),
      );
      expect(manifest?.givens[0].control).toBe(undefined);
   });
});

describe("service/dashboard manifest (composite form)", () => {
   /**
    * A tile discovery cannot resolve statically contributes no `givenNames`, so
    * a union over the resolved tiles alone would publish NO control for a given
    * its refinement still filters by, and the tile would run at that given's
    * default with nothing reported. One unresolvable tile therefore widens the
    * row to the file's whole surfaced set. This is also what
    * `DashboardTile.givenNames` tells a client to do when it is absent, which
    * only works if the row contains them.
    *
    * Deliberately MIXED, because a single-tile fixture cannot distinguish `some`
    * from `every`, and the older mixed test pairs its refinement with a
    * resolvable tile supplying the SAME given, so both branches agree there.
    * Here the file surfaces two givens no tile mentions, so only `some` yields
    * them.
    */
   it("widens the control row when ANY tile cannot be resolved", () => {
      const manifest = build(
         facts({
            modelAnnotations: [
               '## artifact { tiles=["orders -> by_month", "orders -> by_brand + { limit: 5 }"] }\n',
            ],
            queries: [
               { name: "by_month", annotations: [], givens: ["MONTHLY"] },
            ],
            viewGivens: new Map([["orders -> by_month", ["MONTHLY"]]]),
            givens: new Map([
               given("BRAND", "filter<string>", []),
               given("MONTHLY", "filter<string>", []),
               given("REGION", "filter<string>", []),
            ]),
         }),
      );
      // Deliberately MIXED: one resolvable tile contributing MONTHLY, one
      // refinement contributing nothing. `some` must widen to the file's whole
      // surfaced set, so REGION and BRAND appear even though no resolvable tile
      // names them. With `every` this yields only MONTHLY, which is the
      // mutation the single-tile version of this test could not catch, since
      // `some` and `every` agree when there is one tile.
      expect((manifest?.givens ?? []).map((g) => g.name).sort()).toEqual([
         "BRAND",
         "MONTHLY",
         "REGION",
      ]);
   });

   it("reads tiles and the grid width off the model-level tag", () => {
      const manifest = build(
         facts({
            modelPath: "dashboards/seasonality.malloy",
            modelAnnotations: [
               "##! experimental.givens\n",
               '## artifact { title="Seasonality" tiles=["orders -> by_month", "users -> signups"] dashboard_columns=4 }\n',
            ],
         }),
      );
      expect(manifest).toMatchObject({
         name: "seasonality",
         title: "Seasonality",
         dashboardColumns: 4,
         autorun: true,
      });
      expect(manifest?.tiles).toEqual([
         { query: "orders -> by_month" },
         { query: "users -> signups" },
      ]);
      expect(manifest?.query).toBe(undefined);
   });

   it("resolves each tile's givens and unions them into one control row", () => {
      const manifest = build(
         facts({
            modelAnnotations: [
               '## artifact { tiles=["orders -> by_brand", "orders->by_region", "orders -> plain"] }\n',
            ],
            // Written with irregular spacing above to pin normalization.
            viewGivens: new Map([
               ["orders -> by_brand", ["BRAND"]],
               ["orders -> by_region", ["REGION"]],
               ["orders -> plain", []],
            ]),
            givens: new Map([
               given("BRAND", "filter<string>", []),
               given("REGION", "filter<string>", []),
               given("UNUSED", "filter<string>", []),
            ]),
         }),
      );
      // Each tile carries only its own givens, so a viewer can re-run just the
      // tiles a changed control affects.
      expect(manifest?.tiles).toEqual([
         { query: "orders -> by_brand", givenNames: ["BRAND"] },
         { query: "orders->by_region", givenNames: ["REGION"] },
         { query: "orders -> plain", givenNames: [] },
      ]);
      // The control row is the union — every given any tile can filter by.
      expect(manifest?.givens.map((s) => s.name)).toEqual(["BRAND", "REGION"]);
   });

   it("resolves a tile naming a model-level query, and leaves an unresolvable one absent", () => {
      const manifest = build(
         facts({
            modelAnnotations: [
               '## artifact { tiles=["top_brands", "orders -> by_brand + { limit: 5 }"] }\n',
            ],
            queries: [
               { name: "top_brands", annotations: [], givens: ["BRAND"] },
            ],
            givens: new Map([given("BRAND", "filter<string>", [])]),
         }),
      );
      expect(manifest?.tiles).toEqual([
         { query: "top_brands", givenNames: ["BRAND"] },
         // A refinement is not a form discovery resolves statically; absent
         // rather than guessed, which stays compile-safe.
         { query: "orders -> by_brand + { limit: 5 }" },
      ]);
      expect(manifest?.givens.map((s) => s.name)).toEqual(["BRAND"]);
   });

   it("omits a tile given the entry file cannot bind", () => {
      // A tile's `where: ... ~ $REGION` can live in an imported file, so the
      // reference exists while the entry file never imported REGION itself.
      // Malloy's given namespace is per-file, so binding it would fail with
      // "unknown given" — advertise a control only for what is bindable, which
      // is what `facts.givens` holds.
      const manifest = build(
         facts({
            modelAnnotations: [
               '## artifact { tiles=["orders -> by_brand", "orders -> by_region"] }\n',
            ],
            viewGivens: new Map([
               ["orders -> by_brand", ["BRAND"]],
               ["orders -> by_region", ["REGION"]],
            ]),
            givens: new Map([given("BRAND", "filter<string>", [])]),
         }),
      );
      expect(manifest?.tiles).toEqual([
         { query: "orders -> by_brand", givenNames: ["BRAND"] },
         { query: "orders -> by_region", givenNames: [] },
      ]);
      expect(manifest?.givens.map((s) => s.name)).toEqual(["BRAND"]);
   });

   it("prefers a composite declaration over a query-level tag in the same file", () => {
      const manifest = build(
         facts({
            modelAnnotations: [
               '## artifact { tiles=["orders -> by_month"] }\n',
            ],
            queries: [
               {
                  name: "stray",
                  annotations: ['# artifact { title="Stray" }\n'],
                  givens: [],
               },
            ],
         }),
      );
      expect(manifest?.tiles).toHaveLength(1);
      expect(manifest?.query).toBe(undefined);
   });

   it("is not composite when the model tag declares no tiles", () => {
      const manifest = build(
         facts({
            modelAnnotations: ['## artifact { title="Not composite" }\n'],
            queries: [
               {
                  name: "overview",
                  annotations: ['# artifact { title="Real one" }\n'],
                  givens: [],
               },
            ],
         }),
      );
      expect(manifest?.title).toBe("Real one");
      expect(manifest?.query).toBe("overview");
   });
});

describe("service/dashboard lint", () => {
   /** Lint a facts bundle through the manifest the same file would produce. */
   function lint(f: DashboardModelFacts) {
      const manifest = build(f);
      if (!manifest) throw new Error("expected a dashboard");
      return lintDashboard(f, manifest).map((finding) => finding.message);
   }

   it("passes a well-formed dashboard silently", () => {
      expect(
         lint(
            facts({
               modelAnnotations: [
                  '## artifact { tiles=["orders -> by_brand"] dashboard_columns=2 }\n',
               ],
               viewGivens: new Map([["orders -> by_brand", ["BRAND"]]]),
               sourceFields: new Map([["orders", new Set(["by_brand"])]]),
               givens: new Map([given("BRAND", "filter<string>", [])]),
            }),
         ),
      ).toEqual([]);
   });

   // The `@env.` drop is per LINE, so an env reference on a SIBLING line leaves
   // the artifact tag intact and the dashboard builds. That is the case the
   // same-line tests never reach: with a surviving MOTLY line the parse
   // succeeds, so it returns down the direct-success path rather than the
   // no-annotations early return.
   //
   // It also lands on the branch of the lint that runs for a dashboard that
   // EXISTS. `lintUndiscoveredDashboard` reports the drop, but package.ts picks
   // exactly one of the two lints, so a built dashboard would never see it: the
   // author's line is dropped and nothing anywhere says so.
   it("reports an @env line dropped from a dashboard that still builds", () => {
      expect(
         lint(
            facts({
               modelAnnotations: [
                  '## artifact { tiles=["orders -> by_brand"] }\n',
                  "## note=@env.SLICE6_SIBLING_SECRET\n",
               ],
               viewGivens: new Map([["orders -> by_brand", []]]),
               sourceFields: new Map([["orders", new Set(["by_brand"])]]),
               givens: new Map(),
            }),
         ),
      ).toEqual([expect.stringContaining("@env.")]);
   });

   // Authored directly in the artifact tag, so no query mentions it and the two
   // query-side unbindable checks never see it. `Model.givens` is the surface
   // `filterGivensToModelSurface` enforces at query time, so the value is
   // dropped there and the dashboard opens at the declaration's default, with
   // no control in the row either. Silent in all three places without this.
   it("flags a starting given the file cannot bind", () => {
      expect(
         lint(
            facts({
               modelAnnotations: [
                  "## artifact { tiles=[\"orders -> by_brand\"] givens { GHOST=f'Nike' } }\n",
               ],
               viewGivens: new Map([["orders -> by_brand", []]]),
               sourceFields: new Map([["orders", new Set(["by_brand"])]]),
               givens: new Map(),
            }),
         ),
      ).toEqual([expect.stringContaining('starting value for given "GHOST"')]);
   });

   // The control. Without it the check above is satisfied by reporting EVERY
   // starting given, which would fire on every correct dashboard that sets one.
   it("stays silent for a starting given the file does bind", () => {
      expect(
         lint(
            facts({
               modelAnnotations: [
                  "## artifact { tiles=[\"orders -> by_brand\"] givens { BRAND=f'Nike' } }\n",
               ],
               viewGivens: new Map([["orders -> by_brand", ["BRAND"]]]),
               sourceFields: new Map([["orders", new Set(["by_brand"])]]),
               givens: new Map([given("BRAND", "filter<string>", [])]),
            }),
         ),
      ).toEqual([]);
   });

   it("flags a dashboard_columns that is not a positive integer", () => {
      // `numeric()` returns undefined for these, so the manifest simply omits
      // the grid width — invisible without the warning.
      for (const value of ["wide", "0", "-3", "2.5"]) {
         expect(
            lint(
               facts({
                  modelAnnotations: [
                     `## artifact { tiles=["orders -> a"] dashboard_columns=${value} }\n`,
                  ],
                  viewGivens: new Map([["orders -> a", []]]),
               }),
            ),
         ).toEqual([
            expect.stringContaining("dashboard_columns must be a positive"),
         ]);
      }
   });

   it("flags a plain tile that names a missing view or source", () => {
      const messages = lint(
         facts({
            modelAnnotations: [
               '## artifact { tiles=["orders -> nope", "ghost -> x", "no_query"] }\n',
            ],
            viewGivens: new Map([["orders -> by_brand", []]]),
            sourceFields: new Map([["orders", new Set(["by_brand"])]]),
         }),
      );
      expect(messages).toEqual([
         expect.stringContaining('source "orders" has no view "nope"'),
         expect.stringContaining('no source "ghost" in this file'),
         expect.stringContaining('no query named "no_query" in this file'),
      ]);
   });

   it("leaves a tile it cannot statically resolve alone", () => {
      // A refinement is legal Malloy and legal Malloyyo; warning on it would
      // train authors to ignore the lint.
      expect(
         lint(
            facts({
               modelAnnotations: [
                  '## artifact { tiles=["orders -> by_brand + { limit: 5 }"] }\n',
               ],
            }),
         ),
      ).toEqual([]);
   });

   it("flags a tile filtering by a given the entry file cannot bind", () => {
      const messages = lint(
         facts({
            modelAnnotations: [
               '## artifact { tiles=["orders -> by_region"] }\n',
            ],
            viewGivens: new Map([["orders -> by_region", ["REGION"]]]),
            sourceFields: new Map([["orders", new Set(["by_region"])]]),
            givens: new Map(),
         }),
      );
      expect(messages).toEqual([
         expect.stringContaining(
            'given "REGION", which this file does not import',
         ),
      ]);
   });

   it("flags a suggest naming a query, source, or dimension that is absent", () => {
      const messages = lint(
         facts({
            queries: [
               {
                  name: "overview",
                  annotations: ['# artifact { title="Overview" }\n'],
                  givens: ["A", "B", "C"],
               },
            ],
            sourceFields: new Map([["orders", new Set(["region"])]]),
            givens: new Map([
               given("A", "filter<string>", [
                  "# suggest { query=missing_q }\n",
               ]),
               given("B", "filter<string>", ["# suggest { source=ghost }\n"]),
               given("C", "filter<string>", [
                  "# suggest { source=orders dimension=nope }\n",
               ]),
            ]),
         }),
      );
      // Still three findings, but "B" reaches a different one. `source=` with
      // no `dimension=` is not a runnable suggest form, so the spec is dropped
      // before it can be resolved against `sourceFields`, so it is reported as an
      // unusable declaration rather than as a missing source.
      expect(messages).toEqual([
         expect.stringContaining('query "missing_q"'),
         expect.stringContaining("source= alone names no column"),
         expect.stringContaining('has no field "nope"'),
      ]);
   });

   describe("a file that produced no dashboard", () => {
      it("is silent when it simply carries no tag", () => {
         // A shared include is a legitimate pattern, not a mistake.
         expect(
            lintUndiscoveredDashboard(
               facts({ modelPath: "dashboards/_shared.malloy" }),
            ),
         ).toEqual([]);
      });

      it("explains a tag that does not parse", () => {
         // MOTLY discards the whole tag on a syntax error, so the file silently
         // stops being a dashboard — the least debuggable outcome there is.
         const findings = lintUndiscoveredDashboard(
            facts({
               modelPath: "dashboards/broken.malloy",
               modelAnnotations: ['## artifact { title="x" tiles=[ }\n'],
            }),
         );
         expect(findings).toEqual([
            {
               subject: "broken",
               severity: "error",
               message: expect.stringContaining("treated as a shared include"),
            },
         ]);
      });
   });

   describe("drill targets", () => {
      const drill = (dimension: string, to: string[]) => ({
         source: "orders",
         dimension,
         to,
      });

      it("accepts a known slug and self, and flags anything else", () => {
         const messages = lintDrillTargets(
            [
               facts({
                  drills: [
                     drill("brand", ["overview"]),
                     drill("region", ["self"]),
                     drill("channel", ["gone"]),
                     drill("sku", []),
                  ],
               }),
            ],
            new Set(["overview"]),
         ).map((f) => f.message);
         expect(messages).toEqual([
            expect.stringContaining('targets "gone"'),
            expect.stringContaining("names no destination"),
         ]);
      });

      it("reports a broken target once however many dashboards import it", () => {
         // The same tagged dimension is reachable from every dashboard that
         // imports its source, so the finding is keyed on the dimension.
         const messages = lintDrillTargets(
            [
               facts({
                  modelPath: "dashboards/a.malloy",
                  drills: [drill("brand", ["gone"])],
               }),
               facts({
                  modelPath: "dashboards/b.malloy",
                  drills: [drill("brand", ["gone"])],
               }),
            ],
            new Set(),
         );
         expect(messages).toHaveLength(1);
         expect(messages[0].subject).toBe("orders.brand");
      });
   });

   describe("self drills", () => {
      const selfDrill = (dimension: string, givenName?: string) => ({
         source: "orders",
         dimension,
         to: ["self"],
         ...(givenName === undefined ? {} : { given: givenName }),
      });
      const surfacing = (...names: string[]) =>
         facts({
            givens: new Map(
               names.map((name) => given(name, "filter<string>", [])),
            ),
         });

      it("flags a self drill whose given no model in the package declares", () => {
         const findings = lintSelfDrills([
            facts({ drills: [selfDrill("warehouse")] }),
         ]);
         expect(findings).toEqual([
            {
               subject: "orders.warehouse",
               severity: "error",
               message: expect.stringContaining('a given "warehouse"'),
            },
         ]);
      });

      it("is silent when any model surfaces the given, not only the one holding the tag", () => {
         // The tag is on a dimension many documents import; a control declared
         // anywhere in the package is enough for the drill to be able to fire.
         expect(
            lintSelfDrills([
               facts({ drills: [selfDrill("region")] }),
               surfacing("region"),
            ]),
         ).toEqual([]);
      });

      /**
       * The runtime seeds the dimension name exactly as spelled
       * (`resolveDrill.ts`, `drillGivenName`), and the consumers FOLD CASE when
       * they look it up: the SDK dashboard viewer builds `givenNamesByFold` for
       * precisely this, "so one tag behaves identically on both surfaces". So
       * `# drill` on `region` against a declared `REGION` resolves, and a
       * finding about it would be a finding about a working drill.
       *
       * This test has now been wrong in BOTH directions, which is worth keeping.
       * It first asserted silence while the lint upper-cased, pinning a real
       * defect as correct. It then asserted an error, correct against the viewer
       * as it stood when that was checked and wrong eighteen minutes later when
       * the fold landed. The lesson is not about case: a lint that copies a
       * runtime rule is only as current as the last time someone compared them.
       */
      it("stays silent when the given differs from the dimension only in case", () => {
         expect(
            lintSelfDrills([
               facts({ drills: [selfDrill("region")] }),
               surfacing("REGION"),
            ]),
         ).toEqual([]);
      });

      it("stays silent on an upper-case dimension whose given matches", () => {
         // The other half: the fix must not start flagging the conventional
         // spelling that already worked.
         expect(
            lintSelfDrills([
               facts({ drills: [selfDrill("REGION")] }),
               surfacing("REGION"),
            ]),
         ).toEqual([]);
      });

      it("honors given= rather than upper-casing the dimension", () => {
         expect(
            lintSelfDrills([
               facts({ drills: [selfDrill("ships_from", "REGION")] }),
               surfacing("REGION"),
            ]),
         ).toEqual([]);
         expect(
            lintSelfDrills([
               facts({ drills: [selfDrill("ships_from", "REGION")] }),
               surfacing("SHIPS_FROM"),
            ]),
         ).toHaveLength(1);
      });

      it("ignores a drill that does not name self", () => {
         expect(
            lintSelfDrills([
               facts({
                  drills: [
                     { source: "orders", dimension: "brand", to: ["overview"] },
                  ],
               }),
            ]),
         ).toEqual([]);
      });

      it("reports once per dimension however many files see the tag", () => {
         const findings = lintSelfDrills([
            facts({ modelPath: "a.malloy", drills: [selfDrill("warehouse")] }),
            facts({ modelPath: "b.malloy", drills: [selfDrill("warehouse")] }),
         ]);
         expect(findings).toHaveLength(1);
      });
   });
});

// The dashboard name's URL safety is pinned by the integration suite, which
// follows the `resource` the listing actually published and asserts it resolves
// (`publishes a followable URL ...`). A unit test here asserted only that
// `encodeURIComponent` behaves like `encodeURIComponent`, which is the runtime's
// job and not this module's, and it stayed green when the encoding was reverted.
// Removed rather than kept as decoration.

describe("service/dashboard multi-line doc comment as a title", () => {
   // Fixed once on the notebook path and left on both dashboard paths, which is
   // the incomplete-fix pattern: the title is a one-line field and the doc
   // comment is deliberately newline-joined markdown.
   const twoLine = ['#" Business Overview\n', '#" Updated nightly.\n'];

   it("takes only the first line on the single-query form", () => {
      const m = build(
         facts({
            queries: [
               {
                  name: "overview",
                  annotations: [...twoLine, "# artifact\n"],
                  givens: [],
               },
            ],
         }),
      );
      expect(m?.title).toBe("Business Overview");
      expect(m?.title ?? "").not.toContain("\n");
      // The description is what the title did NOT take. Asserting the whole
      // comment here is what pinned the duplicate: the first line was published
      // twice, as the heading and again under it.
      expect(m?.description).toBe("Updated nightly.");
   });

   it("takes only the first line on the composite form", () => {
      const m = build(
         facts({
            modelAnnotations: [
               ...twoLine,
               '## artifact { tiles=["orders -> totals"] }\n',
            ],
            viewGivens: new Map([["orders -> totals", []]]),
            sourceFields: new Map([["orders", new Set(["totals"])]]),
         }),
      );
      expect(m?.title).toBe("Business Overview");
      expect(m?.title ?? "").not.toContain("\n");
      // The description is what the title did NOT take. Asserting the whole
      // comment here is what pinned the duplicate: the first line was published
      // twice, as the heading and again under it.
      expect(m?.description).toBe("Updated nightly.");
   });
});

describe("service/dashboard single-query given lint", () => {
   // The tile form warned about this and the single-query form did not, so a
   // single-query dashboard dropped the control in silence while
   // `buildGivenSpecs` claimed the lint covered it.
   it("names a given the query filters by but the file does not import", () => {
      const f = facts({
         queries: [
            {
               name: "overview",
               annotations: ["# artifact\n"],
               givens: ["REGION"],
            },
         ],
      });
      const manifest = build(f);
      if (!manifest) throw new Error("expected a dashboard");
      expect(lintDashboard(f, manifest).map((x) => x.message)).toEqual([
         expect.stringContaining(
            'query "overview" filters by given "REGION", which this file does not import',
         ),
      ]);
   });

   it("stays silent when the file does import it", () => {
      const f = facts({
         queries: [
            {
               name: "overview",
               annotations: ["# artifact\n"],
               givens: ["REGION"],
            },
         ],
         givens: new Map([given("REGION", "filter<string>", [])]),
      });
      const manifest = build(f);
      if (!manifest) throw new Error("expected a dashboard");
      expect(lintDashboard(f, manifest)).toEqual([]);
   });
});

describe("service/dashboard name matches the documented pattern", () => {
   // This predicate no longer gates anything. Curation still can withhold a
   // dashboard; what this settles is that a NAME is never the reason, and a
   // served one has its name percent-encoded into the published URL. An earlier
   // version of this comment said "every dashboard is served", which the
   // curation tests in this same suite falsify. What it decides is whether
   // the name matches the pattern the API documents, which is what a client
   // generated from the spec will accept, so a mismatch is advisory only.
   it("reports a name outside the documented pattern", () => {
      expect(
         matchesDocumentedDashboardName(
            dashboardSlug("dashboards/v1.2.malloy"),
         ),
      ).toBe(false);
      expect(matchesDocumentedDashboardName("has space")).toBe(false);
      expect(matchesDocumentedDashboardName("has/slash")).toBe(false);
      expect(matchesDocumentedDashboardName("")).toBe(false);
      expect(matchesDocumentedDashboardName("..")).toBe(false);
   });

   it("accepts the names the path parameter declares", () => {
      expect(matchesDocumentedDashboardName("overview")).toBe(true);
      expect(matchesDocumentedDashboardName("q3-2026_v2")).toBe(true);
   });
});

describe("service/dashboard silent-vanish lint", () => {
   const messages = (f: DashboardModelFacts) =>
      lintUndiscoveredDashboard(f).map((finding) => finding.message);

   // The composite branch is gated on having at least one tile, and a tag that
   // PARSES is invisible to the parse-error check, so this file produced no
   // dashboard and no finding at all.
   it("explains a model-level artifact tag that declares no tiles", () => {
      expect(
         messages(
            facts({
               modelAnnotations: ['## artifact { title="Overview" }\n'],
            }),
         ),
      ).toEqual([expect.stringContaining("declares no tiles=")]);
   });

   it("explains an empty tiles= as well", () => {
      expect(
         messages(facts({ modelAnnotations: ["## artifact { tiles=[] }\n"] })),
      ).toEqual([expect.stringContaining("declares an empty tiles=")]);
   });

   it("stays silent for a file that simply carries no tag", () => {
      expect(messages(facts({ modelAnnotations: ["## nothing\n"] }))).toEqual(
         [],
      );
   });

   // MOTLY's grammar stops at the SPACE in `@2024-03-01 10:00`, which is what
   // this covers. It does NOT stop at the ISO `T` form, which parses fine and is
   // handled in `readStartingGivens`; an earlier version of this comment claimed
   // otherwise and the claim was false. What is pinned here is only that the
   // space form is reported rather than silently dropped: it stops at
   // the space in `@2024-03-01 10:00`, which discards the WHOLE artifact tag,
   // dashboard and all. Pinned because that is a natural thing to write next to
   // a timestamp given, and the reason it is acceptable is that it is reported
   // here rather than lost. `readStartingGivens` says so and this is the proof.
   it("reports a space-separated timestamp, which the grammar cannot parse", () => {
      expect(
         messages(
            facts({
               modelAnnotations: [
                  '## artifact { tiles=["orders -> by_month"] givens { WHEN=@2024-03-01 10:00 } }\n',
               ],
            }),
         ),
      ).toEqual([expect.stringContaining("Tag does not parse")]);
   });

   // Every finding here carries the same subject (the file's slug), so two tags
   // failing the SAME way are byte-identical and an author is told the same
   // thing twice about a file with one problem. The pair below is the natural
   // shape: two queries in one dashboard file whose tags break identically.
   it("reports one finding when two tags in a file fail the same way", () => {
      const broken = "# artifact { givens { WHEN=@2024-03-01 10:00 } }\n";
      expect(
         messages(
            facts({
               queries: [
                  { name: "a", annotations: [broken], givens: [] },
                  { name: "b", annotations: [broken], givens: [] },
               ],
            }),
         ),
      ).toEqual([expect.stringContaining("Tag does not parse")]);
   });

   // The control, and the half that matters: dedup must collapse repeats, not
   // distinct causes. Without it this passes just as well as the test above,
   // since returning only the first finding would satisfy that one alone.
   it("still reports two findings when the two tags fail differently", () => {
      expect(
         messages(
            facts({
               queries: [
                  {
                     name: "a",
                     annotations: [
                        "# artifact { givens { WHEN=@2024-03-01 10:00 } }\n",
                     ],
                     givens: [],
                  },
                  {
                     name: "b",
                     annotations: [
                        `# artifact { title="${"x".repeat(9000)}" }\n`,
                     ],
                     givens: [],
                  },
               ],
            }),
         ),
      ).toHaveLength(2);
   });
});

describe("service/dashboard given-tag lint", () => {
   const messages = (f: DashboardModelFacts) =>
      lintGivenTags([f]).map((finding) => finding.message);

   // A failed MOTLY parse yields an EMPTY tag rather than undefined, so
   // `readGivenControlSpec` cannot detect it: the given silently loses its
   // whole control contract. This is the only signal that exists for it.
   it("reports a given whose annotation does not parse", () => {
      expect(
         messages(
            facts({
               givens: new Map([given("REGION", "filter<string>", ["# (\n"])]),
            }),
         ),
      ).toEqual([expect.stringContaining('given "REGION" has an annotation')]);
   });

   it("stays silent on a well-formed given", () => {
      expect(
         messages(
            facts({
               givens: new Map([
                  given("REGION", "filter<string>", ['# label="Region"\n']),
               ]),
            }),
         ),
      ).toEqual([]);
   });

   // The rescue runs before the parse, so a documented Malloyyo starting value
   // must not be reported as broken.
   it("stays silent on a bare filter literal, which is rescued", () => {
      expect(
         messages(
            facts({
               givens: new Map([
                  given("REGION", "filter<string>", ["# default=f'US'\n"]),
               ]),
            }),
         ),
      ).toEqual([]);
   });
});

describe("service/dashboard grid width and hostile literals", () => {
   function lintOf(f: DashboardModelFacts) {
      const manifest = build(f);
      if (!manifest) throw new Error("expected a dashboard");
      return lintDashboard(f, manifest).map((finding) => finding.message);
   }

   const singleQuery = (annotation: string) =>
      facts({
         queries: [{ name: "overview", annotations: [annotation], givens: [] }],
      });

   // BOTH spellings reach the manifest, but only `dashboard_columns` was ever
   // checked, and `# dashboard { columns=N }` is the form the shipped example
   // dashboards use, so a bad value there was dropped with no finding at all.
   it("validates the # dashboard { columns= } spelling", () => {
      expect(
         lintOf(singleQuery('# artifact dashboard { columns="wide" }\n')),
      ).toEqual([expect.stringContaining("# dashboard { columns=… } must be")]);
   });

   it("validates the composite dashboard_columns spelling", () => {
      expect(
         lintOf(
            facts({
               modelAnnotations: [
                  '## artifact { tiles=["orders -> totals"] dashboard_columns="wide" }\n',
               ],
               viewGivens: new Map([["orders -> totals", []]]),
               sourceFields: new Map([["orders", new Set(["totals"])]]),
            }),
         ),
      ).toEqual([expect.stringContaining("dashboard_columns must be")]);
   });

   // The lint said the value was dropped while `Tag.numeric()` (parseFloat)
   // still put a number on the wire: `12abc` arrived as 12, and `1e999` as
   // Infinity, which JSON renders as null against a field the spec declares an
   // integer. Both sides now share one helper so they cannot disagree.
   it("keeps an invalid width off the manifest, not just out of the lint", () => {
      for (const bad of [
         '# artifact dashboard { columns="12abc" }\n',
         "# artifact dashboard { columns=1e999 }\n",
         "# artifact dashboard { columns=0 }\n",
         "# artifact dashboard { columns=-4 }\n",
         "# artifact dashboard { columns=2.5 }\n",
      ]) {
         const manifest = build(singleQuery(bad));
         expect(manifest?.dashboardColumns).toBeUndefined();
         expect(lintOf(singleQuery(bad))).toHaveLength(1);
      }
   });

   // The two spellings are read with `??`, so a bad value in one is not always
   // a fallback to the renderer default: when the other spelling is good, the
   // grid quietly uses it, and the finding must say so.
   it("names the width actually used when the other spelling supplies one", () => {
      const both = singleQuery(
         '# artifact { dashboard_columns="wide" } dashboard { columns=6 }\n',
      );
      expect(build(both)?.dashboardColumns).toBe(6);
      expect(lintOf(both)).toEqual([
         expect.stringContaining("The grid uses 6 instead."),
      ]);
   });

   it("still says renderer default when neither spelling is usable", () => {
      const neither = singleQuery(
         '# artifact { dashboard_columns="wide" } dashboard { columns="wider" }\n',
      );
      expect(build(neither)?.dashboardColumns).toBeUndefined();
      for (const m of lintOf(neither)) {
         expect(m).toContain("falls back to the renderer default");
      }
   });

   // The same strictness, on the OTHER spelling. Both reach the manifest, and a
   // mutation sweep showed only the render-tag half was pinned: reverting the
   // composite half to raw `Tag.numeric()` failed nothing. Enumerating the set
   // is the point, not testing one member of it twice.
   it("keeps an invalid composite dashboard_columns off the manifest too", () => {
      const composite = (annotation: string) =>
         facts({
            modelAnnotations: [annotation],
            viewGivens: new Map([["orders -> totals", []]]),
            sourceFields: new Map([["orders", new Set(["totals"])]]),
         });
      for (const bad of ['"12abc"', "1e999", "0", "-4", "2.5"]) {
         const f = composite(
            `## artifact { tiles=["orders -> totals"] dashboard_columns=${bad} }\n`,
         );
         expect(build(f)?.dashboardColumns).toBeUndefined();
         const manifest = build(f);
         if (!manifest) throw new Error("expected a dashboard");
         expect(lintDashboard(f, manifest)).toHaveLength(1);
      }
   });

   it("keeps a valid composite dashboard_columns ON the manifest", () => {
      const f = facts({
         modelAnnotations: [
            '## artifact { tiles=["orders -> totals"] dashboard_columns=4 }\n',
         ],
         viewGivens: new Map([["orders -> totals", []]]),
         sourceFields: new Map([["orders", new Set(["totals"])]]),
      });
      expect(build(f)?.dashboardColumns).toBe(4);
      const manifest = build(f);
      if (!manifest) throw new Error("expected a dashboard");
      expect(lintDashboard(f, manifest)).toEqual([]);
   });

   it("accepts a valid width in either spelling", () => {
      expect(
         lintOf(singleQuery("# artifact dashboard { columns=6 }\n")),
      ).toEqual([]);
   });

   // `Tag.text()` THROWS on a bad date literal rather than returning undefined.
   // The reporting path calls it on the very value it is complaining about, and
   // the lint's try/catch wraps the whole package loop, so one bad literal would
   // have cost every dashboard finding in the package.
   // The reported value must be the author's, not the word `undefined`.
   // `tagText` returns undefined for exactly the bad-literal case, and
   // `JSON.stringify(undefined)` is the literal text `undefined`.
   it("does not print the word undefined for an unreadable width", () => {
      const messages = lintOf(
         singleQuery("# artifact dashboard { columns=@2024-13-01 }\n"),
      );
      expect(messages).toHaveLength(1);
      expect(messages[0]).toContain("could not be read");
      expect(messages[0]).not.toContain("undefined");
   });

   it("reports a bad date literal as a width rather than throwing", () => {
      expect(() =>
         lintOf(singleQuery("# artifact dashboard { columns=@2024-13-01 }\n")),
      ).not.toThrow();
      expect(
         lintOf(singleQuery("# artifact dashboard { columns=@2024-13-01 }\n")),
      ).toEqual([expect.stringContaining("# dashboard { columns=… } must be")]);
   });

   // Same throwing reader, reached through the manifest build rather than the
   // lint: a title that is a bad date literal must cost the title, not the
   // dashboard.
   it("still builds a manifest when the title is a bad date literal", () => {
      const manifest = build(singleQuery("# artifact { title=@2024-13-01 }\n"));
      expect(manifest).toMatchObject({ name: "overview", query: "overview" });
      // Falls through the title chain to the slug.
      expect(manifest?.title).toBe("overview");
   });
});

describe("service/dashboard inherits the annotation guards", () => {
   // Dashboard discovery reads `# artifact`, so it is a NEW reader of
   // reserved-namespace tags and must inherit both guards `motly.ts` installs.
   // Structurally it does, because every read here goes through `motlyTag`
   // rather than `parseAnnotation`; these pin that it stays that way.

   // MOTLY hydrates `@env.NAME` from the server's own environment. Plain `#`
   // tags used to stay server-side, so it was harmless until deriving a
   // control contract put them on the wire.
   it("does not hydrate @env into a dashboard title", () => {
      process.env.SLICE6_PROBE_SECRET = "super-secret-value";
      try {
         const manifest = build(
            facts({
               queries: [
                  {
                     name: "overview",
                     annotations: [
                        "# artifact { title=@env.SLICE6_PROBE_SECRET }\n",
                     ],
                     givens: [],
                  },
               ],
            }),
         );
         // Three assertions, because the obvious one proves nothing on its own.
         // `manifest` is undefined for an `@env` tag, so a `not.toContain` over
         // `JSON.stringify(manifest ?? {})` runs against the string "{}" and
         // passes whether the guard worked or the whole dashboard vanished. An
         // earlier version of this test did exactly that.
         //
         // So: the value must be absent, the drop must be REPORTED rather than
         // silent, and a sibling tag carrying no `@env` must still build.
         expect(JSON.stringify(manifest ?? {})).not.toContain(
            "super-secret-value",
         );
         expect(
            lintUndiscoveredDashboard(
               facts({
                  modelAnnotations: [],
                  queries: [
                     {
                        name: "overview",
                        annotations: [
                           "# artifact { title=@env.SLICE6_PROBE_SECRET }\n",
                        ],
                        givens: [],
                     },
                  ],
               }),
            ).map((f) => f.message),
         ).toEqual([expect.stringContaining("@env.")]);
         expect(
            build(
               facts({
                  queries: [
                     {
                        name: "overview",
                        annotations: ['# artifact { title="Plain" }\n'],
                        givens: [],
                     },
                  ],
               }),
            )?.title,
         ).toBe("Plain");
      } finally {
         delete process.env.SLICE6_PROBE_SECRET;
      }
   });

   // The parser's property bag is a plain object, so `__proto__` reaches the
   // prototype chain. The block form throws RangeError and poisons the process
   // for every later parse; the bare form pollutes silently. Both must be
   // stopped BEFORE the parse, which is why the guard cannot be a try/catch.
   //
   // Read the scope of this narrowly. It proves the guard covers the route
   // dashboard discovery uses, which is every read going through `motlyTag`.
   // It does NOT mean the process is safe from `__proto__`, and this comment
   // says so because the title alone invites that inference. `##!` and `#@` are
   // parsed EAGERLY BY THE COMPILER during `getModel()`, before any parse of
   // ours runs, and `motlyAnnotations` drops both routes, so the guard never
   // sees them and cannot undo damage that predates its snapshot. Measured on
   // this tree rather than taken on trust: `##! __proto__ { a=b }` leaves
   // `Object.prototype` carrying `location` and `properties`, and the next
   // ordinary parse throws RangeError. That is live on `main` today and is not
   // this slice's to fix; the real repair is upstream in `motly-ts-parser`,
   // where the property bags want to be `Object.create(null)`.
   it("survives a __proto__ artifact tag without poisoning later parses", () => {
      for (const hostile of [
         "# artifact { __proto__ { a=b } }\n",
         "# __proto__ { a=b }\n",
         "# __proto__=x\n",
      ]) {
         expect(() =>
            build(
               facts({
                  queries: [
                     { name: "overview", annotations: [hostile], givens: [] },
                  ],
               }),
            ),
         ).not.toThrow();
      }
      // The damage would persist for the life of the process, so an ordinary
      // unrelated tag parsed afterwards is the real assertion.
      const after = build(
         facts({
            queries: [
               {
                  name: "overview",
                  annotations: ['# artifact { title="Still fine" }\n'],
                  givens: [],
               },
            ],
         }),
      );
      expect(after?.title).toBe("Still fine");
      expect(({} as Record<string, unknown>).location).toBeUndefined();
      expect(({} as Record<string, unknown>).eq).toBeUndefined();
   });
});

describe("service/dashboard tile normalization", () => {
   /**
    * The expression this replaced, kept verbatim as an oracle. It backtracked
    * quadratically over a whitespace run, which a package author controls
    * through `tiles=[…]`, and discovery runs on the main server process, so the
    * stall was every tenant's. The refactor is only safe if it is
    * behaviour-identical, so that is asserted rather than assumed.
    */
   const oracle = (tile: string) =>
      tile
         .trim()
         .replace(/\s*->\s*/g, " -> ")
         .replace(/\s+/g, " ");

   it("matches the expression it replaced on every shape that occurs", () => {
      const parts = ["orders", "by_month", "->", " ", "  ", "\t", "", "a b"];
      const cases: string[] = [];
      for (const a of parts)
         for (const b of parts)
            for (const c of parts) cases.push(`${a}${b}${c}`);
      cases.push(
         "orders -> by_month",
         "orders->by_month",
         "orders   ->   by_month",
         "a->b->c",
         "  padded -> x  ",
      );
      for (const tile of cases) {
         expect([tile, normalizeTileExpression(tile)]).toEqual([
            tile,
            oracle(tile),
         ]);
      }
   });

   it("stays linear on the input that made the old one quadratic", () => {
      const hostile = `orders${" ".repeat(80_000)}bymonth`;
      // The oracle is evaluated OUTSIDE the timed region: it is the quadratic
      // one, so timing it here would measure the very thing being replaced.
      const expected = oracle(hostile);
      const started = performance.now();
      const actual = normalizeTileExpression(hostile);
      const elapsed = performance.now() - started;
      expect(actual).toBe(expected);
      // The old expression took ~2.3s on this input; a generous ceiling still
      // fails by orders of magnitude if the quadratic form comes back.
      expect(elapsed).toBeLessThan(500);
   });
});
