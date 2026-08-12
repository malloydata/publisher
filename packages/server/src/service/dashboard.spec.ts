import { describe, expect, it } from "bun:test";
import {
   buildDashboardManifest,
   dashboardSlug,
   docCommentText,
   filterPublisherOwnedRenderLogs,
   isDashboardModelPath,
   isServableDashboardSlug,
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
      expect(withDoc?.description).toBe("Business health at a glance.");

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
         }),
      );
      expect(manifest?.autorun).toBe(false);
      // Starting values arrive in run shape — the filter body, not the literal.
      expect(manifest?.startingGivens).toEqual({
         MANUFACTURER: "Ford Motor Company",
         PERIOD: "30 days",
      });
   });
});

describe("service/dashboard given specs (the control contract)", () => {
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
            default: "f''",
            label: "Brand",
            control: "select",
            suggest: {
               query: undefined,
               source: "order_items",
               dimension: "brand",
            },
         },
         {
            name: "MIN_PRICE",
            type: "filter<number>",
            default: "f''",
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
         { name: "REGION", type: "string", default: "'US'" },
      ]);
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
               message: expect.stringContaining('a given "WAREHOUSE"'),
            },
         ]);
      });

      it("is silent when any model surfaces the given, not only the one holding the tag", () => {
         // The tag is on a dimension many documents import; a control declared
         // anywhere in the package is enough for the drill to be able to fire.
         expect(
            lintSelfDrills([
               facts({ drills: [selfDrill("region")] }),
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

describe("service/dashboard slug is servable", () => {
   // `dashboards/.malloy` yields an empty slug, whose published `resource`
   // would end in `/dashboards/` and fold onto the LIST route under Express's
   // non-strict routing, so following the manifest's own link returns an array.
   it("rejects a slug that cannot round-trip through its own URL", () => {
      expect(isServableDashboardSlug(dashboardSlug("dashboards/.malloy"))).toBe(
         false,
      );
      expect(
         isServableDashboardSlug(dashboardSlug("dashboards/...malloy")),
      ).toBe(false);
      expect(isServableDashboardSlug("has space")).toBe(false);
      expect(isServableDashboardSlug("has/slash")).toBe(false);
   });

   it("accepts the names the path parameter declares", () => {
      expect(isServableDashboardSlug("overview")).toBe(true);
      expect(isServableDashboardSlug("q3-2026_v2")).toBe(true);
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

   it("accepts a valid width in either spelling", () => {
      expect(
         lintOf(singleQuery("# artifact dashboard { columns=6 }\n")),
      ).toEqual([]);
   });

   // `Tag.text()` THROWS on a bad date literal rather than returning undefined.
   // The reporting path calls it on the very value it is complaining about, and
   // the lint's try/catch wraps the whole package loop, so one bad literal would
   // have cost every dashboard finding in the package.
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
         expect(JSON.stringify(manifest ?? {})).not.toContain(
            "super-secret-value",
         );
      } finally {
         delete process.env.SLICE6_PROBE_SECRET;
      }
   });

   // The parser's property bag is a plain object, so `__proto__` reaches the
   // prototype chain. The block form throws RangeError and poisons the process
   // for every later parse; the bare form pollutes silently. Both must be
   // stopped BEFORE the parse, which is why the guard cannot be a try/catch.
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
