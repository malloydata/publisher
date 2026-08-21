// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

// Real-compiler tests for the `#@ preaggregate` reader. Every annotation below
// is COMPILED, because the facts that shaped this module are MOTLY parse
// behaviors, not choices: that the documented `grain="…"` lands as a sibling
// top-level key, that negating the annotation therefore orphans it, and that the
// merged tag holds only ONE grain per measure — which is why the reader walks the
// annotation notes in order instead.
//
// Two cases to read first. The orphan case is why a negation is honoured by
// clearing rather than by trusting the merged tag's `grain`; a regression there
// would pre-aggregate a measure the author explicitly turned off. The
// several-grains case is why the notes are read at all: one measure rolled up at
// two grains is two rollups, and the merged tag can only ever report one.
import type { FixedConnectionMap } from "@malloydata/malloy";
import { beforeAll, describe, expect, it } from "bun:test";
import {
   parseGrainDimensions,
   readPreaggregateAnnotation,
   type AnnotatableMeasure,
} from "./preaggregation_annotation";
import {
   duckdbTestConnections,
   loadTestModel,
} from "./incremental_test_harness";

const MODEL = `##! experimental.persistence
source: s is duckdb.sql("""
  SELECT 1 AS amount, 'A' AS category, TIMESTAMP '2024-01-01' AS order_time
""") extend {
  #@ preaggregate grain="order_time.day, category"
  measure: m_documented is amount.sum()

  #@ preaggregate.grain="order_time.day, category"
  measure: m_dotted is amount.sum()

  #@ preaggregate { grain="order_time.day, category" }
  measure: m_braced is amount.sum()

  measure: m_plain is amount.sum()

  #@ preaggregate
  measure: m_no_grain is amount.sum()

  #@ preaggregate grain=""
  measure: m_empty_grain is amount.sum()

  #@ preaggregate grain="category , order_time.day ,, category"
  measure: m_messy is amount.sum()

  #@ preaggregate grain="category"
  #@ -preaggregate
  measure: m_negated is amount.sum()

  #@ preaggregate.grain="category"
  #@ -preaggregate
  measure: m_negated_dotted is amount.sum()

  #@ preaggregate grain="category"
  #@ preaggregate grain="order_time.day"
  measure: m_two_grains is amount.sum()

  #@ preaggregate grain="category"
  #@ preaggregate grain="category"
  measure: m_duplicate_grain is amount.sum()

  #@ preaggregate grain="b_dim, a_dim"
  #@ preaggregate grain="a_dim, b_dim"
  measure: m_one_grain_two_spellings is amount.sum()

  #@ preaggregate grain="category"
  #@ preaggregate
  measure: m_one_good_one_bad is amount.sum()

  #@ preaggregate grain="a_dim"
  #@ -preaggregate
  #@ preaggregate grain="b_dim"
  measure: m_renegotiated is amount.sum()

  #@ preaggregate grain="a_dim" namespace="scratch"
  #@ -preaggregate
  #@ preaggregate grain="a_dim"
  measure: m_renegotiated_namespace is amount.sum()

  #@ preaggregate grain="a_dim" namespace="ns_a"
  #@ preaggregate grain="b_dim" namespace="ns_b"
  measure: m_grain_own_namespace is amount.sum()

  #@ preaggregate grain="a_dim" namespace=""
  measure: m_empty_namespace is amount.sum()

  #@ preaggregate grain="a_dim" namespace="ns_a"
  #@ preaggregate grain="a_dim"
  measure: m_restated_grain is amount.sum()

  #@ preaggregate grain="b_dim, a_dim"
  measure: m_unsorted is amount.sum()
}

// An extending source, to prove inherited annotations are still read.
source: s_ext is s extend {
  #@ preaggregate grain="category"
  measure: m_added_here is amount.sum()
}
`;

let lookup: (name: string) => AnnotatableMeasure;
let lookupExt: (name: string) => AnnotatableMeasure;

beforeAll(async () => {
   const { connections }: { connections: FixedConnectionMap } =
      duckdbTestConnections();
   const compiled = await loadTestModel(connections, MODEL).getModel();
   // eslint-disable-next-line @typescript-eslint/no-explicit-any
   const contents = (compiled as any)._modelDef.contents;
   const finder =
      (sourceName: string) =>
      (name: string): AnnotatableMeasure => {
         const field = contents[sourceName].fields.find(
            (f: { name: string; as?: string }) => (f.as ?? f.name) === name,
         );
         if (!field) throw new Error(`no field ${name} in ${sourceName}`);
         return field as AnnotatableMeasure;
      };
   lookup = finder("s");
   lookupExt = finder("s_ext");
});

/** The grains a measure declares, as dimension lists. */
function grainsOf(name: string): string[][] {
   return readPreaggregateAnnotation(lookup(name)).grains.map(
      (g) => g.dimensions,
   );
}

describe("#@ preaggregate reader: reading the grain", () => {
   for (const name of ["m_documented", "m_dotted", "m_braced"]) {
      it(`${name}: reads the grain`, () => {
         // All three syntaxes must agree — the documented sibling-key form and
         // the two nested forms — because authors will write whichever the docs
         // in front of them show.
         expect(readPreaggregateAnnotation(lookup(name)).declared).toBe(true);
         expect(grainsOf(name)).toEqual([["category", "order_time.day"]]);
      });
   }

   it("an unannotated measure is undeclared", () => {
      expect(readPreaggregateAnnotation(lookup("m_plain"))).toMatchObject({
         declared: false,
         grains: [],
      });
   });

   it("dimensions come back sorted, trimmed and de-duplicated", () => {
      // Canonical order is load-bearing: synthesis derives the rollup's identity
      // from this list, so two spellings of one grain must not become two tables.
      expect(grainsOf("m_messy")).toEqual([["category", "order_time.day"]]);
      expect(grainsOf("m_unsorted")).toEqual([["a_dim", "b_dim"]]);
   });
});

describe("#@ preaggregate reader: a measure may declare several grains", () => {
   it("two annotations are two grains, not one overriding the other", () => {
      // The reason the reader walks notes at all. The merged tag reports only
      // `order_time.day` here, so reading it would silently drop a rollup — and
      // the author would still see one built, which is worse than none.
      expect(grainsOf("m_two_grains")).toEqual([
         ["category"],
         ["order_time.day"],
      ]);
   });

   it("grains come back in canonical order, not authoring order", () => {
      // Both legs of synthesis must emit byte-identical text, so nothing may
      // depend on the order the annotations were written in.
      const declared = grainsOf("m_two_grains");
      expect(declared).toEqual([...declared].sort());
   });

   it("the same grain declared twice is ONE grain", () => {
      expect(grainsOf("m_duplicate_grain")).toEqual([["category"]]);
   });

   it("two spellings of one grain are ONE grain", () => {
      // `"b_dim, a_dim"` and `"a_dim, b_dim"` are the same GROUP BY. Two entries
      // would mean two identical tables built and maintained.
      expect(grainsOf("m_one_grain_two_spellings")).toEqual([
         ["a_dim", "b_dim"],
      ]);
   });

   it("each grain keeps the namespace written on its own line", () => {
      // A grain IS a table, so two grains can genuinely be created in two places.
      // A namespace scoped to the measure instead would apply one line's choice to
      // the other's table — a rollup moved somewhere its author never named, with
      // no diagnostic, decided by whichever line the IR happened to report first.
      const declaration = readPreaggregateAnnotation(
         lookup("m_grain_own_namespace"),
      );
      expect(
         declaration.grains.map((g) => [g.dimensions, g.namespace]),
      ).toEqual([
         [["a_dim"], "ns_a"],
         [["b_dim"], "ns_b"],
      ]);
   });

   it("re-stating a grain takes the namespace on its own line, including none", () => {
      // The deliberate consequence of binding a namespace to its line: the second
      // line re-declares this grain and names no namespace, so it has none. The
      // alternative — a namespace that persists across lines that do not mention
      // it — is the outranging this binding exists to prevent, and it would leave
      // no way to drop one by re-declaring.
      //
      // It matters most through an extend chain, since inherited notes come first:
      // an extending source that re-states its base's grain must restate the
      // namespace too. `#@ -preaggregate` is how you turn a declaration off.
      const declaration = readPreaggregateAnnotation(
         lookup("m_restated_grain"),
      );
      expect(declaration.grains).toEqual([
         { dimensions: ["a_dim"], text: "a_dim", namespace: undefined },
      ]);
   });

   it("an inherited annotation is still read on an extending source", () => {
      // Measured: an extending source carries the base's notes in its own
      // blockNotes, which is what lets the reader skip walking an inherits chain.
      // If that ever changes, this fails rather than quietly losing rollups.
      expect(
         readPreaggregateAnnotation(lookupExt("m_documented")).grains.map(
            (g) => g.dimensions,
         ),
      ).toEqual([["category", "order_time.day"]]);
      expect(
         readPreaggregateAnnotation(lookupExt("m_added_here")).grains.map(
            (g) => g.dimensions,
         ),
      ).toEqual([["category"]]);
   });
});

describe("#@ preaggregate reader: negation", () => {
   it("a negated measure is undeclared, documented syntax", () => {
      // The trap: this measure's `grain` key SURVIVES the negation in the merged
      // tag. Trusting that grain would pre-aggregate a measure whose author
      // turned it off.
      expect(readPreaggregateAnnotation(lookup("m_negated"))).toMatchObject({
         declared: false,
         grains: [],
      });
   });

   it("a negated measure is undeclared, nested syntax", () => {
      expect(
         readPreaggregateAnnotation(lookup("m_negated_dotted")),
      ).toMatchObject({ declared: false, grains: [] });
   });

   it("negation yields no error, because nothing was asked for", () => {
      // An undeclared measure must not produce a publish error — otherwise
      // turning the feature off for one measure would fail the package.
      expect(readPreaggregateAnnotation(lookup("m_negated")).errors).toEqual(
         [],
      );
   });

   it("a declaration AFTER a negation is honoured", () => {
      // Where this reader deliberately diverges from the merged tag, which
      // reports this measure as undeclared while still holding `grain="b_dim"`.
      // Read in order the author declared `b_dim` last, and only that grain is in
      // effect — the earlier `a_dim` was cleared.
      expect(grainsOf("m_renegotiated")).toEqual([["b_dim"]]);
   });

   it("a negation clears the namespace with the grain it was written for", () => {
      // `namespace` rides its own grain, so "everything above is off" needs no
      // separate rule for it. A namespace that outlived a negation would be the
      // worst kind of survivor: the re-declaration below reads as clean, and the
      // rollup would be built in `scratch` with nothing on the page saying so.
      // Inherited notes are ordered FIRST, so a base that named one and an extend
      // that negates and re-declares is exactly this shape.
      const declaration = readPreaggregateAnnotation(
         lookup("m_renegotiated_namespace"),
      );
      expect(declaration.grains).toEqual([
         { dimensions: ["a_dim"], text: "a_dim", namespace: undefined },
      ]);
   });
});

describe("#@ preaggregate reader: unusable declarations are errors", () => {
   // Refusing beats silently skipping: an annotation that does nothing is the
   // failure mode this feature must not have.
   it("a declaration with no grain is an error naming the measure and the fix", () => {
      const result = readPreaggregateAnnotation(lookup("m_no_grain"));
      expect(result.declared).toBe(true);
      expect(result.grains).toEqual([]);
      expect(result.errors).toHaveLength(1);
      expect(result.errors[0].kind).toBe("missing_grain");
      expect(result.errors[0].message).toContain("`m_no_grain`");
      expect(result.errors[0].message).toContain("grain");
   });

   it("a declaration with an empty grain is an error", () => {
      const result = readPreaggregateAnnotation(lookup("m_empty_grain"));
      expect(result.declared).toBe(true);
      expect(result.errors[0].kind).toBe("empty_grain");
      expect(result.errors[0].message).toContain("`m_empty_grain`");
   });

   it("an empty namespace is an error, not an ignored key", () => {
      // An author who typed `namespace=` meant something by it. Ignoring the empty
      // value would build the rollup in the base's namespace or the connection
      // default — a location they did not choose and were never told about.
      const result = readPreaggregateAnnotation(lookup("m_empty_namespace"));
      expect(result.errors.map((e) => e.kind)).toEqual(["invalid_namespace"]);
      expect(result.grains).toEqual([]);
   });

   it("a good grain and a bad one are reported independently", () => {
      // Both matter: the good grain must not hide the broken line, and the broken
      // line must name what to fix.
      const result = readPreaggregateAnnotation(lookup("m_one_good_one_bad"));
      expect(result.grains.map((g) => g.dimensions)).toEqual([["category"]]);
      expect(result.errors.map((e) => e.kind)).toEqual(["missing_grain"]);
   });
});

describe("#@ preaggregate reader: total on bad input", () => {
   it("no annotations at all is undeclared, not a throw", () => {
      expect(readPreaggregateAnnotation({ name: "m" })).toMatchObject({
         declared: false,
         grains: [],
      });
   });

   it("junk in the annotations slot is undeclared, not a throw", () => {
      // A package load must not die on an annotation shape we did not expect.
      expect(
         readPreaggregateAnnotation({
            name: "m",
            annotations: { notes: "not-an-array" },
         }),
      ).toMatchObject({ declared: false });
   });
});

describe("parseGrainDimensions", () => {
   const cases: [string, string[]][] = [
      ["category", ["category"]],
      ["order_time.day, category", ["category", "order_time.day"]],
      ["  a ,  b  ", ["a", "b"]],
      ["a,,b", ["a", "b"]],
      ["b,a,b", ["a", "b"]],
      ["", []],
      ["   ", []],
      [",,,", []],
   ];
   for (const [input, expected] of cases) {
      it(`${JSON.stringify(input)} -> ${JSON.stringify(expected)}`, () => {
         expect(parseGrainDimensions(input)).toEqual(expected);
      });
   }
});
