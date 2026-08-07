import { parseAnnotation, type Tag } from "@malloydata/malloy-tag";
import { describe, expect, it } from "bun:test";
import { readGivenControlSpec } from "./given";
import {
   motlyTag,
   quoteFilterLiterals,
   readAutorun,
   readStartingGivens,
} from "./motly";

/** Throw-safe read so a fixture cannot fail for the wrong reason. */
function tagTextOf(tag: Tag | undefined, key: string): string | undefined {
   try {
      return tag?.text(key);
   } catch {
      return undefined;
   }
}

/**
 * The control contract is read off the `given:` declaration rather than off the
 * surface presenting it, which is what lets a notebook and a dashboard render
 * the same control. These pin that reading, independently of either surface.
 */
describe("readGivenControlSpec", () => {
   it("reads the presentation a declaration asks for", () => {
      expect(
         readGivenControlSpec([
            `# label="Brand" control=select suggest { source=orders dimension=brand }`,
         ]),
      ).toEqual({
         label: "Brand",
         control: "select",
         // Only the keys the declaration carried. `query` is absent rather than
         // present-and-undefined, which matters across the worker's structured
         // clone, where an explicit undefined survives and reads as declared.
         suggest: { source: "orders", dimension: "brand" },
      });
   });

   it("reads a bounded numeric range", () => {
      expect(
         readGivenControlSpec([`# label="Minimum" range_min=0 range_max=500`]),
      ).toEqual({ label: "Minimum", rangeMin: 0, rangeMax: 500 });
   });

   it("returns nothing for an untagged declaration, so the type decides", () => {
      expect(readGivenControlSpec([])).toEqual({});
      expect(readGivenControlSpec([`#(doc) A caller-facing note\n`])).toEqual(
         {},
      );
   });

   it("ignores a control kind it does not recognize", () => {
      expect(readGivenControlSpec([`# control=radio`])).toEqual({});
   });

   it("reads helper text from the plain-tag description form", () => {
      expect(
         readGivenControlSpec([
            `# description="Earliest report date to include"`,
         ]),
      ).toEqual({ description: "Earliest report date to include" });
   });

   it("leaves the older #(description=…) spelling to the client that parses it", () => {
      // Not an oversight: `#(description="…")` is an app-route annotation, so it
      // survives on `MalloyGivenApi.annotations` and its existing client-side
      // reader keeps working. Only the plain-tag form is derived here, and the
      // two must not both populate `description` or a given carrying both would
      // depend on which one won.
      expect(
         readGivenControlSpec([`#(description="Region to focus on")`]),
      ).toEqual({});
   });

   it("omits suggest entirely when the tag names no target", () => {
      expect(readGivenControlSpec([`# control=select suggest { }`])).toEqual({
         control: "select",
      });
      // `dimension` picks a column out of a query or source; on its own it
      // names nothing to run, so it is not a target either.
      expect(
         readGivenControlSpec([`# control=select suggest { dimension=brand }`]),
      ).toEqual({ control: "select" });
      // `source` alone is equally unrunnable: it names no column to read.
      expect(
         readGivenControlSpec([`# control=select suggest { source=orders }`]),
      ).toEqual({ control: "select" });
      // ...but a source with its dimension is one of the documented forms.
      expect(
         readGivenControlSpec([
            `# control=select suggest { source=orders dimension=brand }`,
         ]),
      ).toEqual({
         control: "select",
         suggest: { source: "orders", dimension: "brand" },
      });
   });

   it("reads the query form of suggest, which needs no dimension", () => {
      // The primary documented form. Without this the `query` arm of the
      // runnable guard is unexercised, so dropping it would pass the suite.
      expect(
         readGivenControlSpec([`# control=select suggest { query=brands }`]),
      ).toEqual({ control: "select", suggest: { query: "brands" } });
      expect(
         readGivenControlSpec([
            `# control=select suggest { query=brands dimension=name }`,
         ]),
      ).toEqual({
         control: "select",
         suggest: { query: "brands", dimension: "name" },
      });
   });

   it("drops a bad literal instead of throwing and failing the package load", () => {
      // MOTLY accepts `@2024-13-01`; `Date` does not, and `Tag.text()` calls
      // `toISOString()` on the Invalid Date it built. Both production callers map
      // over every given with no try/catch, so a throw here would take down every
      // model in the package over one typo. Near misses do not throw:
      // `@2024-06-31` rolls forward, which is how this hides.
      expect(() => readGivenControlSpec([`# label=@2024-13-01`])).not.toThrow();
      expect(readGivenControlSpec([`# label=@2024-13-01`])).toEqual({});
      expect(
         readGivenControlSpec([`# label=@2024-01-32 control=select`]),
      ).toEqual({ control: "select" });
   });

   it("ignores a range bound that is not wholly a number", () => {
      // `Tag.numeric()` is parseFloat, so `100px` would arrive as 100 and, with
      // its pair present, turn a plain number input into a slider capped at a
      // bound the author never wrote.
      expect(readGivenControlSpec([`# range_max=100px`])).toEqual({});
      // Hex is decimal-only territory too: `Number()` would take this as 16 and
      // parseFloat took it as 0. Absent is the answer the author can act on.
      expect(readGivenControlSpec([`# range_max=0x10`])).toEqual({});
      expect(readGivenControlSpec([`# range_max=1_000`])).toEqual({});
      expect(readGivenControlSpec([`# range_min=0 range_max=12abc`])).toEqual({
         rangeMin: 0,
      });
      // Zero and negatives are legitimate bounds and must survive.
      expect(readGivenControlSpec([`# range_min=-5 range_max=0`])).toEqual({
         rangeMin: -5,
         rangeMax: 0,
      });
   });

   it("treats a non-finite range bound as absent", () => {
      // `numeric()` is parseFloat, so an overflowing bound is Infinity, which
      // JSON renders as null against a field the spec declares `type: number`.
      expect(readGivenControlSpec([`# range_min=1 range_max=1e999`])).toEqual({
         rangeMin: 1,
      });
   });

   it("never derives a value hydrated from the server's environment", () => {
      // MOTLY resolves `@env.NAME` against the Publisher process environment.
      // Plain `#` tags used to stay server-side, so that was harmless; deriving
      // and returning them would otherwise let a model author read a worker's
      // credentials back out of the model endpoint.
      process.env.GIVEN_SPEC_SPEC_SECRET = "must-not-leak";
      try {
         expect(
            readGivenControlSpec([`# label=@env.GIVEN_SPEC_SPEC_SECRET`]),
         ).toEqual({});
         // The whole annotation is dropped, so a sibling on the same line goes
         // too. That is the safe direction, and it is deliberate.
         expect(
            readGivenControlSpec([
               `# label="Brand" description=@env.GIVEN_SPEC_SPEC_SECRET`,
            ]),
         ).toEqual({});
      } finally {
         delete process.env.GIVEN_SPEC_SPEC_SECRET;
      }
   });

   it("ignores a tag whose sigil separator Malloy would call malformed", () => {
      // JS `\s` matches a non-breaking space and Malloy's separator class does
      // not, so admitting one here would leave the parser treating
      // `# label="Region"` as the route and reading only what follows, quietly
      // keeping `control` and dropping `label`.
      expect(
         readGivenControlSpec([`#\u00A0label="Region" control=select`]),
      ).toEqual({});
   });

   it("keeps a tag whose quoted value happens to contain a filter literal", () => {
      // The filter-literal workaround rewrites bare `=f'…'` so MOTLY can parse
      // it. Inside a quoted value those same characters are ordinary text, and
      // rewriting them corrupts a line Malloy parses perfectly well, which
      // costs the whole control contract rather than just that one value.
      expect(
         readGivenControlSpec([
            `# label="Single" description="Set when status=f'open'"`,
         ]),
      ).toEqual({
         label: "Single",
         description: "Set when status=f'open'",
      });
   });
});

describe("parse-first rescue", () => {
   // Pinned as an invariant against the parser rather than case by case, because
   // the case-by-case version kept passing for the wrong reason: every specific
   // input got fixed in the rewrite, so the test stopped distinguishing anything.
   // Whatever delimiter form the walk mishandles next, this fails if the rescue
   // fires on a line MOTLY already accepts.
   it("matches the parser exactly on every annotation it already accepts", () => {
      const valid = [
         `# label="Brand" control=select`,
         `# label="""a " x=f'US'""" control=select`,
         `# label='''a ' b''' control=select`,
         `# description="Set when status=f'open'"`,
         `# \`a"b\`=1 control=select`,
         `# \`k with space\`="v" label="L"`,
         `# description="""see \\""" note=f'Y' """`,
         `# label="tail \\\\" control=multiselect`,
         `# artifact { givens { R="US" } } dashboard { columns=12 }`,
         `# range_min=0 range_max=500`,
      ];
      for (const text of valid) {
         const direct = parseAnnotation([text]);
         expect(direct.log.length).toBe(0); // guard: the fixture must be valid
         const viaModule = motlyTag([text]);
         for (const key of ["label", "description", "control"]) {
            expect(tagTextOf(viaModule, key)).toBe(tagTextOf(direct.tag, key));
         }
      }
   });

   it("still rescues the documented filter-literal form", () => {
      expect(
         readStartingGivens(motlyTag([`## givens { REGION=f'US' }`])),
      ).toEqual({ REGION: "US" });
   });
});

describe("quoteFilterLiterals", () => {
   it("quotes a bare filter literal so MOTLY can parse the line", () => {
      expect(quoteFilterLiterals(`# artifact { givens { R=f'US' } }`)).toBe(
         `# artifact { givens { R="f'US'" } }`,
      );
   });

   it("leaves a filter literal inside a quoted value untouched", () => {
      const inString = `# description="Set when status=f'open'"`;
      expect(quoteFilterLiterals(inString)).toBe(inString);
   });

   it("is idempotent, so a second pass cannot double-quote", () => {
      const once = quoteFilterLiterals(`# artifact { givens { R=f'US' } }`);
      expect(quoteFilterLiterals(once)).toBe(once);
   });

   it("escapes a quote inside the literal body", () => {
      expect(
         quoteFilterLiterals(`# artifact { givens { M=f'say "hi"' } }`),
      ).toBe(`# artifact { givens { M="f'say \\"hi\\"'" } }`);
   });

   it("leaves a filter literal inside a triple-quoted value untouched", () => {
      // MOTLY has four string forms, and a triple-quoted one may hold a bare
      // quote. Reading `"""` as an empty string plus a new one puts the walk
      // inside the value, where it would rewrite and destroy the whole tag.
      const triple = `# label="""a " x=f'US'""" control=select`;
      expect(quoteFilterLiterals(triple)).toBe(triple);
      expect(readGivenControlSpec([triple])).toEqual({
         label: `a " x=f'US'`,
         control: "select",
      });
   });

   it("leaves a backtick-quoted key alone while rescuing the real literal", () => {
      // A rescued line is rewritten wherever the walk thinks a value starts, so a
      // delimiter form it does not know about gets corrupted on a line that then
      // parses, and the corruption is kept. Backtick keys and heredoc bodies are
      // two such forms, both taken from the grammar rather than from guesswork.
      expect(quoteFilterLiterals(`# \`status=f'x'flag\`=1 R=f'US'`)).toBe(
         `# \`status=f'x'flag\`=1 R="f'US'"`,
      );
   });

   it("pairs backslashes in a triple-quoted value while rescuing the line", () => {
      // Only reachable on a line the rescue actually runs on, so parse-first does
      // not cover it: the bare `f'X'` triggers the rewrite, and an escaped `\"""`
      // inside the description must not end the region early and let the walk
      // rewrite `note=f'Y'` in the string body. The result would still parse, so
      // the corruption would be kept and shipped as helper text.
      expect(
         quoteFilterLiterals(
            `# label=f'X' description="""see \\""" note=f'Y' """`,
         ),
      ).toBe(`# label="f'X'" description="""see \\""" note=f'Y' """`);
   });

   it("leaves a heredoc body alone while rescuing outside it", () => {
      expect(
         quoteFilterLiterals(`# d=<<<\nUse status=f'open' here\n>>>\nR=f'US'`),
      ).toBe(`# d=<<<\nUse status=f'open' here\n>>>\nR="f'US'"`);
   });

   it("quotes filter literals in array position, not only after =", () => {
      // An array-position literal is just as fatal to the line, so anchoring on
      // `=` alone left `columns` to be lost with it.
      expect(quoteFilterLiterals(`# givens { R=[f'US', f'CA'] }`)).toBe(
         `# givens { R=["f'US'", "f'CA'"] }`,
      );
      expect(
         motlyTag([
            `# artifact { givens { R=[f'US'] } } dashboard { columns=12 }`,
         ])
            ?.tag("dashboard")
            ?.numeric("columns"),
      ).toBe(12);
   });
});

/**
 * `autorun` means the same thing on a notebook's `## autorun=false` and a
 * dashboard's `# artifact { autorun=false }`, so it is read in one place.
 */
describe("readAutorun", () => {
   it("defaults to true, including with no tag at all", () => {
      expect(readAutorun(undefined)).toBe(true);
      expect(readAutorun(motlyTag([`# label="x"`]))).toBe(true);
   });

   it("is false only for an explicit false", () => {
      expect(readAutorun(motlyTag([`## autorun=false`]))).toBe(false);
      expect(readAutorun(motlyTag([`## autorun=true`]))).toBe(true);
   });
});

/**
 * Starting values, read from the same block whether it sits at a notebook's file
 * level or inside a dashboard's artifact tag.
 */
describe("readStartingGivens", () => {
   it("reads a notebook's file-level block", () => {
      expect(
         readStartingGivens(
            motlyTag([`## givens { SINCE="2024-03-01" REGION=f'US' }`]),
         ),
      ).toEqual({ SINCE: "2024-03-01", REGION: "US" });
   });

   it("unwraps a filter literal to the body the query endpoint takes", () => {
      expect(
         readStartingGivens(
            motlyTag([`## givens { REGION=f'us-east, us-west' }`]),
         )?.REGION,
      ).toBe("us-east, us-west");
   });

   it("reads the same block inside an artifact tag", () => {
      const artifact = motlyTag([
         `# artifact { givens { REGION=f'US' } }`,
      ])?.tag("artifact");
      expect(readStartingGivens(artifact)).toEqual({ REGION: "US" });
   });

   it("is undefined with no block, and with an empty one", () => {
      expect(readStartingGivens(undefined)).toBeUndefined();
      expect(
         readStartingGivens(motlyTag([`## autorun=false`])),
      ).toBeUndefined();
      expect(readStartingGivens(motlyTag([`## givens { }`]))).toBeUndefined();
   });
});
