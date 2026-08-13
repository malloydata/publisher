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

   it("treats an empty value as absent, so it cannot name a target either", () => {
      // `text()` returns "" here, which passes an `!== undefined` check. Left
      // alone it defeats the runnable guard below by a different route: an empty
      // `query` is a target as far as `!== undefined` is concerned, so the block
      // ships and tells a client to fetch its options from nothing.
      expect(
         readGivenControlSpec([`# control=select suggest { query="" }`]),
      ).toEqual({ control: "select" });
      expect(
         readGivenControlSpec([
            `# control=select suggest { source="" dimension="" }`,
         ]),
      ).toEqual({ control: "select" });
      // Same reasoning for the scalar fields: an empty label is not a label, and
      // whitespace-only is the same intent typed differently.
      expect(readGivenControlSpec([`# label=""`])).toEqual({});
      expect(readGivenControlSpec([`# label="   "`])).toEqual({});
      expect(readGivenControlSpec([`# description=""`])).toEqual({});
      // The value still has to survive when it says something, or the guard
      // above would pass by deleting the feature.
      expect(readGivenControlSpec([`# label="Region"`])).toEqual({
         label: "Region",
      });
   });

   it("still reads the contract on a line carrying an escaped backtick key", () => {
      // The filter-literal rescue used to stop at the escaped backtick, leaving
      // the rest of the line unscanned and the annotation unparseable, which
      // loses every field rather than the one key: measured `{}` before.
      expect(
         readGivenControlSpec([
            "# label=\"Region\" control=select `a\\`b`=1 R=f'US'",
         ]),
      ).toEqual({ label: "Region", control: "select" });
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

   it("never parses an annotation that could declare __proto__", () => {
      // The tag parser's property bag is a plain object, so a `__proto__`
      // property assigns through Object.prototype. `# __proto__ { a=b }` pollutes
      // and then throws RangeError; `# __proto__=x` pollutes silently with an
      // empty log. Catching the throw is not enough on its own, since one shape
      // does not throw and the other has already done the damage by the time it
      // does, so the guard snapshots Object.prototype and deletes whatever the
      // parse added, which measurably restores both the prototype and later
      // parses.
      //
      // The blast radius is the whole process, not the offending model: once
      // Object.prototype is polluted every later parse throws, so one tenant's
      // tag would take out tag parsing for every package on a shared worker.
      // Watch every object the parser can reach, not just Object.prototype, and
      // diff the names rather than looking for two we happen to expect. Watching
      // one object is what let `constructor { k=v }` write onto the global
      // `Object` unseen, and hardcoding key names is the same error one level
      // down.
      const targets: object[] = [Object.prototype];
      for (const key of Object.getOwnPropertyNames(Object.prototype)) {
         const descriptor = Object.getOwnPropertyDescriptor(
            Object.prototype,
            key,
         );
         const value =
            descriptor && "value" in descriptor ? descriptor.value : undefined;
         if (
            (typeof value === "object" || typeof value === "function") &&
            value !== null
         ) {
            targets.push(value as object);
         }
      }
      const baselines = targets.map((t) => Object.getOwnPropertyNames(t));
      const polluted = () =>
         targets.some((t, i) =>
            Object.getOwnPropertyNames(t).some(
               (k) => !baselines[i].includes(k),
            ),
         );
      // No entry assertion here on purpose: `polluted()` compares the targets
      // against a baseline just taken from those same targets, so asserting it
      // before anything runs is a tautology. The checks inside the loop are the
      // ones that mean something.

      for (const hostile of [
         `# __proto__ { a=b }`,
         `# label="ok" __proto__=x`,
         `# label="ok" givens { __proto__=x }`,
         `# label="ok" suggest { __proto__=q }`,
         `# artifact { __proto__ { a=b } }`,
         `# label="Region" __proto__=x`,
         // These spell the same property with NO literal `__proto__` anywhere in
         // the text, because MOTLY decodes escapes inside a backtick-quoted
         // identifier. The first version of this guard was a substring denylist
         // and these defeated it, which is why the guard now observes the effect
         // on Object.prototype rather than trying to recognise the input.
         "# `__prot\\o__` { a=b }",
         '# label="ok" `__prot\\o__`=x',
         "# suggest { `__prot\\o__` { a=b } }",
         "# `\\u005f\\u005fproto__` { a=b }",
         // The prototype CHAIN, not just Object.prototype: `constructor` resolves
         // to the global `Object` and `toString` to the built-in method object,
         // and writes there accumulate for the life of the process.
         // EVERY fixture carries a real control key on purpose. `# toString=x` alone
         // yields `{}` whether or not the guard runs, so asserting `{}` on it
         // would prove nothing; with a `label` present, the guard is the only
         // reason the result is empty rather than `{label: "ok"}`.
         `# label="ok" constructor { tenant_key="payload" }`,
         `# label="ok" toString=x`,
         `# label="ok" valueOf { a=b }`,
         `# label="ok" hasOwnProperty=y`,
      ]) {
         expect(() => readGivenControlSpec([hostile])).not.toThrow();
         expect(readGivenControlSpec([hostile])).toEqual({});
         expect(polluted()).toBe(false);
      }

      // A target that appears AFTER module load must be watched too. A cached
      // target list goes stale the moment anything extends Object.prototype, and
      // a library that does so would create an unwatched write target.
      Object.defineProperty(Object.prototype, "zzLateTarget", {
         value: { marker: true },
         configurable: true,
         enumerable: false,
         writable: true,
      });
      try {
         const late = (Object.prototype as unknown as Record<string, object>)
            .zzLateTarget;
         const lateBefore = Object.getOwnPropertyNames(late);
         expect(readGivenControlSpec([`# zzLateTarget { pwned=yes }`])).toEqual(
            {},
         );
         expect(
            Object.getOwnPropertyNames(late).filter(
               (k) => !lateBefore.includes(k),
            ),
         ).toEqual([]);
      } finally {
         delete (Object.prototype as unknown as Record<string, unknown>)
            .zzLateTarget;
      }

      // ...and the same target defined as an ACCESSOR rather than a data
      // property. Reading descriptors and skipping the accessor branch left a
      // getter-returned object unwatched, and the guard reported it clean.
      // `__proto__` is itself an accessor on Object.prototype, so this shape is
      // not exotic.
      const shared: Record<string, unknown> = { legit: 1 };
      Object.defineProperty(Object.prototype, "zzAccessorTarget", {
         get: () => shared,
         configurable: true,
         enumerable: false,
      });
      try {
         const sharedBefore = Object.getOwnPropertyNames(shared);
         expect(
            readGivenControlSpec([`# label="ok" zzAccessorTarget { k="v" }`]),
         ).toEqual({});
         expect(
            Object.getOwnPropertyNames(shared).filter(
               (k) => !sharedBefore.includes(k),
            ),
         ).toEqual([]);
      } finally {
         delete (Object.prototype as unknown as Record<string, unknown>)
            .zzAccessorTarget;
      }

      // An accessor that cannot be probed safely, or that hands back a different
      // object each read, is not watchable at all. Both shapes leaked on the
      // version that simply called the getter once: a throwing getter was dropped
      // from the watch set and its object then took the write, and an unstable one
      // left the parser writing into an object the guard never saw. Refusing is
      // the only honest answer, so both must come back empty.
      for (const [name, get] of [
         [
            "zzThrows",
            () => {
               throw new Error("unprobeable");
            },
         ],
         ["zzUnstable", () => ({ fresh: true })],
      ] as Array<[string, () => unknown]>) {
         Object.defineProperty(Object.prototype, name, {
            get,
            configurable: true,
            enumerable: false,
         });
         try {
            expect(
               readGivenControlSpec([`# label="ok" ${name} { k="v" }`]),
            ).toEqual({});
         } finally {
            delete (Object.prototype as unknown as Record<string, unknown>)[
               name
            ];
         }
      }

      // `__proto__` is skipped rather than refused, because it is always an
      // accessor and refusing it would refuse everything. That is only sound for
      // the engine's own getter: redefined to return some other object, it let a
      // write land there with the annotation reported clean.
      const original = Object.getOwnPropertyDescriptor(
         Object.prototype,
         "__proto__",
      );
      const decoy: Record<string, unknown> = { legit: 1 };
      Object.defineProperty(Object.prototype, "__proto__", {
         get: () => decoy,
         set: () => {},
         configurable: true,
      });
      try {
         const decoyBefore = Object.getOwnPropertyNames(decoy);
         expect(
            readGivenControlSpec([`# label="ok" __proto__ { k="v" }`]),
         ).toEqual({});
         expect(
            Object.getOwnPropertyNames(decoy).filter(
               (k) => !decoyBefore.includes(k),
            ),
         ).toEqual([]);
      } finally {
         if (original) {
            Object.defineProperty(Object.prototype, "__proto__", original);
         }
      }

      // Still parsing normally afterwards, which is what pollution would break.
      expect(readGivenControlSpec([`# label="ok"`])).toEqual({ label: "ok" });
      expect(
         readStartingGivens(motlyTag([`## givens { __proto__=x }`])),
      ).toBeUndefined();
      expect(polluted()).toBe(false);
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

   it("pairs backslashes inside a quoted identifier, like the quote branches", () => {
      // MOTLY decodes escapes inside backticks, so the escaped backtick here is
      // part of the key and not its close. Scanning with a bare `indexOf` ended
      // the identifier early and left the rest of the line unscanned, so the
      // filter literal after it was never quoted and the annotation stayed
      // unparseable, which costs the WHOLE contract, not one key (below).
      expect(quoteFilterLiterals("# `a\\`b`=1 R=f'US'")).toBe(
         "# `a\\`b`=1 R=\"f'US'\"",
      );
      // An ordinary identifier was never affected; pinned so a regression here
      // is distinguishable from one in the escape handling.
      expect(quoteFilterLiterals("# `plain`=1 R=f'US'")).toBe(
         "# `plain`=1 R=\"f'US'\"",
      );
   });

   it("ends a heredoc only where MOTLY does, at a line break it recognizes", () => {
      // MOTLY splits heredoc lines on \n alone and ends the region at the first
      // line whose trim() is `>>>`. A JS /m regex disagrees: its anchors also
      // break at \r, U+2028 and U+2029, so it finds a terminator MOTLY does not
      // and hands the caller a "heredoc" that ends early. The rescue then
      // rewrites inside what MOTLY still treats as body.
      //
      // Each case below puts a decoy `>>>` after one of those three characters,
      // with a bare filter literal behind it. The real terminator is the `>>>`
      // alone on its own line further down, so the whole annotation must come
      // back untouched.
      for (const sep of ["\r", " ", " "]) {
         const input = `# a=<<<\nbody${sep}>>>${sep} x=f'INJECTED'\nreal\n>>>\n label="keep"`;
         expect(quoteFilterLiterals(input)).toBe(input);
      }
      // A terminator MOTLY DOES recognize still ends the region, so the fix
      // cannot pass by never ending a heredoc at all.
      expect(quoteFilterLiterals(`# a=<<<\nbody\n>>>\n R=f'US'`)).toBe(
         `# a=<<<\nbody\n>>>\n R="f'US'"`,
      );
   });

   it("treats an escaped newline as part of the identifier, not as unterminated", () => {
      // The grammar says a quoted identifier cannot span a newline; the parser
      // disagrees, decoding `\<newline>` into a literal one, so `# `a\<nl>`=1`
      // is the single key "a\n". The old scan saw a newline before the closing
      // backtick, called the identifier unterminated, and left the rest of the
      // line unscanned, so the filter literal was never quoted: measured, it
      // came back byte-identical and unparseable.
      expect(quoteFilterLiterals("# `a\\\n`=1 R=f'US'")).toBe(
         "# `a\\\n`=1 R=\"f'US'\"",
      );
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
