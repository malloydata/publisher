// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { routeOf } from "@malloydata/malloy";
import { afterEach, describe, expect, it } from "bun:test";
import { createRequire } from "module";
import { BadRequestError } from "../errors";
import {
   AUTHORIZE_TAG_LIKE,
   assertNoAuthorizeTagLike,
   assertNoCallerAuthorizeAnnotation,
   hasCallerAuthorizeAnnotation,
   lastCallerGuardRefusalWasWholeText,
   malformedAuthorizeAttemptPattern,
   setMalloyParserLoaderForTest,
} from "./authorize";
import {
   canonicalAuthorizeRoute,
   RECOGNIZED_AUTHORIZE_SPELLINGS,
} from "./authorize_routes";

/** The guard as it was before it read the lexer: the pattern over the whole text. */
const wholeTextRegex = (text: string) =>
   new RegExp(AUTHORIZE_TAG_LIKE, "iu").test(text);

const REFUSAL = /not permitted in caller-submitted Malloy text/;

/** Both exported forms must give one answer, since the five call sites use one or the other. */
function refuses(text: string): boolean {
   const predicate = hasCallerAuthorizeAnnotation(text);
   let thrown: unknown;
   try {
      assertNoCallerAuthorizeAnnotation(text);
   } catch (error) {
      thrown = error;
   }
   expect(thrown !== undefined).toBe(predicate);
   if (thrown !== undefined) {
      expect(thrown).toBeInstanceOf(BadRequestError);
      expect((thrown as Error).message).toMatch(REFUSAL);
   }
   return predicate;
}

const BRACKETS = [
   ["(", ")"],
   ["[", "]"],
   ["<", ">"],
   ["{", "}"],
] as const;

/** Every gate spelling the old spec pinned: sigil, block, bracket and case. */
const EVERY_SPELLING: string[] = RECOGNIZED_AUTHORIZE_SPELLINGS.flatMap(
   (route) =>
      ["#", "##"].flatMap((sigil) =>
         ["", "|"].flatMap((block) =>
            BRACKETS.flatMap(([open, close]) =>
               [
                  route,
                  route.toUpperCase(),
                  route[0].toUpperCase() + route.slice(1),
               ].map((name) => `${sigil}${block}${open}${name}${close}`),
            ),
         ),
      ),
);

const NEAR_MISSES = [
   "# (authorize)",
   "#( authorize )",
   "#(authorize )",
   "#authorize",
   "#(row_authorize)",
   "#(source-authorize)",
   "#(accessfilter)",
   "#(access-filter)",
   "#(AUTHORIZE)",
   "##|(authorize)",
];

const RUN = "run: orders -> { aggregate: c }";

describe("hasCallerAuthorizeAnnotation: markdown and text block bodies are prose", () => {
   const accepted: [string, string][] = [
      [
         "an attached #|(markdown) block",
         `#|(markdown)\nThe base is gated with #(authorize) and #(access_filter).\n|#\n${RUN}\n`,
      ],
      [
         "a floating ##|(markdown) block, closed by |##",
         `##|(markdown)\nMentions #(authorize) true on its own line.\n#(access_filter) org_id in $G\n|##\n${RUN}\n`,
      ],
      [
         "a #|(text) block",
         `#|(text)\n#(authorize) is how a source is locked.\n|#\n${RUN}\n`,
      ],
      [
         "a body line with |# at a column other than the opener's",
         `#|(markdown)\n  |# not a closer #(authorize)\n|#\n${RUN}\n`,
      ],
      [
         "an indented opener whose body has |# at column 0",
         `  #|(markdown)\n|# #(authorize) still prose\n  |#\n${RUN}\n`,
      ],
      [
         "a ##| block whose body has a |# line, which closes only #|",
         `##|(markdown)\n|# #(authorize) still prose\n|##\n${RUN}\n`,
      ],
      [
         "CRLF line endings",
         `#|(markdown)\r\nSee #(authorize).\r\n|#\r\n${RUN}\r\n`,
      ],
      [
         "unicode around the mention",
         `#|(markdown)\n🔒 é 中文 #(authorize)\u200b — gated\n|#\n${RUN}\n`,
      ],
      [
         "a notebook file whose prose mentions both tags",
         `##! experimental.givens\n## artifact { kind=notebook }\n\n##|(markdown)\nRows are limited by #(access_filter); the source is locked by #(authorize).\n|##\n\n${RUN}\n`,
      ],
   ];
   for (const [name, text] of accepted) {
      it(`accepts ${name}`, () => {
         expect(wholeTextRegex(text)).toBe(true);
         expect(refuses(text)).toBe(false);
      });
   }

   it("accepts every gate spelling and near miss inside a markdown body", () => {
      for (const tag of [...EVERY_SPELLING, ...NEAR_MISSES]) {
         const text = `#|(markdown)\nprose ${tag} 'x' in $G\n${tag} true\n|#\n${RUN}\n`;
         expect(wholeTextRegex(text)).toBe(true);
         expect(refuses(text)).toBe(false);
      }
   });

   it("accepts every gate spelling and near miss in the rest of a markdown or text line note", () => {
      for (const tag of [...EVERY_SPELLING, ...NEAR_MISSES]) {
         for (const text of [
            `#(markdown) a line note names ${tag}\n${RUN}\n`,
            `##(markdown) Rows are limited by ${tag} 'x' in $G.\n${RUN}\n`,
            `##(text) ${tag}\n${RUN}\n`,
         ]) {
            if (!wholeTextRegex(text)) continue;
            expect(refuses(text)).toBe(false);
         }
      }
   });

   it("accepts the one-line markdown cell the notebook writer emits", () => {
      expect(
         refuses(
            `## artifact { kind=notebook }\n\n##(markdown) Rows are limited by #(access_filter).\n\n${RUN}\n`,
         ),
      ).toBe(false);
   });
});

describe("hasCallerAuthorizeAnnotation: everything outside a prose body is refused as before", () => {
   it("refuses every gate spelling and near miss, bare and in every other position", () => {
      for (const tag of [...EVERY_SPELLING, ...NEAR_MISSES]) {
         for (const text of [
            `${tag} 'x' in $G\nsource: mine is locked extend {}\n`,
            `#(Markdown) routed elsewhere: ${tag}\n${RUN}\n`,
            `##(markdown)${tag} x\n${RUN}\n`,
            `#(doc) a doc line note: ${tag}\n${RUN}\n`,
            `#|(markdown) on the opener line ${tag}\nbody\n|#\n${RUN}\n`,
            `#|(markdown)\nbody\n|# ${tag}\n${RUN}\n`,
            `#|(markdown)\nbody\n|#\n${tag} true\nsource: mine is locked extend {}\n`,
            `#|(doc)\n${tag}\n|#\n${RUN}\n`,
            `#| (markdown)\n${tag}\n|#\n${RUN}\n`,
            `/* ${tag} */\n${RUN}\n`,
            `// ${tag}\n${RUN}\n`,
            `run: orders -> { where: name = '${tag}' }\n`,
         ]) {
            if (!wholeTextRegex(text)) continue;
            expect(refuses(text)).toBe(true);
         }
      }
   });

   it("refuses a line note whose prefix runs into the tag, and a mixed-case route", () => {
      expect(
         refuses(
            "##(markdown)#(authorize) x\nsource: mine is locked extend {}\n",
         ),
      ).toBe(true);
      expect(refuses("##(Markdown) see #(authorize)\n" + RUN + "\n")).toBe(
         true,
      );
   });

   it("refuses a real #(authorize) and a real #|(authorize) block", () => {
      expect(
         refuses("#(authorize) true\nsource: mine is locked extend {}\n"),
      ).toBe(true);
      expect(
         refuses("#|(authorize)\ntrue\n|#\nsource: mine is locked extend {}\n"),
      ).toBe(true);
   });

   it("refuses a gate after a #| block closed early at the opener's column", () => {
      expect(
         refuses(
            "#|(markdown)\nprose\n|#\n#(authorize) true\nsource: mine is locked extend {}\n|#\n",
         ),
      ).toBe(true);
      expect(
         refuses(
            "  #|(markdown)\nprose\n  |#\n#(authorize) true\nsource: mine is locked extend {}\n",
         ),
      ).toBe(true);
   });

   it("falls back to the whole-text pattern when the text cannot be lexed cleanly", () => {
      // Unclosed block: the lexer reads the rest as body without complaint.
      expect(refuses("#|(markdown)\n#(authorize) true\n")).toBe(true);
      // A ##| block is closed by |##, so a lone |# leaves it open.
      expect(refuses("##|(markdown)\n#(authorize) true\n|#\n")).toBe(true);
      // A syntax error anywhere.
      expect(
         refuses(`#|(markdown)\n#(authorize)\n|#\nrun: orders -> {\n`),
      ).toBe(true);
      expect(
         refuses(`#|(markdown)\n#(authorize)\n|#\nrun: orders\n'open\n`),
      ).toBe(true);
   });

   it("falls back on a lexer error that drops characters inside a block, and prints nothing", () => {
      const printed: unknown[] = [];
      const original = console.error;
      console.error = (...args: unknown[]) => void printed.push(args);
      try {
         // A lone CR inside a block is a token-recognition error: the lexer skips it and its neighbours.
         expect(
            refuses(`#|(markdown)\nx\r#(authorize) true\n|#\n${RUN}\n`),
         ).toBe(true);
         expect(lastCallerGuardRefusalWasWholeText()).toBe(true);
         expect(refuses(`run: orders """#(authorize)`)).toBe(true);
      } finally {
         console.error = original;
      }
      expect(printed).toEqual([]);
   });

   it("says whether a refusal came from the lexer or the whole-text fallback", () => {
      expect(refuses(`#(authorize) true\n${RUN}\n`)).toBe(true);
      expect(lastCallerGuardRefusalWasWholeText()).toBe(false);
      expect(refuses("#|(markdown)\n#(authorize) true\n")).toBe(true);
      expect(lastCallerGuardRefusalWasWholeText()).toBe(true);
   });

   describe("when the Malloy parser cannot be loaded", () => {
      afterEach(() => setMalloyParserLoaderForTest());

      it("warns once, without caller text, and still refuses", () => {
         setMalloyParserLoaderForTest(() => null);
         const warned: unknown[][] = [];
         const original = console.warn;
         console.warn = (...args: unknown[]) => void warned.push(args);
         try {
            expect(refuses("#|(markdown)\n#(authorize) SECRET\n")).toBe(true);
            expect(refuses(`#(authorize) true\n${RUN}\n`)).toBe(true);
            expect(refuses(`${RUN}\n`)).toBe(false);
         } finally {
            console.warn = original;
         }
         expect(warned).toHaveLength(1);
         expect(String(warned[0][0])).toContain("guard disabled");
         expect(String(warned[0][0])).not.toContain("SECRET");
         expect(lastCallerGuardRefusalWasWholeText()).toBe(true);
      });
   });

   it("keeps the whole-text match for names, which are never lexed on their own", () => {
      const name = "#|(markdown)\n#(authorize) true\n|#";
      expect(refuses(name)).toBe(false);
      expect(() => assertNoAuthorizeTagLike(name)).toThrow(REFUSAL);
      expect(() => assertNoAuthorizeTagLike("orders")).not.toThrow();
   });

   it("stays linear on a long prose body full of hits", () => {
      const prose = (lines: number) =>
         "#|(markdown)\n" +
         "#(authorize) x\n".repeat(lines) +
         "|#\nrun: orders -> { aggregate: c }";
      // Three asks, as the query path makes; the lex is shared, the scan is linear.
      let fresh = 0;
      // A distinct text per measurement, so the kept lex never answers for it.
      const time = (base: string) => {
         const text = `${base}\n// ${fresh++}`;
         const started = performance.now();
         for (let site = 0; site < 3; site++)
            expect(hasCallerAuthorizeAnnotation(text)).toBe(false);
         return performance.now() - started;
      };
      time(prose(1000));
      const single = Math.min(time(prose(32500)), time(prose(32500)));
      const double = Math.min(time(prose(65000)), time(prose(65000)));
      console.log(
         `caller-annotation guard, three asks: ${Math.round(single)}ms at ~490KB, ${Math.round(double)}ms at ~975KB`,
      );
      // Quadratic doubles to 4x; linear stays near 2x, so 3x leaves room for noise.
      expect(double).toBeLessThan(3 * single + 50);
      expect(
         hasCallerAuthorizeAnnotation(`${prose(65000)}\n#(authorize) y\n`),
      ).toBe(true);
   }, 120_000);

   it("matches tag-like text in linear time, without backtracking over whitespace", () => {
      const tabs = "#" + "\t".repeat(64000);
      let started = performance.now();
      expect(hasCallerAuthorizeAnnotation(tabs)).toBe(false);
      const tagLike = performance.now() - started;
      started = performance.now();
      expect(malformedAuthorizeAttemptPattern("authorize").test(tabs)).toBe(
         false,
      );
      const malformed = performance.now() - started;
      console.log(
         `64000 tabs: tag-like ${tagLike.toFixed(1)}ms, malformed-attempt ${malformed.toFixed(1)}ms`,
      );
      expect(tagLike).toBeLessThan(100);
      expect(malformed).toBeLessThan(100);
   });

   it("keeps the language of both patterns: every spelling and near miss matches where it did", () => {
      // The patterns as they were before the backtracking fix.
      const OLD_TAG_LIKE = String.raw`##?\|?[ \t]*[([{<]?[ \t]*(?:(?:(?:row|source)[-_]?)?authorize|access[-_]?filter)(?=[)\]}>]|[ \t]|$)`;
      const oldMalformed = (words: string) =>
         new RegExp(`^##?\\|?[ \\t]*[([{<]?[ \\t]*(?:${words})`, "iu");
      const newMalformed = malformedAuthorizeAttemptPattern("authorize");
      const WORDS = newMalformed.source.slice(
         newMalformed.source.lastIndexOf("(?:") + 3,
         -1,
      );
      const samples = [...EVERY_SPELLING, ...NEAR_MISSES].flatMap((tag) => [
         tag,
         ` ${tag}`,
         tag.replace(/^(##?\|?)/, "$1 \t "),
         tag.replace(/([([{<])/, "$1\t "),
         `${tag}x`,
         `${tag})`,
         `prose ${tag} true`,
         tag.replace(/[)\]}>]$/, ""),
      ]);
      const sticky = (source: string) => new RegExp(source, "iuy");
      for (const text of samples) {
         for (let at = 0; at < text.length; at++) {
            const a = sticky(OLD_TAG_LIKE);
            const b = sticky(AUTHORIZE_TAG_LIKE);
            a.lastIndex = b.lastIndex = at;
            expect(b.exec(text)?.[0]).toBe(a.exec(text)?.[0]);
         }
         expect(newMalformed.test(text)).toBe(oldMalformed(WORDS).test(text));
      }
   });

   it("leaves text with no tag-like hit alone", () => {
      expect(refuses(`${RUN}\n`)).toBe(false);
      expect(refuses("#(authorize-v2) x = 1")).toBe(false);
      expect(refuses("#|(markdown)\nnever closed\n")).toBe(false);
   });
});

// ---------------------------------------------------------------------------
// Differential corpus: the old whole-text pattern against the new predicate.
// The new one may accept a text the old refused only when EVERY tag-like hit
// lies inside a markdown/text block body, which a line-based reference works
// out from MalloyLexer.g4's block rules rather than by asking the guard.
// ---------------------------------------------------------------------------

function mulberry32(seed: number): () => number {
   let a = seed >>> 0;
   return () => {
      a = (a + 0x6d2b79f5) >>> 0;
      let t = a;
      t = Math.imul(t ^ (t >>> 15), t | 1);
      t ^= t + Math.imul(t ^ (t >>> 7), t | 61);
      return ((t ^ (t >>> 14)) >>> 0) / 4294967296;
   };
}

function corpusGenerator(seed: number): () => string[] {
   const rand = mulberry32(seed);
   const pick = <T>(items: readonly T[]): T =>
      items[Math.floor(rand() * items.length)];
   const chance = (p: number) => rand() < p;

   const NAMES = [
      "authorize",
      "access_filter",
      "access-filter",
      "accessfilter",
      "row_authorize",
      "source-authorize",
      "AUTHORIZE",
      "Access_Filter",
      "authorize-v2",
      "authorized",
   ];
   const hit = () => {
      const [open, close] = chance(0.8) ? pick(BRACKETS) : ["", ""];
      return (
         pick(["#", "##"]) +
         pick(["", "", "|"]) +
         pick(["", "", " ", "\t"]) +
         open +
         pick(["", "", " "]) +
         pick(NAMES) +
         close +
         pick(["", " true", ")", " 'x' in $G", "\u200b"])
      );
   };
   const WORDS = [
      "the",
      "base",
      "gated",
      "rows",
      "é",
      "🔒",
      "中文",
      "á",
      "—",
      "$G",
      "*/",
      "|",
      "#",
   ];
   const filler = () =>
      Array.from({ length: 1 + Math.floor(rand() * 4) }, () =>
         pick(WORDS),
      ).join(" ");
   const indent = (col: number) =>
      Array.from({ length: col }, () => pick([" ", "\t"])).join("");

   const block = (last: boolean): string[] => {
      const col = pick([0, 0, 0, 1, 2, 3]);
      const lead = indent(col);
      const doc = chance(0.4);
      const closer = doc ? "|##" : "|#";
      const lines = [
         lead +
            (doc ? "##|" : "#|") +
            pick([
               "(markdown)",
               "(markdown)",
               "(text)",
               "(markdown) intro",
               `(markdown) ${hit()}`,
               "(doc)",
               " (markdown)",
               "[markdown]",
               "(authorize)",
               "(access_filter)",
            ]),
      ];
      for (let n = Math.floor(rand() * 5); n > 0; n--) {
         lines.push(
            pick([
               () => filler(),
               () => `${filler()} ${hit()} ${filler()}`,
               () => hit(),
               () => `${pick([" ", "\t", "  "])}${hit()} true`,
               // A closer at another column: still body.
               () =>
                  `${indent(col + 1 + Math.floor(rand() * 2))}${closer} ${hit()}`,
               // An early closer at the opener's column, so what follows is code.
               () => `${lead}${closer}${pick(["", " ", ` ${hit()}`])}`,
               // The other sigil's closer: `|##` does not close `#|`, `|#` does not close `##|`.
               () => `${lead}${doc ? "|#" : "|##"} ${hit()}`,
               // An opener: text inside a block, a real one after an early close.
               () => `${lead}${pick(["#|(markdown)", "##|(markdown)"])}`,
               () => `${filler()} #|(markdown)`,
            ])(),
         );
      }
      if (!last || chance(0.9))
         lines.push(`${lead}${closer}${pick(["", "", " ", ` ${hit()}`])}`);
      return lines;
   };

   const statement = (): string[] =>
      pick([
         () => ["run: orders -> { aggregate: c }"],
         () => ["source: s is orders extend { dimension: d is 1 }"],
         () => [`run: orders -> { where: name = '${hit()}' }`],
         () => [`#(markdown) ${filler()} ${hit()}`],
         () => [hit()],
         () => [`// ${hit()}`],
         () => [`-- ${hit()}`],
         () => [`/* ${hit()} */ run: orders -> { aggregate: c }`],
         () => ["/*", hit(), "*/"],
         () => ['source: q is duckdb.sql("""', `select '${hit()}'`, '""")'],
      ])();

   return () => {
      const lines: string[] = [];
      const segments = 1 + Math.floor(rand() * 5);
      for (let s = 0; s < segments; s++)
         lines.push(
            ...(chance(0.55) ? block(s === segments - 1) : statement()),
         );
      const eol = pick(["\n", "\n", "\r\n"]);
      return lines.map((line) => line + eol);
   };
}

/**
 * The lines (with their endings) a prose block's body covers, read line by line
 * the way MalloyLexer.g4 reads block notes. Sound rather than complete: when it
 * meets something it does not model it stops calling anything prose.
 */
function referenceProse(lines: string[]): [number, number][] {
   const prose: [number, number][] = [];
   let block:
      | { col: number; closer: string; prose: boolean; lines: number[] }
      | undefined;
   let pending: string | undefined;
   let offset = 0;
   const ends = lines.map((line) => (offset += line.length));
   for (let n = 0; n < lines.length; n++) {
      const cps = [...lines[n].replace(/\r?\n$/, "")];
      if (block) {
         const lead = cps.slice(0, block.col);
         const rest = cps.slice(block.col).join("");
         const closes =
            lead.length === block.col &&
            lead.every((c) => c === " " || c === "\t") &&
            rest.startsWith(block.closer) &&
            !(block.closer === "|#" && rest[2] === "#");
         if (closes) {
            if (block.prose)
               for (const at of block.lines)
                  prose.push([at === 0 ? 0 : ends[at - 1], ends[at]]);
            block = undefined;
         } else {
            block.lines.push(n);
         }
         continue;
      }
      // Default mode: find what the first token-opening character on the line starts.
      const line = cps.join("");
      let i = 0;
      if (pending) {
         const close = line.indexOf(pending);
         if (close === -1) continue;
         i = close + pending.length;
         pending = undefined;
      }
      while (i < line.length) {
         const c = line[i];
         if (c === "#") {
            const sigil = line.startsWith("##|", i)
               ? "##|"
               : line.startsWith("#|", i)
                 ? "#|"
                 : undefined;
            if (!sigil) {
               // A line note runs to the end of the line; its payload is prose when routed exactly to markdown or text.
               const lineRoute = routeOf({
                  value: line.slice(i),
               } as Parameters<typeof routeOf>[0]);
               if (lineRoute === "markdown" || lineRoute === "text")
                  prose.push([(n === 0 ? 0 : ends[n - 1]) + i + 1, ends[n]]);
               break;
            }
            const route = routeOf({
               value: line.slice(i),
            } as Parameters<typeof routeOf>[0]);
            block = {
               col: [...line.slice(0, i)].length,
               closer: sigil === "##|" ? "|##" : "|#",
               prose: route === "markdown" || route === "text",
               lines: [],
            };
            break;
         }
         if (line.startsWith("//", i) || line.startsWith("--", i)) break;
         // A SQL string and a block comment both run across lines to their closer.
         const multiline = line.startsWith('"""', i)
            ? ['"""', '"""']
            : line.startsWith("/*", i)
              ? ["/*", "*/"]
              : undefined;
         if (multiline) {
            const [open, close] = multiline;
            const at = line.indexOf(close, i + open.length);
            if (at === -1) {
               pending = close;
               break;
            }
            i = at + close.length;
            continue;
         }
         if (c === "'" || c === '"' || c === "`") {
            const close = line.indexOf(c, i + 1);
            if (close === -1) return prose;
            i = close + 1;
            continue;
         }
         i++;
      }
   }
   // An unclosed block is never prose: the guard must fall back for it.
   return prose;
}

// The compiler's own reading, as a gate oracle: parse the text and route every
// annotation the parser recognised. Internal modules, test-only.
const malloyInternal = createRequire(
   createRequire(import.meta.url).resolve("@malloydata/malloy"),
);
const { makeMalloyParser } = malloyInternal("./lang/run-malloy-parser");
const { getAnnotationText } = malloyInternal("./lang/parse-utils");
const { AnnotationContext, DocAnnotationContext } = malloyInternal(
   "./lang/lib/Malloy/MalloyParser",
);

interface ParseNode {
   start?: { startIndex: number };
   childCount?: number;
   getChild(index: number): ParseNode;
}

/** UTF-16 offsets of every note the parser routes to a gate, or `undefined` when the text does not parse. */
function gateNotesAt(text: string): number[] | undefined {
   let errors = 0;
   const listener = { syntaxError: () => void errors++ };
   const { parser } = makeMalloyParser(text, {
      lexerErrorListener: listener,
      parserErrorListener: listener,
   });
   const root = parser.malloyDocument() as ParseNode;
   if (errors > 0) return undefined;
   const utf16: number[] = [];
   for (let i = 0; i < text.length; ) {
      utf16.push(i);
      i += (text.codePointAt(i) as number) > 0xffff ? 2 : 1;
   }
   const gates: number[] = [];
   const visit = (node: ParseNode) => {
      if (
         node instanceof AnnotationContext ||
         node instanceof DocAnnotationContext
      ) {
         const route = routeOf({
            value: (getAnnotationText(node) as string).trimStart(),
         } as Parameters<typeof routeOf>[0]);
         if (canonicalAuthorizeRoute(route) !== undefined)
            gates.push(utf16[node.start?.startIndex ?? 0]);
      }
      for (let k = 0; k < (node.childCount ?? 0); k++) visit(node.getChild(k));
   };
   visit(root);
   return gates;
}

/** Every position where the pattern matches, so overlapping hits each count. */
function hitsOf(text: string): [number, number][] {
   const sticky = new RegExp(AUTHORIZE_TAG_LIKE, "iuy");
   const hits: [number, number][] = [];
   for (let i = text.indexOf("#"); i !== -1; i = text.indexOf("#", i + 1)) {
      sticky.lastIndex = i;
      const match = sticky.exec(text);
      if (match) hits.push([i, i + match[0].length]);
   }
   return hits;
}

describe("hasCallerAuthorizeAnnotation: differential corpus against the whole-text pattern", () => {
   it("accepts a text the old guard refused only when every hit lies in a prose block body", () => {
      const CORPUS = 20000;
      const next = corpusGenerator(0x5eed);
      let oldRefused = 0;
      let newlyAccepted = 0;
      const violations: string[] = [];
      for (let n = 0; n < CORPUS; n++) {
         const lines = next();
         const text = lines.join("");
         const before = wholeTextRegex(text);
         const after = hasCallerAuthorizeAnnotation(text);
         if (after && !before)
            violations.push(
               `new refuses, old accepts: ${JSON.stringify(text)}`,
            );
         if (!before) continue;
         oldRefused++;
         if (after) continue;
         newlyAccepted++;
         const gates = gateNotesAt(text);
         if (gates === undefined || gates.length > 0)
            violations.push(`gate oracle: ${JSON.stringify(text)}`);
         const prose = referenceProse(lines);
         const outside = hitsOf(text).filter(
            ([start, end]) =>
               !prose.some(([from, to]) => from <= start && end <= to),
         );
         if (outside.length > 0) violations.push(JSON.stringify(text));
      }
      // The same corpus split into a preceding model and an appended caller text.
      const split = mulberry32(0xc0ffee);
      let splitAccepted = 0;
      for (let n = 0; n < CORPUS / 4; n++) {
         const lines = next();
         const k = 1 + Math.floor(split() * lines.length);
         const preceding = lines.slice(0, k).join("");
         const caller = lines.slice(k).join("");
         const before = wholeTextRegex(caller);
         const after = hasCallerAuthorizeAnnotation(caller, preceding);
         if (after && !before)
            violations.push(`split: ${JSON.stringify([preceding, caller])}`);
         if (!before || after) continue;
         splitAccepted++;
         const callerLine =
            preceding.lastIndexOf("\n", preceding.length - 1) + 1;
         const gates = gateNotesAt(preceding + caller);
         if (gates === undefined || gates.some((at) => at >= callerLine))
            violations.push(
               `split gate oracle: ${JSON.stringify([preceding, caller])}`,
            );
         const prose = referenceProse(lines);
         const outside = hitsOf(preceding + caller).filter(
            ([start, end]) =>
               end > preceding.length &&
               !prose.some(([from, to]) => from <= start && end <= to),
         );
         if (outside.length > 0)
            violations.push(`split: ${JSON.stringify([preceding, caller])}`);
      }
      expect(splitAccepted).toBeGreaterThan(0);
      // Quoted in the report; a corpus that never exercises the exemption proves nothing.
      console.log(
         `caller-annotation corpus: ${CORPUS} texts, ${oldRefused} refused by the whole-text pattern, ${newlyAccepted} of those now accepted; split pass ${CORPUS / 4} texts, ${splitAccepted} accepted`,
      );
      expect(violations.slice(0, 5)).toEqual([]);
      expect(newlyAccepted).toBeGreaterThan(CORPUS / 50);
      // The oracle must see gates, or a clean pass says nothing.
      expect(
         gateNotesAt("#(authorize) true\nsource: s is t extend {}\n"),
      ).toEqual([0]);
      expect(oldRefused - newlyAccepted).toBeGreaterThan(CORPUS / 10);
   }, 60_000);
});
