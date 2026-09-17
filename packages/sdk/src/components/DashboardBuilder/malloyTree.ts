// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * Where everything in a dashboard file IS, according to Malloy's own parser.
 *
 * The only module that touches the parse tree. The reader and the writer both
 * locate through it, which is what makes a reader/writer disagreement about
 * where a declaration starts or ends impossible rather than unlikely — the
 * defect class that every hand-written scan here kept reintroducing.
 *
 * The rule the rest of the builder follows: LOCATORS come from this module,
 * RECOGNIZERS may read text over a span this module bounded, and EMITTERS take
 * positions only from a span or a token here. Nothing outside re-derives
 * Malloy's grammar from raw text.
 *
 * Node identity is by ACCESSOR, never `constructor.name`: `@malloydata/malloy`
 * is a peerDependency, so the host bundler minifies it and every class name
 * becomes a single letter. Accessor and token names are properties, which
 * minifiers leave alone.
 */

/** A half-open range of UTF-16 offsets into the source text. */
export interface Span {
   start: number;
   end: number;
}

/** Anything the writer has to find again: where it is, and on which line. */
export interface Positioned {
   span: Span;
   /** 0-based, to index the `lines` array the tag scan still works in. */
   line: number;
}

/** A `field <op> $GIVEN` comparison: the only filter shape the builder owns. */
export interface TreeBinding {
   field: string;
   op: string;
   given: string;
}

/** One entry of a `where:` clause list, whether or not the builder owns it. */
export interface TreeClause extends Positioned {
   binding?: TreeBinding;
   /**
    * Every `$GIVEN` this clause references, binding or not. A clause the
    * builder does not own that names a given is what makes binding that given
    * again a double filter, and the tree says so exactly — a scan for `$NAME`
    * could be fooled by an apostrophe in a comment, and was.
    */
   givens: string[];
}

/** A `where:` statement, with its clauses located individually. */
export interface TreeWhere extends Positioned {
   clauses: TreeClause[];
}

/** A `{ … }` query-properties block: a view body, or a `+ { … }` refinement. */
export interface TreeStage {
   /** The whole block, both braces included. */
   span: Span;
   /** Just after `{`, and at `}` — where a first or last statement goes. */
   openEnd: number;
   closeStart: number;
   statements: Positioned[];
   /** This block's OWN `where:`s. A nested `nest:`'s never appear here. */
   wheres: TreeWhere[];
   /** The indent a sibling statement sits at, for anything inserted. */
   indent: string;
   /** True when `{ … }` is written on one line, which changes how we insert. */
   oneLine: boolean;
}

export type TreeViewBody =
   /** `view: v is base` or `view: v is base + { … }`. */
   | { kind: "reference"; from: string; fromSpan: Span; refinement?: TreeStage }
   /**
    * `view: v is { … }`, or the first stage of `{ … } -> { … }`. Only the
    * first stage is ever the tile's own, so `pipeline` says a later one
    * follows and nothing may be appended to the end of the declaration.
    */
   | { kind: "inline"; stage: TreeStage; pipeline?: boolean }
   /** `view: v is a -> b`, and anything else: read, never rewritten. */
   | { kind: "unsupported"; why: string };

export interface TreeView extends Positioned {
   name: string;
   /** `name is <expr>` — this entry alone, not the `view:` statement. */
   span: Span;
   /** The whole `view: …` statement, its `#` tag block included. */
   statement: Span;
   /** The `#` tag lines of that statement, in order. */
   tags: Array<{ line: number; text: string }>;
   /**
    * How many views the one `view:` statement declares. Above 1 it is a
    * comma-separated list, which the writer refuses to restructure.
    */
   siblings: number;
   body: TreeViewBody;
}

export interface TreeDimension extends Positioned {
   name: string;
   expression: string;
   statement: Span;
   tags: Array<{ line: number; text: string }>;
}

export interface TreeSource extends Positioned {
   name: string;
   /** What it extends, as written — `orders`, or `` `odd name` ``. */
   base: string;
   /** The whole `source: …` statement, its `#` tag block included. */
   statement: Span;
   /** The `extend { … }` block, when it has one. */
   properties?: TreeStage;
   views: TreeView[];
   dimensions: TreeDimension[];
   /** Source-level `where:`s, which are the source's own and not a tile's. */
   wheres: TreeWhere[];
}

export interface TreeGiven extends Positioned {
   name: string;
   /** `NAME :: type is default`, as written. */
   declaration: string;
   statement: Span;
   tags: Array<{ line: number; text: string }>;
   /**
    * The line of the `given:` keyword, when this is one entry of a block
    * rather than a one-line declaration. Removing the last entry of a block
    * has to take the header with it.
    */
   blockHeader?: number;
}

export interface TreeImport extends Positioned {
   /** The path, with its quotes stripped. */
   from: string;
   /** The `{ a, b }` selection, when there is one. */
   names?: string[];
   statement: Span;
}

export interface ParsedMalloy {
   text: string;
   /** 0-based line -> offset of its first character. */
   lineStarts: number[];
   imports: TreeImport[];
   sources: TreeSource[];
   givens: TreeGiven[];
   /** The `//` comment token on `line`, if the line ends with one. */
   trailingComment(line: number): Span | undefined;
   /**
    * The `#`/`//` block immediately above `line`, stopping at a blank line —
    * the unit that travels with a declaration when it moves.
    */
   blockStart(line: number): number;
}

export interface ParseRefusal {
   ok: false;
   reason: string;
   line?: number;
}

export type ParseResult = { ok: true; parsed: ParsedMalloy } | ParseRefusal;

export const parseRefused = (r: ParseResult): r is ParseRefusal =>
   r.ok === false;

/* ------------------------------------------------------------------ */
/* Node identity                                                       */
/* ------------------------------------------------------------------ */

interface TokenStream {
   tokenSource?: {
      vocabulary?: { getSymbolicName(type: number): string | undefined };
   };
   getTokens?(): Array<{ type: number; startIndex: number; stopIndex: number }>;
}

type Ctx = Record<string, unknown> & {
   ruleIndex?: number;
   childCount?: number;
   getChild(i: number): Ctx;
   start?: { startIndex: number; line: number };
   stop?: { stopIndex: number };
};

const has = (c: unknown, ...accessors: string[]): boolean =>
   accessors.every(
      (a) => typeof (c as Record<string, unknown>)?.[a] === "function",
   );

/** A rule context we can take a range from; terminals and empties are not. */
const isRule = (c: Ctx | undefined): boolean =>
   c !== undefined && c.ruleIndex !== undefined && (c.childCount ?? 0) > 0;

const IS = {
   sourceDefinition: (c: Ctx) =>
      has(c, "sourceNameDef", "isDefine", "sqExplore"),
   exploreQueryDef: (c: Ctx) =>
      has(c, "exploreQueryNameDef", "isDefine", "vExpr"),
   defExploreQuery: (c: Ctx) => has(c, "VIEW", "subQueryDefList"),
   defDimension: (c: Ctx) => has(c, "defList", "DIMENSION"),
   exploreProperties: (c: Ctx) =>
      has(c, "exploreStatement", "OCURLY", "CCURLY"),
   queryProperties: (c: Ctx) => has(c, "queryStatement", "OCURLY", "CCURLY"),
   queryStatement: (c: Ctx) =>
      has(c, "whereStatement", "aggregateStatement", "nestStatement"),
   whereStatement: (c: Ctx) => has(c, "WHERE", "filterClauseList"),
   givenDef: (c: Ctx) => has(c, "givenNameDef"),
   givenStatement: (c: Ctx) => has(c, "GIVEN", "givenDefList"),
   importStatement: (c: Ctx) => has(c, "IMPORT", "importURL"),
   // A `vExpr` is either one stage or an `->` pipeline; a `segExpr` is a bare
   // reference, a `{ … }` body, a `( … )`, or `lhs + rhs`.
   vArrow: (c: Ctx) => has(c, "ARROW", "vExpr", "segExpr"),
   segRefine: (c: Ctx) => has(c, "PLUS", "segExpr"),
   segOps: (c: Ctx) => has(c, "queryProperties"),
   segField: (c: Ctx) => has(c, "fieldPath") && !has(c, "segExpr"),
   segParen: (c: Ctx) => has(c, "OPAREN", "CPAREN", "vExpr"),
   /** `a ~ $A`: exactly a comparison, so `a ~ $A and c = 1` is not one. */
   compare: (c: Ctx) => has(c, "fieldExpr", "compareOp"),
   givenRef: (c: Ctx) => has(c, "GIVEN_REF"),
};

/* ------------------------------------------------------------------ */
/* Offsets                                                             */
/* ------------------------------------------------------------------ */

/**
 * Code-point index -> UTF-16 offset.
 *
 * ANTLR reads a `CodePointCharStream`, so every token index counts CODE
 * POINTS while every JavaScript string index counts UTF-16 units. Measured on
 * a line holding two emoji, a clause the parser puts at 24 is at 26 in JS, and
 * slicing by the parser's number cuts a surrogate pair in half. One map per
 * parse converts every offset once and the rest of the module is plain JS
 * indices.
 */
function codePointMap(text: string): Int32Array {
   const map = new Int32Array([...text].length + 1);
   let cp = 0;
   for (let i = 0; i < text.length; ) {
      map[cp++] = i;
      i += (text.codePointAt(i) as number) > 0xffff ? 2 : 1;
   }
   map[cp] = text.length;
   return map;
}

function lineStartsOf(text: string): number[] {
   const out = [0];
   for (let i = 0; i < text.length; i++) if (text[i] === "\n") out.push(i + 1);
   return out;
}

/* ------------------------------------------------------------------ */

class Reader {
   private readonly map: Int32Array;
   readonly lineStarts: number[];

   constructor(readonly text: string) {
      this.map = codePointMap(text);
      this.lineStarts = lineStartsOf(text);
   }

   /**
    * The span a context covers, or `undefined` when it covers nothing we can
    * trust. Empty rule contexts report inverted or whole-file ranges without
    * throwing, so every span is checked rather than assumed.
    */
   span(c: Ctx | undefined): Span | undefined {
      if (!isRule(c)) return undefined;
      const start = c!.start;
      const stop = c!.stop;
      if (!start || !stop) return undefined;
      const s = this.map[start.startIndex];
      const e = this.map[stop.stopIndex + 1];
      if (s === undefined || e === undefined || e < s) return undefined;
      return { start: s, end: e };
   }

   /**
    * The span of a TERMINAL node — a keyword or punctuation. A terminal has no
    * rule range, only the token it stands for, and asking `span` for one
    * silently yields nothing.
    */
   terminal(node: unknown): Span | undefined {
      const symbol = (
         node as { symbol?: { startIndex: number; stopIndex: number } }
      )?.symbol;
      if (!symbol) return undefined;
      const start = this.map[symbol.startIndex];
      const end = this.map[symbol.stopIndex + 1];
      if (start === undefined || end === undefined || end < start)
         return undefined;
      return { start, end };
   }

   /** A parser code-point index as a JS string offset. */
   utf16(codePoint: number): number | undefined {
      return this.map[codePoint];
   }

   text_(c: Ctx | undefined): string | undefined {
      const s = this.span(c);
      return s && this.text.slice(s.start, s.end);
   }

   line(offset: number): number {
      let lo = 0;
      let hi = this.lineStarts.length - 1;
      while (lo < hi) {
         const mid = (lo + hi + 1) >> 1;
         if (this.lineStarts[mid] <= offset) lo = mid;
         else hi = mid - 1;
      }
      return lo;
   }

   positioned(c: Ctx | undefined): Positioned | undefined {
      const span = this.span(c);
      return span && { span, line: this.line(span.start) };
   }

   /** The whitespace a line begins with, which anything inserted matches. */
   indentAt(offset: number): string {
      const start = this.lineStarts[this.line(offset)];
      return /^[ \t]*/.exec(this.text.slice(start))?.[0] ?? "";
   }
}

/** Every descendant that satisfies `pred`, outermost first. */
function collect(root: Ctx, pred: (c: Ctx) => boolean, out: Ctx[] = []): Ctx[] {
   if (isRule(root) && pred(root)) out.push(root);
   for (let i = 0; i < (root.childCount ?? 0); i++)
      collect(root.getChild(i), pred, out);
   return out;
}

/** A `{ … }` block of any kind, which is where one scope ends and another begins. */
const opensScope = (c: Ctx) => has(c, "OCURLY", "CCURLY");

/**
 * Descendants that satisfy `pred`, not descending past one that does — nor
 * past a nested `{ … }`, so a filtered measure's `where:` and a `nest:`'s
 * never surface as the enclosing block's own. Depth is structure here, which
 * is the point: no brace counting can be wrong about it.
 */
function collectShallow(root: Ctx, pred: (c: Ctx) => boolean): Ctx[] {
   const out: Ctx[] = [];
   const walk = (c: Ctx) => {
      if (!isRule(c)) return;
      if (pred(c)) {
         out.push(c);
         return;
      }
      if (opensScope(c)) return;
      for (let i = 0; i < (c.childCount ?? 0); i++) walk(c.getChild(i));
   };
   for (let i = 0; i < (root.childCount ?? 0); i++) walk(root.getChild(i));
   return out;
}

const call = (c: Ctx, name: string): Ctx | undefined =>
   (c[name] as (() => Ctx) | undefined)?.call(c);

const callAll = (c: Ctx, name: string): Ctx[] =>
   ((c[name] as (() => Ctx[]) | undefined)?.call(c) as Ctx[]) ?? [];

/* ------------------------------------------------------------------ */
/* Building the model                                                  */
/* ------------------------------------------------------------------ */

function readBinding(r: Reader, clause: Ctx): TreeBinding | undefined {
   if (!IS.compare(clause) || (clause.childCount ?? 0) !== 3) return undefined;
   const right = clause.getChild(2);
   if (!IS.givenRef(right)) return undefined;
   const field = r.text_(clause.getChild(0));
   const op = r.text_(clause.getChild(1));
   const given = r.text_(right) ?? "";
   if (field === undefined || op === undefined) return undefined;
   return { field, op: op.trim(), given: given.replace(/^\$/, "") };
}

function readWhere(r: Reader, ctx: Ctx): TreeWhere | undefined {
   const at = r.positioned(ctx);
   if (!at) return undefined;
   const list = call(ctx, "filterClauseList");
   const clauses: TreeClause[] = [];
   for (const fe of list ? callAll(list, "fieldExpr") : []) {
      const cat = r.positioned(fe);
      if (!cat) continue;
      const givens = collect(fe, IS.givenRef)
         .map((g) => r.text_(g))
         .filter((g): g is string => g !== undefined)
         .map((g) => g.replace(/^\$/, ""));
      clauses.push({ ...cat, givens, ...{ binding: readBinding(r, fe) } });
   }
   return { ...at, clauses };
}

/**
 * A `{ … }` block. `wheres` are the block's OWN `where:` statements: a
 * `nest:`'s body is a `queryProperties` of its own, so its filters never
 * surface here, which is what makes depth a matter of structure rather than
 * of counting braces.
 */
function readStage(r: Reader, props: Ctx): TreeStage | undefined {
   const span = r.span(props);
   if (!span) return undefined;
   const statements: Positioned[] = [];
   const wheres: TreeWhere[] = [];
   for (const st of collectShallow(props, IS.queryStatement)) {
      const at = r.positioned(st);
      if (at) statements.push(at);
      const w = call(st, "whereStatement");
      if (w && IS.whereStatement(w)) {
         const read = readWhere(r, w);
         if (read) wheres.push(read);
      }
   }
   const openEnd = span.start + 1;
   const closeStart = span.end - 1;
   const oneLine = !r.text.slice(span.start, span.end).includes("\n");
   const indent = statements.length
      ? r.indentAt(statements[0].span.start)
      : r.indentAt(span.start) + "   ";
   return { span, openEnd, closeStart, statements, wheres, indent, oneLine };
}

/** The `#` annotation lines of a statement's own tag block. */
function readTags(
   r: Reader,
   statement: Span,
   declStart: number,
): Array<{ line: number; text: string }> {
   const out: Array<{ line: number; text: string }> = [];
   const first = r.line(statement.start);
   const last = r.line(declStart);
   for (let i = first; i < last; i++) {
      const text: string = r.text
         .slice(r.lineStarts[i], r.lineStarts[i + 1] ?? r.text.length)
         .trim();
      // `##` is a MODEL annotation and never belongs to a declaration.
      if (text.startsWith("#") && !text.startsWith("##"))
         out.push({ line: i, text });
   }
   return out;
}

function readViewBody(r: Reader, vExpr: Ctx): TreeViewBody {
   // A `->` pipeline: only its FIRST stage is the tile's own, so that is what
   // is read and all that may ever be rewritten.
   const seg = call(vExpr, "segExpr");
   if (!seg) return { kind: "unsupported", why: "an unreadable expression" };
   const pipeline = IS.vArrow(vExpr);
   if (IS.segOps(seg)) {
      const props = call(seg, "queryProperties");
      const stage = props && readStage(r, props);
      return stage
         ? { kind: "inline", stage, ...(pipeline ? { pipeline: true } : {}) }
         : { kind: "unsupported", why: "an unreadable body" };
   }
   if (pipeline)
      return { kind: "unsupported", why: "a `->` pipeline from a named view" };
   if (IS.segField(seg)) {
      const fromSpan = r.span(seg);
      if (!fromSpan) return { kind: "unsupported", why: "an unreadable name" };
      return {
         kind: "reference",
         from: r.text.slice(fromSpan.start, fromSpan.end),
         fromSpan,
      };
   }
   if (IS.segRefine(seg)) {
      // `vx + vy + { … }` nests left, so the refinement is the RIGHTMOST
      // operand and the base is everything to its left, as written. Reading
      // only the immediate left operand is what deleted `vy` at head.
      const operands = callAll(seg, "segExpr");
      const lhs = operands[0];
      const rhs = operands[operands.length - 1];
      // A `{ … }` anywhere in the BASE means a second refinement block --
      // `{ … } + { … }`, or `vx + { … } + { … }`. There is then no one block a
      // binding belongs in, and a given already filtered on in the other would
      // be bound a second time, so the whole declaration is left alone.
      let base = lhs;
      while (IS.segRefine(base)) {
         const inner = callAll(base, "segExpr");
         if (inner.some((o) => IS.segOps(o) || IS.segParen(o)))
            return { kind: "unsupported", why: "a chained refinement" };
         base = inner[0];
      }
      if (!base || !IS.segField(base))
         return {
            kind: "unsupported",
            why: "a `{ … } + { … }` compound refinement",
         };
      const fromSpan = r.span(lhs);
      if (!fromSpan || !rhs)
         return { kind: "unsupported", why: "an unreadable refinement" };
      if (!IS.segOps(rhs)) {
         // `vx + vy` with no `{ … }` at all: the base is the WHOLE expression,
         // not its left operand, or a rewrite would drop everything after it.
         const whole = r.span(seg);
         return whole
            ? {
                 kind: "reference",
                 from: r.text.slice(whole.start, whole.end),
                 fromSpan: whole,
              }
            : { kind: "unsupported", why: "an unreadable refinement" };
      }
      const from = r.text.slice(fromSpan.start, fromSpan.end);
      const props = call(rhs, "queryProperties");
      const stage = props && readStage(r, props);
      return stage
         ? { kind: "reference", from, fromSpan, refinement: stage }
         : { kind: "reference", from, fromSpan };
   }
   return { kind: "unsupported", why: "a parenthesized expression" };
}

function readViews(r: Reader, props: Ctx): TreeView[] {
   const out: TreeView[] = [];
   for (const stmt of collectShallow(props, IS.defExploreQuery)) {
      const statement = r.span(stmt);
      const list = call(stmt, "subQueryDefList");
      if (!statement || !list) continue;
      const defs = callAll(list, "exploreQueryDef").filter(IS.exploreQueryDef);
      const keyword = r.terminal(call(stmt, "VIEW"));
      for (const def of defs) {
         const span = r.span(def);
         const nameCtx = call(def, "exploreQueryNameDef");
         const name = r.text_(nameCtx);
         const vExpr = call(def, "vExpr");
         if (!span || !name || !vExpr) continue;
         out.push({
            name,
            span,
            line: r.line(span.start),
            statement,
            tags: readTags(r, statement, keyword?.start ?? span.start),
            siblings: defs.length,
            body: readViewBody(r, vExpr),
         });
      }
   }
   return out;
}

function readDimensions(r: Reader, props: Ctx): TreeDimension[] {
   const out: TreeDimension[] = [];
   for (const stmt of collectShallow(props, IS.defDimension)) {
      const statement = r.span(stmt);
      const list = call(stmt, "defList");
      if (!statement || !list) continue;
      const keyword = r.terminal(call(stmt, "DIMENSION"));
      for (const def of callAll(list, "fieldDef")) {
         const span = r.span(def);
         if (!span) continue;
         const whole = r.text.slice(span.start, span.end);
         const isAt = /\bis\b/.exec(whole);
         if (!isAt) continue;
         out.push({
            name: whole.slice(0, isAt.index).trim(),
            expression: whole.slice(isAt.index + 2).trim(),
            span,
            line: r.line(span.start),
            statement,
            tags: readTags(r, statement, keyword?.start ?? span.start),
         });
      }
   }
   return out;
}

function readSources(r: Reader, root: Ctx): TreeSource[] {
   const out: TreeSource[] = [];
   for (const def of collect(root, IS.sourceDefinition)) {
      const span = r.span(def);
      const name = r.text_(call(def, "sourceNameDef"));
      const explore = call(def, "sqExplore");
      if (!span || !name || !explore) continue;
      const exploreSpan = r.span(explore);
      // The base is the leading name of what it extends, bounded by the tree.
      const baseText = exploreSpan
         ? r.text.slice(exploreSpan.start, exploreSpan.end)
         : "";
      const base = (
         /^\s*(`[^`]*`|[A-Za-z_][A-Za-z0-9_.]*)/.exec(baseText)?.[1] ?? ""
      ).trim();
      const propsCtx = collectShallow(explore, IS.exploreProperties)[0];
      const properties = propsCtx && readStage(r, propsCtx);
      const statement: Span = {
         start: r.lineStarts[r.line(span.start)],
         end: span.end,
      };
      const wheres: TreeWhere[] = [];
      if (propsCtx)
         for (const st of collectShallow(propsCtx, IS.whereStatement)) {
            const w = readWhere(r, st);
            if (w) wheres.push(w);
         }
      out.push({
         name,
         base,
         span,
         line: r.line(span.start),
         statement,
         ...(properties ? { properties } : {}),
         views: propsCtx ? readViews(r, propsCtx) : [],
         dimensions: propsCtx ? readDimensions(r, propsCtx) : [],
         wheres,
      });
   }
   return out;
}

function readGivens(r: Reader, root: Ctx): TreeGiven[] {
   const out: TreeGiven[] = [];
   for (const statement of collect(root, IS.givenStatement)) {
      const keyword = r.terminal(call(statement, "GIVEN"));
      const list = call(statement, "givenDefList");
      const headerLine = keyword ? r.line(keyword.start) : undefined;
      for (const def of list ? callAll(list, "givenDef") : []) {
         const whole = r.span(def);
         const nameSpan = r.span(call(def, "givenNameDef"));
         const name = nameSpan && r.text.slice(nameSpan.start, nameSpan.end);
         if (!whole || !nameSpan || !name) continue;
         // The context takes in the `#` tag block above the name, which is the
         // control contract rather than part of the declaration.
         const span = { start: nameSpan.start, end: whole.end };
         const line = r.line(span.start);
         out.push({
            name,
            declaration: r.text.slice(span.start, span.end),
            span,
            line,
            statement: { start: r.lineStarts[line], end: span.end },
            tags: readTags(r, whole, span.start),
            ...(headerLine !== undefined && headerLine !== line
               ? { blockHeader: headerLine }
               : {}),
         });
      }
   }
   return out;
}

function readImports(r: Reader, root: Ctx): TreeImport[] {
   const out: TreeImport[] = [];
   for (const stmt of collect(root, IS.importStatement)) {
      const span = r.span(stmt);
      const url = r.text_(call(stmt, "importURL"));
      if (!span || url === undefined) continue;
      const select = call(stmt, "importSelect");
      const names = select
         ? callAll(select, "importItem")
              .map((i) => r.text_(i))
              .filter((n): n is string => n !== undefined)
              .map((n) => n.trim())
         : undefined;
      out.push({
         from: url.replace(/^['"]|['"]$/g, ""),
         ...(names && names.length > 0 ? { names } : {}),
         span,
         line: r.line(span.start),
         statement: span,
      });
   }
   return out;
}

/* ------------------------------------------------------------------ */

/**
 * Parse `text`, or refuse.
 *
 * A file with ANY syntax error is refused rather than edited. ANTLR recovers
 * from an error by inventing structure: measured on a file with one, the tree
 * reports a view that is not there, a range that runs backwards, and drops
 * every declaration after the error — all without raising anything a caller
 * could notice. An edit located in that tree deletes real code, and neither
 * the read-back gate nor the parse gate can see it. The server cannot compile
 * such a file either, so the way out of one is the code editor, not this.
 */
export async function parseMalloy(text: string): Promise<ParseResult> {
   // Imported dynamically, never statically: `builder-entry.ts` installs the
   // `process.env` shim the parser's dependencies read at module scope, and a
   // static import would be evaluated before that shim runs.
   const { MalloyTranslator } = await import("@malloydata/malloy");
   const url = "file://dashboard-builder.malloy";

   let translator: {
      translate(): { problems?: Array<{ code?: string; message?: string }> };
      parseStep?: {
         response?: {
            parse?: {
               root?: unknown;
               tokenStream?: unknown;
               malloyVersion?: string;
            };
         };
      };
   };
   let problems: Array<{ code?: string; message?: string }>;
   try {
      translator = new MalloyTranslator(url, null, {
         urls: { [url]: text },
      }) as never;
      // Stops at the first step: a document with imports comes back asking for
      // urls, by which point it has been parsed and nothing has been resolved.
      // So this sees every syntax error and none of the semantic ones.
      problems = translator.translate().problems ?? [];
   } catch (error) {
      return { ok: false, reason: `Malloy could not read this file: ${error}` };
   }

   const syntax = problems.filter((p) => p.code === "syntax-error");
   if (syntax.length > 0)
      return {
         ok: false,
         reason: `This file has a Malloy syntax error, so it cannot be edited here: ${
            syntax[0].message ?? "unknown"
         }`,
      };

   const parse = translator.parseStep?.response?.parse;
   const root = parse?.root as Ctx | undefined;
   if (!root || !isRule(root) || typeof root.getChild !== "function")
      return {
         ok: false,
         reason:
            "This build of Malloy does not expose a parse tree the builder can " +
            `read (malloy ${parse?.malloyVersion ?? "unknown"}), so editing is off.`,
      };

   const r = new Reader(text);
   const sources = readSources(r, root);

   // The shape assertion, on real content rather than on the API's presence:
   // a file that declares a source and yields none means the tree is not what
   // this module was written against, and locating an edit in it is guesswork.
   if (sources.length === 0 && /^\s*source:/m.test(text))
      return {
         ok: false,
         reason:
            "The builder could not locate this file's sources in Malloy's parse " +
            `tree (malloy ${parse?.malloyVersion ?? "unknown"}), so editing is off.`,
      };

   const comments = commentIndex(r, (parse?.tokenStream ?? {}) as TokenStream);
   return {
      ok: true,
      parsed: {
         text,
         lineStarts: r.lineStarts,
         imports: readImports(r, root),
         sources,
         givens: readGivens(r, root),
         trailingComment: (line) => comments.trailing.get(line),
         blockStart: (line) => blockStart(r, comments.lines, line),
      },
   };
}

/**
 * The `//` comments, taken from the LEXER's own tokens rather than from a scan
 * for `//`: a `//` inside a string literal is not a comment, and every scan
 * here that had to know that got it wrong at least once.
 *
 * A comment alone on its line belongs to the block above a declaration; one
 * that follows code is a trailing comment, and anything appended to that line
 * has to go before it.
 */
function commentIndex(
   r: Reader,
   tokenStream: TokenStream,
): { trailing: Map<number, Span>; lines: Set<number> } {
   const trailing = new Map<number, Span>();
   const lines = new Set<number>();
   const vocabulary = tokenStream.tokenSource?.vocabulary;
   for (const token of tokenStream.getTokens?.() ?? []) {
      if (vocabulary?.getSymbolicName(token.type) !== "COMMENT_TO_EOL")
         continue;
      const start = r.utf16(token.startIndex);
      let end = r.utf16(token.stopIndex + 1);
      if (start === undefined || end === undefined || end < start) continue;
      // The token runs to the newline and takes it with it; the span must not,
      // or anything inserted before the comment lands on the next line.
      while (
         end > start &&
         (r.text[end - 1] === "\n" || r.text[end - 1] === "\r")
      )
         end--;
      const line = r.line(start);
      if (r.text.slice(r.lineStarts[line], start).trim() === "")
         lines.add(line);
      else trailing.set(line, { start, end });
   }
   return { trailing, lines };
}

/**
 * The first line of the `#`/`//` block above `line`, stopping at a blank line.
 * A blank line is the author's own separator, which is why it is the boundary
 * rather than a count or a guess about what a comment says.
 */
function blockStart(
   r: Reader,
   commentLines: Set<number>,
   line: number,
): number {
   let start = line;
   for (let i = line - 1; i >= 0; i--) {
      const text = r.text
         .slice(r.lineStarts[i], r.lineStarts[i + 1] ?? r.text.length)
         .trim();
      if (text === "") break;
      if (text.startsWith("#") || commentLines.has(i)) start = i;
      else break;
   }
   return start;
}
