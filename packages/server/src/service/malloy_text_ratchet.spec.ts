// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * Every name, path, string and annotation value the server writes into Malloy
 * text goes through the Malloy-text module (malloy_text.ts), so a quoting rule
 * is fixed once and an injection or round-trip bug cannot hide in a private
 * copy. This spec fails when new code splices a value into generated Malloy
 * without it.
 *
 * It reads every non-test source with the TypeScript parser and finds
 * generated Malloy in three shapes: a template literal, a `+` chain of strings,
 * and an array of strings `.join()`ed. Each counts as Malloy when its fixed
 * text carries a Malloy construct (MALLOY_CONTEXT: a statement keyword, a
 * clause such as `where:` or `join_one:`, `-> {`, `extend {`, `+ {`,
 * `.table(`, `.sql(`, `.virtual(`, `compose(`, `#@ persist`, `##!`, `import {`
 * or `given:`). Every value spliced into one must be SAFE:
 *
 * - a call to one of the module's emitters (`escapeString` only between
 *   quotes, since it escapes but does not quote);
 * - a string or number literal, or a template, `+` chain, ternary or `.join()`
 *   whose every part is safe (`xs.map((x) => identifier(x)).join(", ")`);
 * - a `const` whose declaration, resolved by scope rather than by name across
 *   the file, is initialized safe; or an array whose initializer and every
 *   value pushed onto it are safe.
 *
 * A parameter, a property access (`${n.foo}`) or an imported name is never
 * safe on its own. A template that is just one interpolation in backticks is
 * a private quoter and is flagged anywhere.
 *
 * A message that shows Malloy is skipped by where it sits, not by how it
 * reads: a template thrown, passed to `new …Error(…)` or a logger, or held by
 * a `message`/`detail`/`hint`/`reason`-style property or variable.
 *
 * A legitimate exception is marked at the site, with a
 * `// malloy-text: <reason>` comment on the line before the value, or before
 * the parameter or variable that holds it (then trusted wherever it flows).
 * The mark covers that one site, never new code elsewhere in the file, and a
 * mark that no longer excuses anything fails too, so the marks cannot rot.
 * Findings name the enclosing function, to find them.
 *
 * What it still cannot see: Malloy assembled from pieces none of which carries
 * a construct on its own (each piece is plain text until it is joined in some
 * other function), and a value laundered through a helper function's return.
 */

import { describe, expect, it } from "bun:test";
import * as fs from "fs";
import * as path from "path";
import * as ts from "typescript";

const SRC = path.resolve(__dirname, "..");

const QUOTING_EMITTERS = new Set([
   "identifier",
   "quotedIdentifier",
   "quotedPath",
   "stringLiteral",
   "doubleQuotedString",
   "annotationValue",
]);
/** Escapes but does not quote: safe only between quotes. */
const ESCAPING_EMITTERS = new Set(["escapeString"]);

const MALLOY_CONTEXT = new RegExp(
   [
      String.raw`(^|\n)\s*(source|query|run|view|type):\s`,
      String.raw`->\s*\{`,
      String.raw`\bextend\s*\{`,
      String.raw`\+\s*\{`,
      String.raw`(^|\n)\s*#@\s*persist(?!\s+sources?\s)`,
      String.raw`##!`,
      String.raw`\bcompose\(`,
      String.raw`\.(virtual|table|sql)\(`,
      String.raw`\b(group_by|aggregate|select|where|having|nest|order_by|calculate|dimension|measure|primary_key|rename|except|accept):`,
      String.raw`\bjoin_(one|many|cross):`,
      String.raw`(^|\n)\s*import \{`,
      String.raw`(^|\n)\s*given:`,
   ].join("|"),
);

/**
 * Where generated Malloy starts a line: a statement or clause keyword. Text
 * that runs on as prose with no such line is a message quoting Malloy inline
 * (`Views are defined like 'source: s is … extend { view: … }'`).
 */
const STATEMENT_START =
   /(^|\n)\s*((source|query|run|view|type|given|dimension|measure|where|group_by|aggregate|select|nest|order_by|join_(one|many|cross)):|#@|##!|import \{|\}|->)/;
/** Plain English words, outside any backtick-quoted code. */
const proseWords = (fixed: string) =>
   fixed.replace(/`[^`]*`/g, " ").match(/\b[a-z]{3,}\b/gi)?.length ?? 0;
const isProseMessage = (fixed: string) =>
   proseWords(fixed) >= 5 && !STATEMENT_START.test(fixed);

const MESSAGE_NAME =
   /^(message|msg|detail|details|hint|reason|description|error|warning|suggestion|help)$/i;

/**
 * The `// malloy-text: <reason>` comments that excused a value, by
 * "file:line", so one that excuses nothing can be reported as stale.
 */
const usedAllowances = new Set<string>();
const ALLOW_COMMENT = /\/\/\s*malloy-text:\s*\S/;

interface Finding {
   file: string;
   fn: string;
   line: number;
   expr: string;
}

function isFunctionLike(n: ts.Node): n is ts.SignatureDeclaration {
   return (
      ts.isFunctionDeclaration(n) ||
      ts.isFunctionExpression(n) ||
      ts.isArrowFunction(n) ||
      ts.isMethodDeclaration(n) ||
      ts.isConstructorDeclaration(n) ||
      ts.isGetAccessorDeclaration(n) ||
      ts.isSetAccessorDeclaration(n)
   );
}

/** The name of the nearest named function a node sits in, to find a finding by. */
function enclosingFunctionName(n: ts.Node, sf: ts.SourceFile): string {
   for (let p: ts.Node | undefined = n.parent; p; p = p.parent) {
      if (!isFunctionLike(p)) continue;
      if (ts.isConstructorDeclaration(p)) return "constructor";
      if (
         (ts.isFunctionDeclaration(p) ||
            ts.isMethodDeclaration(p) ||
            ts.isGetAccessorDeclaration(p) ||
            ts.isSetAccessorDeclaration(p)) &&
         p.name
      ) {
         return p.name.getText(sf);
      }
      const holder = p.parent;
      if (
         holder &&
         ts.isVariableDeclaration(holder) &&
         ts.isIdentifier(holder.name)
      ) {
         return holder.name.text;
      }
      if (
         holder &&
         (ts.isPropertyAssignment(holder) || ts.isPropertyDeclaration(holder))
      ) {
         return holder.name.getText(sf);
      }
      // An anonymous callback: keep walking to the function that holds it.
   }
   return "<top>";
}

function bindsName(n: ts.BindingName, name: string): boolean {
   if (ts.isIdentifier(n)) return n.text === name;
   return n.elements.some(
      (e) => !ts.isOmittedExpression(e) && bindsName(e.name, name),
   );
}

function importsName(st: ts.ImportDeclaration, name: string): boolean {
   const c = st.importClause;
   if (!c) return false;
   if (c.name?.text === name) return true;
   const b = c.namedBindings;
   if (!b) return false;
   if (ts.isNamespaceImport(b)) return b.name.text === name;
   return b.elements.some((e) => e.name.text === name);
}

/** The declaration a name resolves to, walking scopes outward. */
function resolve(id: ts.Identifier): ts.Node | undefined {
   const name = id.text;
   const declaresHere = (scope: ts.Node): ts.Node | undefined => {
      const statements =
         ts.isSourceFile(scope) ||
         ts.isBlock(scope) ||
         ts.isModuleBlock(scope) ||
         ts.isCaseClause(scope) ||
         ts.isDefaultClause(scope)
            ? scope.statements
            : undefined;
      for (const st of statements ?? []) {
         if (ts.isVariableStatement(st)) {
            for (const d of st.declarationList.declarations) {
               if (ts.isIdentifier(d.name) && d.name.text === name) return d;
               if (!ts.isIdentifier(d.name) && bindsName(d.name, name)) {
                  return d.name; // destructured: never safe
               }
            }
         } else if (
            (ts.isFunctionDeclaration(st) || ts.isClassDeclaration(st)) &&
            st.name?.text === name
         ) {
            return st;
         } else if (ts.isImportDeclaration(st) && importsName(st, name)) {
            return st;
         }
      }
      if (isFunctionLike(scope)) {
         for (const p of scope.parameters) {
            if (bindsName(p.name, name)) return p;
         }
      }
      if (
         (ts.isForStatement(scope) ||
            ts.isForOfStatement(scope) ||
            ts.isForInStatement(scope)) &&
         scope.initializer &&
         ts.isVariableDeclarationList(scope.initializer)
      ) {
         for (const d of scope.initializer.declarations) {
            if (bindsName(d.name, name)) return d.name;
         }
      }
      if (
         ts.isCatchClause(scope) &&
         scope.variableDeclaration &&
         bindsName(scope.variableDeclaration.name, name)
      ) {
         return scope.variableDeclaration.name;
      }
      return undefined;
   };
   for (let s: ts.Node | undefined = id.parent; s; s = s.parent) {
      const d = declaresHere(s);
      if (d) return d;
   }
   return undefined;
}

/** The values pushed onto an array variable within its scope, or null if it escapes analysis. */
function pushedValues(decl: ts.VariableDeclaration): ts.Expression[] | null {
   if (!ts.isIdentifier(decl.name)) return null;
   const name = decl.name.text;
   let scope: ts.Node | undefined = decl.parent;
   while (scope && !ts.isBlock(scope) && !ts.isSourceFile(scope)) {
      scope = scope.parent;
   }
   if (!scope) return null;
   const out: ts.Expression[] = [];
   let opaque = false;
   const walk = (n: ts.Node): void => {
      if (
         ts.isPropertyAccessExpression(n) &&
         ts.isIdentifier(n.expression) &&
         n.expression.text === name &&
         ts.isCallExpression(n.parent) &&
         n.parent.expression === n
      ) {
         const m = n.name.text;
         if (m === "push" || m === "unshift") out.push(...n.parent.arguments);
         else if (m === "splice") opaque = true;
      }
      if (
         ts.isElementAccessExpression(n) &&
         ts.isIdentifier(n.expression) &&
         n.expression.text === name &&
         ts.isBinaryExpression(n.parent) &&
         n.parent.left === n
      ) {
         out.push(n.parent.right);
      }
      ts.forEachChild(n, walk);
   };
   walk(scope);
   return opaque ? null : out;
}

/** The expressions a function's body can return. */
function returnedExpressions(fn: ts.SignatureDeclaration): ts.Expression[] {
   const body = (fn as ts.FunctionLikeDeclaration).body;
   if (!body) return [];
   if (!ts.isBlock(body)) return [body];
   const out: ts.Expression[] = [];
   const walk = (n: ts.Node): void => {
      if (isFunctionLike(n)) return;
      if (ts.isReturnStatement(n) && n.expression) out.push(n.expression);
      ts.forEachChild(n, walk);
   };
   walk(body);
   return out;
}

/** A function this file declares, by the name a call uses. */
function localFunction(id: ts.Identifier): ts.SignatureDeclaration | undefined {
   const d = resolve(id);
   if (!d) return undefined;
   if (ts.isFunctionDeclaration(d)) return d;
   if (ts.isVariableDeclaration(d) && d.initializer) {
      const init = unwrap(d.initializer);
      if (ts.isArrowFunction(init) || ts.isFunctionExpression(init))
         return init;
   }
   return undefined;
}

/**
 * Whether every value a local helper returns is safe. Its parameters are not
 * safe, so a helper passes only when it quotes what it is given.
 */
function returnsSafe(fn: ts.SignatureDeclaration, seen: Set<ts.Node>): boolean {
   if (seen.has(fn)) return true;
   seen.add(fn);
   const rs = returnedExpressions(fn);
   return rs.length > 0 && rs.every((r) => isSafe(r, false, seen));
}

/** The index parameter of an array callback (`xs.map((x, i) => …)`): a number. */
function isMapIndex(p: ts.ParameterDeclaration): boolean {
   const fn = p.parent;
   if (!(ts.isArrowFunction(fn) || ts.isFunctionExpression(fn))) return false;
   if (fn.parameters.indexOf(p) !== 1) return false;
   const call = fn.parent;
   return (
      ts.isCallExpression(call) &&
      ts.isPropertyAccessExpression(call.expression) &&
      ["map", "flatMap", "forEach", "filter"].includes(
         call.expression.name.text,
      )
   );
}

/** The array an element callback (`xs.map((x) => …)`) iterates, for its first parameter. */
function callbackReceiver(
   p: ts.ParameterDeclaration,
): ts.Expression | undefined {
   const fn = p.parent;
   if (!(ts.isArrowFunction(fn) || ts.isFunctionExpression(fn)))
      return undefined;
   if (fn.parameters.indexOf(p) !== 0) return undefined;
   const call = fn.parent;
   if (
      ts.isCallExpression(call) &&
      ts.isPropertyAccessExpression(call.expression) &&
      ["map", "flatMap", "forEach", "filter", "some", "every", "find"].includes(
         call.expression.name.text,
      )
   ) {
      return call.expression.expression;
   }
   return undefined;
}

/** The values a `let` is reassigned, or null if analysis cannot see them all. */
function assignedValues(decl: ts.VariableDeclaration): ts.Expression[] | null {
   if (!ts.isIdentifier(decl.name)) return null;
   const name = decl.name.text;
   let scope: ts.Node | undefined = decl.parent;
   while (scope && !ts.isBlock(scope) && !ts.isSourceFile(scope)) {
      scope = scope.parent;
   }
   if (!scope) return null;
   const out: ts.Expression[] = [];
   let opaque = false;
   const walk = (n: ts.Node): void => {
      if (
         ts.isBinaryExpression(n) &&
         ts.isIdentifier(n.left) &&
         n.left.text === name
      ) {
         const k = n.operatorToken.kind;
         if (
            k === ts.SyntaxKind.EqualsToken ||
            k === ts.SyntaxKind.PlusEqualsToken
         ) {
            out.push(n.right);
         } else if (
            k >= ts.SyntaxKind.FirstAssignment &&
            k <= ts.SyntaxKind.LastAssignment
         ) {
            opaque = true;
         }
      }
      ts.forEachChild(n, walk);
   };
   walk(scope);
   return opaque ? null : out;
}

function unwrap(e: ts.Expression): ts.Expression {
   while (
      ts.isParenthesizedExpression(e) ||
      ts.isAsExpression(e) ||
      ts.isNonNullExpression(e) ||
      ts.isTypeAssertionExpression(e) ||
      ts.isSatisfiesExpression(e)
   ) {
      e = e.expression;
   }
   return e;
}

/**
 * Whether an expression is safe to splice into Malloy. `quoted` is whether it
 * sits between quotes, where an escaping emitter is enough.
 */
function isSafe(
   expr: ts.Expression,
   quoted: boolean,
   seen: Set<ts.Node> = new Set(),
): boolean {
   const e = unwrap(expr);
   if (seen.has(e)) return true; // a cycle adds nothing new
   seen.add(e);
   if (hasAllowComment(e)) return true;
   if (
      ts.isStringLiteral(e) ||
      ts.isNoSubstitutionTemplateLiteral(e) ||
      ts.isNumericLiteral(e)
   ) {
      return true;
   }
   if (ts.isPrefixUnaryExpression(e) && ts.isNumericLiteral(e.operand)) {
      return true;
   }
   if (ts.isTemplateExpression(e)) {
      return templateParts(e).every(
         (p) => p.expr === undefined || isSafe(p.expr, p.quoted, seen),
      );
   }
   if (
      ts.isBinaryExpression(e) &&
      e.operatorToken.kind === ts.SyntaxKind.PlusToken
   ) {
      return isSafe(e.left, quoted, seen) && isSafe(e.right, quoted, seen);
   }
   if (ts.isConditionalExpression(e)) {
      return (
         isSafe(e.whenTrue, quoted, seen) && isSafe(e.whenFalse, quoted, seen)
      );
   }
   if (ts.isArrayLiteralExpression(e)) {
      return e.elements.every((el) =>
         ts.isSpreadElement(el)
            ? isSafeArray(el.expression, seen)
            : isSafe(el, quoted, seen),
      );
   }
   if (
      ts.isBinaryExpression(e) &&
      [
         ts.SyntaxKind.MinusToken,
         ts.SyntaxKind.AsteriskToken,
         ts.SyntaxKind.SlashToken,
         ts.SyntaxKind.PercentToken,
      ].includes(e.operatorToken.kind)
   ) {
      return true; // arithmetic yields a number
   }
   if (ts.isPropertyAccessExpression(e) && e.name.text === "length") {
      return true;
   }
   if (ts.isCallExpression(e)) {
      if (ts.isIdentifier(e.expression)) {
         const name = e.expression.text;
         if (QUOTING_EMITTERS.has(name)) return true;
         if (ESCAPING_EMITTERS.has(name)) return quoted;
         if (name === "Number") return true;
         const fn = localFunction(e.expression);
         return fn !== undefined && returnsSafe(fn, seen);
      }
      if (
         ts.isPropertyAccessExpression(e.expression) &&
         ts.isIdentifier(e.expression.expression) &&
         e.expression.expression.text === "Math"
      ) {
         return true;
      }
      if (
         ts.isPropertyAccessExpression(e.expression) &&
         ["trim", "trimEnd", "trimStart"].includes(e.expression.name.text)
      ) {
         return isSafe(e.expression.expression, quoted, seen);
      }
      if (
         ts.isPropertyAccessExpression(e.expression) &&
         ["map", "flatMap", "filter", "slice", "sort", "reverse"].includes(
            e.expression.name.text,
         )
      ) {
         return isSafeArray(e, seen);
      }
      if (
         ts.isPropertyAccessExpression(e.expression) &&
         e.expression.name.text === "join" &&
         (e.arguments.length === 0 || isSafe(e.arguments[0], false, seen))
      ) {
         return isSafeArray(e.expression.expression, seen);
      }
      return false;
   }
   if (ts.isIdentifier(e)) {
      const d = resolve(e);
      if (d && ts.isParameter(d)) {
         if (isMapIndex(d) || hasAllowComment(d)) return true;
         // The element of an array callback over a safe array.
         const over = callbackReceiver(d);
         return over !== undefined && isSafeArray(over, seen);
      }
      // The element of a `for … of` over a safe array.
      if (
         d &&
         ts.isIdentifier(d) &&
         ts.isVariableDeclaration(d.parent) &&
         ts.isVariableDeclarationList(d.parent.parent) &&
         ts.isForOfStatement(d.parent.parent.parent)
      ) {
         return isSafeArray(d.parent.parent.parent.expression, seen);
      }
      if (!d || !ts.isVariableDeclaration(d)) return false;
      if (hasAllowComment(d)) return true;
      if (!d.initializer) {
         const assigned = assignedValues(d);
         return (
            assigned !== null &&
            assigned.length > 0 &&
            assigned.every((a) => isSafe(a, quoted, seen))
         );
      }
      const init = unwrap(d.initializer);
      if (ts.isArrayLiteralExpression(init)) {
         const pushed = pushedValues(d);
         return (
            pushed !== null &&
            isSafe(init, quoted, seen) &&
            pushed.every((p) => isSafe(p, quoted, seen))
         );
      }
      if (!isSafe(init, quoted, seen)) return false;
      const isConst = (ts.getCombinedNodeFlags(d) & ts.NodeFlags.Const) !== 0;
      if (isConst) return true;
      const assigned = assignedValues(d);
      return (
         assigned !== null && assigned.every((a) => isSafe(a, quoted, seen))
      );
   }
   return false;
}

/** Whether an array expression only ever holds safe strings. */
function isSafeArray(arr: ts.Expression, seen: Set<ts.Node>): boolean {
   const a = unwrap(arr);
   if (hasAllowComment(a)) return true;
   if (ts.isArrayLiteralExpression(a) || ts.isIdentifier(a)) {
      return isSafe(a, false, seen);
   }
   if (ts.isCallExpression(a) && ts.isPropertyAccessExpression(a.expression)) {
      const m = a.expression.name.text;
      if (m === "map" || m === "flatMap") {
         const cb = a.arguments[0] ? unwrap(a.arguments[0]) : undefined;
         if (cb && ts.isIdentifier(cb)) {
            if (QUOTING_EMITTERS.has(cb.text)) return true;
            const fn = localFunction(cb);
            return fn !== undefined && returnsSafe(fn, seen);
         }
         if (cb && (ts.isArrowFunction(cb) || ts.isFunctionExpression(cb))) {
            const rs = returnedExpressions(cb);
            return rs.length > 0 && rs.every((r) => isSafe(r, false, seen));
         }
         return false;
      }
      if (["filter", "slice", "sort", "reverse"].includes(m)) {
         return isSafeArray(a.expression.expression, seen);
      }
   }
   return false;
}

interface Part {
   /** The fixed text before this part's value (or the trailing text). */
   before: string;
   expr?: ts.Expression;
   quoted: boolean;
}

function quotedBetween(before: string, after: string): boolean {
   return /[`'"]%?$/.test(before) && /^%?[`'"]/.test(after);
}

function templateParts(t: ts.TemplateExpression): Part[] {
   const parts: Part[] = [];
   let before = t.head.text;
   for (const span of t.templateSpans) {
      const after = span.literal.text;
      parts.push({
         before,
         expr: span.expression,
         quoted: quotedBetween(before, after),
      });
      before = after;
   }
   parts.push({ before, quoted: false });
   return parts;
}

/** A `+` chain flattened left to right. */
function flattenPlus(
   e: ts.Expression,
   out: ts.Expression[] = [],
): ts.Expression[] {
   const u = unwrap(e);
   if (
      ts.isBinaryExpression(u) &&
      u.operatorToken.kind === ts.SyntaxKind.PlusToken
   ) {
      flattenPlus(u.left, out);
      flattenPlus(u.right, out);
   } else {
      out.push(u);
   }
   return out;
}

/** A string operand's fixed text, or undefined for a value. */
function fixedText(e: ts.Expression): string | undefined {
   if (ts.isStringLiteral(e) || ts.isNoSubstitutionTemplateLiteral(e)) {
      return e.text;
   }
   if (ts.isTemplateExpression(e)) {
      return templateParts(e)
         .map((p) => p.before)
         .join(" ");
   }
   return undefined;
}

function isLogCall(c: ts.CallExpression, sf: ts.SourceFile): boolean {
   return /(^|\.)(logger|console|log)\.(debug|info|warn|error|log|trace)$/.test(
      c.expression.getText(sf),
   );
}

/** Whether a node is a message that shows Malloy rather than generated text. */
function isMessage(n: ts.Node, sf: ts.SourceFile): boolean {
   for (let p: ts.Node | undefined = n.parent; p; p = p.parent) {
      if (ts.isThrowStatement(p)) return true;
      if (ts.isNewExpression(p) && /Error$/.test(p.expression.getText(sf))) {
         return true;
      }
      if (ts.isCallExpression(p) && isLogCall(p, sf)) return true;
      if (
         (ts.isPropertyAssignment(p) || ts.isPropertyDeclaration(p)) &&
         MESSAGE_NAME.test(p.name.getText(sf))
      ) {
         return true;
      }
      if (
         ts.isVariableDeclaration(p) &&
         ts.isIdentifier(p.name) &&
         MESSAGE_NAME.test(p.name.text.replace(/(Text|Line|Message)$/, ""))
      ) {
         return true;
      }
      // A callback inside a message (`keys.map((k) => \`${k}\`)`) is still
      // the message; a function declared on its own is not.
      if (
         (ts.isArrowFunction(p) || ts.isFunctionExpression(p)) &&
         p.parent &&
         ts.isCallExpression(p.parent)
      ) {
         continue;
      }
      if (ts.isStatement(p) || isFunctionLike(p)) return false;
   }
   return false;
}

/**
 * Whether a `// malloy-text: <reason>` comment excuses this node: on the line
 * just before it, or before the statement or declaration it starts. A
 * parameter or variable so marked is trusted wherever it is used.
 */
function hasAllowComment(n: ts.Node): boolean {
   const sf = n.getSourceFile();
   const lines = sf.text.split("\n");
   const start = sf.getLineAndCharacterOfPosition(n.getStart(sf)).line;
   for (let l = start; l >= Math.max(0, start - 3); l--) {
      if (ALLOW_COMMENT.test(lines[l - 1] ?? "")) {
         usedAllowances.add(`${sf.fileName}:${l - 1}`);
         return true;
      }
      if (l < start && /[;{}]\s*$/.test(lines[l] ?? "")) break;
   }
   return false;
}

/** The values spliced into one file's generated Malloy without an emitter. */
export function scanForUnquotedMalloy(file: string, text: string): Finding[] {
   const sf = ts.createSourceFile(file, text, ts.ScriptTarget.Latest, true);
   const findings: Finding[] = [];
   const reported = new Set<ts.Node>();
   const report = (e: ts.Expression) => {
      if (reported.has(e) || hasAllowComment(e)) return;
      reported.add(e);
      findings.push({
         file,
         fn: enclosingFunctionName(e, sf),
         line: sf.getLineAndCharacterOfPosition(e.getStart(sf)).line + 1,
         expr: e.getText(sf),
      });
   };
   /** Check one operand of a chain or joined array. */
   const checkOperand = (op: ts.Expression, quoted: boolean) => {
      if (ts.isTemplateExpression(op)) {
         for (const p of templateParts(op)) {
            if (p.expr && !isSafe(p.expr, p.quoted)) report(p.expr);
         }
      } else if (fixedText(op) === undefined && !isSafe(op, quoted)) {
         report(op);
      }
   };
   /** The fixed text of the `+` chain a template sits in, if any. */
   const chainText = (n: ts.Node): string | undefined => {
      let top: ts.Node = n;
      while (
         ts.isBinaryExpression(top.parent) &&
         top.parent.operatorToken.kind === ts.SyntaxKind.PlusToken
      ) {
         top = top.parent;
      }
      if (top === n) return undefined;
      return flattenPlus(top as ts.Expression)
         .map(fixedText)
         .filter((t) => t !== undefined)
         .join(" ");
   };
   const visit = (n: ts.Node): void => {
      if (ts.isTemplateExpression(n)) {
         const parts = templateParts(n);
         const chained = chainText(n);
         const fixed = chained ?? parts.map((p) => p.before).join(" ");
         const quoter =
            n.templateSpans.length === 1 &&
            n.head.text === "`" &&
            n.templateSpans[0].literal.text === "`";
         if (
            (quoter && !isMessage(n, sf)) ||
            (MALLOY_CONTEXT.test(fixed) &&
               !isProseMessage(fixed) &&
               !isMessage(n, sf))
         ) {
            for (const p of parts) {
               if (p.expr && !isSafe(p.expr, p.quoted)) report(p.expr);
            }
         }
      } else if (
         ts.isBinaryExpression(n) &&
         n.operatorToken.kind === ts.SyntaxKind.PlusToken &&
         !(
            ts.isBinaryExpression(n.parent) &&
            n.parent.operatorToken.kind === ts.SyntaxKind.PlusToken
         )
      ) {
         const ops = flattenPlus(n);
         const texts = ops.map(fixedText);
         const fixed = texts.filter((t) => t !== undefined).join(" ");
         if (
            MALLOY_CONTEXT.test(fixed) &&
            !isProseMessage(fixed) &&
            !isMessage(n, sf)
         ) {
            ops.forEach((op, i) =>
               checkOperand(
                  op,
                  quotedBetween(texts[i - 1] ?? "", texts[i + 1] ?? ""),
               ),
            );
         }
      } else if (
         ts.isCallExpression(n) &&
         ts.isPropertyAccessExpression(n.expression) &&
         n.expression.name.text === "join" &&
         ts.isArrayLiteralExpression(unwrap(n.expression.expression))
      ) {
         const arr = unwrap(
            n.expression.expression,
         ) as ts.ArrayLiteralExpression;
         const fixed = arr.elements
            .map((el) => (ts.isSpreadElement(el) ? undefined : fixedText(el)))
            .filter((t) => t !== undefined)
            .join(" ");
         if (
            MALLOY_CONTEXT.test(fixed) &&
            !isProseMessage(fixed) &&
            !isMessage(n, sf)
         ) {
            for (const el of arr.elements) {
               if (ts.isSpreadElement(el)) {
                  if (!isSafeArray(el.expression, new Set())) {
                     report(el.expression);
                  }
               } else {
                  checkOperand(el, false);
               }
            }
         }
      }
      ts.forEachChild(n, visit);
   };
   visit(sf);
   return findings;
}

function sources(dir: string, out: string[] = []): string[] {
   for (const e of fs.readdirSync(dir, { withFileTypes: true })) {
      const p = path.join(dir, e.name);
      if (e.isDirectory()) {
         if (!/node_modules|__fixtures__|test_helpers/.test(p)) sources(p, out);
      } else if (/\.tsx?$/.test(e.name) && !/\.(spec|d)\.tsx?$/.test(e.name)) {
         out.push(p);
      }
   }
   return out;
}

describe("generated Malloy goes through the Malloy-text module", () => {
   const files = sources(SRC).map((p) => ({
      rel: path.relative(SRC, p).split(path.sep).join("/"),
      text: fs.readFileSync(p, "utf8"),
   }));
   const findings = files.flatMap((f) => scanForUnquotedMalloy(f.rel, f.text));

   it("splices no value into generated Malloy without an emitter", () => {
      const unexplained = findings.map(
         (f) => `${f.file}:${f.line} (${f.fn}): \${${f.expr}}`,
      );
      expect(unexplained).toEqual([]);
   });

   it("keeps no `// malloy-text:` allowance that excuses nothing", () => {
      const stale: string[] = [];
      for (const f of files) {
         f.text.split("\n").forEach((line, i) => {
            if (
               ALLOW_COMMENT.test(line) &&
               !usedAllowances.has(`${f.rel}:${i}`)
            ) {
               stale.push(`${f.rel}:${i + 1}: ${line.trim()}`);
            }
         });
      }
      expect(stale).toEqual([]);
   });
});

describe("the scanner itself", () => {
   const scan = (body: string) =>
      scanForUnquotedMalloy("x.ts", body).map((f) => f.expr);

   it("flags a bare name in a template", () => {
      expect(scan("const t = `source: ${name} is duckdb.table('t')`;")).toEqual(
         ["name"],
      );
      expect(scan("const t = `run: s -> { group_by: ${dim} }`;")).toEqual([
         "dim",
      ]);
      expect(
         scan("const t = `#@ persist storage=${dest}\\nsource: a is b`;"),
      ).toEqual(["dest"]);
   });

   it("flags a private quoter wherever it is", () => {
      expect(scan("const q = `\\`${name}\\``;")).toEqual(["name"]);
   });

   it("flags string concatenation", () => {
      expect(
         scan('const t = "source: " + name + " is duckdb.table(\'t\')";'),
      ).toEqual(["name"]);
   });

   it("flags an array joined into Malloy", () => {
      expect(
         scan(
            'const t = ["source:", name, "is", "duckdb.table(\'t\')"].join(" ");',
         ),
      ).toEqual(["name"]);
   });

   it("flags an expression that only contains an emitter, such as a ternary with a raw branch", () => {
      expect(
         scan("const t = `source: ${c ? identifier(a) : b} is x`;"),
      ).toEqual(["c ? identifier(a) : b"]);
   });

   it("flags escapeString outside quotes, and accepts it between them", () => {
      expect(scan("const t = `source: ${escapeString(n)} is x`;")).toEqual([
         "escapeString(n)",
      ]);
      expect(
         scan("const t = `run: s -> { where: a = '${escapeString(v)}' }`;"),
      ).toEqual([]);
   });

   it("resolves a variable by scope, not by name across the file", () => {
      expect(
         scan(
            "function a(x) { const name = identifier(x); return `source: ${name} is t`; }\n" +
               "function b(name) { return `source: ${name} is t`; }",
         ),
      ).toEqual(["name"]);
   });

   it("flags a member of a safe variable", () => {
      expect(
         scan("const n = identifier(x);\nconst t = `source: ${n.foo} is t`;"),
      ).toEqual(["n.foo"]);
   });

   it("flags values in Malloy fragments", () => {
      expect(scan("const t = `where: ${x}`;")).toEqual(["x"]);
      expect(scan("const t = `s + {where: ${x}}`;")).toEqual(["x"]);
      expect(scan("const t = `join_one: ${j} with ${k}`;")).toEqual(["j", "k"]);
      expect(scan("const t = `${conn}.table(${p})`;")).toEqual(["conn", "p"]);
   });

   it("checks generated Malloy even when it reads like prose", () => {
      expect(
         scan(
            "const t = `-- the server writes this file for you\\nsource: ${name} is t`;",
         ),
      ).toEqual(["name"]);
   });

   it("accepts emitters, literals, and safe compositions", () => {
      expect(
         scan("const t = `source: ${identifier(name)} is duckdb.table('t')`;"),
      ).toEqual([]);
      expect(
         scan(
            "const n = identifier(x);\nconst t = `source: ${n} is duckdb.table(${stringLiteral(p)})`;",
         ),
      ).toEqual([]);
      expect(
         scan(
            "const cols = xs.map((x) => identifier(x)).join(', ');\nconst t = `run: s -> { group_by: ${cols} limit: ${10} }`;",
         ),
      ).toEqual([]);
      expect(
         scan(
            "const lines = [];\nfor (const x of xs) lines.push(`dimension: ${identifier(x)} is 1`);\nconst t = `source: s is t extend { ${lines.join('\\n')} }`;",
         ),
      ).toEqual([]);
   });

   it("flags an array that ever holds a raw value", () => {
      expect(
         scan(
            "const lines = [];\nfor (const x of xs) lines.push(x);\nconst t = `source: s is t extend { ${lines.join('\\n')} }`;",
         ),
      ).toEqual(["lines.join('\\n')"]);
   });

   it("skips a message that shows Malloy, by where it sits", () => {
      expect(
         scan(
            "throw new BadRequestError(`run: ${name} failed: the source is missing`);",
         ),
      ).toEqual([]);
      expect(scan("logger.warn(`source: ${name} is not served`);")).toEqual([]);
      expect(scan("const o = { message: `source: ${name} is gone` };")).toEqual(
         [],
      );
   });

   it("honours an inline allowance at the site", () => {
      expect(
         scan(
            "// malloy-text: the author's own expression, spliced verbatim\nconst t = `where: ${authorText}`;",
         ),
      ).toEqual([]);
   });

   it("keys an allowance on the enclosing function", () => {
      const found = scanForUnquotedMalloy(
         "x.ts",
         "function lift(t) { return `run: ${t}`; }\nconst other = (q) => `run: ${q}`;",
      );
      expect(found.map((f) => [f.fn, f.expr])).toEqual([
         ["lift", "t"],
         ["other", "q"],
      ]);
   });
});
