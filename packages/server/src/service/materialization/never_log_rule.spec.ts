// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * The never-log rule, read from the source: a log call anywhere in
 * materialization carries an error's code (`errorCodeOf`), never its text.
 * A driver's message can quote SQL or a row value, and an error's stack
 * starts with its message, so neither may reach a log line in any form.
 */

import { describe, expect, it } from "bun:test";
import * as fs from "fs";
import * as path from "path";

/** An identifier that names an error (or what one wraps), never a code. */
const ERR = String.raw`(?:\w*(?:[eE]rr|[cC]ause)\w*(?<![cC]odes?)|e|ex)`;

/**
 * An error's text in a log call: its message (`errMessage`, `.message`, a
 * redacted message), its stack (`.stack`), the error itself as a field in
 * any form (`error`, `{ err }`, `reason: err`, `result.error`) or as a
 * positional argument,
 * or as text (`String(err)`, `${err}`, `err.toString()`,
 * `JSON.stringify(err)`), or what it wraps (`.cause`).
 */
const RAW_ERROR = new RegExp(
   [
      String.raw`errMessage\(`,
      String.raw`\.message\b`,
      String.raw`\.stack\b`,
      String.raw`redact\w*\(`,
      String.raw`[{,]\s*${ERR}\s*[,}]`,
      String.raw`\w+:\s*${ERR}\s*[,}]`,
      String.raw`logger\.\w+\(\s*("[^"]*"|'[^']*'|\x60[^\x60]*\x60)\s*,\s*${ERR}\s*[,)]`,
      String.raw`String\(\s*${ERR}\s*\)`,
      String.raw`\$\{\s*${ERR}\s*\}`,
      String.raw`\b${ERR}\.toString\(`,
      String.raw`JSON\.stringify\(\s*${ERR}\s*[,)]`,
      String.raw`\binspect\(\s*${ERR}\b`,
      String.raw`\.cause\b`,
      // A field whose value is a property holding an error (`result.error`),
      // not a type (`as NodeJS.ErrnoException`) or a code's argument.
      String.raw`\w+:\s*[\w.]+\.(?=[a-z])${ERR}\s*[,}]`,
   ].join("|"),
);

/**
 * The same forms over a caught error whatever it is named (`catch (failure)`,
 * `.catch((problem) => …)`), which {@link ERR}'s naming convention misses.
 */
function rawErrorOf(name: string): RegExp {
   return new RegExp(
      [
         String.raw`[{,]\s*${name}\s*[,}]`,
         String.raw`\w+:\s*${name}\s*[,}]`,
         String.raw`logger\.\w+\(\s*("[^"]*"|'[^']*'|\x60[^\x60]*\x60)\s*,\s*${name}\s*[,)]`,
         String.raw`String\(\s*${name}\s*\)`,
         String.raw`\$\{\s*${name}\s*\}`,
         String.raw`\b${name}\.(message|stack|cause|toString)\b`,
         String.raw`(JSON\.stringify|inspect)\(\s*${name}\b`,
         String.raw`errMessage\(\s*${name}\b`,
      ].join("|"),
   );
}

/** Every name a catch clause or a `.catch` callback binds in `text`. */
function caughtNames(text: string): string[] {
   const names = new Set<string>();
   for (const m of text.matchAll(/\bcatch\s*\(\s*(\w+)/g)) names.add(m[1]);
   for (const m of text.matchAll(
      /\.catch\(\s*(?:async\s*)?\(?\s*(\w+)\s*\)?\s*=>/g,
   )) {
      names.add(m[1]);
   }
   return [...names];
}

/**
 * Each logger call in `text`, `logger.<level>(...)` or with its level
 * chosen at the call (`logger[level](...)`), with the line it starts on.
 */
function loggerCalls(text: string): { text: string; line: number }[] {
   const calls: { text: string; line: number }[] = [];
   const start = /logger(\.(debug|info|warn|error)|\[[^\]]*\])\(/g;
   for (let m = start.exec(text); m; m = start.exec(text)) {
      let depth = 0;
      let i = m.index + m[0].length - 1;
      for (; i < text.length; i++) {
         const c = text[i];
         if (c === "(") depth++;
         else if (c === ")" && --depth === 0) break;
         else if (c === "/" && text[i + 1] === "/") {
            i = text.indexOf("\n", i);
         } else if (c === "/" && text[i + 1] === "*") {
            i = text.indexOf("*/", i) + 1;
         } else if (c === '"' || c === "'" || c === "`") {
            for (i++; i < text.length && text[i] !== c; i++) {
               if (text[i] === "\\") i++;
            }
         }
      }
      calls.push({
         text: text.slice(m.index, i + 1),
         line: text.slice(0, m.index).split("\n").length,
      });
   }
   return calls;
}

/** Every non-spec module under materialization/, relative to it. */
function modules(): string[] {
   return fs
      .readdirSync(__dirname, { recursive: true, encoding: "utf8" })
      .filter((f) => f.endsWith(".ts") && !f.endsWith(".spec.ts"))
      .sort();
}

describe("materialization logs an error's code, never its text", () => {
   it("has no logger call carrying an error's message, stack, the error itself, or what it wraps", () => {
      const offenders: string[] = [];
      for (const file of modules()) {
         const text = fs.readFileSync(path.join(__dirname, file), "utf8");
         const caught = caughtNames(text).map(rawErrorOf);
         for (const call of loggerCalls(text)) {
            if (
               RAW_ERROR.test(call.text) ||
               caught.some((raw) => raw.test(call.text))
            ) {
               offenders.push(`${file}:${call.line}`);
            }
         }
      }
      expect(offenders).toEqual([]);
   });

   it("logs through the logger, never the console", () => {
      const consoled = modules().filter((file) =>
         /\bconsole\.\w+\(/.test(
            fs.readFileSync(path.join(__dirname, file), "utf8"),
         ),
      );
      expect(consoled).toEqual([]);
   });

   it("follows a caught error whatever it is named", () => {
      const text =
         "try { f(); } catch (failure) { logger.warn('x', { failure }); }\n" +
         "p.catch((problem) => logger.warn('y', { detail: problem.message }));\n" +
         "q.catch((oops) => logger.warn('z', { code: errorCodeOf(oops) }));";
      const caught = caughtNames(text);
      expect(caught.sort()).toEqual(["failure", "oops", "problem"]);
      const flagged = loggerCalls(text).filter((c) =>
         caught.map(rawErrorOf).some((raw) => raw.test(c.text)),
      );
      expect(flagged.map((c) => c.text)).toEqual([
         "logger.warn('x', { failure })",
         "logger.warn('y', { detail: problem.message })",
      ]);
   });

   it("recognises each way an error's text reaches a log call", () => {
      const raw = [
         "logger.warn('x', { error: errMessage(err) })",
         "logger.warn('x', { error: err.message })",
         "logger.warn('x', { error: redacted(err) })",
         "logger.warn('x', { error })",
         "logger.warn('x', { a, error, b })",
         "logger.warn('x', { error: err })",
         "logger.warn('x', { cleanupError: cleanupErr })",
         "logger.warn('x', { reason: String(err) })",
         "logger.warn(`x: ${String(e)}`)",
         "logger.warn(`x: ${err}`)",
         "logger.warn('x', { cause: err.cause })",
         "logger.warn('x', { err })",
         "logger.warn('x', { a, ex })",
         "logger.warn('x', { reason: err })",
         "logger.warn('x', { failure: e })",
         "logger.warn('x', { stack: err.stack })",
         "logger.warn('x', { detail: err.toString() })",
         "logger.warn('x', { detail: JSON.stringify(err) })",
         'logger.warn("x", err)',
         "logger[level]('x', { error: errMessage(err) })",
         "logger.error('x', error)",
         "logger.warn('x', { reason: String(ex) })",
         "logger.warn('x', { detail: inspect(err) })",
         "logger.warn('x', { detail: util.inspect(error) })",
         "logger.warn('x', { error: variant.error })",
      ];
      expect(raw.filter((t) => !RAW_ERROR.test(t))).toEqual([]);
      expect(
         loggerCalls("a; logger[quiet ? 'debug' : 'warn']('x', { err });"),
      ).toEqual([
         { text: "logger[quiet ? 'debug' : 'warn']('x', { err })", line: 1 },
      ]);
      // A comment's apostrophe does not open a string.
      expect(
         loggerCalls(
            "logger.warn('x', {\n // the run's record\n a });\nf(e.error);",
         ),
      ).toEqual([
         { text: "logger.warn('x', {\n // the run's record\n a })", line: 1 },
      ]);
      const clean = [
         "logger.warn('x', { code: errorCodeOf(err) })",
         "logger.warn('x', { errorCode })",
         "logger.warn('x', { restoreCode: errorCodeOf(restoreErr), stack: stackFrames(err) })",
         "logger.info('x', { reason: refusal.reason })",
         "logger.debug('x', { code: (err as NodeJS.ErrnoException).code })",
         "logger.error('x', { code: errorCodeOf(failure.error) })",
         "logger.warn('x', { packageName, sourceName })",
      ];
      expect(clean.filter((t) => RAW_ERROR.test(t))).toEqual([]);
   });
});
