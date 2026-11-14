// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { escapeMalloyString } from "./malloyString";

/**
 * A value as a Malloy literal: `'text'`, `42`, `true`, `@2024-01-31`,
 * `@2024-01-31 09:30:00`, or `f'…'` for a `filter<…>` type.
 *
 * With `type` — a given's declared type — the value is spelled the way that
 * type reads it, so `"42"` for a `number` given is `42` and `true` for a
 * `boolean` is `true`. Without it the JavaScript type decides, which is what a
 * clicked cell's value needs. Undefined when there is nothing to write: no
 * value, or one the type cannot spell.
 */
export function malloyLiteral(
   value: unknown,
   type?: string,
): string | undefined {
   if (value === undefined || value === null || value === "") return undefined;
   const quote = (text: string) => `'${escapeMalloyString(text)}'`;
   const scalar =
      type === undefined
         ? inferredType(value)
         : type.startsWith("filter<")
           ? "filter"
           : type;
   switch (scalar) {
      case "filter":
         return malloyFilterLiteral(String(value));
      case "string":
         return quote(String(value));
      case "number": {
         const n = typeof value === "number" ? value : Number(value);
         return Number.isFinite(n) ? String(n) : undefined;
      }
      case "boolean":
         return value === true || value === "true" ? "true" : "false";
      case "date": {
         const day = datePart(value);
         return day && `@${day}`;
      }
      case "timestamp": {
         const day = datePart(value);
         if (!day) return undefined;
         const time =
            value instanceof Date
               ? value.toISOString().slice(11, 19)
               : (/[T ](\d{2}:\d{2}(?::\d{2})?)/.exec(String(value))?.[1] ??
                 "00:00:00");
         return `@${day} ${time}`;
      }
      default:
         return undefined;
   }
}

function inferredType(value: unknown): string | undefined {
   if (typeof value === "string") return "string";
   if (typeof value === "number") return "number";
   if (typeof value === "boolean") return "boolean";
   if (value instanceof Date && !Number.isNaN(value.getTime()))
      return value.toISOString().slice(11, 19) === "00:00:00"
         ? "date"
         : "timestamp";
   return undefined;
}

/**
 * Filter text as a Malloy filter literal: `f'…'`.
 *
 * Malloy hands the text between the quotes to the filter parser as written, so
 * the filter grammar's own escapes (`\,` for a comma that is part of a value,
 * `\%` for a percent sign) have to reach it untouched. Only a quote the text
 * leaves bare is escaped, and a lone trailing backslash is doubled so it cannot
 * swallow the closing quote. Escaping filter text the way a string literal is
 * escaped doubles every backslash, and `a\,b`, the one value "a,b", runs as
 * "a\" or "b".
 */
export function malloyFilterLiteral(filter: string): string {
   let body = "";
   for (let i = 0; i < filter.length; i++) {
      const char = filter[i];
      if (char === "\\") {
         body += i + 1 < filter.length ? char + filter[++i] : "\\\\";
      } else {
         body += char === "'" ? "\\'" : char;
      }
   }
   return `f'${body}'`;
}

/** `2024-01-31` out of a Date (UTC) or an ISO-ish string, else undefined. */
function datePart(value: unknown): string | undefined {
   if (value instanceof Date)
      return Number.isNaN(value.getTime())
         ? undefined
         : value.toISOString().slice(0, 10);
   return /^(\d{4}-\d{2}-\d{2})/.exec(String(value))?.[1];
}

/** An ISO date or timestamp, the only text a Malloy `@` literal is written from. */
const ISO_DATE_OR_TIMESTAMP =
   /^\d{4}-\d{2}-\d{2}(?:[ T]\d{2}:\d{2}(?::\d{2}(?:\.\d+)?)?)?$/;

/**
 * A filter value as a Malloy literal; unlike `malloyLiteral`, a Date is spelled
 * as a local-time day. A string is a quoted string, whatever it holds, except
 * where `isDate` says the dimension is a date and the string is an ISO date or
 * timestamp, which is written as a date literal (`@2024-01-31`). Any other
 * string stays quoted with `isDate` too: written after `@` unquoted, a value
 * like `2024-01-31 or true` became Malloy.
 */
export function formatMalloyValue(
   value: string | number | boolean | Date | null | undefined,
   isDate: boolean = false,
): string {
   if (value === null || value === undefined) {
      return "null";
   }

   if (value instanceof Date) {
      return `@${value.getFullYear()}-${String(value.getMonth() + 1).padStart(2, "0")}-${String(value.getDate()).padStart(2, "0")}`;
   }

   if (
      isDate &&
      typeof value === "string" &&
      ISO_DATE_OR_TIMESTAMP.test(value)
   ) {
      return `@${value}`;
   }

   if (typeof value === "string") {
      return `'${escapeMalloyString(value)}'`;
   }

   if (typeof value === "number") {
      return String(value);
   }

   return value ? "true" : "false";
}
