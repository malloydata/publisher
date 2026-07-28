/**
 * Between a given's value and the two wire forms it takes: a URL query
 * parameter, and the `givens` map on a query request.
 *
 * Given state is URL-addressable so a filtered view is a link you can send —
 * and so a drill can arrive somewhere with its filter already applied. A URL
 * carries strings, while a given is typed, so a value makes a round trip
 * through here on the way out to the address bar and back.
 *
 * The declared type is what decides the reading: `"100"` is the number 100 for
 * a `number` given and the string `"100"` for a `filter<number>`, whose values
 * are filter syntax rather than numbers.
 */

import type { GivenValue } from "../../hooks/givenValue";
import { isFilterType } from "./filterValue";

/**
 * A given's value as a query-parameter string, or undefined when there is
 * nothing to put in the URL (an unset given, which the server fills from the
 * model default).
 *
 * An empty string is a real value, not an absence: for a `filter<…>` it is the
 * empty filter, i.e. "All", which is meaningfully different from leaving the
 * declared default in place.
 */
export function givenToParam(
   value: GivenValue | undefined,
): string | undefined {
   if (value === undefined || value === null) return undefined;
   if (value instanceof Date) return value.toISOString().slice(0, 10);
   if (Array.isArray(value)) return value.map(String).join(",");
   return String(value);
}

/** Read a query-parameter string back as a value of the declared type. */
export function paramToGiven(
   type: string | undefined,
   raw: string,
): GivenValue {
   // A filter's value IS a string of filter syntax, whatever it filters on, so
   // it never gets coerced to the inner type.
   if (isFilterType(type)) return raw;

   switch (type) {
      case "number": {
         const parsed = Number(raw);
         // A URL is user-editable, so a value that is not a number is passed
         // through rather than turned into NaN; the server rejects it with a
         // message naming the given, which beats a silently empty control.
         return Number.isFinite(parsed) ? parsed : raw;
      }
      case "boolean":
         return raw === "true";
      case "date":
      case "timestamp":
      case "timestamptz": {
         const parsed = new Date(raw);
         return Number.isNaN(parsed.getTime()) ? raw : parsed;
      }
      default:
         if ((type ?? "").startsWith("array<")) {
            return raw === "" ? [] : raw.split(",");
         }
         return raw;
   }
}

/**
 * The whole control row as query parameters, dropping unset givens so a
 * default-state dashboard has a clean URL.
 */
export function givensToParams(
   values: ReadonlyMap<string, GivenValue>,
): Record<string, string> {
   const params: Record<string, string> = {};
   for (const [name, value] of values) {
      const encoded = givenToParam(value);
      if (encoded !== undefined) params[name] = encoded;
   }
   return params;
}

/**
 * A `Date` in the spelling the server accepts for the given's declared type.
 *
 * The three time types take three different forms, and each rejects the other
 * two, so this cannot be one blanket `toISOString()`:
 *
 * - `date` wants a bare `YYYY-MM-DD`, and rejects anything longer.
 * - `timestamp` is naive: an offset or a trailing `Z` is rejected outright,
 *   with a message pointing at `timestamptz`.
 * - `timestamptz` is the instant, and is the only one that takes the full ISO
 *   string.
 *
 * Everything is read in UTC, matching the controls, which pick dates through
 * dayjs's UTC plugin.
 */
function dateToRequest(value: Date, type: string | undefined): string {
   const iso = value.toISOString();
   switch (type) {
      case "timestamptz":
         return iso;
      case "timestamp":
         // Same instant with the zone marker dropped, which is what "naive"
         // means here.
         return iso.slice(0, -1);
      default:
         return iso.slice(0, 10);
   }
}

/**
 * Control values as the `givens` map a query request takes.
 *
 * Unlike the URL form this keeps types — a `number` given goes over as a
 * number — because the server type-checks each value against its declaration.
 * Only `Date` needs converting, since JSON has no date, and converting it
 * needs the declared type (see {@link dateToRequest}).
 *
 * `only` narrows the map to a named subset, which is how a composite tile is
 * run with just the givens it references instead of the whole control row.
 */
export function givensToRequest(
   values: ReadonlyMap<string, GivenValue>,
   declaredTypes: ReadonlyMap<string, string | undefined>,
   only?: readonly string[],
): Record<string, unknown> {
   const names = only ? new Set(only) : undefined;
   const request: Record<string, unknown> = {};
   for (const [name, value] of values) {
      if (names && !names.has(name)) continue;
      if (value === null || value === undefined) continue;
      request[name] =
         value instanceof Date
            ? dateToRequest(value, declaredTypes.get(name))
            : value;
   }
   return request;
}

/**
 * Read query parameters into control values, keeping only the names the
 * dashboard actually declares.
 *
 * A URL can name anything; binding a given the model does not surface fails the
 * query outright, so an unrecognized parameter is ignored rather than passed
 * along. That also means an unrelated query parameter on the page's URL — a
 * tracking tag, say — cannot break the dashboard.
 */
export function paramsToGivens(
   params: Record<string, string>,
   declaredTypes: ReadonlyMap<string, string | undefined>,
): Map<string, GivenValue> {
   const values = new Map<string, GivenValue>();
   for (const [name, raw] of Object.entries(params)) {
      if (!declaredTypes.has(name)) continue;
      values.set(name, paramToGiven(declaredTypes.get(name), raw));
   }
   return values;
}
