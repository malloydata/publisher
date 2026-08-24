// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * Between a given's value and the two wire forms it takes: a URL query
 * parameter, and the `givens` map on a query request.
 *
 * Given state is URL-addressable so a filtered view is a link you can send,
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
   // Required, not optional: the only thing this decides is a Date's spelling,
   // and a caller that forgot it would silently truncate a timestamp to its
   // date, which is the defect this parameter exists to fix. Better to fail to
   // compile than to lose the time of day again.
   type: string | undefined,
): string | undefined {
   if (value === undefined || value === null) return undefined;
   // Written in the same spelling the request body uses, so the URL keeps a
   // timestamp's time of day. Encoding every Date as `YYYY-MM-DD` truncated a
   // `timestamp` or `timestamptz` given the moment it round-tripped through the
   // address bar, and the query then re-ran at midnight against a different
   // instant than the reader had chosen.
   if (value instanceof Date) return dateToRequest(value, type);
   // Refused, not joined. `GivenValue` used to admit `string[]` and this joined
   // on `,` with no escaping, which this codec cannot undo: `paramToGiven`
   // never returns an array for any type, so the list came back as one string,
   // and a value CONTAINING a comma ("Ben & Jerry, Inc") could not even be
   // split back to the right count. The type no longer promises arrays; this
   // stays as a guard for a JavaScript caller the types cannot reach, where
   // dropping the parameter is at least visible as "unset" rather than sending
   // a value the reader never chose.
   if (Array.isArray(value)) return undefined;
   return String(value);
}

/** The three calendar fields a picked day is read from, as dayjs exposes them. */
export interface PickedDay {
   year(): number;
   /** Zero-based, as `Date` and dayjs both have it. */
   month(): number;
   date(): number;
}

/**
 * The instant to store for a calendar day the user picked in a date control.
 *
 * Built in UTC from the day's own fields, NOT by taking the instant off the
 * picker. MUI's adapter resolves its default timezone to LOCAL here (this SDK
 * loads dayjs's `utc` plugin but not `timezone`), so the value it hands back is
 * local midnight-ish at the reference time-of-day, while both the read side of
 * the control and {@link dateToRequest} take the UTC day. Those two readings
 * differ by a day for any offset that crosses midnight: in Sydney, clicking
 * 5 January stored 4 January and the field then redrew as the 4th, so the click
 * appeared to jump backwards.
 */
export function pickedDayToUtc(day: PickedDay, previous?: Date): Date {
   // The time of day is carried over from the value being replaced, because a
   // DatePicker offers no way to re-enter it. Zeroing it instead silently moved
   // a `timestamp` given's boundary back to midnight every time the user
   // adjusted the day, which is a different filter, not a re-spelling of the
   // same one. Nothing to carry (a fresh pick, or a `date` given) means
   // midnight, which is what a bare calendar day means anyway.
   return new Date(
      Date.UTC(
         day.year(),
         day.month(),
         day.date(),
         previous?.getUTCHours() ?? 0,
         previous?.getUTCMinutes() ?? 0,
         previous?.getUTCSeconds() ?? 0,
         previous?.getUTCMilliseconds() ?? 0,
      ),
   );
}

/**
 * The spellings {@link parseTemporal} accepts: an ISO-shaped civil datetime with
 * an optional zone suffix. Deliberately strict, and deliberately the only thing
 * that decides.
 */
const TEMPORAL =
   /^(\d{4})-(\d{1,2})-(\d{1,2})(?:[T\s]+(\d{1,2}):(\d{2})(?::(\d{2}))?(?:\.(\d{1,9}))?)?\s*(Z|[+-]\d{2}:?\d{2}|[+-]\d{2}|UTC|GMT)?$/i;

/**
 * A value with its offset `+` put back, if the address bar ate one.
 *
 * A `+` in a query string means a SPACE, so an offset survives only when the
 * writer percent-encoded it. `URLSearchParams` hands us `2024-01-05T10:30:00
 * 05:00` for a hand-pasted `+05:00`, and refusing that made the parser work for
 * the western half of the world and not the eastern: `-05:00` arrived intact
 * and `+05:00` did not, with nothing on screen to say the URL had eaten it
 * rather than the grammar rejecting it.
 *
 * Applied only when a full datetime precedes the group, so nothing else can
 * match, and reached only by the three temporal types (`date`, `timestamp`,
 * `timestamptz`), so it cannot touch a `string` or `filter<…>` value. Kept
 * apart from `parseTemporal` so a REFUSED value is still reported back as it was
 * typed, rather than in the mangled space form the URL delivered.
 */
function restoreUrlPlus(raw: string): string {
   const trimmed = raw.trim();
   // Whitespace and nothing else: hand back `raw` untouched. Trimming it yields
   // `""`, which has already passed the empty-means-unset check above, so the
   // given was sent as an empty date while its control read as unset.
   //
   // Only that case. An earlier form of this guard returned `raw` whenever no
   // `+` was restored, which stopped trimming ordinary values too: ` 2024-01-05`
   // was then refused while ` 2024-01-05T10:30:00 05:00` still parsed, since the
   // restoring path trimmed and the non-restoring path no longer did. Same
   // leading space, opposite answers, decided by whether the URL had eaten a `+`.
   if (trimmed === "") return raw;
   return trimmed.replace(
      /^(\d{4}-\d{1,2}-\d{1,2}[T\s]+\d{1,2}:\d{2}(?::\d{2})?(?:\.\d{1,9})?)\s+(\d{2}:?\d{2}|\d{2})$/,
      "$1+$2",
   );
}

/**
 * A date or datetime parameter as an instant, or undefined when it is not one.
 *
 * The zone is parsed HERE rather than handed to `new Date`, which is the point
 * of this function and took four attempts to reach. Outside the exact ISO
 * grammar `new Date` is implementation-defined, so delegating to it made the
 * answer depend on the reader's browser. Measured on this machine:
 * `new Date("2024-01-05 10:30:00 UTC")`, which is what BigQuery prints for a
 * TIMESTAMP, gives the right instant in V8 and in Bun and **Invalid Date in
 * Safari's JavaScriptCore**. A unit suite running under Bun cannot see that, so
 * a green test bench would have shipped it.
 *
 * Everything the earlier attempts got wrong is a case here:
 *
 * - **No zone means UTC**, built from the value's own parts. `new Date` reads an
 *   offset-less datetime as LOCAL, so one shared link resolved to a different
 *   instant per reader and was rewritten to the shifted one on the next report.
 * - **The parts are checked, not just parsed.** `Date.UTC` normalises rather
 *   than refusing, so `2024-13-45` arrived as 2025-02-14, silently repairing a
 *   mistyped link to a date nobody chose.
 * - **A meridiem is not a zone.** A gate of `\s[A-Z]{2,5}$` matches ` AM`, which
 *   put `Dec 25 2024 10:30 AM` back to being read as local time.
 * - **Two-digit offsets count.** `+00` and `-05` are what Postgres and DuckDB
 *   print for a TIMESTAMPTZ, and demanding four digits rejected the default
 *   output of the two databases Publisher ships with.
 *
 * A spelling this grammar does not cover (`Dec 25 2024`, `12/25/2024`) is passed
 * through, and the server names the given in its error. Narrower than what V8
 * alone would take, on purpose: an answer that depends on the browser is worse
 * than a refusal that does not.
 */
function parseTemporal(raw: string, calendarDay = false): Date | undefined {
   // A `+` in a query string means a SPACE, so an offset survives the address
   // bar only when the writer percent-encoded it. `URLSearchParams` hands us
   // `2024-01-05T10:30:00 05:00` for a hand-pasted `+05:00`, and refusing that
   // made the parser work for the western half of the world and not the
   // eastern: `-05:00` arrived intact and `+05:00` did not, with nothing on
   // screen to say the URL had eaten it rather than the grammar. Read back as
   // `+` only when a full datetime precedes it, so nothing else can match.
   const text = restoreUrlPlus(raw);
   if (text === "") return undefined;

   const m = TEMPORAL.exec(text);
   if (!m) return undefined;
   const [, y, mo, d, h, mi, sec, frac, zone] = m;

   const parts = {
      year: Number(y),
      month: Number(mo),
      day: Number(d),
      hour: Number(h ?? 0),
      minute: Number(mi ?? 0),
      second: Number(sec ?? 0),
      // Truncated to milliseconds, which is all a `Date` holds. Accepting more
      // digits than that is the point: warehouses print microseconds as a
      // matter of course, and capping the pattern at three meant a perfectly
      // ordinary `...:00.123456` failed to parse at all.
      milli: Number((frac ?? "").slice(0, 3).padEnd(3, "0") || 0),
   };

   const civil = Date.UTC(
      parts.year,
      parts.month - 1,
      parts.day,
      parts.hour,
      parts.minute,
      parts.second,
      parts.milli,
   );
   // Read the parts back off the result. `Date.UTC` normalises rather than
   // refusing, so `2024-13-45` became 2025-02-14 and, because the value is
   // reported back outward, a mistyped link was silently REPAIRED to a date the
   // reader never typed.
   const built = new Date(civil);
   const survived =
      built.getUTCFullYear() === parts.year &&
      built.getUTCMonth() === parts.month - 1 &&
      built.getUTCDate() === parts.day &&
      built.getUTCHours() === parts.hour &&
      built.getUTCMinutes() === parts.minute &&
      built.getUTCSeconds() === parts.second;
   if (!survived) return undefined;

   // A zone qualifies a time of day, so it is meaningless on a bare date and
   // that combination is refused rather than resolved: `2024-01-05+05`
   // otherwise became 2024-01-04T19:00Z, reported back as the 4th, a day
   // earlier than what was written and silently rewritten into the URL.
   //
   // This catches the literal spelling, NOT the one a URL delivers. A pasted
   // `2024-01-05+05:00` arrives as `2024-01-05 05:00`, which is a well-formed
   // time of day and is read as one; `restoreUrlPlus` deliberately does not
   // fire without a full datetime in front, because there is nothing to
   // distinguish the two readings and guessing wrong is worse than either.
   if (zone !== undefined && h === undefined) return undefined;

   const offset = zoneOffsetMinutes(zone);
   if (offset === undefined) return undefined;
   const instant = new Date(civil - offset * 60_000);

   // For a `date` given, and only for a value that reached here CARRYING A TIME
   // (a bare date with a zone was already refused above, for every type), refuse
   // a zone just when applying it MOVES THE DAY. The qualifier is spelled out
   // rather than left implied by position: stating this rule without it reads
   // as "a `date` accepts `Z`", which is false for a bare date.
   //
   // Refusing every zone was the previous attempt and it was far too broad: it
   // rejected `Z`, `UTC` and `+00:00`, so `2024-01-05T00:00:00.000Z`, which is
   // exactly what `Date#toISOString()` emits and therefore what any host or
   // link generator produces, stopped resolving at all. What the day-shift
   // defect actually needs is this comparison, and nothing wider.
   if (
      calendarDay &&
      zone !== undefined &&
      instant.getUTCDate() !== parts.day
   ) {
      return undefined;
   }
   return instant;
}

/**
 * Minutes east of UTC for a matched zone suffix; absent means the value is
 * naive, so UTC. Undefined for a suffix that is shaped like an offset but is
 * not one.
 *
 * The range check is the same principle as the parts-survived check above:
 * refuse rather than silently produce a different instant. Without it `+99:99`
 * was read as 99 hours and 99 minutes, so `2024-01-05T10:30:00+99:99` resolved
 * to 1 January, four days off, with nothing to say so. Real offsets run from
 * -12:00 to +14:00 (Kiribati is the outer edge) and their minutes are always
 * under 60.
 */
function zoneOffsetMinutes(zone: string | undefined): number | undefined {
   if (zone === undefined) return 0;
   const z = zone.toUpperCase();
   if (z === "Z" || z === "UTC" || z === "GMT") return 0;
   const m = /^([+-])(\d{2}):?(\d{2})?$/.exec(z);
   if (!m) return undefined;
   const mins = Number(m[3] ?? 0);
   if (mins > 59) return undefined;
   const minutes = Number(m[2]) * 60 + mins;
   // Not symmetric, whatever the tidier `Math.abs` version would suggest. The
   // real range runs -12:00 (Baker Island) to +14:00 (Kiribati), so a single
   // ±14:00 bound admitted -13:00 and -14:00, which exist nowhere.
   const east = m[1] !== "-";
   if (minutes > (east ? 14 * 60 : 12 * 60)) return undefined;
   return east ? minutes : -minutes;
}

/**
 * Whether `""` is a value the user can mean for this type, rather than "unset".
 *
 * A DENYLIST, to agree with `paramToGiven`'s `default` arm. As an allowlist it
 * named the three types that keep `""` and treated everything else as unset,
 * while the switch below passes any unrecognised type through untouched: a
 * deliberate `""` on a type neither knew about was written to the URL and read
 * back as unset. Naming the types that cannot hold an empty value keeps the two
 * in step as new types arrive.
 */
function isEmptyStringMeaningful(type: string | undefined): boolean {
   return !EMPTY_MEANS_UNSET.has(type ?? "");
}

/** Types with no empty value, so `?X=` on one of them means "unset". */
const EMPTY_MEANS_UNSET = new Set([
   "boolean",
   "number",
   "date",
   "timestamp",
   "timestamptz",
]);

/** Read a query-parameter string back as a value of the declared type. */
export function paramToGiven(
   type: string | undefined,
   raw: string,
): GivenValue {
   // An empty parameter means UNSET for every type that has no empty value,
   // so it is dropped here rather than carried as `""`. `?FLAG=` was reaching
   // the request as `{"FLAG": ""}`, which no boolean, number or date can be,
   // while the control showed the model default or an empty box: the screen
   // and the request disagreed, and nothing on screen said so. `string` and
   // `filter<...>` are deliberately excluded, because there the empty string is
   // a value a user can mean, and typing it is how they ask for it.
   if (raw === "" && !isEmptyStringMeaningful(type)) return null;

   // A filter's value IS a string of filter syntax, whatever it filters on, so
   // it never gets coerced to the inner type.
   if (isFilterType(type)) return raw;

   switch (type) {
      case "number": {
         // `Number("")` is 0, not NaN, so an empty parameter would arrive as a
         // deliberate zero and filter accordingly. Treated as unparseable
         // instead, which is what the pass-through below is for.
         const parsed = raw.trim() === "" ? NaN : Number(raw);
         // A URL is user-editable, so a value that is not a number is passed
         // through rather than turned into NaN; the server rejects it with a
         // message naming the given, which beats a silently empty control.
         return Number.isFinite(parsed) ? parsed : raw;
      }
      case "boolean":
         // Passed through when it is neither, the way the number and date cases
         // deliberately do: a URL is hand-editable, and coercing `?ACTIVE=yes`
         // to `false` sends the OPPOSITE of what was typed with no complaint.
         // The raw text gets an error naming the given instead.
         if (raw === "true") return true;
         if (raw === "false") return false;
         return raw;
      case "date":
      case "timestamp":
      case "timestamptz": {
         // A URL is hand-editable, so a value that is not a date is passed
         // through rather than becoming an Invalid Date; the server rejects it
         // with a message naming the given.
         //
         // A `date` refuses a zone that would MOVE THE DAY, which is why the
         // type reaches here at all. A date is a calendar day, so resolving
         // `2024-01-05T00:30:00+05:30` lands on the 4th, and because the value
         // is reported back outward the reader's address bar was then rewritten
         // to a day they never wrote. On a value that CARRIES A TIME, a zone
         // leaving the day alone reads normally, `Z` and `+00:00` included:
         // refusing every zone was tried and was far too broad, since it
         // rejected `Date#toISOString()` output. A zone on a BARE date is a
         // separate rule and is always refused, for every temporal type, since
         // an offset has no time of day to qualify. Both comparisons live in
         // `parseTemporal`.
         // The RESTORED text on refusal, not `raw`: otherwise a value the URL
         // mangled is reported straight back out in its mangled form, so the
         // reader's address bar loses the `+` they typed.
         const text = restoreUrlPlus(raw);
         return parseTemporal(text, type === "date") ?? text;
      }
      default:
         // No `array<…>` case. `malloyGivenToApi` renders a non-filter given
         // as the bare `type.type`, so `"array<…>"` is a spelling the server
         // cannot produce, and Malloy emits only scalar parameter types for
         // givens anyway. The branch that used to be here split on `,` with no
         // escaping, so it was also the one place a value could be corrupted
         // on the URL round trip.
         return raw;
   }
}

/**
 * The whole control row as query parameters, dropping unset givens so a
 * default-state dashboard has a clean URL.
 */
export function givensToParams(
   values: ReadonlyMap<string, GivenValue>,
   declaredTypes: ReadonlyMap<string, string | undefined>,
): Record<string, string> {
   const params: Record<string, string> = {};
   for (const [name, value] of values) {
      const encoded = givenToParam(value, declaredTypes.get(name));
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
 * Unlike the URL form this keeps types: a `number` given goes over as a
 * number, because the server type-checks each value against its declaration.
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
      // Arrays are refused HERE too, not just in `givenToParam`. `GivenValue`
      // no longer admits one, but the two functions are the URL side and the
      // REQUEST side of the same rule, and fixing only the URL side left an
      // array from a JavaScript caller filtering every query while being
      // invisible in the address bar: the two disagreed about the same value,
      // which is the drift this pair exists to avoid.
      if (Array.isArray(value)) continue;
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
 * along. That also means an unrelated query parameter on the page's URL (a
 * tracking tag, say) cannot break the dashboard.
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
