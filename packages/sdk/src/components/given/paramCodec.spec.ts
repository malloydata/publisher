import { describe, expect, it } from "bun:test";
import dayjs from "dayjs";
import utc from "dayjs/plugin/utc";
import type { GivenValue } from "../../hooks/givenValue";
import {
   givenToParam,
   givensToParams,
   givensToRequest,
   paramToGiven,
   paramsToGivens,
   pickedDayToUtc,
} from "./paramCodec";

dayjs.extend(utc);

describe("givenToParam", () => {
   it("omits what the server should fill from the model default", () => {
      expect(givenToParam(undefined, "string")).toBe(undefined);
      expect(givenToParam(null, "string")).toBe(undefined);
   });

   it("keeps an empty string, which is a real value", () => {
      // For a filter this is the empty filter: "All", and is meaningfully
      // different from leaving the declaration's default in place.
      expect(givenToParam("", "filter<string>")).toBe("");
   });

   it("renders each value type as one parameter", () => {
      expect(givenToParam("Nike", "string")).toBe("Nike");
      expect(givenToParam(42, "number")).toBe("42");
      expect(givenToParam(false, "boolean")).toBe("false");
      expect(givenToParam(new Date("2024-03-01T00:00:00Z"), "date")).toBe(
         "2024-03-01",
      );
   });

   it("refuses an array rather than joining it into one parameter", () => {
      // `GivenValue` used to admit `string[]`, and this joined on `,` with no
      // escaping, which the codec cannot undo: `paramToGiven` never returns an
      // array for ANY type, so the list came back as a single string. Sha-Bang's
      // case is the one that shows it is not merely lossy but unrecoverable,
      // because the value itself contains the separator: `["Ben & Jerry, Inc",
      // "Nike"]` joined to `Ben & Jerry, Inc,Nike`, which splits back to three
      // values, not two.
      //
      // The type no longer promises arrays. Cast here because this pins the
      // runtime guard for a JavaScript caller the types cannot reach.
      const asGiven = (value: unknown) => value as never;
      expect(givenToParam(asGiven(["a", "b"]), "string")).toBeUndefined();
      expect(
         givenToParam(asGiven(["Ben & Jerry, Inc", "Nike"]), "string"),
      ).toBeUndefined();
      expect(givenToParam(asGiven([1, 2]), "number")).toBeUndefined();
   });
});

describe("pickedDayToUtc", () => {
   // dayjs in LOCAL mode, which is what MUI's adapter actually hands the
   // control: the clicked day carrying the reference time-of-day.
   const clicked = (iso: string, hour: number) =>
      dayjs(iso).hour(hour).minute(0).second(0);

   it("carries the previous value's time of day onto the new day", () => {
      // A DatePicker offers no way to re-enter a time, so zeroing it silently
      // moved a `timestamp` given's boundary to midnight on every day change.
      const previous = new Date("2024-01-01T14:45:30.250Z");
      const day = { year: () => 2024, month: () => 5, date: () => 9 };
      expect(pickedDayToUtc(day, previous).toISOString()).toBe(
         "2024-06-09T14:45:30.250Z",
      );
   });

   it("uses midnight when there is no previous time to carry", () => {
      const day = { year: () => 2024, month: () => 5, date: () => 9 };
      expect(pickedDayToUtc(day).toISOString()).toBe(
         "2024-06-09T00:00:00.000Z",
      );
   });

   it("reads the day's own fields back under UTC, in any zone", () => {
      // The zone-carrying tests below only bite where the local offset actually
      // crosses midnight, so at UTC they are vacuous and CI runs at UTC. This
      // one holds the invariant everywhere: the fields that went in are the UTC
      // fields that come out.
      //
      // Measured, rather than assumed, by reverting `pickedDayToUtc` to the
      // `toDate()` it replaced and counting failures in this file: UTC 3,
      // Asia/Kolkata 3, America/Los_Angeles 4, Australia/Sydney 5. So the zone
      // tests do discriminate (Sydney catches two that a UTC run cannot), and a
      // half-hour offset adds nothing over UTC at these hours: the useful pair
      // is one zone ahead of UTC and one behind.
      //
      // The variable is offset DIRECTION, not zone count. Adding zones for
      // symmetry buys nothing: Kolkata scores identically to UTC despite being
      // the most exotic-looking of the four. If only one non-UTC zone can be
      // afforded, pick a far-ahead one.
      const day = { year: () => 2024, month: () => 0, date: () => 5 };
      const stored = pickedDayToUtc(day);
      expect([
         stored.getUTCFullYear(),
         stored.getUTCMonth(),
         stored.getUTCDate(),
      ]).toEqual([2024, 0, 5]);
   });

   it("stores the day that was clicked, whatever the reader's offset", () => {
      const day = clicked("2024-01-05", 8);
      expect(givenToParam(pickedDayToUtc(day), "date")).toBe("2024-01-05");
   });

   it("survives an evening pick, which shifts the other way behind UTC", () => {
      const day = clicked("2024-01-05", 22);
      expect(givenToParam(pickedDayToUtc(day), "date")).toBe("2024-01-05");
   });

   it("is what `toDate()` is not", () => {
      // Pins the defect itself: in an ahead-of-UTC zone the old expression
      // really does produce a different day, so this test is meaningful there
      // and vacuous at UTC. Asserted as a round-trip either way.
      const day = clicked("2024-01-05", 8);
      expect(givenToParam(day.toDate(), "date")).toBe(
         dayjs(day.toDate()).utc().format("YYYY-MM-DD"),
      );
      expect(givenToParam(pickedDayToUtc(day), "date")).toBe("2024-01-05");
   });
});

/** Every spelling the grammar admits, with the instant it must resolve to. */
const TEMPORAL_CASES: ReadonlyArray<readonly [string, string]> = [
   ["2024-01-05T10:30:00Z", "2024-01-05T10:30:00.000Z"],
   ["2024-01-05 10:30:00 UTC", "2024-01-05T10:30:00.000Z"],
   ["2024-01-05 10:30:00 GMT", "2024-01-05T10:30:00.000Z"],
   ["2024-01-05 10:30:00", "2024-01-05T10:30:00.000Z"],
   ["2024-01-05", "2024-01-05T00:00:00.000Z"],
   ["2024-01-05T10:30:00+05:30", "2024-01-05T05:00:00.000Z"],
   ["2024-01-05T10:30:00+0530", "2024-01-05T05:00:00.000Z"],
   ["2026-08-10 12:00:00.123+00", "2026-08-10T12:00:00.123Z"],
   ["2026-08-10 12:00:00-05", "2026-08-10T17:00:00.000Z"],
   ["2024-01-05T10:30:00.123456", "2024-01-05T10:30:00.123Z"],
];

describe("parseTemporal zone gate", () => {
   it("does not mistake a meridiem for a zone", () => {
      // The gate first read `\s[A-Z]{2,5}$` as "carries a zone abbreviation",
      // which matches ` AM` and ` PM`, so this spelling reached `new Date` and
      // was read as LOCAL time: five hours apart between two readers, which is
      // the exact defect the gate exists to prevent.
      for (const raw of [
         "Dec 25 2024 10:30 AM",
         "Dec 25 2024 10:30 PM",
         "1/5/2024 8:00 AM",
      ]) {
         expect(paramToGiven("timestamp", raw)).toBe(raw);
      }
   });

   it("resolves every accepted spelling without asking the engine", () => {
      // The reason this module parses the zone itself. Outside strict ISO,
      // `new Date` is implementation-defined: `new Date("2024-01-05 10:30:00
      // UTC")` is the right instant in V8 and Bun and **Invalid Date in
      // Safari's JavaScriptCore**. A suite running under Bun cannot see that,
      // so this asserts the value we compute, never what an engine would.
      expect(TEMPORAL_CASES.length).toBeGreaterThan(0);
      for (const [raw, iso] of TEMPORAL_CASES) {
         const v = paramToGiven("timestamp", raw);
         expect(v instanceof Date && v.toISOString()).toBe(iso);
      }
   });

   it("reads the two unambiguous zone abbreviations", () => {
      // `2024-01-05 10:30:00 UTC` is what BigQuery prints for a TIMESTAMP, so
      // it is what a user pastes. Removing the abbreviation branch wholesale to
      // kill the ` AM` match broke it: the fix has to be an allow-list, not an
      // absence.
      for (const raw of [
         "2024-01-05 10:30:00 UTC",
         "2024-01-05 10:30:00 GMT",
      ]) {
         const v = paramToGiven("timestamp", raw);
         expect(v instanceof Date && v.toISOString()).toBe(
            "2024-01-05T10:30:00.000Z",
         );
      }
   });

   it("reads the two-digit offset Postgres and DuckDB print", () => {
      // `SELECT now()` prints `2026-08-10 12:00:00.123+00` in both, and a gate
      // demanding four offset digits rejected the default output of the two
      // databases Publisher ships with.
      const a = paramToGiven("timestamp", "2026-08-10 12:00:00.123+00");
      expect(a instanceof Date && a.toISOString()).toBe(
         "2026-08-10T12:00:00.123Z",
      );
      const b = paramToGiven("timestamp", "2026-08-10 12:00:00-05");
      expect(b instanceof Date && b.toISOString()).toBe(
         "2026-08-10T17:00:00.000Z",
      );
   });

   it("reads a half-hour offset in both spellings", () => {
      for (const raw of [
         "2024-01-05T10:30:00+05:30",
         "2024-01-05T10:30:00+0530",
      ]) {
         const v = paramToGiven("timestamp", raw);
         expect(v instanceof Date && v.toISOString()).toBe(
            "2024-01-05T05:00:00.000Z",
         );
      }
   });

   it("refuses an offset that is not a real one", () => {
      // `+99:99` was read as 99 hours 99 minutes, resolving four days off with
      // nothing to say so. Same principle as the parts check: refuse rather
      // than silently produce a different instant.
      for (const raw of [
         "2024-01-05T10:30:00+99:99",
         "2024-01-05T10:30:00+1599",
         "2024-01-05T10:30:00+15",
      ]) {
         expect(paramToGiven("timestamp", raw)).toBe(raw);
      }
   });

   it("accepts the real extremes of the offset range", () => {
      // Kiribati is +14:00 and Baker Island is -12:00.
      const a = paramToGiven("timestamp", "2024-01-05T10:30:00+1400");
      expect(a instanceof Date && a.toISOString()).toBe(
         "2024-01-04T20:30:00.000Z",
      );
      const b = paramToGiven("timestamp", "2024-01-05T10:30:00-1200");
      expect(b instanceof Date && b.toISOString()).toBe(
         "2024-01-05T22:30:00.000Z",
      );
   });

   it("refuses a zone on a bare date, which cannot mean anything", () => {
      // `2024-01-05+05` otherwise resolved to the 4th in UTC, so a `date` given
      // reported back a day earlier than what was written.
      expect(paramToGiven("date", "2024-01-05+05")).toBe("2024-01-05+05");
   });

   it("applies an offset across a month boundary", () => {
      const v = paramToGiven("timestamp", "2024-02-29T23:30:00-05");
      expect(v instanceof Date && v.toISOString()).toBe(
         "2024-03-01T04:30:00.000Z",
      );
   });

   it("reads the print form of every warehouse Publisher ships with", () => {
      for (const [raw, iso] of [
         ["2024-01-05 10:30:00 UTC", "2024-01-05T10:30:00.000Z"],
         ["2024-01-05 10:30:00.123456 UTC", "2024-01-05T10:30:00.123Z"],
         ["2026-08-10 12:00:00.123+00", "2026-08-10T12:00:00.123Z"],
         ["2026-08-10 12:00:00.123456+05:30", "2026-08-10T06:30:00.123Z"],
         ["2024-01-05 10:30:00.123 +0000", "2024-01-05T10:30:00.123Z"],
         ["2024-01-05 10:30:00", "2024-01-05T10:30:00.000Z"],
      ] as const) {
         const v = paramToGiven("timestamp", raw);
         expect(v instanceof Date && v.toISOString()).toBe(iso);
      }
   });

   it("does not treat a named regional zone as unambiguous", () => {
      // `CST` alone is three different offsets, so it is passed through and the
      // server names the given rather than the UI guessing.
      expect(paramToGiven("timestamp", "2024-01-05 10:30:00 CST")).toBe(
         "2024-01-05 10:30:00 CST",
      );
   });

   it("still reads a value carrying a real zone", () => {
      for (const [raw, iso] of [
         ["2024-01-05T10:30:00Z", "2024-01-05T10:30:00.000Z"],
         ["2024-01-05T10:30:00+05:00", "2024-01-05T05:30:00.000Z"],
      ] as const) {
         const v = paramToGiven("timestamp", raw);
         expect(v instanceof Date && v.toISOString()).toBe(iso);
      }
   });

   it("reads warehouse microsecond precision, truncating to milliseconds", () => {
      // Capping the pattern at three fractional digits made an ordinary
      // `...:00.123456` fail to parse at all.
      for (const raw of [
         "2024-01-05T10:30:00.123456",
         "2024-01-05 10:30:00.123456",
         "2024-01-05T10:30:00.123456789",
      ]) {
         const v = paramToGiven("timestamp", raw);
         expect(v instanceof Date && v.toISOString()).toBe(
            "2024-01-05T10:30:00.123Z",
         );
      }
   });
});

describe("paramToGiven temporal edges", () => {
   it("passes through a date whose parts do not exist rather than rolling it over", () => {
      // Date.UTC normalises instead of refusing, so without the parts check
      // this arrives as 2025-02-14 AND is reported back out, rewriting the
      // reader's URL to a date they never typed.
      for (const raw of ["2024-13-45", "2024-99-99", "2024-01-01 99:99"]) {
         expect(paramToGiven("date", raw)).toBe(raw);
      }
   });

   it("passes through a zone-less spelling it cannot read as UTC", () => {
      // `new Date("12/25/2024")` is LOCAL midnight, so this resolved to a
      // different instant for every reader.
      expect(paramToGiven("timestamp", "12/25/2024")).toBe("12/25/2024");
      expect(paramToGiven("timestamp", "Dec 25 2024")).toBe("Dec 25 2024");
   });

   it("still reads a value that states its own zone", () => {
      const v = paramToGiven("timestamp", "2024-12-25T00:00:00Z");
      expect(v instanceof Date && v.toISOString()).toBe(
         "2024-12-25T00:00:00.000Z",
      );
   });

   it("accepts the boundaries the check must not reject", () => {
      for (const raw of ["2024-02-29", "2024-12-31 23:59:59", "2024-01-01"]) {
         expect(paramToGiven("date", raw)).toBeInstanceOf(Date);
      }
   });
});

describe("paramToGiven", () => {
   it("leaves a filter as the filter syntax it is", () => {
      // `filter<number>` values are filter expressions, not numbers, so the
      // inner type must not pull them through a numeric coercion.
      expect(paramToGiven("filter<number>", ">= 100")).toBe(">= 100");
      expect(paramToGiven("filter<string>", "us-east, us-west")).toBe(
         "us-east, us-west",
      );
      expect(paramToGiven("filter<date>", "2024-03-01")).toBe("2024-03-01");
   });

   it("reads plain types back as their type", () => {
      expect(paramToGiven("number", "42")).toBe(42);
      expect(paramToGiven("boolean", "true")).toBe(true);
      expect(paramToGiven("boolean", "false")).toBe(false);
      expect(paramToGiven("string", "Nike")).toBe("Nike");
      expect(paramToGiven("date", "2024-03-01")).toBeInstanceOf(Date);
   });

   it("leaves an unrecognized type as the raw string", () => {
      // Including `array<…>`, which no server can emit: `malloyGivenToApi`
      // renders a non-filter given as the bare `type.type`. Splitting on `,`
      // here would corrupt any value containing one, for no reachable gain.
      expect(paramToGiven("array<string>", "a,b")).toBe("a,b");
      expect(paramToGiven("record", "x")).toBe("x");
      expect(paramToGiven(undefined, "x")).toBe("x");
   });

   it("passes a nonsense value through for the server to reject", () => {
      // A URL is hand-editable. A NaN or an Invalid Date would reach the server
      // as null and read as "unset"; the raw text gets an error naming the
      // given instead, which is the more useful failure.
      expect(paramToGiven("number", "abc")).toBe("abc");
      // `Number("")` is 0, not NaN, so an empty parameter would otherwise
      // arrive as a deliberate zero and filter on it. It now reads as UNSET,
      // which serves that purpose better: `""` was still being sent.
      expect(paramToGiven("number", "")).toBeNull();
      expect(paramToGiven("number", "   ")).toBe("   ");
      expect(paramToGiven("date", "not-a-date")).toBe("not-a-date");
   });
});

describe("paramsToGivens", () => {
   const declared = new Map<string, string | undefined>([
      ["REGION", "filter<string>"],
      ["LIMIT", "number"],
   ]);

   it("reads the declared givens out of a URL", () => {
      expect(
         paramsToGivens({ REGION: "us-east", LIMIT: "10" }, declared),
      ).toEqual(
         new Map<string, GivenValue>([
            ["REGION", "us-east"],
            ["LIMIT", 10],
         ]),
      );
   });

   it("ignores a parameter the dashboard does not declare", () => {
      // Binding an undeclared given fails the query, and an unrelated query
      // parameter on the page URL must not be able to break the dashboard.
      expect(
         paramsToGivens({ REGION: "us-east", utm_source: "email" }, declared),
      ).toEqual(new Map<string, GivenValue>([["REGION", "us-east"]]));
   });
});

describe("givensToParams", () => {
   it("drops unset givens so a default view has a clean URL", () => {
      const values = new Map<string, GivenValue>([
         ["REGION", "us-east"],
         ["LIMIT", null],
      ]);
      expect(
         givensToParams(
            values,
            new Map([
               ["REGION", "filter<string>"],
               ["LIMIT", "number"],
            ]),
         ),
      ).toEqual({ REGION: "us-east" });
   });
});

describe("givensToRequest", () => {
   const values = new Map<string, GivenValue>([
      ["REGION", "us-east"],
      ["LIMIT", 10],
      ["SINCE", new Date("2024-03-01T04:05:06.007Z")],
   ]);
   const types = new Map<string, string | undefined>([
      ["REGION", "filter<string>"],
      ["LIMIT", "number"],
      ["SINCE", "date"],
   ]);

   it("keeps types, unlike the URL form", () => {
      expect(givensToRequest(values, types)).toEqual({
         REGION: "us-east",
         LIMIT: 10,
         SINCE: "2024-03-01",
      });
   });

   it("narrows to a tile's own givens", () => {
      // A composite tile must be run with only the givens it references.
      expect(givensToRequest(values, types, ["REGION"])).toEqual({
         REGION: "us-east",
      });
      expect(givensToRequest(values, types, [])).toEqual({});
   });

   // The server takes a different spelling for each of the three time types and
   // rejects the other two, so one blanket ISO string fails two of them.
   it.each([
      ["date", "2024-03-01"],
      ["timestamp", "2024-03-01T04:05:06.007"],
      ["timestamptz", "2024-03-01T04:05:06.007Z"],
   ])("encodes a Date for a %s given", (type, expected) => {
      const request = givensToRequest(
         new Map<string, GivenValue>([
            ["SINCE", new Date("2024-03-01T04:05:06.007Z")],
         ]),
         new Map([["SINCE", type]]),
      );
      expect(request).toEqual({ SINCE: expected });
   });
});

describe("a Date in the URL keeps the precision its type carries", () => {
   it("truncates a `date` given to the day, which is its whole spelling", () => {
      expect(givenToParam(new Date("2024-03-05T14:30:00Z"), "date")).toBe(
         "2024-03-05",
      );
   });

   it("keeps the time of day for timestamp and timestamptz", () => {
      // Encoding every Date as `YYYY-MM-DD` truncated a timestamp given the
      // moment it round-tripped through the address bar, so the query re-ran at
      // midnight against a different instant than the reader had chosen.
      const at1430 = new Date("2024-03-05T14:30:00Z");
      expect(givenToParam(at1430, "timestamp")).toBe("2024-03-05T14:30:00.000");
      expect(givenToParam(at1430, "timestamptz")).toBe(
         "2024-03-05T14:30:00.000Z",
      );
   });

   it("round-trips a timestamp through the URL without moving it", () => {
      const at1430 = new Date("2024-03-05T14:30:00Z");
      const param = givenToParam(at1430, "timestamp") as string;
      const back = paramToGiven("timestamp", param) as Date;
      expect(back.toISOString()).toBe(at1430.toISOString());
   });

   it("uses each given's own declared type across the whole row", () => {
      const at1430 = new Date("2024-03-05T14:30:00Z");
      const params = givensToParams(
         new Map([
            ["DAY", at1430],
            ["MOMENT", at1430],
         ]),
         new Map([
            ["DAY", "date"],
            ["MOMENT", "timestamptz"],
         ]),
      );
      expect(params).toEqual({
         DAY: "2024-03-05",
         MOMENT: "2024-03-05T14:30:00.000Z",
      });
   });
});

describe("a naive timestamp is read as UTC, in any timezone", () => {
   // A LIMITATION worth knowing: under TZ=UTC the correct and the broken
   // implementations are indistinguishable, because local time IS UTC there. So
   // these assertions only bite when the suite runs outside UTC, and a UTC CI
   // cannot catch a regression here. Verified by hand across UTC,
   // America/New_York, Asia/Kolkata and Pacific/Auckland; to exercise it:
   //     TZ=America/New_York bun test src/components/given/paramCodec.spec.ts
   // Running one suite under a non-UTC zone in CI would close that gap.
   //
   // Each assertion below names its expected value outright rather than
   // round-tripping, so it fails on its own terms rather than agreeing with
   // whatever the writer did. `parseTemporal`'s own doc carries the reason the
   // parser is built this way; this block only pins that it is.
   it("reads the naive spelling dateToRequest emits as the same instant", () => {
      const back = paramToGiven("timestamp", "2024-03-05T14:30:00.000") as Date;
      expect(back.toISOString()).toBe("2024-03-05T14:30:00.000Z");
   });

   it("still honours a zone marker when one is present", () => {
      const utc = paramToGiven(
         "timestamptz",
         "2024-03-05T14:30:00.000Z",
      ) as Date;
      expect(utc.toISOString()).toBe("2024-03-05T14:30:00.000Z");
      const offset = paramToGiven(
         "timestamptz",
         "2024-03-05T14:30:00+02:00",
      ) as Date;
      expect(offset.toISOString()).toBe("2024-03-05T12:30:00.000Z");
   });

   it("reads a date-only value as UTC midnight", () => {
      const day = paramToGiven("date", "2024-03-05") as Date;
      expect(day.toISOString()).toBe("2024-03-05T00:00:00.000Z");
   });

   it("still passes nonsense through for the server to reject", () => {
      expect(paramToGiven("timestamp", "not-a-date")).toBe("not-a-date");
   });
});

describe("a boolean param that is neither true nor false", () => {
   it("passes through instead of being coerced to false", () => {
      // A URL is hand-editable. Coercing `?ACTIVE=yes` to `false` sends the
      // OPPOSITE of what was typed, with no complaint; the raw text gets an
      // error naming the given, which is how the number and date cases behave.
      expect(paramToGiven("boolean", "yes")).toBe("yes");
      expect(paramToGiven("boolean", "1")).toBe("1");
      expect(paramToGiven("boolean", "TRUE")).toBe("TRUE");
      // `""` is the exception: an empty parameter means UNSET for a boolean,
      // so it is dropped rather than passed through. Sending `{"FLAG": ""}` put
      // a value on the wire that no boolean can be, while the checkbox showed
      // the model default.
      expect(paramToGiven("boolean", "")).toBeNull();
   });
});

describe("a datetime with no zone marker, however it is spelled", () => {
   it("reads a space-separated datetime as UTC, like the T-separated one", () => {
      // The spelling a reader copies out of a Malloy literal or a rendered
      // cell. Keying the detection on the literal `T` let this one through as
      // local time, so the same shared link resolved to a different instant for
      // every reader and the URL was rewritten to the shifted value.
      const spaced = paramToGiven("timestamp", "2024-01-15 14:30") as Date;
      const tee = paramToGiven("timestamp", "2024-01-15T14:30") as Date;
      expect(spaced.toISOString()).toBe("2024-01-15T14:30:00.000Z");
      expect(tee.toISOString()).toBe(spaced.toISOString());
   });

   it("still honours an explicit offset or Z", () => {
      expect(
         (
            paramToGiven("timestamptz", "2024-01-15T14:30:00+02:00") as Date
         ).toISOString(),
      ).toBe("2024-01-15T12:30:00.000Z");
      expect(
         (
            paramToGiven("timestamptz", "2024-01-15T14:30:00Z") as Date
         ).toISOString(),
      ).toBe("2024-01-15T14:30:00.000Z");
   });
});

describe("an empty query parameter", () => {
   // `?FLAG=` was reaching the request as `{"FLAG": ""}`, which no boolean,
   // number or date can be, while the control showed the model default or an
   // empty box. The screen and the request disagreed and nothing said so.
   // Decided here, once, for every type: what `?X=` MEANS is a property of the
   // type, not of the widget. The controls keep their own `value !== ""`
   // guards, which is not duplication, because `""` still reaches them by two
   // routes: a host passing it in directly, and this codec itself for `string`
   // and `filter<…>`, where the empty string is a value a user can mean.
   it("means unset for a type that has no empty value", () => {
      for (const type of ["boolean", "number", "date", "timestamp"]) {
         expect(paramToGiven(type, "")).toBeNull();
      }
   });

   it("is dropped from the request rather than sent", () => {
      const values = new Map([["X", paramToGiven("boolean", "")]]);
      expect(givensToRequest(values, new Map([["X", "boolean"]]))).toEqual({});
   });

   it("stays a real value for a string, where the user can mean it", () => {
      expect(paramToGiven("string", "")).toBe("");
      expect(paramToGiven("filter<string>", "")).toBe("");
      expect(
         givensToRequest(new Map([["X", ""]]), new Map([["X", "string"]])),
      ).toEqual({ X: "" });
   });
});

describe("values that survive a real URL, not just a string literal", () => {
   /** What `useSearchParams` actually hands the page for a pasted value. */
   const throughUrl = (raw: string) =>
      new URLSearchParams(`T=${raw}`).get("T") as string;

   it("reads an eastern offset, which the URL turns into a space", () => {
      // `+` in a query string IS a space, so a hand-pasted `+05:00` arrives as
      // ` 05:00`. Refusing it made the parser work for the western half of the
      // world only: `-05:00` survived the address bar and `+05:00` did not.
      const v = paramToGiven(
         "timestamp",
         throughUrl("2024-01-05T10:30:00+05:00"),
      );
      expect(v instanceof Date && v.toISOString()).toBe(
         "2024-01-05T05:30:00.000Z",
      );
   });

   it("agrees with itself east and west", () => {
      const east = paramToGiven(
         "timestamp",
         throughUrl("2024-01-05T10:30:00+05"),
      );
      const west = paramToGiven(
         "timestamp",
         throughUrl("2024-01-05T10:30:00-05"),
      );
      expect(east instanceof Date && east.toISOString()).toBe(
         "2024-01-05T05:30:00.000Z",
      );
      expect(west instanceof Date && west.toISOString()).toBe(
         "2024-01-05T15:30:00.000Z",
      );
   });

   it("does not read a stray trailing number as an offset", () => {
      // The rewrite only fires when a FULL datetime precedes the group.
      expect(paramToGiven("timestamp", "2024-01-05 10")).toBe("2024-01-05 10");
   });
});

describe("offsets that exist, and offsets that do not", () => {
   it("refuses a westward offset past -12:00", () => {
      // The range is not symmetric: -12:00 (Baker Island) to +14:00 (Kiribati).
      for (const raw of [
         "2024-01-05T10:30:00-1300",
         "2024-01-05T10:30:00-1400",
      ]) {
         expect(paramToGiven("timestamp", raw)).toBe(raw);
      }
   });

   it("still accepts both real extremes", () => {
      const east = paramToGiven("timestamp", "2024-01-05T10:30:00+1400");
      const west = paramToGiven("timestamp", "2024-01-05T10:30:00-1200");
      expect(east instanceof Date && east.toISOString()).toBe(
         "2024-01-04T20:30:00.000Z",
      );
      expect(west instanceof Date && west.toISOString()).toBe(
         "2024-01-05T22:30:00.000Z",
      );
   });
});

describe("a `date` given and the zone rules that apply to it", () => {
   it("refuses one that shifts the calendar day", () => {
      // Resolving it lands on the 4th, and because the value is reported back
      // outward the reader's own URL was rewritten to a day they never wrote.
      for (const raw of [
         "2024-01-05T00:30:00+05:30",
         "2024-01-05T23:30:00-05:00",
         "2024-01-05+05",
      ]) {
         expect(paramToGiven("date", raw)).toBe(raw);
      }
   });

   it("accepts one that leaves the day alone", () => {
      // Refusing EVERY zone was the previous attempt and it was far too broad:
      // it rejected `2024-01-05T00:00:00.000Z`, which is exactly what
      // `Date#toISOString()` emits, so any host or link generator serializing a
      // JS date produced a value the control painted red.
      for (const raw of [
         "2024-01-05T00:00:00.000Z",
         "2024-01-05T00:00:00Z",
         "2024-01-05 10:00:00 UTC",
         "2024-01-05T10:00:00+00:00",
         "2024-01-05T10:00:00+05:00",
      ]) {
         const v = paramToGiven("date", raw);
         expect(v instanceof Date && v.toISOString().slice(0, 10)).toBe(
            "2024-01-05",
         );
      }
   });

   it("resolves a raw `+` on a bare date as a time of day, not an offset", () => {
      // The docs are written in query-string notation, so this is the form a
      // reader actually types. Verifying the rule with a string LITERAL missed
      // it: `?SINCE=2024-01-05+00:00` is delivered as `2024-01-05 00:00`, a
      // bare date plus a valid time, and resolves rather than being refused.
      // Same class as the defect the `+` repair exists for: a value behaves
      // differently as a string literal than it does through a URL.
      const delivered = new URLSearchParams("T=2024-01-05+00:00").get(
         "T",
      ) as string;
      expect(delivered).toBe("2024-01-05 00:00");
      const v = paramToGiven("date", delivered);
      expect(v instanceof Date && v.toISOString()).toBe(
         "2024-01-05T00:00:00.000Z",
      );
   });

   it("refuses any zone on a BARE date, day-preserving or not", () => {
      // A separate rule from the day-shift one, and easy to state backwards: an
      // offset qualifies a time of day, and a bare date has none, so `Z` and
      // `+00:00` are refused here too.
      for (const raw of [
         "2024-01-05Z",
         "2024-01-05 UTC",
         "2024-01-05+00:00",
         "2024-01-05-00:00",
      ]) {
         expect(paramToGiven("date", raw)).toBe(raw);
      }
      // With a time on it, a day-preserving zone reads normally.
      expect(paramToGiven("date", "2024-01-05T00:00:00Z")).toBeInstanceOf(Date);
   });

   it("reports a refused value back the way it was typed", () => {
      // Not in the mangled space form the URL delivered, or the reader's own
      // address bar loses the `+` on the next report.
      const delivered = new URLSearchParams("T=2024-01-05T00:30:00+05:30").get(
         "T",
      ) as string;
      expect(paramToGiven("date", delivered)).toBe("2024-01-05T00:30:00+05:30");
   });

   it("still reads the spellings a date can actually have", () => {
      for (const raw of ["2024-01-05", "2024-01-05 10:30:00"]) {
         expect(paramToGiven("date", raw)).toBeInstanceOf(Date);
      }
   });

   it("leaves timestamp types resolving their zone as before", () => {
      for (const type of ["timestamp", "timestamptz"]) {
         const v = paramToGiven(type, "2024-01-05T00:30:00+05:30");
         expect(v instanceof Date && v.toISOString()).toBe(
            "2024-01-04T19:00:00.000Z",
         );
      }
   });
});

describe("the empty-parameter rule agrees with the type switch", () => {
   it('keeps `""` for any type the switch passes through untouched', () => {
      // As an allowlist this named three types and treated everything else as
      // unset, while the switch's `default` arm passes an unrecognised type
      // through: a deliberate `""` on such a type did not survive the round trip.
      for (const type of ["json", "unknown_future_type"]) {
         expect(paramToGiven(type, "")).toBe("");
      }
   });

   it("still reads it as unset for the types with no empty value", () => {
      for (const type of [
         "boolean",
         "number",
         "date",
         "timestamp",
         "timestamptz",
      ]) {
         expect(paramToGiven(type, "")).toBeNull();
      }
   });
});

describe("a bare date with a zone, which is a rule of its own", () => {
   it("is refused for every temporal type, not just `date`", () => {
      // The guard lives in `parseTemporal`, above the type-specific day check,
      // so it is not a `date` rule. Kept in its own describe for that reason: a
      // refactor of the `date` block must not carry away the only coverage of a
      // guard that is type-independent.
      for (const type of ["date", "timestamp", "timestamptz"]) {
         expect(paramToGiven(type, "2024-01-05Z")).toBe("2024-01-05Z");
         expect(paramToGiven(type, "2024-01-05+00:00")).toBe(
            "2024-01-05+00:00",
         );
      }
   });

   it("does not apply once the value carries a time", () => {
      // The distinction the docs and the `case "date"` comment both dropped:
      // an offset needs a time of day to qualify, so the same zone reads
      // normally as soon as there is one.
      for (const type of ["date", "timestamp", "timestamptz"]) {
         expect(paramToGiven(type, "2024-01-05T00:00:00Z")).toBeInstanceOf(
            Date,
         );
      }
   });
});

describe("whitespace, which is not the same as empty", () => {
   it("does not collapse a whitespace-only temporal parameter to empty", () => {
      // `restoreUrlPlus` trims to line its pattern up, and returning the
      // trimmed text turned ` ` into `""` AFTER the empty-means-unset check had
      // already let it past. The given was then sent as `""`: an empty date the
      // control showed as unset, so the screen and the query disagreed.
      for (const type of ["date", "timestamp", "timestamptz"]) {
         expect(paramToGiven(type, " ")).toBe(" ");
         expect(paramToGiven(type, "\t ")).toBe("\t ");
         expect(paramToGiven(type, "")).toBeNull();
      }
   });

   it("reads leading whitespace the same way whether or not a `+` was eaten", () => {
      // These two differ only in whether the URL ate an offset, and they used
      // to disagree: the restoring path trimmed and the non-restoring path did
      // not, so a leading space was fatal to one and harmless to the other.
      expect(paramToGiven("timestamp", " 2024-01-05T10:30:00Z")).toEqual(
         new Date("2024-01-05T10:30:00.000Z"),
      );
      expect(paramToGiven("timestamp", " 2024-01-05T10:30:00 05:00")).toEqual(
         new Date("2024-01-05T05:30:00.000Z"),
      );
      // Trailing was always fine, and stays fine.
      expect(paramToGiven("date", "2024-01-05 ")).toEqual(
         new Date("2024-01-05T00:00:00.000Z"),
      );
   });

   it("still puts back a `+` the address bar ate", () => {
      // The trim above exists for this, so it has to keep working.
      expect(paramToGiven("timestamptz", "2024-03-01 12:30 05:30")).toEqual(
         new Date("2024-03-01T07:00:00.000Z"),
      );
   });
});
