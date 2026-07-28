/**
 * UI-side value for a given. Mirrors the JS shapes the server accepts for
 * `givens` runtime values, plus `Date` (serialized before send — see
 * `givensToRequest`).
 *
 * Its own module because every control, codec, and hook in the givens path
 * speaks it, and it should not have to be imported from whichever hook happens
 * to be canonical this week.
 */
export type GivenValue =
   | string
   | number
   | boolean
   | Date
   | string[]
   | number[]
   | null;
