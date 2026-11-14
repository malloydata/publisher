// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * UI-side value for a given. Mirrors the JS shapes the server accepts for
 * `givens` runtime values, plus `Date` (serialized before send: see
 * `givensToRequest`).
 *
 * Its own module because every control, codec, and hook in the givens path
 * speaks it, and it should not have to be imported from whichever hook happens
 * to be canonical this week.
 */
export type GivenValue = string | number | boolean | Date | null;

/**
 * A value the HOST sets for a given no control shows. Only a host can send a
 * list (`GivenValue` leaves arrays out on purpose: a control's value must
 * round-trip through a URL), and the server accepts one for a `string[]` given.
 */
export type HostGivenValue =
   | GivenValue
   | readonly (string | number | boolean)[];
