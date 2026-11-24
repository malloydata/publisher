// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT
//
// Periods: what "last 30 days" and "compared with" mean, worked out from the data's latest date rather than today's,
// so an app over data that ends in the past still shows full periods. No DOM: test it with node.
//
// A date is a 'YYYY-MM-DD' string, read and written in UTC. A range is { from, to } with `to` exclusive: the day after
// the last day it covers. Every range a query gets comes from here.

const MS = 86400000
const MONTHS = ['Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun', 'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec']

export const toDate = (s) => new Date(`${String(s).slice(0, 10)}T00:00:00Z`)
export const iso = (d) => d.toISOString().slice(0, 10)
export const addDays = (s, n) => iso(new Date(toDate(s).getTime() + n * MS))
export const days = (r) => Math.round((toDate(r.to) - toDate(r.from)) / MS)
/** The same day `n` months on, held to the month's last day (31 March less a month is 28 or 29 February). */
export function addMonths(s, n) {
  const d = toDate(s)
  const y = d.getUTCFullYear()
  const m = d.getUTCMonth() + n
  const last = new Date(Date.UTC(y, m + 1, 0)).getUTCDate()
  return iso(new Date(Date.UTC(y, m, Math.min(d.getUTCDate(), last))))
}
const startOf = (s, unit) => {
  const d = toDate(s)
  const y = d.getUTCFullYear()
  const m = d.getUTCMonth()
  if (unit === 'month') return iso(new Date(Date.UTC(y, m, 1)))
  if (unit === 'quarter') return iso(new Date(Date.UTC(y, m - (m % 3), 1)))
  return iso(new Date(Date.UTC(y, 0, 1)))
}

/**
 * The periods a reader can pick. Each ends on the data's latest day (`last`), so "Last 30 days" is the 30 days up to
 * and including it. `to-date` periods start at the start of that day's month, quarter or year; the `last_` calendar
 * periods are the whole one before it.
 */
export const PRESETS = {
  last_7: { label: 'Last 7 days', short: '7 days', range: (last) => ({ from: addDays(last, -6), to: addDays(last, 1) }) },
  last_28: { label: 'Last 4 weeks', short: '4 weeks', range: (last) => ({ from: addDays(last, -27), to: addDays(last, 1) }) },
  last_91: { label: 'Last 13 weeks', short: '13 weeks', range: (last) => ({ from: addDays(last, -90), to: addDays(last, 1) }) },
  last_364: { label: 'Last 52 weeks', short: '52 weeks', range: (last) => ({ from: addDays(last, -363), to: addDays(last, 1) }) },
  mtd: { label: 'Month to date', unit: 'month', range: (last) => ({ from: startOf(last, 'month'), to: addDays(last, 1) }) },
  qtd: { label: 'Quarter to date', unit: 'quarter', range: (last) => ({ from: startOf(last, 'quarter'), to: addDays(last, 1) }) },
  ytd: { label: 'Year to date', unit: 'year', range: (last) => ({ from: startOf(last, 'year'), to: addDays(last, 1) }) },
  last_month: { label: 'Last full month', unit: 'month', full: true, range: (last) => ({ from: addMonths(startOf(last, 'month'), -1), to: startOf(last, 'month') }) },
  last_quarter: { label: 'Last full quarter', unit: 'quarter', full: true, range: (last) => ({ from: addMonths(startOf(last, 'quarter'), -3), to: startOf(last, 'quarter') }) },
  last_year: { label: 'Last full year', unit: 'year', full: true, range: (last) => ({ from: addMonths(startOf(last, 'year'), -12), to: startOf(last, 'year') }) },
  all: { label: 'All time', range: (last, first) => ({ from: first, to: addDays(last, 1) }) },
}
/** The menu's groups, a rule between each. */
export const PRESET_GROUPS = [['last_7', 'last_28', 'last_91', 'last_364'], ['mtd', 'qtd', 'ytd'], ['last_month', 'last_quarter', 'last_year'], ['all']]
export const COMPARES = {
  previous: 'Previous period',
  year: 'A year earlier',
  none: 'No comparison',
}
/** The comparison as the controls say it: "the 13 weeks before", "the same 13 weeks a year earlier". */
export function compareWords(period, range, mode) {
  if (mode === 'none') return 'nothing'
  const p = PRESETS[period.preset]
  const n = days(range)
  const span = p?.full ? { month: 'month', quarter: 'quarter', year: 'year' }[p.unit] : p?.unit ? `${p.label.toLowerCase().replace(' to date', '')} to the same day` : n % 7 === 0 ? `${n / 7} week${n === 7 ? '' : 's'}` : `${n} days`
  if (mode === 'year') return p?.full ? `the same ${span} a year earlier` : p?.unit ? 'the same days a year earlier' : `the same ${span} a year earlier`
  return p?.full ? `the ${span} before` : p?.unit ? `the same days of the ${p.unit} before` : `the ${span} before`
}
export const GRAINS = ['day', 'week', 'month', 'quarter', 'year']

/**
 * The range a period covers. `period` is { preset } or { preset: 'custom', from, to } with `to` inclusive as a reader
 * types it. Anything outside the data's span is held to it; an empty or reversed custom range falls back to 4 weeks.
 */
export function resolve(period, span) {
  const { first, last } = span
  if (period.preset === 'custom' && period.from && period.to) {
    let from = period.from < first ? first : period.from
    let to = addDays(period.to > last ? last : period.to, 1)
    if (from < to) return { from, to }
  }
  const p = PRESETS[period.preset] || PRESETS.last_28
  const r = p.range(last, first)
  return { from: r.from < first ? first : r.from, to: r.to }
}

/**
 * The range a period is compared with, or null. "Previous" is the same length just before it; for a to-date period it
 * is the same days of the month, quarter or year before (1-14 November against 1-14 October), and for a full calendar
 * period the one before. "Year" is the same dates a year earlier. Returns null when the comparison would start
 * before the data does, since a half-empty comparison reads as growth.
 */
export function compareRange(period, range, mode, span) {
  if (mode === 'none' || period.preset === 'all') return null
  let r
  // A year earlier: 52 weeks back for a period counted in days, so Mondays line up with Mondays; the same dates for a
  // calendar period.
  if (mode === 'year') r = PRESETS[period.preset]?.unit ? { from: addMonths(range.from, -12), to: addMonths(range.to, -12) } : { from: addDays(range.from, -364), to: addDays(range.to, -364) }
  else {
    const unit = PRESETS[period.preset]?.unit
    if (unit) {
      const back = unit === 'month' ? -1 : unit === 'quarter' ? -3 : -12
      r = { from: addMonths(range.from, back), to: addMonths(range.to, back) }
    } else r = { from: addDays(range.from, -days(range)), to: range.from }
  }
  return r.from < span.first ? null : r
}

/**
 * The grain a chart over this range reads best at, about 7 to 60 points. Days and weeks are counted back from the
 * period's last day, so every week is a whole week and lines up with the comparison's; months, quarters and years are
 * the calendar's, and one the period only partly covers is marked as partial.
 */
export function autoGrain(range) {
  const n = days(range)
  if (n <= 31) return 'day'
  if (n <= 371) return 'week'
  if (n <= 1100) return 'month'
  return 'quarter'
}
export const ROLLING = new Set(['day', 'week'])

/** The day after a bucket that starts on `start`, at `grain`. */
export function bucketEnd(start, grain) {
  if (grain === 'day') return addDays(start, 1)
  if (grain === 'week') return addDays(start, 7)
  if (grain === 'month') return addMonths(start, 1)
  if (grain === 'quarter') return addMonths(start, 3)
  return addMonths(start, 12)
}

/**
 * Whether a bucket covers only part of its grain inside the range: the week the range starts mid-way through, or the
 * month the data has not finished. A partial bucket is drawn and labelled as such, never read as a fall.
 */
export function isPartial(start, grain, range) {
  return String(start).slice(0, 10) < range.from || bucketEnd(String(start).slice(0, 10), grain) > range.to
}

/** A Malloy filter for a range on a date or timestamp field: field >= @from and field < @to. */
export function clause(field, range) {
  return `${field} >= @${range.from} and ${field} < @${range.to}`
}

/** "16 Aug – 14 Nov 2022", or "16 Aug 2021 – 14 Nov 2022" across a year; `to` is exclusive. */
export function describe(range) {
  if (!range) return ''
  const a = toDate(range.from)
  const b = toDate(addDays(range.to, -1))
  const d = (x, year) => `${x.getUTCDate()} ${MONTHS[x.getUTCMonth()]}${year ? ` ${x.getUTCFullYear()}` : ''}`
  if (iso(a) === iso(b)) return d(a, true)
  return `${d(a, a.getUTCFullYear() !== b.getUTCFullYear())} – ${d(b, true)}`
}

/** A bucket's label at its grain: "14 Nov", "Week of 13 Nov", "Nov 2022", "Q4 2022", "2022". */
export function bucketLabel(start, grain, { short = false } = {}) {
  const d = toDate(start)
  const y = d.getUTCFullYear()
  const mo = MONTHS[d.getUTCMonth()]
  if (grain === 'day') return `${d.getUTCDate()} ${mo}${short ? '' : ` ${y}`}`
  if (grain === 'week') return `${short ? '' : 'Week of '}${d.getUTCDate()} ${mo}${short ? '' : ` ${y}`}`
  if (grain === 'month') return `${mo} ${y}`
  if (grain === 'quarter') return `Q${Math.floor(d.getUTCMonth() / 3) + 1} ${y}`
  return String(y)
}

/** A full date for prose: "14 November 2022". */
export function longDate(s) {
  const d = toDate(s)
  const full = ['January', 'February', 'March', 'April', 'May', 'June', 'July', 'August', 'September', 'October', 'November', 'December']
  return `${d.getUTCDate()} ${full[d.getUTCMonth()]} ${d.getUTCFullYear()}`
}
