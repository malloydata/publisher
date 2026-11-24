// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT
//
// Numbers, formatted once from each metric's definition: a KPI, an axis, a tooltip and a table cell all read the same
// format, so a metric never shows as $4.7M in one place and 4740000 in another. No DOM.

import { config } from './state.js'

const cache = new Map()
const nf = (opts) => {
  const k = JSON.stringify(opts)
  if (!cache.has(k)) cache.set(k, new Intl.NumberFormat('en-US', opts))
  return cache.get(k)
}

/**
 * A metric's format: 'usd' (or any ISO currency code in lower case, 'eur'), 'number', 'percent' (a fraction: 0.25 is
 * 25%), 'decimal', or { unit: 'min' } for a number with a unit. Copy it from the model's own tag: `# currency=usd` is
 * 'usd', `# percent` is 'percent'.
 */
export function format(kind, v, { compact = false, digits } = {}) {
  if (v === null || v === undefined || !Number.isFinite(Number(v))) return '–'
  v = Number(v)
  const abs = Math.abs(v)
  if (kind === 'percent') {
    const d = digits ?? (abs >= 0.1 ? 1 : abs >= 0.01 ? 1 : 2)
    return nf({ style: 'percent', minimumFractionDigits: d, maximumFractionDigits: d }).format(v)
  }
  if (kind && typeof kind === 'object' && kind.unit) return `${format('decimal', v, { compact, digits })} ${kind.unit}`
  const cur = typeof kind === 'string' && /^[a-z]{3}$/.test(kind) && kind !== 'pct' ? kind.toUpperCase() : null
  if (compact && abs >= 10000) {
    return nf({ notation: 'compact', maximumFractionDigits: abs >= 1e9 || abs < 1e5 ? 1 : abs < 1e6 ? 0 : 2, ...(cur ? { style: 'currency', currency: cur } : {}) }).format(v)
  }
  if (cur) {
    const d = digits ?? (abs < 100 ? 2 : 0)
    return nf({ style: 'currency', currency: cur, minimumFractionDigits: d, maximumFractionDigits: d }).format(v)
  }
  if (kind === 'decimal') return nf({ maximumFractionDigits: digits ?? (abs < 10 ? 2 : abs < 100 ? 1 : 0) }).format(v)
  return nf({ maximumFractionDigits: digits ?? (abs < 10 && !Number.isInteger(v) ? 2 : 0) }).format(v)
}

/** The definition of a metric by its model name, with a label spelled from the name when the app gives none. */
export function metric(name) {
  const m = config?.metrics?.[name] || {}
  return { name, label: m.label || name.replace(/_/g, ' ').replace(/^./, (c) => c.toUpperCase()), format: m.format || 'number', good: m.good ?? 'up', ...m }
}

/** A metric's value as text: fmtMetric('total_sales', 4740000, { compact: true }) -> "$4.74M". */
export const fmtMetric = (name, v, opts) => format(metric(name).format, v, opts)

/**
 * A figure in three parts for setting large: [symbol, number, unit]. $1.47M is ['$', '1.47', 'M']; 52.3% is
 * ['', '52.3', '%']; 9.6 min is ['', '9.6', ' min']. The page sets the symbol and unit a step smaller.
 */
export function parts(name, v, { compact = true } = {}) {
  const t = fmtMetric(name, v, { compact })
  if (t === '–') return ['', '–', '']
  const m = /^([−-]?)([^\d]*)([\d.,]+)(.*)$/.exec(t)
  return m ? [`${m[1] ? '−' : ''}${m[2]}`, m[3], m[4]] : ['', t, '']
}
/** A formatter for an axis or a chart's labels: compact, in the metric's format. */
export const axisFormat = (name) => (v) => fmtMetric(name, v, { compact: true, ...(Number.isInteger(v) && metric(name).format !== 'percent' ? { digits: 0 } : {}) })

/**
 * The change from `prev` to `cur` for a metric: { text, tone, dir }. A percentage metric changes in points ("+1.2 pts"),
 * anything else by its relative change ("+12.4%"). tone is 'good', 'bad' or 'flat' from the metric's `good`
 * direction, so a fall in cost reads as good; a metric with good: null is never coloured.
 */
export function delta(name, cur, prev) {
  const m = metric(name)
  if (cur === null || cur === undefined || prev === null || prev === undefined || !Number.isFinite(cur) || !Number.isFinite(prev)) return { text: 'no comparison', tone: 'flat', dir: 0 }
  const pts = m.format === 'percent'
  const d = pts ? (cur - prev) * 100 : prev === 0 ? null : (cur - prev) / Math.abs(prev)
  if (d === null) return { text: cur === 0 ? 'no change' : 'new', tone: 'flat', dir: Math.sign(cur) }
  const dir = Math.abs(d) < (pts ? 0.05 : 0.0005) ? 0 : Math.sign(d)
  if (!dir) return { text: 'no change', tone: 'flat', dir: 0 }
  const sign = dir > 0 ? '+' : dir < 0 ? '−' : ''
  const text = pts ? `${sign}${nf({ maximumFractionDigits: 1, minimumFractionDigits: 1 }).format(Math.abs(d))} pts` : `${sign}${nf({ style: 'percent', maximumFractionDigits: 1, minimumFractionDigits: 1 }).format(Math.abs(d))}`
  const tone = !dir || !m.good ? 'flat' : (dir > 0) === (m.good === 'up') ? 'good' : 'bad'
  // Past ten times, a percentage stops meaning anything to a reader: say how many times as much.
  if (!pts && d > 9) return { text: `${nf({ maximumFractionDigits: d >= 99 ? 0 : 1 }).format(d + 1)}×`, tone, dir }
  return { text, tone, dir }
}

/**
 * Where a value stands against a metric's thresholds: 'ok', 'watch' or 'breach', or null with none. thresholds is
 * { watch, breach } in the metric's own units; good: 'up' means a value below them is the problem.
 */
export function status(name, v) {
  const m = metric(name)
  const t = m.thresholds
  if (!t || v === null || v === undefined) return null
  const worse = (a, b) => (m.good === 'down' ? a >= b : a <= b)
  if (t.breach !== undefined && worse(v, t.breach)) return 'breach'
  if (t.watch !== undefined && worse(v, t.watch)) return 'watch'
  return 'ok'
}
export const STATUS_WORDS = { ok: 'On track', watch: 'Watch', breach: 'Breaching' }
