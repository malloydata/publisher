// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT
//
// The app's state and every query it runs. The state lives in the address, so any view can be linked and Back
// works; a value from the address or a click reaches a query only when the model returned it at load (`known`).
// No DOM: test it with node by calling configure() and setKnown() with sample values.

import * as P from './period.js'

/** The app's config, from dataApp(). */
export let config = null
/** What the model returned at load: { span: { first, last } | null, [filter key]: [values], [`${key}__n`]: counts }. */
export const known = { span: null }
/** The most values one filter takes from an address. */
export const MAX_PICKS = 100

export function configure(c) {
  config = c
  config.metrics ||= {}
  config.filters ||= []
  config.entities ||= {}
  config.defaults = { period: 'last_91', compare: 'previous', ...(c.defaults || {}) }
  // An entity is also a filter on its field, so its values are known and its page can be filtered to from anywhere.
  for (const [key, e] of Object.entries(config.entities)) {
    e.key = key
    e.label ||= key.replace(/_/g, ' ').replace(/^\w/, (c) => c.toUpperCase())
    if (!config.filters.some((f) => f.field === e.field)) config.filters.push({ key, field: e.field, label: e.label, hidden: true, format: e.name, nameField: e.nameField })
    else if (e.nameField) Object.assign(config.filters.find((f) => f.field === e.field), { nameField: e.nameField })
  }
}
export function setKnown(k) {
  Object.assign(known, k)
}
export const dated = () => Boolean(config.date && known.span)

// ---- Malloy text ----

/** A Malloy string literal. Malloy escapes with a backslash; it does not accept SQL's doubled quote. */
export function lit(v) {
  if (typeof v === 'number' || typeof v === 'boolean') return String(v)
  return "'" + String(v).replace(/\\/g, '\\\\').replace(/'/g, "\\'") + "'"
}
/** The source every query starts from, with the app's own measures when it defines any (config.extend). */
export const src = () => (config.extend ? `${config.source} extend { ${config.extend} }` : config.source)

// ---- State ----

/** The state with nothing chosen: the default period, comparison and filters (defaults.filters: { key: [values] }). */
export const DEFAULT = () => ({ page: config.pages[0].id, id: null, period: { preset: config.defaults.period }, compare: config.defaults.compare, grain: null, metric: null, filters: Object.fromEntries(Object.entries(config.defaults.filters || {}).map(([k, v]) => [k, [...v]])) })
const same = (a = [], b = []) => a.length === b.length && a.every((v, i) => String(v) === String(b[i]))

const RESERVED = new Set(['p', 'from', 'to', 'c', 'g', 'm'])
const filterSpec = (key) => config.filters.find((f) => f.key === key)
export const isKnown = (key, v) => (known[key] || []).some((k) => String(k) === String(v))
const knownValue = (key, v) => (known[key] || []).find((k) => String(k) === String(v))
/** The page a state shows, and the entity it is about when it is an entity's page. */
export const pageOf = (s) => config.pages.find((p) => p.id === s.page)
export const entityOf = (s) => {
  const p = pageOf(s)
  return p?.entity ? config.entities[p.entity] : null
}

/** The address for a state: #/page[/id]?p=…&c=…&g=…&m=…&<filter>=…; defaults are left out. */
export function toHash(s) {
  const d = DEFAULT()
  const q = new URLSearchParams()
  if (s.period.preset !== d.period.preset || s.period.preset === 'custom') q.set('p', s.period.preset)
  if (s.period.preset === 'custom') {
    q.set('from', s.period.from)
    q.set('to', s.period.to)
  }
  if (s.compare !== d.compare) q.set('c', s.compare)
  if (s.grain) q.set('g', s.grain)
  if (s.metric) q.set('m', s.metric)
  // A filter is in the address when it differs from its default; one cleared from a default is written empty.
  for (const f of config.filters) {
    const now = s.filters[f.key] || []
    if (same(now, d.filters[f.key])) continue
    if (!now.length) q.append(f.key, '')
    for (const v of now) q.append(f.key, String(v))
  }
  const page = pageOf(s)
  const path = page?.entity && s.id !== null ? `${s.page}/${encodeURIComponent(s.id)}` : s.page === config.pages[0].id ? '' : s.page
  const qs = q.toString()
  return `#/${path}${qs ? `?${qs}` : ''}`
}

/** Read an address someone may have edited by hand: anything unknown falls back to its default. */
export function fromHash(hash) {
  const s = DEFAULT()
  const raw = String(hash || '').replace(/^#\/?/, '')
  const [path, qs = ''] = raw.split('?')
  const decode = (x) => {
    try {
      return decodeURIComponent(x)
    } catch {
      return x   // a stray % in an address typed by hand
    }
  }
  const [page, id] = path.split('/').map((x) => (x === undefined ? x : decode(x)))
  const p = config.pages.find((x) => x.id === page)
  if (p && !p.entity) s.page = p.id
  // Any id is accepted for an entity's page: it reaches a query only as a quoted literal, and a page whose id matches
  // nothing says so. Entities can have far more values than the kit loads at start (every customer).
  else if (p?.entity && id !== undefined && id !== '') {
    s.page = p.id
    s.id = knownValue(config.entities[p.entity].key, id) ?? id
  }
  const q = new URLSearchParams(qs)
  const preset = q.get('p')
  if (preset === 'custom' && /^\d{4}-\d\d-\d\d$/.test(q.get('from') || '') && /^\d{4}-\d\d-\d\d$/.test(q.get('to') || '')) s.period = { preset, from: q.get('from'), to: q.get('to') }
  else if (preset && P.PRESETS[preset]) s.period = { preset }
  if (P.COMPARES[q.get('c')]) s.compare = q.get('c')
  if (P.GRAINS.includes(q.get('g'))) s.grain = q.get('g')
  if (config.metrics[q.get('m')]) s.metric = q.get('m')
  const seen = new Set()
  for (const [key] of q) {
    if (RESERVED.has(key) || !filterSpec(key) || seen.has(key)) continue
    seen.add(key)
    const all = q.getAll(key)
    // At most MAX_PICKS values a filter, so a link cannot ask for a query as long as the list.
    const vals = [...new Set(all)].filter((v) => isKnown(key, v)).map((v) => knownValue(key, v)).slice(0, MAX_PICKS)
    if (vals.length) s.filters[key] = vals
    else if (all.every((v) => v === '')) delete s.filters[key]
  }
  return s
}

// ---- Periods for a state ----

export const range = (s) => (dated() ? P.resolve(s.period, known.span) : null)
export const compareRange = (s) => (dated() ? P.compareRange(s.period, range(s), s.compare, known.span) : null)
export const grain = (s) => (dated() ? s.grain || P.autoGrain(range(s)) : null)

// ---- Filters as Malloy ----

/**
 * The filter clauses for a state. which: 'current' (the period), 'compare' (the comparison period), 'both' (either),
 * 'none' (no date filter). ignore: filter keys this query leaves out, such as the one a list sets on click; the
 * entity of an entity's page is always applied unless `entity: false`.
 */
export function clauses(s, { which = 'current', ignore = [], entity = true } = {}) {
  const out = []
  if (dated()) {
    const field = config.date.field
    const cur = range(s)
    const cmp = compareRange(s)
    if (which === 'current') out.push(P.clause(field, cur))
    else if (which === 'compare') out.push(cmp ? P.clause(field, cmp) : 'false')
    else if (which === 'both') out.push(cmp ? `((${P.clause(field, cur)}) or (${P.clause(field, cmp)}))` : P.clause(field, cur))
  }
  const e = entity ? entityOf(s) : null
  if (e && s.id !== null) out.push(`${e.field} = ${lit(s.id)}`)
  // On an entity's page, a filter on the entity's own field never applies: the page is scoped to the entity, and a
  // query that sets entity: false compares it with its peers, which that filter would hide.
  const here = entityOf(s)
  for (const f of config.filters) {
    if (ignore.includes(f.key) || (here && s.id !== null && f.field === here.field)) continue
    const vals = (s.filters[f.key] || []).filter((v) => isKnown(f.key, v))
    // `in (…)`, never `= 'x' | 'y' | …`: Malloy's time to compile an alternation grows with the cube of its length.
    if (vals.length) out.push(`${f.field} in (${vals.map(lit).join(', ')})`)
  }
  return out
}

/**
 * A query over the source: `view` is a view the model names, or a block in braces. The state's filters are added to it
 * (opts as for clauses(), and `extra` for more of the block, such as a limit with its order_by).
 *   run('top_brands', s)
 *   run('{ group_by: status; aggregate: order_count }', s, { ignore: ['status'] })
 */
export function run(view, s, opts = {}) {
  // opts.where: a block's own fixed filter ("area_type = 'State'"), beside the state's.
  const where = [...clauses(s, opts), ...[opts.where].flat().filter(Boolean).map((w) => `(${w})`)]
  const extra = opts.extra || ''
  if (/\blimit\s*:/.test(extra) && !/\border_by\s*:/.test(extra)) throw new Error(`A limit needs its order_by beside it: ${extra}`)
  const parts = [where.length ? `where: ${where.join(' and ')}` : '', extra].filter(Boolean).join('; ')
  const v = view.trim()
  if (v.startsWith('{')) return `run: ${src()} -> { ${parts ? `${parts}; ` : ''}${v.slice(1, -1).trim()} }`
  return `run: ${src()} -> ${v}${parts ? ` + { ${parts} }` : ''}`
}

/** `m` in the period, or in the comparison: m { where: <date in range> }; just `m` with no date. */
export function within(m, s, which = 'current') {
  if (!dated()) return which === 'current' ? m : null
  const r = which === 'current' ? range(s) : compareRange(s)
  return r ? `${m} { where: ${P.clause(config.date.field, r)} }` : null
}

/**
 * The figures for the period (which: 'current') or its comparison ('compare'), as <metric>__cur or <metric>__cmp.
 * Two queries, each with the period in its own `where`, rather than one with each measure filtered: a rollup can
 * answer a query whose filters are in its `where`, never one whose measures carry their own. kpis() runs both and
 * merges them.
 */
export function kpiQuery(metrics, s, opts = {}) {
  const which = opts.which === 'compare' ? 'compare' : 'current'
  const suffix = which === 'compare' ? '__cmp' : '__cur'
  return run(`{ aggregate: ${metrics.map((m) => `${m}${suffix} is ${m}`).join(', ')} }`, s, { ...opts, which })
}
/** The figures' queries: the period's, and the comparison's when there is one. */
export const kpiQueries = (metrics, s, opts = {}) => [kpiQuery(metrics, s, opts), ...(dated() && compareRange(s) ? [kpiQuery(metrics, s, { ...opts, which: 'compare' })] : [])]
/** One row from kpiQueries' results: { m__cur, m__cmp }. */
export const kpis = (results) => Object.assign({}, results[0]?.[0], results[1]?.[0])

/**
 * Metrics per bucket of the period (which: 'current') or of the comparison ('compare'). Days and weeks are counted
 * back from the period's end: k = 0 is the last whole day or week. Months and longer are the calendar's.
 */
export function seriesQuery(metrics, s, which = 'current', opts = {}) {
  const g = grain(s)
  const r = which === 'current' ? range(s) : compareRange(s)
  const f = config.date.field
  const by = g === 'day' ? `k is days(${f} to @${r.to})` : g === 'week' ? `k is floor(days(${f} to @${r.to}) / 7)` : `bucket is ${f}.${g}`
  const order = P.ROLLING.has(g) ? 'k desc' : 'bucket'
  return run(`{ group_by: ${by}; aggregate: ${metrics.join(', ')}; order_by: ${order} }`, s, { ...opts, which })
}

/**
 * A series' rows with their dates: each gets `start` and `end` (exclusive) and `partial` when the period covers only
 * part of it. Rolling buckets are placed from their k; the oldest may be short when the period is not whole weeks.
 */
export function buckets(rows, s, which = 'current') {
  const g = grain(s)
  const r = which === 'current' ? range(s) : compareRange(s)
  if (!r) return []
  return rows.map((row) => {
    if (P.ROLLING.has(g)) {
      const step = g === 'day' ? 1 : 7
      const end = P.addDays(r.to, -row.k * step)
      const startFull = P.addDays(end, -step)
      const start = startFull < r.from ? r.from : startFull
      return { ...row, start, end, partial: startFull < r.from }
    }
    const b = String(row.bucket).slice(0, 10)
    const e = P.bucketEnd(b, g)
    return { ...row, start: b < r.from ? r.from : b, end: e > r.to ? r.to : e, partial: b < r.from || e > r.to }
  })
}

/**
 * Metrics split by one field: the top `top` values in the period. Its comparison is a second query,
 * breakdownCompareQuery, merged with withCompare(), so both can be answered by a rollup. The filter on that field is
 * left out, so picking a row does not empty the rest of the list.
 */
export function breakdownQuery(field, metrics, s, { top = 10, ignore, order, extra = [], which = 'current', where } = {}) {
  const fkey = config.filters.find((f) => f.field === field)?.key
  const ign = ignore ?? (fkey ? [fkey] : [])
  const col = colOf(field)
  const cmp = which === 'compare'
  const names = cmp ? metrics.map((m) => `${m}__cmp is ${m}`) : [...metrics, ...extra]
  const by = cmp ? `${metrics[0]}__cmp desc` : order || `${metrics[0]} desc`
  return run(`{ where: ${field} is not null; group_by: ${col === field ? field : `${col} is ${field}`}; aggregate: ${names.join(', ')}; order_by: ${by}, ${col}; limit: ${cmp ? 5000 : top} }`, s, { which, ignore: ign, where })
}
/** The comparison period's figures for every value of the field, to merge into breakdownQuery's rows. */
export const breakdownCompareQuery = (field, metrics, s, opts = {}) => (dated() && compareRange(s) ? breakdownQuery(field, metrics, s, { ...opts, which: 'compare' }) : null)
/** The breakdown's rows with each one's comparison figures beside it (<metric>__cmp), from the comparison's rows. */
export function withCompare(rows, cmpRows, field) {
  if (!cmpRows) return rows
  const col = colOf(field)
  const at = new Map(cmpRows.map((r) => [String(r[col]), r]))
  return rows.map((r) => ({ ...at.get(String(r[col])), ...r }))
}

/**
 * How many values a field has under the state, and whether a metric adds up across them: n (the count), s (the sum of
 * each value's figure) and t (the figure for all of them). When s equals t the metric is a sum or a count and a value's
 * share of the total means something; for an average or a rate it does not, and the kit says no share.
 */
export function partsQuery(field, metric, s, opts = {}) {
  // opts may carry the block's own `where` and `ignore`.
  return `${run(`{ where: ${field} is not null; group_by: f is ${field}; aggregate: m is ${metric}, t is all(${metric}) }`, s, { ...opts, which: 'current' })} -> { aggregate: n is count(), s is m.sum(), t is t.max() }`
}
export const adds = (p) => p && Number.isFinite(p.s) && Number.isFinite(p.t) && Math.abs(p.s - p.t) <= Math.abs(p.t) * 1e-6 + 1e-9

/** The column name a field comes back as: the last part of its path. */
export const colOf = (field) => field.split('.').pop()

// ---- Words for a state ----

export function periodWords(s) {
  if (!dated()) return ''
  const label = s.period.preset === 'custom' ? 'Custom range' : P.PRESETS[s.period.preset]?.label
  return `${label} (${P.describe(range(s))})`
}
export function compareWords(s) {
  if (!dated()) return ''
  const r = compareRange(s)
  if (!r) return s.compare === 'none' ? 'No comparison' : 'No comparison: the data does not reach back that far'
  return `${P.compareWords(s.period, range(s), s.compare).replace(/^the /, 'The ')} (${P.describe(r)})`
}
/** One phrase per active filter: "Category is Jeans or Accessories". */
export function filterWords(s, ignore = []) {
  const e = entityOf(s)
  const out = e && s.id !== null ? [`${e.label} is ${e.name ? e.name(s.id) : s.id}`] : []
  return out.concat(config.filters
    .filter((f) => !ignore.includes(f.key) && (s.filters[f.key] || []).length && !(e && f.field === e.field))
    .map((f) => {
      const vals = s.filters[f.key].map((v) => (f.format ? f.format(v) : String(v)))
      return `${f.label} is ${vals.length > 1 ? `${vals.slice(0, -1).join(', ')} or ${vals.at(-1)}` : vals[0]}`
    }))
}
