// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT
//
// Blocks: the sections a data app's page is made of. Each has a kicker (what is measured), a headline written from its
// rows (what it shows), its evidence, and a back that says how it is made. Each loads as it nears the screen, keeps
// loading, empty and error to itself, and draws again in place when the state changes. Every chart in one does
// something when clicked: it filters the app, opens an entity's page, or zooms the period.

import { h, prose, asText, query, reason, answered, modelDocs, servedTables, inCredible, askCredible } from './core.js'
import * as S from './state.js'
import * as P from './period.js'
import * as F from './format.js'
import * as store from './store.js'
import * as D from './draw.js'

// ---- Lifecycle ----

let epoch = 0
const io = new IntersectionObserver(
  (entries) => {
    for (const e of entries) {
      if (!e.isIntersecting) continue
      io.unobserve(e.target)
      e.target.__load()
    }
  },
  { rootMargin: '480px' }
)
export function nextEpoch() {
  epoch++
  io.disconnect()
}
export function observe(root) {
  root.querySelectorAll('.block').forEach((c) => c.__load && !c.__loaded && io.observe(c))
}
/** body[data-ready="true"] once every block on the page has drawn or failed and nothing marked data-pending waits. */
export function markReady() {
  const blocks = [...document.querySelectorAll('#main .block')]
  const settled = blocks.every((c) => (c.dataset.loaded || c.querySelector('.state--error')) && !c.classList.contains('is-updating'))
  if (settled && !document.querySelector('#main [data-pending]')) document.body.dataset.ready = 'true'
}

/**
 * block({ kicker, of, headline, queries, draw, what, metrics, ignore, span, reserve, key, period, compare, cls })
 *   kicker: what is measured ("Net revenue by category"). of(results, s): what follows it ("· top 10 of 26").
 *   headline(results, s): a sentence from the rows, as a string or prose() parts; null leaves only the kicker.
 *   queries(s): the Malloy queries; draw(results, body, s): fills the body. what: one sentence for its back.
 *   metrics: the metric names it shows, for its back. ignore: filter keys its queries leave out.
 *   span: 12 (default) or 6 for half the page. reserve: the body's height in px while it loads.
 *   period: false when its queries ignore the period; compare: true when it shows the comparison period.
 */
export function block(spec) {
  const { span = 12 } = spec
  let current = spec
  let results = []
  const kick = h('span', { class: 'kicker__of' })
  // An info mark opens the back; it says what it does to a screen reader and on hover.
  const how = h('button', { type: 'button', class: 'how', 'aria-expanded': 'false', 'aria-label': `How “${blockName(spec)}” is made`, title: 'How it’s made' }, icon('info'))
  const head = h('div', { class: 'block__head' }, h('p', { class: 'kicker' }, spec.kicker, kick), how)
  const title = h('h2', { class: 'block__title is-loading', 'data-pending': '' }, h('span', { class: 'skel' }))
  const body = h('div', { class: 'block__body' }, h('div', { class: 'state state--loading', style: { '--reserve': `${spec.reserve ?? 240}px` } }, h('span', { class: 'visually-hidden', text: 'Loading' })))
  const note = h('p', { class: 'block__note' })
  const back = h('div', { class: 'back', role: 'region', 'aria-label': `How “${blockName(spec)}” is made`, inert: true })
  const el = h('article', { class: `block span-${span}${spec.cls ? ` ${spec.cls}` : ''}`, 'data-key': spec.key || spec.kicker, style: spec.rows || null }, head, spec.headline === null ? null : title, body, note, back)
  if (spec.headline === null) title.removeAttribute('data-pending')
  // The back: a circle growing out of "How it's made", closing back into it.
  const open = (on) => {
    if (on === el.classList.contains('is-open')) return
    if (on) fillBack()
    el.classList.toggle('is-open', on)
    how.setAttribute('aria-expanded', String(on))
    back.inert = !on
    const r = how.getBoundingClientRect()
    const box = back.getBoundingClientRect()
    const cx = r.left + r.width / 2 - box.left
    const cy = r.top + r.height / 2 - box.top
    const rad = Math.ceil(Math.hypot(Math.max(cx, box.width - cx), Math.max(cy, box.height - cy)))
    if (D.MOTION) back.animate({ clipPath: on ? [`circle(0 at ${cx}px ${cy}px)`, `circle(${rad}px at ${cx}px ${cy}px)`] : [`circle(${rad}px at ${cx}px ${cy}px)`, `circle(0 at ${cx}px ${cy}px)`] }, { duration: on ? 420 : 280, easing: on ? 'cubic-bezier(.2,.7,.3,1)' : 'cubic-bezier(.5,0,.75,0)', fill: 'backwards' })
    ;(on ? back.querySelector('.back__close') : how).focus({ preventScroll: true })
  }
  how.onclick = () => open(true)
  back.addEventListener('keydown', (e) => e.key === 'Escape' && open(false))
  const fillBack = () => back.replaceChildren(
    h('div', { class: 'back__head' }, h('h3', { text: `How “${blockName(current)}” is made` }), h('button', { type: 'button', class: 'back__close', 'aria-label': 'Close', title: 'Close', onclick: () => open(false) }, icon('close'))),
    ...howMade(current, current.queries(store.state), results, title.textContent))
  el.__spec = spec
  const run = (updating) => {
    const mine = epoch
    const s = current
    const st = store.state
    el.__loaded = true
    if (updating) el.classList.add('is-updating')
    let qs
    try {
      qs = s.queries(st)
    } catch (err) {
      return Promise.resolve(fail(err))
    }
    return Promise.all(qs.map(query))
      .then((rs) => {
        if (mine !== epoch) return
        results = rs
        const was = body.offsetHeight
        if (updating) {
          if (body.contains(document.activeElement)) document.activeElement.blur()
          body.style.minHeight = `${was}px`
        }
        body.replaceChildren()
        note.replaceChildren()
        if (!s.allowEmpty && !results[0]?.length) {
          body.style.minHeight = ''
          setTitle(null)
          body.append(h('div', { class: 'state', text: S.entityOf(st) && st.id !== null ? `Nothing for ${S.entityOf(st).name ? S.entityOf(st).name(st.id) : st.id}${S.dated() ? ' in this period' : ''}.` : `Nothing matches these filters${S.dated() ? ' in this period' : ''}.` }))
          return
        }
        const drawn = s.draw(results, body, st, note)
        try {
          setTitle(s.headline ? s.headline(results, st) : null)
          if (s.of) kick.textContent = s.of(results, st) || ''
        } catch (err) {
          setTitle(null)
          console.error(`[${s.kicker}] headline`, err)
        }
        return Promise.resolve(drawn).then(() => (body.style.minHeight = ''))
      })
      .then(() => {
        if (mine !== epoch) return
        el.dataset.loaded = '1'
        if (el.classList.contains('is-open')) fillBack()
      })
      .finally(() => mine === epoch && el.classList.remove('is-updating'))
      .catch((err) => mine === epoch && fail(err))
      .then(() => mine === epoch && markReady())
    function fail(err) {
      body.style.minHeight = ''
      setTitle(null)
      body.replaceChildren(h('div', { class: 'state state--error', text: `Could not load: ${reason(err)}` }))
      console.error(`[${s.kicker}]`, err)
    }
  }
  function setTitle(parts) {
    title.classList.remove('is-loading')
    title.removeAttribute('data-pending')
    const kids = prose(parts)
    title.replaceChildren(...kids)
    title.hidden = !kids.length
  }
  el.__load = () => run(false)
  el.__respec = (next) => (current = el.__spec = next)
  el.__update = (next) => {
    current = el.__spec = next
    return run(true)
  }
  return el
}

// ---- How it's made ----

/** The info and close marks: drawn in the text colour, 18px in a 28px target. */
function icon(kind) {
  const ns = 'http://www.w3.org/2000/svg'
  const svg = document.createElementNS(ns, 'svg')
  for (const [k, v] of Object.entries({ viewBox: '0 0 20 20', width: 18, height: 18, 'aria-hidden': 'true' })) svg.setAttribute(k, v)
  const add = (tag, attrs) => {
    const el = document.createElementNS(ns, tag)
    for (const [k, v] of Object.entries(attrs)) el.setAttribute(k, v)
    svg.append(el)
  }
  if (kind === 'close') add('path', { d: 'M6 6l8 8M14 6l-8 8' })
  else {
    add('circle', { cx: 10, cy: 10, r: 8.25 })
    add('circle', { cx: 10, cy: 6.2, r: 1.1, class: 'fill' })
    add('path', { d: 'M10 9v5.5' })
  }
  return svg
}

const part = (title, ...kids) => h('section', { class: 'back__part' }, h('h4', { text: title }), ...kids)
const withCode = (t) => String(t).split(/`([^`]+)`/).map((p, i) => (i % 2 ? h('code', { text: p }) : p))

function howMade(spec, qs, results, said = '') {
  const s = store.state
  const ignore = spec.ignore || []
  const ignored = S.config.filters.filter((f) => ignore.includes(f.key) && (s.filters[f.key] || []).length).map((f) => f.label)
  const covers = [
    S.dated() && spec.period !== false ? `${S.config.date.label || 'Dates'}: ${S.periodWords(s)}.` : null,
    S.dated() && spec.period !== false && spec.compare ? `Compared with: ${S.compareWords(s)}.` : null,
    ...S.filterWords(s, ignore).map((w) => `${w}.`),
    !S.filterWords(s, ignore).length ? 'No filters applied.' : null,
    ignored.length ? `It leaves out the ${ignored.join(' and ')} filter${ignored.length > 1 ? 's' : ''}, so you can pick from it here.` : null,
  ].filter(Boolean)
  const defs = h('dl', { class: 'back__defs' })
  const names = spec.metrics || []
  modelDocs().then((docs) => {
    const src = docs.get(S.config.source)
    defs.replaceChildren(...names.flatMap((n) => {
      const m = F.metric(n)
      const doc = m.doc || src?.fields.get(n)
      return [h('dt', { text: m.label }), h('dd', {}, ...(doc ? withCode(doc) : ['The model does not describe it yet.']), ' ', h('code', { class: 'back__name', text: n }))]
    }))
  })
  // Where the answer came from: the table Publisher says answered each query (its path report), described from the
  // package's build plan: a rollup with its grain and measures, a stored copy, or the raw data just now.
  const seen = qs.map((q) => answered.get(q)).filter(Boolean)
  const where = h('p', { class: 'back__where' })
  const fresh = S.dated() ? `The data runs to ${P.longDate(S.known.span.last)}.` : ''
  const kinds = seen.map((a) => a.path?.servedBy?.kind || (a.servedFrom ? 'materialized' : 'live'))
  const ms = seen.length ? Math.max(...seen.map((a) => a.ms || 0)) : null
  const say = (parts) => where.replaceChildren(...prose([...parts, ms !== null ? ` Answered in ${ms} ms.` : '', fresh ? ` ${fresh}` : '']))
  const words = (name) => {
    const f = S.config.filters.find((x) => x.field === name || S.colOf(x.field) === name)
    if (S.config.date && (S.config.date.field === name)) return 'day'
    return (f?.label || name.replace(/_/g, ' ')).toLowerCase()
  }
  const list = (xs) => (xs.length > 1 ? `${xs.slice(0, -1).join(', ')} and ${xs.at(-1)}` : xs[0] || '')
  if (!seen.length) say([])
  else if (kinds.every((k) => k === 'live')) say(['Worked out from the raw data just now.'])
  else {
    say([kinds.includes('rollup') ? 'Read from a summary Credible keeps of the data.' : 'Read from a copy of the data Credible keeps.'])
    servedTables().then((t) => {
      const built = t?.builtAt ? `, last built ${P.longDate(t.builtAt.slice(0, 10))}` : ''
      const used = seen.map((a) => t?.tables.get(a.path?.servedBy?.tableId)).filter(Boolean)
      const roll = used.find((x) => x.rollup)
      const doc = (n) => (F.metric(n).label || n).toLowerCase()
      if (roll) {
        const r = roll.rollup
        const others = kinds.filter((k) => k !== 'rollup').length
        say([`Read from a summary Credible keeps of the ${S.config.recordNoun || 'records'}: one row for each ${list(r.grainDimensions.map(words))}, with ${list(r.measures.filter((m) => S.config.metrics[m]).map(doc)) || 'its figures'} already added up${built}, so it does not add up every row each time it opens.${others ? ' Some of it, a figure the summary cannot hold, comes from a stored copy of the data.' : ''}`])
      } else if (used.length) say([`Read from a copy of the ${S.config.recordNoun || 'data'} Credible keeps${built}, so it does not reach back to the raw data each time it opens.`])
    })
  }
  const deep = h('details', { class: 'back__deep' }, h('summary', { text: 'The queries and their rows' }))
  deep.addEventListener('toggle', () => {
    if (!deep.open || deep.__filled) return
    deep.__filled = true
    deep.append(...qs.map((q, i) => {
      const rows = results[i] || []
      const cols = rows.length ? Object.keys(rows[0]) : []
      const a = answered.get(q)
      const sb = a?.path?.servedBy
      const skipped = (a?.path?.considered || []).filter((c) => c.outcome !== 'used').map((c) => `${c.kind} skipped: ${c.reason}${c.detail ? ` (${c.detail})` : ''}`)
      return h('div', { class: 'back__query' },
        a && h('p', { class: 'back__path', text: `Answered from ${sb?.kind || (a.servedFrom ? 'a stored table' : 'live')}${sb?.tableId ? ` ${sb.tableId.slice(0, 12)}` : ''} in ${a.ms} ms${skipped.length ? `; ${skipped.join('; ')}` : ''}.` }),
        h('pre', { text: q }),
        h('div', { class: 'back__meta' }, h('span', { text: `${rows.length} row${rows.length === 1 ? '' : 's'}` }), cols.length > 0 && h('button', { type: 'button', class: 'ghost ghost--small', text: 'Download CSV', onclick: () => downloadCsv(rows, cols, `query-${i + 1}`) })),
        cols.length > 0 && h('div', { class: 'back__rows' }, h('table', {}, h('thead', {}, h('tr', {}, ...cols.map((c) => h('th', { text: c })))),
          h('tbody', {}, ...rows.slice(0, 30).map((r) => h('tr', {}, ...cols.map((c) => h('td', { text: r[c] === null || r[c] === undefined ? '∅' : typeof r[c] === 'number' ? String(Number(r[c].toPrecision(6))) : String(r[c]) }))))))))
    }))
  })
  // Where the data comes from (config.sources, or the block's own `sources`) and the words a newcomer may not know
  // (config.glossary: the terms that appear in what this block says).
  const srcs = (spec.sources || Object.keys(S.config.sources || {})).map((k) => S.config.sources?.[k]).filter(Boolean)
  const text = [spec.kicker, said, asText(spec.what || ''), ...names.map((n) => F.metric(n).label)].join(' ').toLowerCase()
  const terms = (S.config.glossary || []).filter((g) => [g.term, ...(g.match || [])].some((m) => text.includes(String(m).toLowerCase())))
  return [
    spec.what && part('What it shows', h('p', {}, ...prose(spec.what))),
    part('What it covers', h('ul', {}, ...covers.map((t) => h('li', { text: t })))),
    names.length > 0 && part('What the numbers mean', defs),
    terms.length > 0 && part('Words used here', h('dl', { class: 'back__defs' }, ...terms.flatMap((g) => [h('dt', { text: g.term }), h('dd', { text: g.plain })]))),
    srcs.length > 0 && part('Where the data comes from', sourceList(srcs)),
    part('Where the answer came from', where),
    deep,
    inCredible() && askButton(spec, qs, said, covers),
  ].filter(Boolean)
}

/** What to call a block: its kicker, else its first metric (a subject has no kicker of its own). */
function blockName(spec) {
  return asText(spec.kicker || '').trim() || (spec.metrics?.[0] ? F.metric(spec.metrics[0]).label : '') || 'this section'
}

/** Inside Credible: ask its agent about this block, with what it covers and the queries behind it attached. */
function askButton(spec, qs, said, covers) {
  const kicker = blockName(spec)
  const e = S.entityOf(store.state)
  const about = e ? ` for ${e.name ? e.name(store.state.id) : store.state.id}` : ''
  const prompt = `${S.config.title}, "${kicker}"${about}: ${said ? `${asText(said)} ` : ''}What is behind this, and what should I look at next?`
  const context = {
    scope: { package: decodeURIComponent(location.pathname.match(/\/packages\/([^/]+)/)?.[1] || ''), model: S.config.model },
    view: { type: 'block', id: spec.key || kicker, title: kicker },
    entities: [{ kind: 'source', name: S.config.source }],
    items: [{ label: 'what it covers', value: covers.join(' ').slice(0, 900) }, ...qs.slice(0, 3).map((q, i) => ({ label: `query ${i + 1}`, value: q.slice(0, 900) }))],
    givens: Object.fromEntries(Object.entries(store.state.filters).filter(([, v]) => v.length)),
  }
  return h('button', { type: 'button', class: 'ghost ghost--accent back__ask', text: 'Ask Credible about this', onclick: () => askCredible(prompt, context) })
}

export function downloadCsv(rows, cols, name) {
  const cell = (v) => {
    if (typeof v === 'number') return String(v)
    const t = v === null || v === undefined ? '' : String(v)
    return /[",\n\r]|^[=+\-@]/.test(t) ? `"${(/^[=+\-@]/.test(t) ? `'${t}` : t).replace(/"/g, '""')}"` : t
  }
  const text = [cols, ...rows.map((r) => cols.map((c) => r[c]))].map((r) => r.map(cell).join(',')).join('\r\n')
  const a = h('a', { href: URL.createObjectURL(new Blob([text], { type: 'text/csv' })), download: `${document.title.replace(/\W+/g, '-').toLowerCase()}-${name}.csv` })
  document.body.append(a)
  a.click()
  setTimeout(() => (URL.revokeObjectURL(a.href), a.remove()), 0)
}

// ---- Small parts ----

/** A figure set large: the symbol and unit a step smaller than the number. */
export function figure(name, v, cls = 'figure') {
  const [pre, num, post] = F.parts(name, v)
  return h('span', { class: cls, title: F.fmtMetric(name, v, { compact: false }) }, pre && h('span', { class: 'figure__u', text: pre }), num, post && h('span', { class: 'figure__s', text: post }))
}
/** A change as words in its colour: "+16.2%", "no change". */
export function changeText(name, cur, prev) {
  const d = F.delta(name, cur, prev)
  return h('strong', { class: `chg chg--${d.tone}`, text: d.text })
}
export function statusMark(st) {
  return st ? h('span', { class: `status status--${st}` }, h('i', { 'aria-hidden': 'true' }), F.STATUS_WORDS[st]) : null
}
const lower = (t) => t.replace(/^[A-Z](?![A-Z])/, (c) => c.toLowerCase())
const pct0 = (v) => (v < 0.1 ? `${(v * 100).toFixed(1)}%` : `${Math.round(v * 100)}%`)
const growth = (r, n) => (Number.isFinite(r[`${n}__cmp`]) && r[`${n}__cmp`] !== 0 ? r[n] / r[`${n}__cmp`] - 1 : null)
/** Where a click on a value of `field` goes: its entity's page when it has one, else a filter on it. */
function actionFor(field, entity) {
  const e = entity ? S.config.entities[entity] : Object.values(S.config.entities).find((x) => x.field === field)
  const page = e && S.config.pages.find((p) => p.entity === e.key)
  if (page) return { kind: 'open', label: `Open its page`, go: (v) => store.open(page.id, v), href: (v) => store.linkTo(page.id, v) }
  const f = S.config.filters.find((x) => x.field === field)
  if (f) return { kind: 'filter', label: 'Filter to it', go: (v) => store.toggleFilter(f.key, v), on: (v) => (store.state.filters[f.key] || []).some((x) => String(x) === String(v)), any: () => (store.state.filters[f.key] || []).length > 0 }
  return null
}
const nameOf = (field, entity) => {
  const e = entity ? S.config.entities[entity] : Object.values(S.config.entities).find((x) => x.field === field)
  const f = S.config.filters.find((x) => x.field === field)
  const fmt = e?.name || f?.format
  return (v) => (v === null || v === undefined ? '(none)' : fmt ? fmt(v) : String(v))
}

// ---- The subject: one number, its change, a sentence, its line; the other figures in a ledger under it ----

/**
 * subject({ metrics, by }) — the page's lead. The first metric (or state.metric) is the subject; the rest sit in the
 * ledger, and clicking one makes it the subject. With no date, `by` names a field whose top values stand beside the
 * figure instead of a line.
 */
/**
 * layout: 'split' (the default: the figure beside its line, the rest in a ledger), 'band' (the same on a dark plane,
 * for a monitor whose lead is a status), or 'board' (every figure large in a row, the chosen one's line across the
 * page under them, for a scorecard of a few figures that matter equally).
 */
export function subject({ metrics, by, kicker, layout = 'split' } = {}) {
  return block({
    compare: true, key: `subject|${metrics.join(',')}`, cls: 'block--subject', kicker: kicker || ' ', headline: null, metrics, period: true,
    // Its loading height: the figure and chart, then one ledger row wide, rows of three on a tablet, of two on a phone.
    rows: layout === 'board' ? { '--rw': 1.3, '--rm': 0.6, '--rn': 0.4 + Math.ceil(metrics.length / 2) * 0.9 } : { '--rw': metrics.length > 1 ? 1 : 0, '--rm': Math.ceil((metrics.length - 1) / 3), '--rn': Math.ceil((metrics.length - 1) / 2) },
    what: 'The figure the page is about, with its change on the comparison period and, beside it, the same figure over the period with the comparison in grey. Under it, the other figures; click one to make it the subject. Click a point on the line to zoom to it, or drag across several.',
    queries: (s) => {
      const n = metrics.includes(s.metric) ? s.metric : metrics[0]
      const qs = S.kpiQueries(metrics, s)
      if (S.dated()) {
        qs.push(S.seriesQuery(metrics, s, 'current'))
        if (S.compareRange(s)) qs.push(S.seriesQuery(metrics, s, 'compare'))
      } else if (by) qs.push(S.breakdownQuery(by, [n], s, { top: 8 }), S.partsQuery(by, n, s))
      return qs
    },
    draw: (rs, body, s) => {
      // The figures' one or two queries first (the period's, then the comparison's), then the series or the list.
      const nk = S.dated() && S.compareRange(s) ? 2 : 1
      const k = S.kpis(rs.slice(0, nk))
      const [cur, cmp] = rs.slice(nk)
      const n = metrics.includes(s.metric) ? s.metric : metrics[0]
      const m = F.metric(n)
      const partsOf = !S.dated() && by ? cmp?.[0] : null // with no date the third query is the parts check
      const v = k[`${n}__cur`]
      const p = k[`${n}__cmp`]
      const cmpR = S.compareRange(s)
      const cw = S.dated() ? P.compareWords(s.period, S.range(s), s.compare) : ''
      const lead = h('div', { class: 'subject__text' },
        h('p', { class: 'kicker' }, m.label, S.dated() && h('span', { class: 'kicker__of', text: ` · ${S.periodWords(s).replace(/ \(.*/, '').toLowerCase()}` })),
        figure(n, v),
        cmpR && Number.isFinite(p) && h('p', { class: 'subject__change' }, changeText(n, v, p), ` from ${F.fmtMetric(n, p, { compact: true })} in ${cw}`))
      const say = h('p', { class: 'subject__say' })
      const chart = h('div', { class: 'subject__chart' })
      if (layout === 'board') {
        // Every figure large; the chosen one carries the line under them.
        const board = h('div', { class: 'scores', style: { '--n': Math.min(metrics.length, 4) } }, ...metrics.map((x) => {
          const cv = k[`${x}__cur`]
          const pv = k[`${x}__cmp`]
          const st = F.status(x, cv)
          return h('button', { type: 'button', class: 'score', 'aria-pressed': String(x === n), title: x === n ? null : `Show ${lower(F.metric(x).label)} over time`, onclick: () => x !== n && store.setMetric(x) },
            h('span', { class: 'score__label' }, st && h('i', { class: `dot dot--${st}`, title: F.STATUS_WORDS[st] }), F.metric(x).label),
            figure(x, cv),
            h('span', { class: 'score__change' }, cmpR && Number.isFinite(pv) ? [changeText(x, cv, pv), ` from ${F.fmtMetric(x, pv, { compact: true })}`] : st ? F.STATUS_WORDS[st] : ' '))
        }))
        body.append(h('div', { class: 'subject subject--board' }, board, say, chart))
      } else {
        lead.append(say)
        body.append(h('div', { class: `subject${layout === 'band' ? ' subject--band' : ''}` }, lead, chart))
      }
      if (S.dated() && cur?.length) {
        const g = S.grain(s)
        const a = S.buckets(cur, s, 'current')
        const b = cmp ? S.buckets(cmp, s, 'compare') : []
        // The comparison lined up bucket for bucket: rolling buckets match by k; calendar ones by position from the end.
        const bAt = (i) => (P.ROLLING.has(g) ? b.find((r) => r.k === a[i].k) : b[b.length - (a.length - i)]) || null
        const aV = a.map((r) => r[n])
        const bV = a.map((_, i) => (bAt(i) && !bAt(i).partial ? bAt(i)[n] : null))
        const label = (r) => (g === 'day' ? P.describe({ from: r.start, to: r.end }) : `${g === 'week' ? 'Week of ' : ''}${P.describe({ from: r.start, to: r.end })}`)
        D.line(chart, {
          labels: a.map(label), ticks: a.map((r) => tickOf(r, g)), height: 260,
          subject: { name: 'This period', values: aV }, compare: cmpR ? { name: cw.replace(/^the /, '').replace(/^./, (c) => c.toUpperCase()), values: bV } : null,
          partial: a.map((r) => r.partial), format: (x, full) => F.fmtMetric(n, x, { compact: !full }), axis: F.axisFormat(n),
          tipLabel: (i) => label(a[i]) + (a[i].partial ? ' (part)' : ''), delta: (x, y) => F.delta(n, x, y),
          onPick: (i) => store.setPeriod({ preset: 'custom', from: a[i].start, to: P.addDays(a[i].end, -1) }),
          onRange: (i, j) => store.setPeriod({ preset: 'custom', from: a[i].start, to: P.addDays(a[j].end, -1) }),
          label: `${m.label} by ${g}`,
        })
        // The sentence: how many buckets beat their counterpart, and the best one.
        const full = a.map((r, i) => ({ r, v: aV[i], c: bV[i] })).filter((x) => !x.r.partial && Number.isFinite(x.v))
        const better = (x) => (m.good === 'down' ? x.v < x.c : x.v > x.c)
        const paired = full.filter((x) => Number.isFinite(x.c))
        const best = full.reduce((q, x) => (!q || (m.good === 'down' ? x.v < q.v : x.v > q.v) ? x : q), null)
        const unit = { day: 'day', week: 'week', month: 'month', quarter: 'quarter', year: 'year' }[g]
        const parts = []
        if (paired.length >= 3) {
          const nb = paired.filter(better).length
          parts.push(nb === paired.length ? `Every one of the ${paired.length} ${unit}s ${m.good ? 'beat' : 'was above'} the same ${unit} of ${cw}. ` : nb === 0 ? `None of the ${paired.length} ${unit}s ${m.good ? 'beat' : 'was above'} the same ${unit} of ${cw}. ` : `${nb} of the ${paired.length} ${unit}s ${m.good ? 'beat' : 'were above'} the same ${unit} of ${cw}. `)
        }
        if (best && full.length >= 3 && m.good) parts.push(`The ${m.good === 'down' ? 'lowest' : 'best'} was the ${unit} ${g === 'day' ? 'of' : 'to'} `, { b: g === 'day' ? P.describe({ from: best.r.start, to: best.r.end }) : P.describe({ from: P.addDays(best.r.end, -1), to: best.r.end }) }, `, at ${F.fmtMetric(n, best.v, { compact: true })}.`)
        say.replaceChildren(...prose(parts))
      } else if (by && cur?.length) {
        const col = S.colOf(by)
        rankList(chart, cur, { col, n, field: by, name: nameOf(by), compare: false })
        const top = [{ b: nameOf(by)(cur[0][col]) }]
        if (S.adds(partsOf) && v) say.replaceChildren(...prose([...top, ` accounts for ${pct0(cur[0][n] / v)} of it.`]))
        else say.replaceChildren(...prose([...top, ` is highest, at ${F.fmtMetric(n, cur[0][n], { compact: true })} against ${F.fmtMetric(n, v, { compact: true })} for all of them.`]))
      }
      // The ledger
      const others = layout === 'board' ? [] : metrics.filter((x) => x !== n)
      if (others.length) {
        body.append(h('div', { class: 'ledger', style: { '--n': others.length } }, ...others.map((x) => {
          const mm = F.metric(x)
          const cv = k[`${x}__cur`]
          const pv = k[`${x}__cmp`]
          const st = F.status(x, cv)
          return h('button', { type: 'button', class: 'led', title: `Make ${lower(mm.label)} the subject`, onclick: () => store.setMetric(x) },
            h('span', { class: 'led__label' }, st && h('i', { class: `dot dot--${st}`, title: F.STATUS_WORDS[st] }), mm.label),
            figure(x, cv, 'led__value'),
            h('span', { class: 'led__change' }, cmpR && Number.isFinite(pv) ? [changeText(x, cv, pv), ` from ${F.fmtMetric(x, pv, { compact: true })}`] : st ? F.STATUS_WORDS[st] : ' '),
            S.dated() && cur?.length ? D.spark(S.buckets(cur, s).map((r) => (r.partial ? null : r[x])), cmp ? S.buckets(cmp, s, 'compare').map((r) => (r.partial ? null : r[x])) : []) : null)
        })))
      }
    },
  })
}
function tickOf(r, g) {
  const d = P.toDate(g === 'day' || P.ROLLING.has(g) ? P.addDays(r.end, -1) : r.start)
  const mo = ['Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun', 'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec'][d.getUTCMonth()]
  if (g === 'month') return `${mo} ${String(d.getUTCFullYear()).slice(2)}`
  if (g === 'quarter') return `Q${Math.floor(d.getUTCMonth() / 3) + 1} ${String(d.getUTCFullYear()).slice(2)}`
  if (g === 'year') return String(d.getUTCFullYear())
  return `${d.getUTCDate()} ${mo}`
}

// ---- Ranked lists ----

function rankList(body, rows, { col, n, field, entity, name, compare = true, extra = [] }) {
  const act = actionFor(field, entity)
  const max = Math.max(...rows.flatMap((r) => [Math.abs(r[n] ?? 0), compare ? Math.abs(r[`${n}__cmp`] ?? 0) : 0]), 1e-12)
  const anyCmp = compare && rows.some((r) => Number.isFinite(r[`${n}__cmp`]))
  const t = h('table', { class: `rank${act ? ' rank--act' : ''}` },
    h('thead', {}, h('tr', {}, h('th', { class: 'rank__name' }), h('th', { class: 'rank__bar' }), ...extra.map((x) => h('th', { class: 'rank__v rank__v--extra', text: F.metric(x).label })), h('th', { class: 'rank__v', text: F.metric(n).label }), anyCmp ? h('th', { class: 'rank__d', text: 'Change' }) : null)),
    h('tbody', {}, ...rows.map((r) => {
      const v = r[col]
      const on = act?.kind === 'filter' && act.on(v)
      const off = act?.kind === 'filter' && act.any() && !on
      const label = name(v)
      const cells = [
        h('td', { class: 'rank__name', title: label }, act?.kind === 'open' ? h('a', { href: act.href(v), text: label, onclick: (e) => e.stopPropagation() }) : label),
        h('td', { class: 'rank__bar' }, h('div', { class: 'bar' }, anyCmp && Number.isFinite(r[`${n}__cmp`]) ? h('b', { style: { width: `${(Math.abs(r[`${n}__cmp`]) / max) * 100}%` } }) : null, h('i', { style: { width: `${(Math.abs(r[n] ?? 0) / max) * 100}%` } }))),
        ...extra.map((x) => h('td', { class: 'rank__v rank__v--extra', text: F.fmtMetric(x, r[x], { compact: true }) })),
        h('td', { class: 'rank__v', text: F.fmtMetric(n, r[n], { compact: true }) }),
        anyCmp ? h('td', { class: 'rank__d' }, Number.isFinite(r[`${n}__cmp`]) ? changeText(n, r[n], r[`${n}__cmp`]) : h('span', { class: 'chg chg--flat', text: 'new' })) : null,
      ]
      const tr = h('tr', { class: on ? 'is-on' : off ? 'is-off' : null }, ...cells)
      if (act && v !== null && v !== undefined) {
        tr.tabIndex = 0
        tr.setAttribute('role', 'button')
        tr.setAttribute('aria-pressed', act.kind === 'filter' ? String(on) : null)
        tr.setAttribute('aria-label', `${label}: ${F.fmtMetric(n, r[n])}. ${act.kind === 'open' ? 'Open its page' : on ? 'Remove the filter' : 'Filter the app to it'}`)
        tr.addEventListener('click', () => act.go(v))
        tr.addEventListener('keydown', (e) => (e.key === 'Enter' || e.key === ' ') && (e.preventDefault(), act.go(v)))
      }
      return tr
    })))
  body.append(t)
  if (anyCmp) body.append(h('p', { class: 'rank__key' }, h('b', { 'aria-hidden': 'true' }), h('span', { text: 'This period, over the comparison' }), act && h('span', { class: 'rank__hint', text: act.kind === 'open' ? 'Click a row to open its page' : 'Click a row to filter' })))
}

/**
 * rank({ field | entity, metric, top, kicker, headline, extra, span }) — a metric split by a field, largest first:
 * name, a thin bar over a pale one for the comparison, the value, the change. A row opens its entity's page when the
 * field has one, and filters the app to it otherwise. The default headline names the leader and the fastest
 * riser; write your own when the domain says what matters.
 */
export function rank({ field, entity, metric: n, top = 10, kicker, headline, extra = [], span = 6, order, where, what } = {}) {
  const fieldName = field || S.config.entities[entity].field
  const col = S.colOf(fieldName)
  const filter = S.config.filters.find((f) => f.field === fieldName)
  const e = entity ? S.config.entities[entity] : Object.values(S.config.entities).find((x) => x.field === fieldName)
  const plural = e?.plural || filter?.plural || `${(e?.label || filter?.label || col.replace(/_/g, ' ')).toLowerCase()} values`
  const name = nameOf(fieldName, entity)
  return block({
    compare: true, key: `rank|${fieldName}|${n}|${top}|${order || ''}|${kicker || ''}`, kicker: kicker || `${F.metric(n).label} by ${(e?.label || filter?.label || col.replace(/_/g, ' ')).toLowerCase()}`, span, reserve: 30 + 35 * top + 30,
    metrics: [n, ...extra], ignore: filter ? [filter.key] : [],
    what: what || `The ${top} ${plural} with the most ${lower(F.metric(n).label)} in the period. The pale bar behind each is its figure in the comparison period, so a bar that reaches past its pale one grew.`,
    queries: (s) => [S.breakdownQuery(fieldName, [n], s, { top, order, extra, where }), S.partsQuery(fieldName, n, s, { ignore: filter ? [filter.key] : [], where }), S.breakdownCompareQuery(fieldName, [n], s, { where })].filter(Boolean),
    of: ([rows, [c]]) => (c?.n > rows.length ? ` · top ${rows.length} of ${c.n.toLocaleString()}` : ''),
    draw: ([rows, , cmp], body) => rankList(body, S.withCompare(rows, cmp, fieldName), { col, n, field: fieldName, entity, name, extra }),
    headline: headline ? (rs, s) => headline(S.withCompare(rs[0], rs[2], fieldName), s) : ([cur, [c], cmp]) => {
      const rows = S.withCompare(cur, cmp, fieldName)
      if (!rows.length) return null
      const top1 = rows[0]
      const m = F.metric(n)
      const parts = [{ b: name(top1[col]) }, ` leads`]
      if (S.adds(c) && c.t && top1[n] / c.t >= 0.01) parts.push(` with ${pct0(top1[n] / c.t)} of the total`)
      else if (!S.adds(c)) parts.push(` at ${F.fmtMetric(n, top1[n], { compact: true })}`)
      // Only values at least a tenth of the leader's: a tiny one doubling is noise, not news.
      const g = rows.map((r) => ({ r, g: growth(r, n) })).filter((x) => x.g !== null && Math.abs(x.r[`${n}__cmp`]) > 0 && Math.abs(x.r[n]) >= Math.abs(top1[n]) * 0.1)
      if (g.length >= 3 && m.good) {
        // The best mover by what the metric's good direction says, worded by the sign of its change: a price that
        // rose least is not one that fell.
        g.sort((a, b) => b.g - a.g)
        const best = m.good === 'down' ? g.at(-1) : g[0]
        const worst = m.good === 'down' ? g[0] : g.at(-1)
        const verb = (x) => (x.g > 0 ? (m.good === 'down' ? 'rose least' : 'grew fastest') : m.good === 'down' ? 'fell fastest' : 'fell least')
        if (best.r !== top1 && Math.abs(best.g) > 0.03) parts.push('; ', { b: name(best.r[col]) }, ` ${verb(best)}, ${F.delta(n, best.r[n], best.r[`${n}__cmp`]).text}`)
        const bad = m.good === 'down' ? worst.g > 0.03 : worst.g < -0.03
        if (bad && worst.r !== best.r && g.some((x) => (m.good === 'down' ? x.g <= 0 : x.g >= 0))) parts.push('; ', ...(worst.r === top1 ? ['it'] : [{ b: name(worst.r[col]) }]), ` ${worst.g > 0 ? 'rose' : 'fell'} most, ${F.delta(n, worst.r[n], worst.r[`${n}__cmp`]).text}`)
      }
      parts.push('.')
      return parts
    },
  })
}

/**
 * share({ field | entity, metric, top }) — one whole split into its parts: a segmented bar, then a row per part with
 * its share, value and change. A part opens its page or filters, as in rank().
 */
export function share({ field, entity, metric: n, top = 6, kicker, headline, span = 6, where, what } = {}) {
  const fieldName = field || S.config.entities[entity].field
  const col = S.colOf(fieldName)
  const filter = S.config.filters.find((f) => f.field === fieldName)
  const e = entity ? S.config.entities[entity] : null
  const name = nameOf(fieldName, entity)
  return block({
    compare: true, key: `share|${fieldName}|${n}`, kicker: kicker || `${F.metric(n).label} by ${(e?.label || filter?.label || col).toLowerCase()}`, span, reserve: 60 + 35 * top,
    metrics: [n], ignore: filter ? [filter.key] : [],
    what: `How ${lower(F.metric(n).label)} splits across ${(e?.label || filter?.label || col).toLowerCase()}, as shares of the whole, with each part's change on the comparison period. Parts past the first ${top} are grouped as Other.`,
    queries: (s) => [S.breakdownQuery(fieldName, [n], s, { top: 50, where }), S.kpiQuery([n], s, { ignore: filter ? [filter.key] : [], where }), S.breakdownCompareQuery(fieldName, [n], s, { where })].filter(Boolean),
    draw: ([cur, [k], cmp], body) => {
      const rows = S.withCompare(cur, cmp, fieldName)
      const total = k[`${n}__cur`]
      const shown = rows.slice(0, top)
      const rest = total - shown.reduce((a, r) => a + (r[n] || 0), 0)
      const act = actionFor(fieldName, entity)
      const seg = h('div', { class: 'seg', role: 'img', 'aria-label': shown.map((r) => `${name(r[col])} ${pct0(r[n] / total)}`).join(', ') })
      // A part keeps its colour wherever it is drawn: the slot of its place among the field's values, largest first.
      const order = S.known[(filter || e)?.key] || []
      const taken = new Set()
      const slotOf = new Map()
      for (const r of shown) {
        const k = order.findIndex((v) => String(v) === String(r[col]))
        if (k >= 0 && k < 6 && !taken.has(k + 1)) (slotOf.set(r, k + 1), taken.add(k + 1))
      }
      for (const r of shown) if (!slotOf.has(r)) for (let k = 1; k <= 6; k++) if (!taken.has(k)) { slotOf.set(r, k); taken.add(k); break }
      shown.forEach((r) => {
        const sh = r[n] / total
        const part = h('span', { class: `seg__part s${slotOf.get(r) || 6}`, style: { flex: `${sh} 1 0` }, title: `${name(r[col])}: ${pct0(sh)}` }, sh > 0.06 ? `${name(r[col])} ${pct0(sh)}` : '')
        // Its label only where it fits: the name and share, else the share, else nothing (the rows below say it).
        requestAnimationFrame(() => {
          if (part.scrollWidth > part.clientWidth + 1) part.textContent = pct0(sh)
          if (part.scrollWidth > part.clientWidth + 1) part.textContent = ''
        })
        if (act) part.addEventListener('click', () => act.go(r[col]))
        seg.append(part)
      })
      if (rest / total > 0.005) seg.append(h('span', { class: 'seg__part seg__other', style: { flex: `${rest / total} 1 0` }, title: `Other: ${pct0(rest / total)}` }, rest / total > 0.1 ? `Other ${pct0(rest / total)}` : ''))
      body.append(seg)
      const t = h('table', { class: `rank${act ? ' rank--act' : ''}` }, h('thead', {}, h('tr', {}, h('th', { class: 'rank__name' }), h('th', { class: 'rank__v', text: 'Share' }), h('th', { class: 'rank__v', text: F.metric(n).label }), shown.some((r) => Number.isFinite(r[`${n}__cmp`])) ? h('th', { class: 'rank__d', text: 'Change' }) : null)),
        h('tbody', {}, ...shown.map((r, i) => {
          const tr = h('tr', {},
            h('td', { class: 'rank__name' }, h('i', { class: `swatch s${slotOf.get(r) || 6}` }), act?.kind === 'open' ? h('a', { href: act.href(r[col]), text: name(r[col]) }) : name(r[col])),
            h('td', { class: 'rank__v rank__v--muted', text: `${(100 * r[n] / total).toFixed(1)}%` }),
            h('td', { class: 'rank__v', text: F.fmtMetric(n, r[n], { compact: true }) }),
            h('td', { class: 'rank__d' }, Number.isFinite(r[`${n}__cmp`]) ? changeText(n, r[n], r[`${n}__cmp`]) : ''))
          if (act) {
            tr.tabIndex = 0
            tr.setAttribute('role', 'button')
            tr.addEventListener('click', () => act.go(r[col]))
            tr.addEventListener('keydown', (ev) => ev.key === 'Enter' && act.go(r[col]))
          }
          return tr
        })))
      body.append(t)
    },
    headline: headline ? (rs, s) => headline(S.withCompare(rs[0], rs[2], fieldName), s) : ([cur, [k], cmp]) => {
      const rows = S.withCompare(cur, cmp, fieldName)
      const total = k[`${n}__cur`]
      const top1 = rows[0]
      const g = rows.slice(0, top).map((r) => ({ r, g: growth(r, n) })).filter((x) => x.g !== null)
      const parts = [{ b: name(top1[col]) }, ` brings ${pct0(top1[n] / total)} of ${lower(F.metric(n).label)}`]
      if (g.length >= 2) {
        const all = g.every((x) => x.g > 0)
        g.sort((a, b) => b.g - a.g)
        const one = (e?.label || filter?.label || col.replace(/_/g, ' ')).toLowerCase()
        parts.push(all ? `; every ${one} grew, ` : '; ', { b: name(g[0].r[col]) }, ` ${all ? 'fastest' : 'grew fastest'} at ${F.delta(n, g[0].r[n], g[0].r[`${n}__cmp`]).text}`)
      }
      parts.push('.')
      return parts
    },
  })
}

/**
 * trend({ metric, by, top, kicker }) — a metric over the period. With `by`, one line per top value of a field, each
 * labelled at its end, the leader in the accent and the rest in grey until hovered; click a line to open or filter.
 */
export function trend({ metric: n, by, top = 5, kicker, headline, span = 12, height = 260, where, what } = {}) {
  const col = by ? S.colOf(by) : null
  const name = by ? nameOf(by) : null
  return block({
    compare: true, key: `trend|${n}|${by || ''}`, kicker: kicker || `${F.metric(n).label} over time${by ? ` by ${lower(S.config.filters.find((f) => f.field === by)?.label || col)}` : ''}`, span, reserve: height + 30, metrics: [n],
    what: by ? `${F.metric(n).label} for the ${top} largest values over the period, one line each. Click a line to go to it.` : `${F.metric(n).label} over the period, against the comparison. Click a point to zoom to it.`,
    queries: (s) => {
      if (!by) return [S.seriesQuery([n], s, 'current', { where }), ...(S.compareRange(s) ? [S.seriesQuery([n], s, 'compare', { where })] : [])]
      const g = S.grain(s)
      const r = S.range(s)
      const f = S.config.date.field
      const bucket = g === 'day' ? `k is days(${f} to @${r.to})` : g === 'week' ? `k is floor(days(${f} to @${r.to}) / 7)` : `bucket is ${f}.${g}`
      return [S.breakdownQuery(by, [n], s, { top, where }), S.run(`{ where: ${by} is not null; group_by: ${col} is ${by}, ${bucket}; aggregate: ${n} }`, s, { where, ignore: S.config.filters.filter((f) => f.field === by).map((f) => f.key) })]
    },
    draw: ([a, b], body, s) => {
      const g = S.grain(s)
      if (!by) {
        const cur = S.buckets(a, s)
        const cmp = b ? S.buckets(b, s, 'compare') : []
        const bAt = (i) => (P.ROLLING.has(g) ? cmp.find((r) => r.k === cur[i].k) : cmp[cmp.length - (cur.length - i)]) || null
        const box = body.appendChild(h('div', { class: 'chart' }))
        D.line(box, {
          labels: cur.map((r) => P.describe({ from: r.start, to: r.end })), ticks: cur.map((r) => tickOf(r, g)), height,
          subject: { name: 'This period', values: cur.map((r) => r[n]) }, compare: b ? { name: 'Comparison', values: cur.map((_, i) => (bAt(i) && !bAt(i).partial ? bAt(i)[n] : null)) } : null,
          partial: cur.map((r) => r.partial), format: (x, full) => F.fmtMetric(n, x, { compact: !full }), axis: F.axisFormat(n), tipLabel: (i) => P.describe({ from: cur[i].start, to: cur[i].end }), delta: (x, y) => F.delta(n, x, y),
          onPick: (i) => store.setPeriod({ preset: 'custom', from: cur[i].start, to: P.addDays(cur[i].end, -1) }),
          onRange: (i, j) => store.setPeriod({ preset: 'custom', from: cur[i].start, to: P.addDays(cur[j].end, -1) }),
        })
        return
      }
      const names = a.map((r) => r[col])
      const rows = S.buckets(b, s)
      const keyOf = (r) => (P.ROLLING.has(g) ? r.k : String(r.bucket))
      const order = [...new Set(rows.map(keyOf))].sort((x, y) => (P.ROLLING.has(g) ? y - x : x < y ? -1 : 1))
      const at = new Map(rows.map((r) => [`${r[col]}|${keyOf(r)}`, r]))
      const sample = order.map((k) => rows.find((r) => keyOf(r) === k))
      multi(body.appendChild(h('div', { class: 'chart' })), {
        ticks: sample.map((r) => tickOf(r, g)), labels: sample.map((r) => P.describe({ from: r.start, to: r.end })), height,
        series: names.map((v) => ({ id: v, name: name(v), values: order.map((k) => at.get(`${v}|${k}`)?.[n] ?? null) })),
        format: (x, full) => F.fmtMetric(n, x, { compact: !full }), axis: F.axisFormat(n), act: actionFor(by),
      })
    },
    headline: headline ? (rs, s) => headline(rs, s) : null,
  })
}

/** Several lines, the first in the accent, the rest grey until hovered, each labelled at its end; a click acts. */
function multi(el, o) {
  D.sized(el, (w) => {
    const ht = o.height
    const all = o.series.flatMap((s) => s.values).filter(Number.isFinite)
    const m = { t: 14, r: w < 520 ? 90 : 150, b: 28, l: 0 }
    const y = d3.scaleLinear([0, d3.max(all) * 1.08], [ht - m.b, m.t]).nice(4)
    const axis = o.axis || ((t) => o.format(t))
    m.l = Math.max(...y.ticks(4).map((t) => axis(t).length)) * 7 + 12
    const x = d3.scaleLinear([0, Math.max(1, o.ticks.length - 1)], [m.l + 4, w - m.r])
    const svg = d3.select(el).append('svg').attr('width', w).attr('height', ht).attr('role', 'group')
    svg.append('g').attr('class', 'grid').selectAll('line').data(y.ticks(4)).join('line').attr('x1', m.l).attr('x2', w - m.r + 8).attr('y1', y).attr('y2', y)
    svg.append('g').attr('class', 'ax').selectAll('text').data(y.ticks(4)).join('text').attr('x', m.l - 10).attr('y', y).attr('dy', '0.32em').attr('text-anchor', 'end').text((t) => axis(t))
    const every = Math.max(1, Math.ceil(o.ticks.length / Math.max(2, Math.floor((w - m.l - m.r) / 84))))
    svg.append('g').attr('class', 'ax').selectAll('text').data(o.ticks.map((t, i) => [t, i]).filter(([, i]) => (o.ticks.length - 1 - i) % every === 0)).join('text').attr('x', ([, i]) => x(i)).attr('y', ht - 8).attr('text-anchor', 'middle').text(([t]) => t)
    const path = d3.line().defined(Number.isFinite).x((_, i) => x(i)).y(y).curve(d3.curveMonotoneX)
    const lastY = o.series.map((s) => {
      let i = s.values.length - 1
      while (i >= 0 && !Number.isFinite(s.values[i])) i--
      return i >= 0 ? y(s.values[i]) : null
    })
    // End labels, nudged apart and joined to their line with a short leader.
    const ys = lastY.map((v) => v ?? ht)
    const idx = ys.map((_, i) => i).sort((a, b) => ys[a] - ys[b])
    const placed = []
    for (const i of idx) placed[i] = Math.max(ys[i], (placed[idx[idx.indexOf(i) - 1]] ?? -Infinity) + 15)
    const groups = o.series.map((s, i) => {
      const g = svg.append('g').attr('class', `ml${i === 0 ? ' ml--lead' : ''}`).attr('tabindex', o.act ? 0 : null).attr('role', o.act ? 'button' : null).attr('aria-label', `${s.name}${o.act ? `: ${o.act.label}` : ''}`)
      g.append('path').attr('class', 'ml__line').attr('d', path(s.values))
      g.append('path').attr('class', 'ml__hit').attr('d', path(s.values))
      if (lastY[i] !== null) {
        g.append('line').attr('class', 'ml__leader').attr('x1', w - m.r + 4).attr('x2', w - m.r + 10).attr('y1', lastY[i]).attr('y2', placed[i])
        g.append('text').attr('class', 'ml__name').attr('x', w - m.r + 14).attr('y', placed[i]).attr('dy', '0.32em').text(s.name.length > 20 ? `${s.name.slice(0, 19)}…` : s.name)
      }
      g.on('pointerenter', () => svg.classed('is-hovering', true) && g.classed('is-hot', true))
        .on('pointerleave', () => (svg.classed('is-hovering', false), g.classed('is-hot', false), D.untip()))
        .on('pointermove', (ev) => {
          const i2 = Math.max(0, Math.min(o.ticks.length - 1, Math.round(x.invert(d3.pointer(ev, svg.node())[0]))))
          D.tip(ev, () => [D.tipHead(o.labels[i2]), D.tipRow(getComputedStyle(document.documentElement).getPropertyValue('--accent'), s.name, Number.isFinite(s.values[i2]) ? o.format(s.values[i2], true) : 'no value'), ...(o.act ? [D.tipFoot(o.act.kind === 'open' ? 'Click to open its page.' : 'Click to filter to it.', 'tip__hint')] : [])])
        })
        .on('click', () => o.act?.go(s.id)).on('keydown', (ev) => ev.key === 'Enter' && o.act?.go(s.id))
      return g
    })
    return groups
  })
}

/**
 * table({ kicker, query: (s) => S.run(...), columns: [{ key, label, metric | format, bar }], open: entity key and the
 * column holding its id, sort, pageSize }) — dense, ruled, sortable, searchable, 25 rows a page, every row to CSV.
 * With `open`, a row opens that entity's page.
 */
export function table({ kicker, headline = null, query: q, columns, open, sort = null, pageSize = 25, span = 12, metrics, what, period = true } = {}) {
  return block({
    key: `table|${kicker}`, kicker, headline, span, period, reserve: 44 + 34 * Math.min(pageSize, 12), metrics: metrics || columns.filter((c) => c.metric).map((c) => c.metric),
    what: what || 'The rows behind the page, under its period and filters. Sort by any column, search the text, download every row.',
    queries: (s) => [q(s)],
    draw: ([rows], body, s) => dataTable(body, rows, { columns, open, sort, pageSize, limit: Number(/\blimit:\s*(\d+)/.exec(q(s))?.[1]) || null }),
  })
}

export function dataTable(body, rows, { columns, open, sort = null, pageSize = 25, limit = null }) {
  let order = sort
  let page = 0
  let term = ''
  const cols = columns || Object.keys(rows[0] || {}).map((key) => ({ key }))
  // A column is numeric (right-aligned, left out of search) when it is a metric, has a number format, or holds numbers.
  const isNum = (c) => c.align ? c.align === 'right' : c.metric || (c.format && typeof c.format !== 'function' && c.format !== 'text') || rows.every((r) => r[c.key] === null || r[c.key] === undefined || typeof r[c.key] === 'number')
  const text = (c, r) => (typeof c.format === 'function' ? c.format(r[c.key]) : String(r[c.key] ?? ''))
  const fmtCell = (c, v) => (c.metric ? F.fmtMetric(c.metric, v) : typeof c.format === 'function' ? c.format(v) : c.format ? F.format(c.format, v) : v === null || v === undefined ? '–' : String(v))
  const maxOf = Object.fromEntries(cols.filter((c) => c.bar).map((c) => [c.key, Math.max(...rows.map((r) => Math.abs(r[c.key] ?? 0)), 1e-12)]))
  const page_ = open ? S.config.pages.find((p) => p.entity === open.entity) : null
  const input = h('input', { type: 'search', class: 'dt__search', placeholder: `Search ${rows.length.toLocaleString()} rows`, 'aria-label': 'Search the table', oninput: (e) => ((term = e.target.value.trim().toLowerCase()), (page = 0), paint()) })
  const count = h('span', { class: 'dt__count' })
  const bar = h('div', { class: 'dt__bar' }, input, count, h('button', { type: 'button', class: 'ghost ghost--small', text: 'Download CSV', onclick: () => downloadCsv(rows, Object.keys(rows[0] || {}), 'table') }))
  const scroller = h('div', { class: 'dt__scroll' })
  const pager = h('div', { class: 'dt__pager' })
  body.append(bar, scroller, pager)
  function paint() {
    let list = rows
    if (term) list = list.filter((r) => cols.some((c) => !isNum(c) && text(c, r).toLowerCase().includes(term)))
    if (order) {
      const { key, desc } = order
      list = [...list].sort((a, b) => (a[key] === b[key] ? 0 : a[key] === null || a[key] === undefined ? 1 : b[key] === null || b[key] === undefined ? -1 : (a[key] < b[key] ? -1 : 1) * (desc ? -1 : 1)))
    }
    const pages = Math.max(1, Math.ceil(list.length / pageSize))
    page = Math.min(page, pages - 1)
    const capped = limit && rows.length >= limit
    count.textContent = `${term ? `${list.length.toLocaleString()} of ` : ''}${capped ? `the first ${rows.length.toLocaleString()}` : `${rows.length.toLocaleString()} rows`}`
    const th = (c) => {
      const on = order?.key === c.key
      return h('th', { class: isNum(c) ? 'num' : null, 'aria-sort': on ? (order.desc ? 'descending' : 'ascending') : 'none' },
        h('button', { type: 'button', onclick: () => ((order = { key: c.key, desc: on ? !order.desc : isNum(c) }), paint()) }, c.label || (c.metric ? F.metric(c.metric).label : c.key.replace(/_/g, ' ').replace(/^./, (x) => x.toUpperCase())), h('span', { class: 'dt__sort', 'aria-hidden': 'true', text: on ? (order.desc ? '↓' : '↑') : '' })))
    }
    const td = (c, r) => {
      const text = fmtCell(c, r[c.key])
      if (c.bar) return h('td', { class: 'num' }, h('span', { class: 'dt__barcell' }, h('i', { style: { width: `${(Math.abs(r[c.key] ?? 0) / maxOf[c.key]) * 100}%` } }), text))
      if (c.status) return h('td', { class: 'num' }, text, ' ', statusMark(F.status(c.status, r[c.key])))
      return h('td', { class: isNum(c) ? 'num' : null }, page_ && c.key === open.key ? h('a', { href: store.linkTo(page_.id, r[c.key]), text }) : text)
    }
    scroller.replaceChildren(h('table', { class: 'dt' }, h('thead', {}, h('tr', {}, ...cols.map(th))), h('tbody', {}, ...list.slice(page * pageSize, (page + 1) * pageSize).map((r) => {
      const tr = h('tr', {}, ...cols.map((c) => td(c, r)))
      if (page_) {
        tr.classList.add('is-link')
        tr.addEventListener('click', (e) => !e.target.closest('a') && store.open(page_.id, r[open.key]))
      }
      return tr
    }))))
    pager.replaceChildren(...(pages > 1 ? [h('button', { type: 'button', class: 'ghost ghost--small', text: 'Previous', disabled: page === 0, onclick: () => (page--, paint()) }), h('span', { text: `Page ${page + 1} of ${pages}` }), h('button', { type: 'button', class: 'ghost ghost--small', text: 'Next', disabled: page >= pages - 1, onclick: () => (page++, paint()) })] : []))
  }
  paint()
}

/**
 * heat({ rows: { expr, label, name }, cols: { expr, label, name }, metric, kicker, onPick }) — a metric across two
 * dimensions, such as weekday by hour. A cell filters to its row value when that field is a filter, or calls onPick.
 */
export function heat({ rows: R, cols: Cc, metric: n, kicker, headline, min = 0, volume, span = 12, onPick, where, period = true, what } = {}) {
  return block({
    key: `heat|${R.expr}|${Cc.expr}|${n}`, kicker, span, reserve: 60 + 28 * (R.count || 7), metrics: [n, ...(volume ? [volume] : [])],
    what: what || `${F.metric(n).label} for each ${R.label.toLowerCase()} and ${Cc.label.toLowerCase()}${period ? ' in the period' : ''}; darker is more.${min ? ` Cells with fewer than ${min} ${F.metric(volume).label.toLowerCase()} are left blank.` : ''}`,
    period, queries: (s) => [S.run(`{ group_by: r is ${R.expr}, c is ${Cc.expr}; aggregate: ${n}${volume ? `, vol is ${volume}` : ''}; order_by: r, c }`, s, { where, which: period ? 'current' : 'none' })],
    draw: ([rows], body) => {
      const rs = [...new Set(rows.map((r) => r.r))]
      const cs = [...new Set(rows.map((r) => r.c))].sort((a, b) => (a < b ? -1 : 1))
      const at = new Map(rows.filter((r) => !volume || r.vol >= min).map((r) => [`${r.r}|${r.c}`, r[n]]))
      const f = S.config.filters.find((x) => x.field === R.expr)
      D.heat(body.appendChild(h('div', { class: 'chart' })), {
        rows: rs, cols: cs, value: (r, c) => at.get(`${r}|${c}`) ?? null, format: (v) => F.fmtMetric(n, v), valueName: F.metric(n).label,
        rowLabel: R.name, colLabel: Cc.name, label: kicker,
        onPick: onPick || (f ? (r) => store.toggleFilter(f.key, r) : null),
      })
    },
    headline: headline ? (rs, s) => headline(rs[0], s) : ([rows]) => {
      const ok = rows.filter((r) => !volume || r.vol >= min)
      if (ok.length < 4) return null
      const m = F.metric(n)
      const hi = ok.reduce((a, b) => (b[n] > a[n] ? b : a))
      const lo = ok.reduce((a, b) => (b[n] < a[n] ? b : a))
      const nm = (r) => `${R.name ? R.name(r.r) : r.r} ${Cc.name ? Cc.name(r.c) : r.c}`
      return [m.good === 'down' ? 'Worst: ' : 'Highest: ', { b: nm(m.good === 'down' ? hi : hi) }, ` at ${F.fmtMetric(n, hi[n])}; ${m.good === 'down' ? 'best' : 'lowest'}: `, { b: nm(lo) }, ` at ${F.fmtMetric(n, lo[n])}.`]
    },
  })
}

/**
 * watch({ field | entity, metric, volume, min, extra, limit }) — a monitor's list of what needs attention: every value
 * with its status against the metric's thresholds, the worst first, its change and volume. Values under `min` of the
 * volume are left out and counted.
 */
export function watch({ field, entity, metric: n, volume, min = 0, extra = [], limit = 10, kicker, span = 6, where } = {}) {
  const fieldName = field || S.config.entities[entity].field
  const col = S.colOf(fieldName)
  const name = nameOf(fieldName, entity)
  const m = F.metric(n)
  return block({
    compare: true, key: `watch|${fieldName}|${n}`, kicker: kicker || `${m.label} by ${col.replace(/_/g, ' ')}`, span, reserve: 70 + 35 * limit, metrics: [n, volume, ...extra],
    what: `Each value against the thresholds for ${lower(m.label)}, the furthest past them first.${min ? ` Those with fewer than ${F.fmtMetric(volume, min)} ${lower(F.metric(volume).label)} in the period are left out, so a handful of records cannot top the list.` : ''}`,
    queries: (s) => [S.breakdownQuery(fieldName, [n], s, { top: 1000, order: `${volume} desc`, extra: [volume, ...extra], where }), S.breakdownCompareQuery(fieldName, [n], s, { where })].filter(Boolean),
    draw: ([cur, cmp], body) => {
      const all = S.withCompare(cur, cmp, fieldName)
      const rows = all.filter((r) => (r[volume] ?? 0) >= min && Number.isFinite(r[n]))
      const t = m.thresholds || {}
      const past = (v) => (m.good === 'down' ? v - (t.watch ?? t.breach ?? v) : (t.watch ?? t.breach ?? v) - v)
      const rk = { breach: 0, watch: 1, ok: 2 }
      const scored = rows.map((r) => ({ r, st: F.status(n, r[n]) || 'ok' })).sort((a, b) => rk[a.st] - rk[b.st] || past(b.r[n]) - past(a.r[n]))
      const c = (st) => scored.filter((x) => x.st === st).length
      body.append(h('p', { class: 'watch__sum' }, h('span', {}, h('strong', { class: 'chg--bad', text: String(c('breach')) }), ' breaching'), h('span', {}, h('strong', { class: 'chg--watch', text: String(c('watch')) }), ' to watch'), h('span', {}, h('strong', { class: 'chg--good', text: String(c('ok')) }), ' on track'),
        all.length > rows.length && h('span', { class: 'watch__left', text: `${all.length - rows.length} too small to judge` })))
      rankList(body, scored.slice(0, limit).map((x) => x.r), { col, n, field: fieldName, entity, name, extra: [volume] })
      body.querySelectorAll('tbody tr').forEach((tr, i) => {
        const st = scored[i].st
        tr.classList.add(`is-${st}`)
        tr.querySelector('.rank__name').prepend(h('i', { class: `dot dot--${st}`, title: F.STATUS_WORDS[st] }))
      })
    },
    headline: ([all]) => {
      const bad = all.filter((r) => (r[volume] ?? 0) >= min && F.status(n, r[n]) === 'breach').sort((a, b) => (m.good === 'down' ? b[n] - a[n] : a[n] - b[n]))
      if (!bad.length) return `Everything with enough ${lower(F.metric(volume).label)} is inside the thresholds.`
      return [{ b: name(bad[0][col]) }, ` is furthest past the line at ${F.fmtMetric(n, bad[0][n])}${bad.length > 1 ? `, one of ${bad.length} breaching` : ''}.`]
    },
  })
}

/**
 * entityHead({ metric }) — the top of an entity's page: what it is, its name, and where it stands: its rank among its
 * peers by the metric, its share of the whole, and its change. The peers are the same filters without the entity.
 */
export function entityHead({ metric: n } = {}) {
  const s = store.state
  const e = S.entityOf(s)
  const id = s.id
  const nm = e.name ? e.name(id) : String(id)
  const facts = h('p', { class: 'entity__facts', 'data-pending': '' }, h('span', { class: 'skel skel--wide' }))
  const ign = { entity: false, which: 'both' }
  const q = S.run(`{ where: ${e.field} is not null; group_by: e is ${e.field}; aggregate: m is ${S.within(n, s)}${S.within(n, s, 'compare') ? `, mc is ${S.within(n, s, 'compare')}` : ''}, tot is all(${S.within(n, s)}); calculate: r is rank(); order_by: m desc }`, s, ign).replace(/ }$/, ` } -> { where: e = ${S.lit(id)}; select: * }`)
  const qn = S.partsQuery(e.field, n, s, { entity: false })
  Promise.all([query(q), query(qn)]).then(([[r], [c]]) => {
    if (!r) return facts.replaceChildren(`No ${lower(F.metric(n).label)} for this ${lower(e.label)} ${S.dated() ? 'in the period' : 'in this data'}.`)
    const ord = (k) => `${k}${['th', 'st', 'nd', 'rd'][(k % 100 > 10 && k % 100 < 14) || k % 10 > 3 ? 0 : k % 10]}`
    const parts = [{ b: `${ord(r.r)} of ${c.n.toLocaleString()}` }, ` ${e.plural || `${lower(e.label)}s`} by ${lower(F.metric(n).label)}, with `, { b: F.fmtMetric(n, r.m, { compact: true }) }]
    if (S.adds(c) && r.tot) parts.push(`, ${(100 * r.m / r.tot).toFixed(1)}% of the total`)
    if (Number.isFinite(r.mc)) parts.push(`; ${F.delta(n, r.m, r.mc).text.replace(/^\+/, 'up ').replace(/^−/, 'down ')} on ${P.compareWords(s.period, S.range(s), s.compare)}`)
    parts.push('.')
    facts.replaceChildren(...prose(parts))
  }, (err) => facts.replaceChildren(`Could not place it among its peers: ${reason(err)}`)).finally(() => {
    facts.removeAttribute('data-pending')
    markReady()
  })
  // Back to where the reader came from: the last page that was not an entity's, else the page that lists this entity.
  const back = S.config.pages.find((p) => p.id === store.cameFrom && !p.entity) || S.config.pages.find((p) => !p.entity && !p.hidden && p.lists === e.key) || S.config.pages[0]
  const title = h('h1', { class: 'entity__name', text: nm })
  // An entity addressed by an id (a customer number) can name a field holding what to call it.
  if (e.nameField) query(S.run(`{ group_by: nm is ${e.nameField}; limit: 1; order_by: nm }`, s, { which: 'none' })).then(([r]) => r?.nm && (title.textContent = r.nm, document.title = `${r.nm} · ${S.config.title}`), () => {})
  return h('header', { class: 'entity', 'data-redraw': `entity|${e.key}|${id}` },
    h('p', { class: 'crumbs' }, h('a', { href: store.linkTo(back.id), text: back.title }), h('span', { 'aria-hidden': 'true', text: ' / ' }), e.label),
    title,
    facts)
}

/** Each source: its name linked, who publishes it, what it is, its licence. */
/** The articles the app responds to (config.articles: { title, outlet, date, url }), each linked; no quoted text. */
export function newsList(items = S.config.articles || []) {
  return h('ul', { class: 'news' }, ...items.map((a) => h('li', {},
    h('a', { href: webUrl(a.url), target: '_blank', rel: 'noopener', text: a.title }),
    h('span', { class: 'news__by', text: [a.outlet, a.date && (/^\d{4}-\d{2}-\d{2}$/.test(a.date) ? P.longDate(a.date) : a.date)].filter(Boolean).join(', ') }))))
}
/** A link only to a web page: anything but http(s) becomes no link at all. */
function webUrl(u) {
  return /^https?:\/\//i.test(String(u || '')) ? u : null
}
/** "In the news": a section of the articles, for the first page of an app made from a story. */
export const news = ({ kicker = 'In the news' } = {}) => h('section', { class: 'news-block' }, h('p', { class: 'kicker', text: kicker }), newsList())

export function sourceList(srcs = Object.values(S.config.sources || {})) {
  return h('ul', { class: 'sources' }, ...srcs.map((x) => h('li', {},
    webUrl(x.url) ? h('a', { href: x.url, target: '_blank', rel: 'noopener', text: x.name }) : h('strong', { text: x.name }),
    x.by ? ` · ${x.by}. ` : '. ', x.what || '', x.license ? h('span', { class: 'sources__license', text: ` ${x.license}.` }) : null)))
}

/**
 * aboutPage({ lede, notes }) — the page that says where the data comes from: each source with its licence, then
 * notes ({ title, text }: what was reshaped, what is left out, caveats), then how fresh it is. Every app built on
 * public data has one.
 */
export function aboutPage({ title = 'About the data', lede, notes = [] } = {}) {
  return h('div', { class: 'about' }, pageHead(title, lede),
    h('section', { class: 'about__part' }, h('h2', { text: 'Where the data comes from' }), sourceList()),
    (S.config.articles || []).length > 0 && h('section', { class: 'about__part' }, h('h2', { text: 'In the news' }), newsList()),
    ...notes.map((n) => h('section', { class: 'about__part' }, h('h2', { text: n.title }), h('p', {}, ...prose(n.text)))),
    S.dated() && h('p', { class: 'about__fresh', text: `The data runs to ${P.longDate(S.known.span.last)}.` }))
}

// ---- Layout ----

/** A page's title and lede, for pages that are not an entity's. */
export const pageHead = (title, lede) => h('header', { class: 'page-head' }, h('h1', { text: title }), lede && h('p', { class: 'lede' }, ...prose(lede)))
/** Blocks on the grid: span-12 blocks take the row, span-6 ones pair up. */
export const blocks = (...items) => h('div', { class: 'blocks' }, ...items.flat())
export { asText }
