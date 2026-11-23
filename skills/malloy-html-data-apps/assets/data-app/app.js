// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT
import * as S from './scope.js'
import * as C from './charts.js'

const $ = (sel, root = document) => root.querySelector(sel)

/** h('div', { class: 'x', text: 'y', onclick }, ...children): a small element builder. */
function h(tag, props = {}, ...kids) {
  const el = document.createElement(tag)
  for (const [k, v] of Object.entries(props || {})) {
    if (v === null || v === undefined || v === false) continue
    if (k === 'text') el.textContent = v
    else if (k === 'class') el.className = v
    else if (k.startsWith('on')) el.addEventListener(k.slice(2), v)
    else el.setAttribute(k, v === true ? '' : v)
  }
  el.append(...kids.flat().filter((k) => k !== null && k !== undefined && k !== false))
  return el
}

// ---- State: the route and the scope, both in the address ----

const known = {} // filled by boot() from distinct-value queries
let route = { view: 'overview', id: null, compare: [] }
let scope = { ...S.DEFAULT_SCOPE }

// ---- Queries: one request per distinct text, at most four in flight ----

const cache = new Map()
const queue = []
let inFlight = 0
function pump() {
  while (inFlight < 4 && queue.length) {
    const job = queue.shift()
    inFlight++
    job().finally(() => {
      inFlight--
      pump()
    })
  }
}
export function query(q) {
  if (!cache.has(q)) {
    const p = new Promise((resolve, reject) => {
      queue.push(() => Publisher.query(S.MODEL, q).then(resolve, reject))
      pump()
    })
    p.catch(() => cache.delete(q))
    cache.set(q, p)
  }
  return cache.get(q)
}

/** d3, loaded once beside the first queries rather than ahead of them; boot() waits for it before anything draws. */
const d3Ready = window.d3
  ? Promise.resolve()
  : new Promise((resolve, reject) => {
      const s = document.createElement('script')
      s.src = new URL('./vendor/d3.min.js', import.meta.url).href
      s.onload = resolve
      s.onerror = () => reject(Object.assign(new Error('Could not load the chart library (vendor/d3.min.js).'), { whole: true }))
      document.head.append(s)
    })

/** What a failed query says: the server's reason when it gave one. */
const reason = (err) => err?.response?.message ?? err?.message ?? String(err)

// ---- Navigation ----

function go(nextRoute, nextScope = scope, { push = true, toTop = push } = {}) {
  route = nextRoute
  scope = nextScope
  const hash = S.buildHash(route, scope)
  if (hash !== location.hash) history[push ? 'pushState' : 'replaceState'](null, '', hash)
  render({ toTop })
}
const setScope = (patch) => go(route, { ...scope, ...patch }, { push: false })
const toggleScope = (key, value) => setScope({ [key]: scope[key] === value ? S.DEFAULT_SCOPE[key] : value })
const openEntity = (id) => go({ view: S.ENTITY.view, id, compare: [] })
const linkTo = (view, id = null, compare = []) => S.buildHash({ view, id, compare }, scope)

// ---- Compare: the entities picked to set side by side, kept until the page reloads. The header's Compare link
// carries and counts them, so a + on a ranked list shows where it went. The Compare view shows what its own address
// holds, so Back and Forward show each step; opening it with picks in the address makes them the picks. Pass
// `inCompare` and `onCompare: toggleCompare` to every ranked list of entities. ----

let picked = []
/** The picks a control on this view acts on: the Compare view's own, elsewhere the kept ones. */
const current = () => (route.view === 'compare' ? route.compare : picked)
const inCompare = (id) => current().some((x) => String(x) === String(id))
/** A control to focus once the next view is drawn, such as the next × after one is pressed. */
let focusNext = null
/** Open Compare with `ids`; past the limit the oldest picks go, as with the +. Changing the picks on Compare itself
 *  keeps the reader's place (`toTop: false`). */
function goCompare(ids, { toTop = true } = {}) {
  picked = ids.slice(-S.COMPARE_MAX)
  go({ view: 'compare', id: null, compare: picked }, scope, { toTop })
}
function toggleCompare(id) {
  const ids = inCompare(id) ? current().filter((x) => String(x) !== String(id)) : [...current(), id]
  if (route.view === 'compare') goCompare(ids, { toTop: false })
  else {
    picked = ids.slice(-S.COMPARE_MAX)
    showPicked()
  }
}
/** The header's Compare link, and the + on every ranked list on the page, not only the one clicked. */
function showPicked() {
  const a = $('.app-nav a[data-view="compare"]')
  if (a) {
    a.href = linkTo('compare', null, picked)
    // The count is drawn by the stylesheet, so the link's own text stays "Compare" for whatever reads it.
    if (picked.length) a.dataset.count = picked.length
    else delete a.dataset.count
    a.setAttribute('aria-label', picked.length ? `Compare, ${picked.length} picked` : 'Compare')
  }
  for (const b of document.querySelectorAll('#main .rt__act button')) {
    const on = inCompare(b.closest('tr')?.dataset.id)
    const name = (b.getAttribute('aria-label') || '').replace(/^(Add|Remove) /, '').replace(/ (to|from) compare$/, '')
    b.textContent = on ? '✓' : '+'
    b.setAttribute('aria-pressed', String(on))
    b.setAttribute('aria-label', `${on ? 'Remove' : 'Add'} ${name} ${on ? 'from' : 'to'} compare`)
  }
}

window.addEventListener('popstate', () => {
  ;({ route, scope } = S.parseHash(location.hash, known))
  render({ toTop: true })
})

// ---- Tiles: a card that loads its queries when it comes near the screen ----

let epoch = 0
const io = new IntersectionObserver(
  (entries) => {
    for (const e of entries) {
      if (!e.isIntersecting) continue
      io.unobserve(e.target)
      e.target.__load()
    }
  },
  { rootMargin: '400px' }
)

/**
 * tile({ title, sub, cls, reserve, queries: () => [q...], explain: [what each query counts, in plain words],
 *        draw: (results, body) => void or a promise, finding: (results) => sentence or prose() parts,
 *        method: what the card leaves out and why, note })
 * Once loaded, a card is drawn again in place when the scope changes: its chart stays, dimmed, until the new rows
 * arrive, then its marks move, its numbers count, and its body eases to its new height.
 */
function tile(spec) {
  const { title, sub, cls = '', note } = spec
  let current = spec
  // reserve: the chart's height in px while it loads (240 when not given), so the page does not jump when it draws.
  const body = h('div', { class: 'card__body' }, h('div', { class: 'state state--loading', text: 'Loading', style: spec.reserve ? `--card-reserve: ${spec.reserve}px` : null }))
  const found = h('p', { class: 'card__finding' })
  const methodEl = h('p', { class: 'card__method', text: spec.method || '' })
  let results = []
  const details = h('details', {}, h('summary', {}, h('span', { class: 'when-closed', text: 'Show the working' }), h('span', { class: 'when-open', text: 'Hide the working' })))
  details.addEventListener('toggle', () => {
    details.querySelector('.working')?.remove()
    if (details.open) details.append(working(current.queries(), current.explain || [], results))
  })
  const card = h(
    'article',
    { class: `card ${cls}`, 'data-key': `${cls}|${title}` },
    h('h3', { class: 'card__title', text: title }),
    sub && h('p', { class: 'card__sub', text: sub }),
    body,
    found,
    spec.method && methodEl,
    h('div', { class: 'card__foot' }, h('span', { text: note || '' }), details)
  )
  card.__spec = spec
  const run = (updating) => {
    const mine = epoch
    const s = current
    card.__loaded = true
    if (updating) card.classList.add('is-updating')
    return Promise.all(s.queries().map(query))
      .then((rs) => {
        if (mine !== epoch) return
        results = rs
        const was = updating ? snapshot(body) : null
        if (updating) {
          // Emptying the body while a mark in it has focus lets the browser lay out a shorter page and move the
          // scroll: the focus goes first, and the body keeps its height until the new chart is in.
          if (body.contains(document.activeElement)) document.activeElement.blur()
          body.style.minHeight = `${was.height}px`
          body.__pool = [...body.querySelectorAll('.chart')]
        }
        body.replaceChildren()
        if (!results[0]?.length) {
          body.style.minHeight = ''
          body.append(h('div', { class: 'state', text: 'Nothing matches these filters.' }))
          return
        }
        // The chart first: a finding that cannot be written for this scope leaves its line empty, never the card.
        const drawn = s.draw(results, body)
        try {
          if (s.finding) found.replaceChildren(...prose(s.finding(results)))
        } catch (err) {
          found.replaceChildren()
          console.error(`[${title}] finding`, err)
        }
        return Promise.resolve(drawn).then(() => {
          body.__pool = null
          body.style.minHeight = ''
          if (was) settleInPlace(body, was)
        })
      })
      .then(() => {
        if (mine === epoch) card.dataset.loaded = '1'
      })
      .finally(() => mine === epoch && card.classList.remove('is-updating'))
      .catch((err) => {
        if (mine !== epoch) return
        body.replaceChildren(h('div', { class: 'state state--error', text: `Could not load: ${reason(err)}` }))
        console.error(`[${title}]`, err)
      })
  }
  card.__load = () => run(false)
  const respec = (next) => {
    current = card.__spec = next
    if (next.method) methodEl.textContent = next.method
  }
  card.__respec = respec // not loaded yet: it draws with the new scope when it comes into view
  card.__update = (next) => {
    respec(next)
    details.open = false
    return run(true)
  }
  return card
}

/** What a card body showed before it is drawn again: its height, its headline numbers, where each ranked row sat. */
function snapshot(body) {
  const rows = new Map()
  for (const tr of body.querySelectorAll('tr[data-id]')) {
    const bar = tr.querySelector('.rt__bar i')
    rows.set(tr.dataset.id, { top: tr.getBoundingClientRect().top, bar: bar?.getBoundingClientRect().width, num: tr.querySelector('.rt__bar span')?.textContent })
  }
  return { height: body.offsetHeight, numbers: [...body.querySelectorAll('.kpi__value')].map((n) => n.textContent), rows }
}

/** After a card is drawn again: its height eases to the new one, numbers count, rows slide to their new places. */
function settleInPlace(body, was) {
  if (!C.MOTION) return
  const ease = { duration: C.DUR, easing: 'cubic-bezier(.2,.7,.3,1)' }
  const height = body.offsetHeight
  if (Math.abs(height - was.height) > 4) body.animate([{ height: `${was.height}px`, overflow: 'hidden' }, { height: `${height}px`, overflow: 'hidden' }], ease)
  ;[...body.querySelectorAll('.kpi__value')].forEach((n, i) => was.numbers[i] !== undefined && C.countTo(n, was.numbers[i], n.textContent))
  for (const tr of body.querySelectorAll('tr[data-id]')) {
    const old = was.rows.get(tr.dataset.id)
    if (!old) {
      tr.animate([{ opacity: 0 }, { opacity: 1 }], ease)
      continue
    }
    const dy = old.top - tr.getBoundingClientRect().top
    if (Math.abs(dy) > 1) tr.animate([{ transform: `translateY(${dy}px)` }, { transform: 'none' }], ease)
    const bar = tr.querySelector('.rt__bar i')
    if (bar && old.bar !== undefined) bar.animate([{ width: `${old.bar}px` }, { width: `${bar.getBoundingClientRect().width}px` }], ease)
    const num = tr.querySelector('.rt__bar span')
    if (num && old.num) C.countTo(num, old.num, num.textContent)
  }
}

/** The working behind a card: for each query, what it counts, the query itself, and the rows it returned. */
function working(qs, explain, results) {
  const cell = (v) => (v === null || v === undefined ? '∅' : typeof v === 'number' ? String(Number.isInteger(v) ? v : Number(v.toPrecision(4))) : String(v))
  return h('div', { class: 'working' }, ...qs.map((q, i) => {
    const rows = results[i] || []
    const cols = rows.length ? Object.keys(rows[0]) : []
    return h('div', { class: 'working__item' },
      explain[i] && h('p', { text: explain[i] }),
      h('pre', { text: q }),
      h('span', { text: `${rows.length} row${rows.length === 1 ? '' : 's'}${rows.length > 100 ? ', the first 100 shown' : ''}` }),
      cols.length && h('button', { type: 'button', class: 'btn btn--small', text: 'Download CSV', onclick: () => downloadCsv(rows, cols, i) }),
      cols.length && h('div', { class: 'working__rows' }, h('table', {},
        h('thead', {}, h('tr', {}, ...cols.map((c) => h('th', { text: c })))),
        h('tbody', {}, ...rows.slice(0, 100).map((r) => h('tr', {}, ...cols.map((c) => h('td', { class: typeof r[c] === 'number' ? 'num' : null, text: cell(r[c]) }))))))))
  }))
}

/** Every row a query returned, as a CSV file the reader saves: the numbers behind a card, unrounded. */
function downloadCsv(rows, cols, i) {
  // Quoted where a spreadsheet would split or run it: commas, quotes, line breaks, a leading = + - @.
  const cell = (v) => {
    const t = v === null || v === undefined ? '' : String(v)
    return /[",\n\r]|^[=+\-@]/.test(t) ? `"${(/^[=+\-@]/.test(t) ? `'${t}` : t).replace(/"/g, '""')}"` : t
  }
  const text = [cols, ...rows.map((r) => cols.map((c) => r[c]))].map((r) => r.map(cell).join(',')).join('\r\n')
  const a = h('a', { href: URL.createObjectURL(new Blob([text], { type: 'text/csv' })), download: `${document.title.replace(/\W+/g, '-').toLowerCase()}-${i + 1}.csv` })
  document.body.append(a)
  a.click()
  setTimeout(() => (URL.revokeObjectURL(a.href), a.remove()), 0)
}

/**
 * Prose with bold entities: prose(['', { b: 'Idaho' }, ' grew fastest']) or a plain string. A lede or a finding is a
 * sentence built from the data, and the entity it names is bold.
 */
const prose = (parts) =>
  !parts ? [] : typeof parts === 'string' ? [parts] : parts.map((p) => (p && typeof p === 'object' ? h('strong', { text: String(p.b ?? '') }) : String(p ?? '')))

/**
 * A box for a chart from the chart module, which sizes itself to the box's width and redraws when it changes. A card
 * drawn again in place reuses its boxes in order, so its charts move rather than redraw.
 */
const chartBox = (body, size = '') => {
  const reuse = body.__pool?.shift()
  if (!reuse) return body.appendChild(h('div', { class: `chart ${size}` }))
  reuse.className = `chart ${size}`
  return body.appendChild(reuse)
}

const section = (title, sub, ...cards) =>
  h('section', { class: 'section' }, h('div', { class: 'section__head' }, h('h2', { text: title }), h('p', { text: sub })), h('div', { class: 'grid-2' }, ...cards))

// ---- The scope bar: shown on every view that the scope filters ----

const SCOPED = new Set(['explore', 'records', 'compare', S.ENTITY.view])

function renderScope() {
  const bar = $('#scope')
  bar.hidden = !SCOPED.has(route.view)
  if (bar.hidden) return
  // The sentence look reads "Showing <select> in <select>": each SCOPE entry's `word` goes before its select.
  const sentence = document.body.dataset.filters === 'sentence'
  // An entity's page and Compare pick the entity themselves: a scope key on its field would read "All" beside it.
  const skip = S.skipFor(route.view)
  const controls = []
  for (const [key, spec] of Object.entries(S.SCOPE)) {
    if (skip.includes(key)) continue
    if (spec.type === 'flag') {
      controls.push(h('button', { class: 'btn', type: 'button', 'aria-pressed': String(scope[key]), text: spec.label(true), onclick: () => setScope({ [key]: !scope[key] }) }))
      continue
    }
    const values = spec.type === 'pick' ? known[key] : range(...known[spec.range])
    const sel = h('select', { 'aria-label': key, onchange: (e) => setScope({ [key]: parse(spec, e.target.value) }) },
      h('option', { value: '', text: spec.type === 'pick' ? `All` : spec.type === 'from' ? 'Earliest' : 'Latest' }),
      ...values.map((v) => h('option', { value: v, text: v, selected: scope[key] !== null && String(scope[key]) === String(v) })))
    controls.push(h('label', {}, sentence ? spec.word ?? key : key[0].toUpperCase() + key.slice(1), sel))
  }
  const had = new Set([...document.querySelectorAll('#scope-bar .chip')].map((c) => c.dataset.label))
  const chips = h('div', { class: 'chips' }, ...S.scopeChips(scope, skip).map((c) =>
    h('span', { class: `chip${had.has(c.label) || !C.MOTION ? '' : ' is-new'}`, 'data-label': c.label }, c.label, h('button', { type: 'button', 'aria-label': `Clear ${c.label}`, text: '×', onclick: () => setScope(c.clear) }))))
  const end = h('div', { class: 'scope__end' },
    h('button', { class: 'btn', type: 'button', text: 'Reset', onclick: () => setScope({ ...S.DEFAULT_SCOPE }) }),
    h('button', { class: 'btn btn--primary', type: 'button', text: 'Copy link', onclick: () => navigator.clipboard?.writeText(location.href) }))
  $('#scope-bar').replaceChildren(...controls, chips, end)
}
const range = (lo, hi) => Array.from({ length: hi - lo + 1 }, (_, i) => lo + i)
const parse = (spec, v) => (v === '' ? null : spec.type === 'pick' ? v : Number(v))

// ---- Search: find an entity by name or id, open its page ----

function wireSearch() {
  const input = $('#search')
  const list = $('#search-list')
  let hits = []
  let active = -1
  const close = () => {
    list.hidden = true
    input.setAttribute('aria-expanded', 'false')
    active = -1
  }
  const show = () => {
    list.replaceChildren(...hits.map((id, i) =>
      h('li', { role: 'option', id: `opt-${i}`, 'aria-selected': String(i === active), text: S.ENTITY.label(id), onmousedown: (e) => (e.preventDefault(), pick(id)) })))
    list.hidden = !hits.length
    input.setAttribute('aria-expanded', String(!list.hidden))
    input.setAttribute('aria-activedescendant', active >= 0 ? `opt-${active}` : '')
  }
  const pick = (id) => {
    input.value = ''
    close()
    openEntity(id)
  }
  input.addEventListener('input', () => {
    const t = input.value.trim().toLowerCase()
    // Names that start with what was typed come first: "Ida" should find Idaho before Florida.
    const label = (id) => S.ENTITY.label(id).toLowerCase()
    const starts = t ? known.entity.filter((id) => label(id).startsWith(t) || label(id).includes(` ${t}`)) : []
    hits = t ? [...starts, ...known.entity.filter((id) => label(id).includes(t) && !starts.includes(id))].slice(0, 8) : []
    active = hits.length ? 0 : -1
    show()
  })
  input.addEventListener('keydown', (e) => {
    if (e.key === 'ArrowDown' || e.key === 'ArrowUp') {
      e.preventDefault()
      active = (active + (e.key === 'ArrowDown' ? 1 : -1) + hits.length) % Math.max(1, hits.length)
      show()
    } else if (e.key === 'Enter' && active >= 0) pick(hits[active])
    else if (e.key === 'Escape') close()
  })
  input.addEventListener('blur', close)
}

// ---- Keeping the reader's place: what they clicked stays where it was on screen while cards around it change ----

let lastCard = null
addEventListener('pointerdown', (e) => (lastCard = e.target.closest?.('#main .card') || null), true)
addEventListener('keydown', (e) => (e.key === 'Enter' || e.key === ' ') && (lastCard = e.target.closest?.('#main .card') || null), true)
let keeping = null
/** Hold `anchor` at its place on screen as the cards around it draw again; the reader scrolling ends it. */
function keepInView(anchor, ms = 1600) {
  keeping?.()
  const want = anchor.getBoundingClientRect().top
  const stops = ['wheel', 'touchmove', 'keydown']
  let live = true
  const stop = (keeping = () => {
    live = false
    keeping = null
    for (const t of stops) removeEventListener(t, stop, true)
  })
  for (const t of stops) addEventListener(t, stop, { capture: true, passive: true })
  const t0 = performance.now()
  const hold = () => {
    if (!live || !anchor.isConnected) return
    const off = anchor.getBoundingClientRect().top - want
    if (Math.abs(off) > 0.5) scrollBy(0, off)
    if (performance.now() - t0 < ms) requestAnimationFrame(hold)
    else stop()
  }
  requestAnimationFrame(hold)
}

// ---- Render: a new scope on the same view draws its cards again in place; another view fades in ----

let shown = null
function render({ toTop = false } = {}) {
  const here = `${route.view}|${route.id ?? ''}`
  const same = shown === here
  const first = shown === null
  shown = here
  if (!same && !first && C.MOTION && document.startViewTransition) document.startViewTransition(() => paint(toTop, same))
  else paint(toTop, same)
}

function paint(toTop, same) {
  epoch++
  io.disconnect()
  if (route.view === 'compare' && route.compare.length) picked = [...route.compare]
  for (const a of document.querySelectorAll('.app-nav a')) {
    a.href = linkTo(a.dataset.view)
    const on = a.dataset.view === route.view || (a.dataset.view === 'explore' && route.view === S.ENTITY.view)
    if (on) a.setAttribute('aria-current', 'page')
    else a.removeAttribute('aria-current')
  }
  showPicked()
  // Where the reader's card is, measured before anything above it changes: the scope bar gains or loses a row of chips.
  const here = [...document.querySelectorAll('#main .card')]
  const anchor = !same ? null : here.includes(lastCard) && lastCard.getBoundingClientRect().bottom > 0 && lastCard.getBoundingClientRect().top < innerHeight ? lastCard : here.find((c) => c.getBoundingClientRect().bottom > 0)
  const want = anchor?.getBoundingClientRect().top
  renderScope()
  const views = { overview, explore, records, compare, about, [S.ENTITY.view]: entity }
  const next = views[route.view]()
  // The same view: every card stays where it is in the page and is drawn again with its new spec. Moving a card out
  // of the document, even for a moment, drops the focus a click gave a mark and lets the browser move the scroll.
  const fresh = [...next.querySelectorAll('.card')]
  const byKey = new Map(here.map((c) => [c.dataset.key, c]))
  if (same && fresh.length === here.length && fresh.every((c) => c.__spec && byKey.has(c.dataset.key))) {
    for (const c of fresh) {
      const old = byKey.get(c.dataset.key)
      if (old.__loaded) old.__update(c.__spec)
      else old.__respec(c.__spec)
    }
    // Parts of a view that are not cards but follow the address, such as Compare's picks, marked data-redraw.
    for (const el of next.querySelectorAll('[data-redraw]')) $(`#main [data-redraw="${el.dataset.redraw}"]`)?.replaceWith(el)
    if (anchor) {
      scrollBy(0, anchor.getBoundingClientRect().top - want)
      keepInView(anchor)
    }
  } else {
    const y = scrollY
    $('#main').replaceChildren(next)
    if (!toTop) scrollTo(0, y)
  }
  document.querySelectorAll('#main .card').forEach((c) => c.__load && !c.__loaded && io.observe(c)) // only tile() cards load
  if (toTop) {
    $('.app-header').scrollIntoView({ block: 'start' })
    $('#main').focus({ preventScroll: true })
  }
  if (focusNext) {
    ;($(focusNext) || $('#main')).focus({ preventScroll: true })
    focusNext = null
  }
}

// ---- The headline check: open the app at ?check=1#/ and the page says whether its headline is the model's ----

async function selfCheck() {
  const card = await new Promise((done) => {
    const look = () => { const c = $('#main .card'); if (c?.dataset.loaded) done(c); else setTimeout(look, 200) }
    look()
  })
  const fails = []
  const qs = card.__spec.queries()
  for (const q of qs) if (!/^run: \w+ -> \w+$/.test(q)) fails.push(`The headline card adds to a view: ${q}`)
  const values = (await Promise.all(qs.map(query))).flat().flatMap((r) => Object.values(r)).filter((v) => typeof v === 'number')
  // A number shown agrees with a model value when the value, or its percentage, rounds to it at the shown decimals.
  const agrees = (t) => {
    const m = /([−-]?)([\d,]*\.?\d+)/.exec(t)
    const n = Number(m[2].replace(/,/g, '')) * (m[1] ? -1 : 1)
    const d = (m[2].split('.')[1] || '').length
    const near = (v) => Math.abs(v - n) <= 0.5 * 10 ** -d + 1e-9 || Math.abs(Math.abs(v) - Math.abs(n)) <= 0.5 * 10 ** -d + 1e-9
    return values.some((v) => near(v) || near(v * 100))
  }
  const shown = [...document.querySelectorAll('.answer strong, .answer b')].concat([...card.querySelectorAll('.kpi__value, .kpi__sub strong')]).map((n) => n.textContent).filter((t) => /\d/.test(t))
  for (const t of shown) if (!agrees(t)) fails.push(`"${t}" is not a value the headline queries return`)
  const text = document.body.innerText
  for (const bad of [/\bundefined\b/, /\bNaN\b/, /\bTODO\b/, /placeholder/i, /returned no text/i]) if (bad.test(text)) fails.push(`The page says ${bad}`)
  if (!qs.length || !shown.length) fails.push('The headline card shows no queries or no numbers to check')
  document.body.dataset.check = fails.length ? 'fail' : 'ok'
  document.body.prepend(h('pre', { class: 'self-check', text: fails.length ? `Headline check failed:\n${fails.join('\n')}` : `Headline check passed: ${shown.length} numbers from ${qs.length} bare views of the model.` }))
}

async function boot() {
  wireSearch()
  try {
    // ADAPT: one query per list the scope and search need. Keep the values as strings, and drop nulls:
    // a null in a list becomes a blank option that matches "nothing selected".
    const [[years, boroughs, kinds, zips]] = await Promise.all([
      Promise.all([
        query('run: complaints -> { aggregate: lo is min(created_year), hi is max(created_year) }'),
        query('run: complaints -> { group_by: borough; order_by: borough }'),
        query('run: complaints -> { group_by: descriptor; order_by: descriptor }'),
        query('run: complaints -> { group_by: incident_zip; where: incident_zip is not null; order_by: incident_zip }'),
      ]),
      d3Ready, // keep: nothing draws before d3 is in
    ])
    known.years = [years[0].lo, years[0].hi]
    const values = (rows, f) => rows.map((r) => r[f]).filter((v) => v !== null && v !== undefined)
    known.borough = values(boroughs, 'borough')
    known.descriptor = values(kinds, 'descriptor')
    known.entity = values(zips, 'incident_zip')
  } catch (err) {
    $('#main').replaceChildren(h('div', { class: 'wrap state state--error', text: err?.whole ? err.message : `Could not reach the model: ${reason(err)}` }))
    return
  }
  ;({ route, scope } = S.parseHash(location.hash, known))
  history.replaceState(null, '', S.buildHash(route, scope))
  $('#footer').replaceChildren(footer())
  $('#data-through').textContent = `Data through ${known.years[1]}` // ADAPT: the data's latest date, from a query
  render()
  if (new URLSearchParams(location.search).has('check')) selfCheck()
}

// ---- Views: ADAPT. Each returns one element; tiles inside it load as they near the screen. ----

/** A view's head: crumbs, a kicker naming the view (shown by the light headers' looks), its title and lede. */
function viewHead(title, lede, crumbs) {
  const kicker = $(`.app-nav a[data-view="${route.view}"]`)?.textContent || ''
  return [crumbs && h('p', { class: 'crumbs' }, ...crumbs), h('p', { class: 'kicker', text: kicker }), h('h1', { class: 'view-title', text: title }), lede && h('p', { class: 'lede' }, ...prose(lede))]
}

function overview() {
  // The headline runs bare views of the source that carries the question's scope, and every word of the answer comes
  // from their rows: so the page, an agent and an analyst get the same numbers.
  const qK = `run: ${S.HEADLINE} -> kpis`
  const answer = h('p', { class: 'answer' })
  query(qK).then(([k]) => answer.replaceChildren(...prose([`Complaints ${k.change < 0 ? 'fell' : 'rose'} by `, { b: C.pct(Math.abs(k.change), 0) }, ` from ${k.prior_year} to ${k.latest_year}.`])))
  const [kicker, title, lede] = viewHead('Did the drop in rat complaints reach every neighborhood?', 'The city counts every rodent complaint to 311. Here is where they fell, and whether it is holding.').filter(Boolean)
  return h('div', { class: 'view wrap' },
    kicker, title, answer, lede,
    h('div', { class: 'grid-2' },
      tile({
        title: 'Complaints per month', sub: 'All rodent complaints to 311.', cls: 'span-2',
        queries: () => [S.viewQuery('by_month', S.DEFAULT_SCOPE, known)],
        explain: ['Complaints per month, all of them, closed as duplicates left out.'],
        draw: ([rows], body) => C.lines(chartBox(body), rows.map((r) => r.m.slice(0, 7)), [{ name: 'Complaints', values: rows.map((r) => r.complaint_count), color: '--accent' }]),
        method: 'Duplicates closed as "already reported" are left out.',
      })))
}

function explore() {
  return h('div', { class: 'view wrap' },
    ...viewHead('Explore', 'Click a bar to filter everything; the address keeps the view.'),
    section('Where', 'Which boroughs and ZIP codes complain most.',
      tile({
        title: 'By borough', sub: 'Click a borough to filter.',
        queries: () => [S.tileQuery('boroughs', scope, known)],
        explain: ['Complaints per borough under the scope.'],
        draw: ([rows], body) => C.bars(chartBox(body), rows.map((r) => ({ label: r.borough, value: r.complaint_count })), { horizontal: true, selected: scope.borough, onPick: (b) => toggleScope('borough', b) }),
      }),
      tile({
        title: 'Top ZIP codes', sub: 'Open a ZIP code’s page.',
        queries: () => [S.tileQuery('zips', scope, known)],
        explain: ['The ZIP codes with the most complaints under the scope, and each one\'s change on the year before.'],
        draw: ([rows], body) => C.rankTable(body, rows.map((r) => ({ id: r.incident_zip, name: S.ENTITY.label(r.incident_zip), value: r.complaint_count, change: r.change_vs_prior })), {
          valueLabel: 'Complaints', changeLabel: 'Against last year', good: 'fall', onOpen: openEntity,
          inCompare, onCompare: toggleCompare,
        }),
        finding: ([rows]) => [{ b: S.ENTITY.label(rows[0].incident_zip) }, ` has the most complaints in scope, ${C.fmt(rows[0].complaint_count)}.`],
        method: 'Complaints without a ZIP code are left out of this list and kept everywhere else.',
      })))
}

function entity() {
  const id = route.id
  return h('div', { class: 'view wrap' },
    ...viewHead(S.ENTITY.label(id), null, [h('a', { href: linkTo('explore'), text: 'Explore' }), ` / ${S.ENTITY.label(id)}`]),
    h('p', {}, h('button', { class: 'btn', type: 'button', text: inCompare(id) ? 'Open compare' : 'Add to compare', onclick: () => goCompare(inCompare(id) ? picked : [...picked, id]) })),
    h('div', { class: 'grid-2' },
      tile({
        title: 'Complaints per month', cls: 'span-2',
        queries: () => [S.entityQuery('by_month', id, scope, known)],
        explain: ['This ZIP code\'s complaints per month under the scope.'],
        draw: ([rows], body) => C.lines(chartBox(body), rows.map((r) => r.m.slice(0, 7)), [{ name: S.ENTITY.label(id), values: rows.map((r) => r.complaint_count), color: '--accent' }]),
      })))
}

function records() {
  return h('div', { class: 'view wrap' }, ...viewHead('Records', 'Every complaint in scope, newest first.'))
}
function compare() {
  const ids = route.compare
  const chips = h('div', { class: 'chips' },
    ...ids.map((id) => h('span', { class: 'chip' }, S.ENTITY.label(id),
      h('button', { type: 'button', 'aria-label': `Remove ${S.ENTITY.label(id)}`, text: '×', onclick: () => {
        focusNext = '.chip button'
        goCompare(ids.filter((x) => x !== id), { toTop: false })
      } }))))
  return h('div', { class: 'view wrap' },
    ...viewHead('Compare', `Up to ${S.COMPARE_MAX} ZIP codes side by side. Add one with the + on a ranked list, or from its page.`),
    // data-redraw: the frame swaps this in place when the picks change, while the cards update where they are.
    h('div', { 'data-redraw': 'picks' }, ids.length ? chips : h('p', { class: 'lede', text: 'Nothing picked yet.' })),
    // ADAPT: a tile per comparison, each query from S.compareQuery(view, ids, scope, known), one series per id.
  )
}
function about() {
  return h('div', { class: 'view wrap' }, ...viewHead('About the data', 'Sources, licenses, what was reshaped, and caveats.'))
}
function footer() {
  return h('div', {}, h('h2', { text: 'Rats in New York' }), h('p', { text: 'Source: NYC Open Data, 311 Service Requests. An independent app, not produced by the City of New York.' }))
}

boot()
