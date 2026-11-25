// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT
//
// The line of controls, read as a sentence: "Last 13 weeks compared with the 13 weeks before · by week". Filters are
// added from one Filter menu (a field, then its values) and shown as tokens. Copy link copies this exact view.

import { h } from './core.js'
import * as S from './state.js'
import * as P from './period.js'
import * as store from './store.js'

let open = null
function close(focusBack = false) {
  if (!open) return
  open.menu.remove()
  open.btn.setAttribute('aria-expanded', 'false')
  if (focusBack) open.btn.focus()
  open = null
}
addEventListener('pointerdown', (e) => open && !open.menu.contains(e.target) && !open.btn.contains(e.target) && close())
addEventListener('keydown', (e) => e.key === 'Escape' && open && close(true))
addEventListener('scroll', () => open && place(open.btn, open.menu), { passive: true })

function place(btn, menu) {
  const r = btn.getBoundingClientRect()
  const w = menu.offsetWidth
  menu.style.left = `${Math.max(12, Math.min(r.left, innerWidth - w - 12)) + scrollX}px`
  menu.style.top = `${r.bottom + 8 + scrollY}px`
}
/** Open a menu under `btn`, built by `fill(menu)`; the same button closes it. */
function menuAt(btn, fill) {
  const same = open?.btn === btn
  close()
  if (same) return
  const menu = h('div', { class: 'menu', role: 'menu' })
  document.body.append(menu)
  fill(menu)
  place(btn, menu)
  btn.setAttribute('aria-expanded', 'true')
  open = { btn, menu }
  ;(menu.querySelector('input') || menu.querySelector('[aria-checked="true"]') || menu.querySelector('button'))?.focus()
  // Arrow keys move between the rows.
  menu.addEventListener('keydown', (e) => {
    if (e.key !== 'ArrowDown' && e.key !== 'ArrowUp') return
    e.preventDefault()
    const rows = [...menu.querySelectorAll('.menu__row:not(:disabled)')]
    const i = rows.indexOf(document.activeElement)
    rows[(i + (e.key === 'ArrowDown' ? 1 : -1) + rows.length) % rows.length]?.focus()
  })
}
const row = (label, hint, checked, onclick, { box = false, disabled = false } = {}) =>
  h('button', { type: 'button', class: 'menu__row', role: box ? 'menuitemcheckbox' : 'menuitemradio', 'aria-checked': String(!!checked), disabled: disabled || null, onclick },
    h('span', { class: box ? 'menu__box' : 'menu__check', 'aria-hidden': 'true' }), h('span', { class: 'menu__label', text: label }), h('span', { class: 'menu__hint', text: hint || '' }))

function periodMenu(menu, s) {
  const span = S.known.span
  P.PRESET_GROUPS.forEach((g, i) => {
    if (i) menu.append(h('div', { class: 'menu__sep' }))
    for (const key of g) {
      const r = P.resolve({ preset: key }, span)
      const hint = P.PRESETS[key].full ? P.describe(r).replace(/^1 (\w+) – \d+ \w+ (\d+)$/, '$1 $2') : P.describe(r)
      menu.append(row(P.PRESETS[key].label, hint, s.period.preset === key, () => (close(true), store.setPeriod({ preset: key }))))
    }
  })
  const cur = S.range(s)
  const from = h('input', { type: 'date', min: span.first, max: span.last, value: cur.from, 'aria-label': 'From' })
  const to = h('input', { type: 'date', min: span.first, max: span.last, value: P.addDays(cur.to, -1), 'aria-label': 'To' })
  menu.append(h('div', { class: 'menu__sep' }), h('div', { class: 'menu__custom' }, h('span', { class: 'menu__label', text: 'From' }), from, h('span', { class: 'menu__label', text: 'to' }), to,
    h('button', { type: 'button', class: 'ghost ghost--small ghost--accent', text: 'Apply', onclick: () => {
      if (!from.value || !to.value) return
      close(true)
      store.setPeriod({ preset: 'custom', from: from.value < to.value ? from.value : to.value, to: from.value < to.value ? to.value : from.value })
    } })),
    h('p', { class: 'menu__foot', text: `Periods end on the latest day in the data, ${P.longDate(span.last)}.` }))
}

function compareMenu(menu, s) {
  for (const mode of ['previous', 'year', 'none']) {
    const r = mode === 'none' ? null : P.compareRange(s.period, S.range(s), mode, S.known.span)
    menu.append(row(P.compareWords(s.period, S.range(s), mode), mode === 'none' ? '' : r ? P.describe(r) : 'before the data', s.compare === mode, () => (close(true), store.setCompare(mode)), { disabled: mode !== 'none' && !r }))
  }
}

function grainMenu(menu, s) {
  const n = P.days(S.range(s))
  const auto = P.autoGrain(S.range(s))
  for (const g of P.GRAINS) {
    const count = Math.ceil(n / { day: 1, week: 7, month: 30.4, quarter: 91, year: 365 }[g])
    menu.append(row(g === 'day' ? 'by day' : `by ${g}`, `${count} point${count === 1 ? '' : 's'}${g === auto ? ', automatic' : ''}`, S.grain(s) === g, () => (close(true), store.setGrain(g === auto ? null : g)), { disabled: count > 400 || count < 2 }))
  }
}

/** The Filter menu: first the fields, each with how many values it has; then that field's values to tick. */
function filterMenu(menu, s) {
  const fields = S.config.filters.filter((f) => !f.hidden)
  const q = h('input', { class: 'menu__search', type: 'search', placeholder: 'Filter by…', 'aria-label': 'Find a field' })
  const list = h('div', { class: 'menu__list' })
  menu.replaceChildren(q, list)
  const paint = () => list.replaceChildren(...fields.filter((f) => f.label.toLowerCase().includes(q.value.toLowerCase())).map((f) => {
    const n = (S.known[f.key] || []).length
    const on = (s.filters[f.key] || []).length
    const b = row(f.label, on ? `${on} picked` : `${n >= 500 ? '500+' : n.toLocaleString()} value${n === 1 ? '' : 's'}`, false, () => valuesMenu(menu, s, f))
    b.querySelector('.menu__check').classList.add('menu__chev')
    return b
  }))
  q.oninput = paint
  paint()
  q.focus()
}
function valuesMenu(menu, s, f) {
  const values = S.known[f.key] || []
  const counts = S.known[`${f.key}__n`] || {}
  const picked = new Set((store.state.filters[f.key] || []).map(String))
  const name = (v) => (f.format ? f.format(v) : String(v))
  const back = h('button', { type: 'button', class: 'menu__back', text: `‹ ${f.label}`, onclick: () => filterMenu(menu, store.state) })
  const q = h('input', { class: 'menu__search', type: 'search', placeholder: `Search ${values.length.toLocaleString()} ${f.plural || 'values'}`, 'aria-label': `Search ${f.label}` })
  const list = h('div', { class: 'menu__list' })
  const unit = S.config.recordNoun || 'records'
  const paint = () => list.replaceChildren(...values.filter((v) => name(v).toLowerCase().includes(q.value.toLowerCase())).slice(0, 200).map((v) =>
    row(name(v), counts[String(v)] !== undefined ? `${Number(counts[String(v)]).toLocaleString()} ${unit}` : '', picked.has(String(v)), (e) => {
      const b = e.currentTarget
      if (picked.has(String(v))) picked.delete(String(v))
      else picked.add(String(v))
      b.setAttribute('aria-checked', String(picked.has(String(v))))
      store.setFilter(f.key, values.filter((x) => picked.has(String(x))))
    }, { box: true })))
  menu.replaceChildren(back, q, list, h('div', { class: 'menu__actions' }, h('button', { type: 'button', class: 'ghost ghost--small', text: 'Clear', onclick: () => (close(true), store.setFilter(f.key, [])) }), h('button', { type: 'button', class: 'ghost ghost--small ghost--accent', text: 'Done', onclick: () => close(true) })))
  q.oninput = paint
  paint()
  q.focus()
}

const chev = () => h('span', { class: 'pick__chev', 'aria-hidden': 'true' })

/** Draw the controls for the state. */
export function renderControls(el, s) {
  close()
  const page = S.pageOf(s)
  const out = []
  if (S.dated() && page.period !== false) {
    const cmp = S.compareRange(s)
    const pick = (text, title, fill) => {
      const b = h('button', { type: 'button', class: 'pick', 'aria-haspopup': 'menu', 'aria-expanded': 'false', title }, text, chev())
      b.onclick = () => menuAt(b, (m) => fill(m, store.state))
      return b
    }
    out.push(h('div', { class: 'ctl__sentence' },
      pick(s.period.preset === 'custom' ? P.describe(S.range(s)) : P.PRESETS[s.period.preset].label, 'Period', periodMenu),
      h('span', { text: 'compared with' }),
      pick(cmp || s.compare === 'none' ? P.compareWords(s.period, S.range(s), s.compare) : 'nothing (too early)', 'Compared with', compareMenu),
      h('span', { text: '·' }),
      pick(`by ${S.grain(s)}`, 'Chart grain', grainMenu),
      h('span', { class: 'ctl__dates', text: P.describe(S.range(s)) })))
  }
  const e = S.entityOf(s)
  const tokens = page.filters === false ? [] : S.config.filters.flatMap((f) => (e && f.field === e.field ? [] : (s.filters[f.key] || []).map((v) => h('span', { class: 'tok' }, h('span', { class: 'tok__k', text: f.label }), h('b', { text: f.format ? f.format(v) : String(v) }),
    h('button', { type: 'button', class: 'tok__x', 'aria-label': `Remove ${f.label} ${v}`, text: '×', onclick: () => store.setFilter(f.key, s.filters[f.key].filter((x) => String(x) !== String(v))) })))))
  const filterBtn = h('button', { type: 'button', class: 'ghost', 'aria-haspopup': 'menu', 'aria-expanded': 'false' }, icon('filter'), 'Filter')
  filterBtn.onclick = () => menuAt(filterBtn, (m) => filterMenu(m, store.state))
  const copy = h('button', { type: 'button', class: 'ghost' }, icon('link'), h('span', { text: 'Copy link' }))
  copy.onclick = () => Promise.resolve(navigator.clipboard?.writeText(location.href)).then(() => flash('Copied'), () => flash('Copy the address bar'))
  const flash = (t) => {
    copy.lastChild.textContent = t
    setTimeout(() => (copy.lastChild.textContent = 'Copy link'), 1800)
  }
  out.push(h('div', { class: 'ctl__end' }, ...tokens, tokens.length > 1 && h('button', { type: 'button', class: 'ghost ghost--small', text: 'Clear all', onclick: store.clearFilters }), page.filters !== false && S.config.filters.some((f) => !f.hidden) && filterBtn, copy))
  el.replaceChildren(...out)
}

function icon(kind) {
  const ns = 'http://www.w3.org/2000/svg'
  const svg = document.createElementNS(ns, 'svg')
  svg.setAttribute('viewBox', '0 0 16 16')
  svg.setAttribute('aria-hidden', 'true')
  const p = document.createElementNS(ns, 'path')
  p.setAttribute('d', kind === 'filter' ? 'M2 4h12M4.5 8h7M7 12h2' : 'M6.5 9.5l3-3M7 4.5l1.2-1.2a2.6 2.6 0 013.7 3.7L10.7 8.2M9 11.5l-1.2 1.2a2.6 2.6 0 01-3.7-3.7L5.3 7.8')
  svg.append(p)
  return svg
}
