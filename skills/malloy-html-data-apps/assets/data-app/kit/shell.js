// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT
//
// dashboard(config): the page around the blocks. It draws the masthead and the controls, learns the data's span and
// each filter's values, reads the address, and draws the page. A change of state on the same page draws its blocks
// again where they are; another page replaces them.

import { h, $, query, reason, d3Ready } from './core.js'
import * as S from './state.js'
import * as P from './period.js'
import * as store from './store.js'
import { renderControls } from './controls.js'
import { nextEpoch, observe, markReady } from './blocks.js'
import { MOTION } from './draw.js'

/**
 * dashboard({
 *   title, sub,                         // the masthead: the dashboard's name and what it covers
 *   model, source, extend,              // where queries go; extend adds measures the app needs
 *   date: { field, label } | null,      // the date every period filters on; null for data with no time
 *   metrics: { name: { label, format, good, thresholds, doc } },
 *   filters: [{ key, field, label, plural, format }],
 *   entities: { key: { field, label, plural, name } },   // things with a page of their own
 *   recordNoun,                          // what one row is, plural: "orders", "flights"
 *   defaults: { period, compare, filters: { key: [values] } }, theme,      // theme: paper, ledger, clinic, transit, terminal, harvest, civic, bloom, stone
 *   pages: [{ id, title, render: (state) => [...blocks], entity, lists, period, filters }],   // period/filters: false hides those controls
 *   sources: { key: { name, by, what, license, url } },   // where the data comes from, shown on every block's back
 *   glossary: [{ term, match: [...], plain }],          // words a newcomer may not know
 *   articles: [{ title, outlet, date, url }],         // the stories an app made from the news responds to
 *   search: 'area',                      // an entity a reader can find by name from the masthead
 *   credit: true,                        // "Made by Credible" in the masthead, for an app Credible publishes
 *   footer,
 * })
 */
export async function dashboard(cfg) {
  S.configure(cfg)
  // The theme, unless the address previews another (?theme=), and its display face, loaded before anything draws.
  const root = document.documentElement
  const preview = new URLSearchParams(location.search).get('theme')
  if (preview || cfg.theme) root.dataset.theme = preview || cfg.theme
  const face = getComputedStyle(root).getPropertyValue('--display-face').trim().replace(/"/g, '')
  const faceReady = !face ? Promise.resolve() : new Promise((done) => {
    const link = document.createElement('link')
    link.rel = 'stylesheet'
    link.href = new URL(`./fonts/${face}.css`, import.meta.url).href
    link.onload = () => (document.fonts?.load ? document.fonts.load(`600 40px "${getComputedStyle(root).getPropertyValue('--display').split(',')[0].replace(/"/g, '').trim()}"`).then(done, done) : done())
    link.onerror = done
    document.head.append(link)
    setTimeout(done, 1500)
  })
  const app = $('#app')
  const navPages = cfg.pages.filter((p) => !p.entity && !p.hidden)
  const fresh = h('span', { class: 'mast__fresh', 'data-pending': '' })
  const find = cfg.search && searchBox(cfg.search)
  const mast = h('header', { class: 'mast' }, h('div', { class: 'wrap mast__row' },
    h('a', { class: 'mast__name', href: '#/' }, cfg.credit && credit(), h('span', { class: 'mast__title', text: cfg.title }), cfg.sub && h('span', { class: 'mast__sub', text: cfg.sub })),
    navPages.length > 1 && h('nav', { class: 'mast__nav', 'aria-label': 'Pages' }, ...navPages.map((p) => h('a', { href: `#/${p.id === cfg.pages[0].id ? '' : p.id}`, 'data-page': p.id, text: p.title }))),
    find, fresh))
  const ctl = h('div', { class: `wrap ctl${cfg.date ? '' : ' ctl--nodate'}`, id: 'controls' })
  const main = h('main', { id: 'main', class: 'wrap', tabindex: '-1' }, h('div', { class: 'state state--loading', style: { '--reserve': '560px' } }))
  const srcs = Object.values(cfg.sources || {})
  const foot = h('footer', { class: 'wrap colophon', hidden: true }, cfg.footer && h('span', { text: cfg.footer }),
    srcs.length > 0 && h('span', {}, 'Data: ', ...srcs.flatMap((x, i) => [i ? '; ' : '', x.url ? h('a', { href: x.url, target: '_blank', rel: 'noopener', text: x.by || x.name }) : x.by || x.name, x.license ? ` (${x.license})` : ''])),
    h('span', { text: 'Built on Credible' }))
  app.replaceChildren(mast, ctl, main, foot)
  try {
    const lists = cfg.filters.map((f) => (f.values ? Promise.resolve(f.values.map((v) => ({ v, n: null })))
      : query(`run: ${S.src()} -> { where: ${f.field} is not null; group_by: v is ${f.field}${f.nameField ? `, nm is ${f.nameField}` : ''}; aggregate: n is count(); order_by: ${f.sort === 'name' ? 'v' : 'n desc'}; limit: ${f.limit || (f.hidden ? 2000 : 500)} }`)))
    const span = cfg.date ? query(`run: ${S.src()} -> { aggregate: first_day is min(${cfg.date.field}), last_day is max(${cfg.date.field}) }`) : Promise.resolve(null)
    const [sp, ...rest] = await Promise.all([span, ...lists, d3Ready, faceReady])
    const known = { span: sp && sp[0]?.last_day ? { first: String(sp[0].first_day).slice(0, 10), last: String(sp[0].last_day).slice(0, 10) } : null }
    cfg.filters.forEach((f, i) => {
      known[f.key] = rest[i].map((r) => r.v)
      // An entity named by a field (a hospital by its id): what to call each one, wherever it is listed.
      if (f.nameField) {
        const names = new Map(rest[i].map((r) => [String(r.v), r.nm]))
        const e = Object.values(cfg.entities || {}).find((x) => x.field === f.field)
        const own = e?.name
        f.format = (id) => names.get(String(id)) ?? (own ? own(id) : String(id))
        if (e) e.name = f.format
      }
      known[`${f.key}__n`] = Object.fromEntries(rest[i].filter((r) => r.n !== null).map((r) => [String(r.v), r.n]))
    })
    S.setKnown(known)
  } catch (err) {
    main.replaceChildren(h('div', { class: 'state state--error', text: err?.whole ? err.message : `Could not reach the model: ${reason(err)}` }))
    return
  }
  fresh.textContent = S.dated() ? `Data through ${P.describe({ from: S.known.span.last, to: P.addDays(S.known.span.last, 1) })}` : ''
  fresh.removeAttribute('data-pending')
  store.init(S.fromHash(location.hash), render)
  render({ toTop: false })
  foot.hidden = false
  if (new URLSearchParams(location.search).has('check')) selfCheck()
}

/** "Made by" and Credible's logo, in the masthead's ink with Credible's blue dot. */
function credit() {
  const svg = new DOMParser().parseFromString(LOGO, 'image/svg+xml').documentElement
  return h('span', { class: 'mast__credit' }, h('span', { text: 'Made by' }), document.importNode(svg, true))
}
const LOGO = '<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 100 26.1" role="img" aria-label="Credible"><path fill="currentColor" d="M 88.6 20.4 C 84.8 20.4 82.4 18 82.4 14.2 C 82.4 10.4 84.8 7.9 88.4 7.9 C 91.9 7.9 94.4 10.2 94.4 13.8 L 94.4 14.7 L 85.1 14.7 L 85.1 14.9 C 85.1 16.9 86.5 18.3 88.5 18.3 C 90.1 18.3 91.3 17.5 91.6 16.2 L 94.2 16.2 C 93.8 18.6 91.7 20.4 88.6 20.4 Z M 85.1 12.8 L 91.7 12.8 C 91.5 11 90.3 10 88.4 10 C 86.8 10 85.4 11 85.1 12.8 Z M 77.4 20.3 L 77.4 2 L 80 2 L 80 20.3 Z M 69.1 20.4 C 67.2 20.4 65.9 19.6 65.1 18.5 L 64.8 20.3 L 62.5 20.3 L 62.5 1 L 65.2 1 L 65.2 9.8 C 65.9 8.8 67.2 7.9 69.1 7.9 C 72.5 7.9 74.9 10.2 74.9 14.2 C 74.9 17.9 72.5 20.4 69.1 20.4 Z M 68.7 18.2 C 70.8 18.2 72.3 16.6 72.3 14.2 C 72.3 11.7 70.8 10.1 68.7 10.1 C 66.5 10.1 65.1 11.7 65.1 14.1 C 65.1 16.6 66.5 18.2 68.7 18.2 Z M 57 20.3 L 57 8 L 59.6 8 L 59.6 20.3 Z"/><path fill="#5c7a93" d="M 56 1.9 C 56 2.9 56.8 3.7 57.9 3.7 C 58.9 3.7 59.7 2.9 59.7 1.9 C 59.7 0.8 58.9 0 57.9 0 C 56.8 0 56 0.8 56 1.9 Z"/><path fill="currentColor" d="M 47.1 20.1 C 43.8 20.1 41.3 17.7 41.3 13.8 C 41.3 10 43.8 7.5 47.1 7.5 C 49 7.5 50.4 8.2 51.1 9.3 L 51.1 1 L 53.7 1 L 53.7 19.9 L 51.4 19.9 L 51.1 18 C 50.3 19.1 49.1 20.1 47.1 20.1 Z M 47.5 17.8 C 49.7 17.8 51.1 16.2 51.1 13.8 C 51.1 11.3 49.7 9.7 47.5 9.7 C 45.4 9.7 44 11.3 44 13.8 C 44 16.2 45.4 17.8 47.5 17.8 Z M 33.7 20.1 C 29.8 20.1 27.4 17.6 27.4 13.8 C 27.4 10 29.8 7.5 33.5 7.5 C 37 7.5 39.4 9.8 39.4 13.5 L 39.4 14.3 L 30.1 14.3 L 30.1 14.5 C 30.1 16.6 31.5 17.9 33.6 17.9 C 35.2 17.9 36.3 17.1 36.7 15.8 L 39.3 15.8 C 38.9 18.2 36.8 20.1 33.7 20.1 Z M 30.2 12.4 L 36.8 12.4 C 36.6 10.6 35.3 9.6 33.5 9.6 C 31.9 9.6 30.4 10.6 30.2 12.4 Z M 19.6 19.9 L 19.6 7.6 L 21.9 7.6 L 22.1 9.5 C 22.8 8.5 23.7 7.6 25.8 7.6 L 26.4 7.6 L 26.4 10 L 25.3 10 C 22.9 10 22.2 11.8 22.2 13.7 L 22.2 19.9 Z M 11.6 20.1 C 7.8 20.1 5.5 17.6 5.5 13.8 C 5.5 10 7.9 7.5 11.7 7.5 C 14.8 7.5 16.8 9.2 17.3 12 L 14.6 12 C 14.3 10.6 13.2 9.7 11.6 9.7 C 9.5 9.7 8.2 11.4 8.2 13.8 C 8.2 16.2 9.5 17.8 11.6 17.8 C 13.2 17.8 14.3 16.9 14.6 15.5 L 17.3 15.5 C 16.8 18.3 14.7 20.1 11.6 20.1 Z"/><path fill="currentColor" d="M 53.8 2.5 C 53.8 2.9 53.5 3.1 53.2 3.1 L 3.2 3.1 C 2.9 3.1 2.6 3.4 2.6 3.7 L 2.6 22.9 C 2.6 23.2 2.9 23.5 3.2 23.5 L 96.6 23.5 C 96.9 23.5 97.1 23.2 97.1 22.9 L 97.1 3.7 C 97.1 3.4 96.9 3.1 96.6 3.1 L 62.9 3.1 C 62.6 3.1 62.3 2.9 62.3 2.5 L 62.3 1.1 C 62.3 0.8 62.6 0.5 62.9 0.5 L 99.2 0.5 C 99.5 0.5 99.8 0.8 99.8 1.1 L 99.8 25.5 C 99.8 25.8 99.5 26.1 99.2 26.1 L 0.5 26.1 C 0.2 26.1 0 25.9 0 25.6 L 0 1.1 C 0 0.8 0.3 0.5 0.6 0.5 L 53.2 0.5 C 53.5 0.5 53.8 0.8 53.8 1.1 Z"/></svg>'

/** Find an entity by part of its name; Enter or a click opens its page. The names are the ones loaded with it. */
function searchBox(key) {
  const e = S.config.entities[key]
  const page = e && S.config.pages.find((p) => p.entity === key)
  if (!page) return null
  const input = h('input', { type: 'search', class: 'mast__find', placeholder: `Search ${e.plural || 'by name'}`, 'aria-label': `Find ${e.label.toLowerCase()}`, autocomplete: 'off', role: 'combobox', 'aria-expanded': 'false', 'aria-controls': 'mast-found' })
  const list = h('ul', { class: 'mast__found', id: 'mast-found', role: 'listbox', hidden: true })
  let hits = []
  let at = 0
  const f = () => S.config.filters.find((x) => x.field === e.field)
  const name = (v) => (f()?.format ? f().format(v) : String(v))
  const pick = (v) => {
    input.value = ''
    show([])
    store.open(page.id, v)
  }
  const show = (xs) => {
    hits = xs
    at = 0
    list.hidden = !xs.length
    input.setAttribute('aria-expanded', String(xs.length > 0))
    list.replaceChildren(...xs.map((v, i) => h('li', { role: 'option', 'aria-selected': String(i === 0), text: name(v), onpointerdown: (ev) => (ev.preventDefault(), pick(v)) })))
  }
  input.oninput = () => {
    const q = input.value.trim().toLowerCase()
    const vals = S.known[f()?.key] || []
    show(q ? vals.filter((v) => name(v).toLowerCase().includes(q)).sort((a, b) => name(a).toLowerCase().indexOf(q) - name(b).toLowerCase().indexOf(q)).slice(0, 8) : [])
  }
  input.onkeydown = (ev) => {
    if (ev.key === 'Enter' && hits.length) pick(hits[at])
    else if (ev.key === 'Escape') show([])
    else if ((ev.key === 'ArrowDown' || ev.key === 'ArrowUp') && hits.length) {
      ev.preventDefault()
      at = (at + (ev.key === 'ArrowDown' ? 1 : -1) + hits.length) % hits.length
      list.querySelectorAll('li').forEach((li, i) => li.setAttribute('aria-selected', String(i === at)))
    }
  }
  input.onblur = () => setTimeout(() => show([]), 100)
  return h('div', { class: 'mast__search' }, input, list)
}

let shown = null
function render({ toTop = false } = {}) {
  const s = store.state
  const here = `${s.page}|${s.id ?? ''}`
  const same = shown === here
  const first = shown === null
  shown = here
  if (!same && !first && MOTION && document.startViewTransition) document.startViewTransition(() => paint(s, same, toTop))
  else paint(s, same, toTop)
}

function paint(s, same, toTop) {
  nextEpoch()
  delete document.body.dataset.ready
  const page = S.pageOf(s)
  const e = S.entityOf(s)
  for (const a of document.querySelectorAll('.mast__nav a')) {
    a.href = store.linkTo(a.dataset.page)
    const on = a.dataset.page === s.page || (e && S.config.pages.find((p) => p.id === a.dataset.page)?.lists === e.key)
    if (on) a.setAttribute('aria-current', 'page')
    else a.removeAttribute('aria-current')
  }
  renderControls($('#controls'), s)
  const main = $('#main')
  const next = h('div', { class: 'page' }, page.render(s))
  const here = [...main.querySelectorAll('.block')]
  const fresh = [...next.querySelectorAll('.block')]
  const byKey = new Map(here.map((c) => [c.dataset.key, c]))
  if (same && fresh.length === here.length && fresh.every((c) => byKey.has(c.dataset.key))) {
    for (const c of fresh) {
      const old = byKey.get(c.dataset.key)
      if (old.__loaded) old.__update(c.__spec)
      else old.__respec(c.__spec)
    }
    for (const el of next.querySelectorAll('[data-redraw]')) main.querySelector(`[data-redraw="${CSS.escape(el.dataset.redraw)}"]`)?.replaceWith(el)
  } else {
    const y = scrollY
    main.replaceChildren(next)
    if (!toTop) scrollTo(0, y)
  }
  observe(main)
  if (toTop) {
    scrollTo(0, 0)
    main.focus({ preventScroll: true })
  }
  const name = e ? (e.name ? e.name(s.id) : String(s.id)) : page.title
  document.title = page === S.config.pages[0] ? S.config.title : `${name} · ${S.config.title}`
  markReady()
}

/** ?check=1: once the page is ready, a banner says whether any block failed or shows a placeholder. */
async function selfCheck() {
  await new Promise((done) => {
    const until = Date.now() + 60000
    const look = () => (document.body.dataset.ready === 'true' || Date.now() > until ? done() : setTimeout(look, 200))
    look()
  })
  const fails = []
  if (document.body.dataset.ready !== 'true') fails.push('The page did not finish loading within a minute')
  for (const c of document.querySelectorAll('#main .block')) if (c.querySelector('.state--error')) fails.push(`“${c.querySelector('.kicker')?.textContent}” failed: ${c.querySelector('.state--error').textContent}`)
  const text = $('#main').innerText
  for (const bad of [/\bundefined\b/, /\bNaN\b/, /\[object /, /\bnull\b/, /Infinity/]) if (bad.test(text)) fails.push(`The page says ${bad}`)
  document.body.dataset.check = fails.length ? 'fail' : 'ok'
  document.body.prepend(h('pre', { class: 'self-check', text: fails.length ? `Check failed:\n${fails.join('\n')}` : `Check passed: ${document.querySelectorAll('#main .block').length} blocks, no errors or placeholders.` }))
}
