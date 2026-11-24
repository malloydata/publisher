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
 *   defaults: { period, compare }, theme,      // theme: paper, ledger, clinic, transit, terminal, harvest, civic, bloom, stone
 *   pages: [{ id, title, render: (state) => [...blocks], entity, lists, period }],
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
  const mast = h('header', { class: 'mast' }, h('div', { class: 'wrap mast__row' },
    h('a', { class: 'mast__name', href: '#/' }, h('span', { class: 'mast__title', text: cfg.title }), cfg.sub && h('span', { class: 'mast__sub', text: cfg.sub })),
    navPages.length > 1 && h('nav', { class: 'mast__nav', 'aria-label': 'Pages' }, ...navPages.map((p) => h('a', { href: `#/${p.id === cfg.pages[0].id ? '' : p.id}`, 'data-page': p.id, text: p.title }))),
    fresh))
  const ctl = h('div', { class: `wrap ctl${cfg.date ? '' : ' ctl--nodate'}`, id: 'controls' })
  const main = h('main', { id: 'main', class: 'wrap', tabindex: '-1' }, h('div', { class: 'state state--loading', style: { '--reserve': '560px' } }))
  const foot = h('footer', { class: 'wrap colophon', hidden: true }, cfg.footer && h('span', { text: cfg.footer }), h('span', { text: 'Built on Credible' }))
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
