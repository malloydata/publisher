// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT
//
// dataApp(config): the page around the blocks. It draws the masthead and the controls, learns the data's span and
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
 * dataApp({
 *   title, sub,                         // the masthead: the app's name and what it covers
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
 *   credit: { name, logo },              // "Made by <name>" in the masthead; logo is an image the app ships
 *   footer,
 * })
 */
/** The kit's version, as kit.js describes it. */
export const KIT_VERSION = '1.0.0'

export async function dataApp(cfg) {
  S.configure(cfg)
  if (!document.querySelector('meta[name="data-app-kit"]')) document.head.append(Object.assign(document.createElement('meta'), { name: 'data-app-kit', content: KIT_VERSION }))
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
    h('a', { class: 'mast__name', href: '#/' }, cfg.credit && credit(cfg.credit), h('span', { class: 'mast__title', text: cfg.title }), cfg.sub && h('span', { class: 'mast__sub', text: cfg.sub })),
    navPages.length > 1 && h('nav', { class: 'mast__nav', 'aria-label': 'Pages' }, ...navPages.map((p) => h('a', { href: `#/${p.id === cfg.pages[0].id ? '' : p.id}`, 'data-page': p.id, text: p.title }))),
    find, fresh))
  const ctl = h('div', { class: `wrap ctl${cfg.date ? '' : ' ctl--nodate'}`, id: 'controls' })
  const main = h('main', { id: 'main', class: 'wrap', tabindex: '-1' }, h('div', { class: 'state state--loading', style: { '--reserve': '560px' } }))
  const srcs = Object.values(cfg.sources || {})
  const foot = h('footer', { class: 'wrap colophon', hidden: true }, cfg.footer && h('span', { text: cfg.footer }),
    srcs.length > 0 && h('span', {}, 'Data: ', ...srcs.flatMap((x, i) => [i ? '; ' : '', /^https?:\/\//i.test(x.url || '') ? h('a', { href: x.url, target: '_blank', rel: 'noopener', text: x.by || x.name }) : x.by || x.name, x.license ? ` (${x.license})` : '']))))
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
    // A default filter value the data does not have would filter nothing while its token says otherwise: drop it,
    // and let ?check=1 say so.
    for (const [k, vs] of Object.entries(cfg.defaults?.filters || {})) {
      const f = cfg.filters.find((x) => x.key === k)
      if (!f) {
        missingDefaults.push(`defaults.filters names “${k}”, which is not one of the config's filters`)
        continue
      }
      // A list cut at its limit may not hold a default value: ask the model for the ones it lacks, and add them.
      const lacking = vs.filter((v) => !(known[k] || []).map(String).includes(String(v)))
      if (lacking.length && known[k]?.length >= (f.limit || (f.hidden ? 2000 : 500))) {
        // A value of the wrong type fails the query; it is then simply not found.
        const found = await query(`run: ${S.src()} -> { where: ${f.field} in (${lacking.map(S.lit).join(', ')}); group_by: v is ${f.field} }`).catch(() => [])
        known[k].push(...found.map((r) => r.v))
      }
      const have = new Set((known[k] || []).map(String))
      const kept = vs.filter((v) => have.has(String(v)))
      if (kept.length < vs.length) {
        missingDefaults.push(...vs.filter((v) => !kept.includes(v)).map((v) => `A default filter names ${f.label} “${v}”, which the data does not have`))
        console.error(`defaults.filters.${k}: the data has no ${vs.filter((v) => !kept.includes(v)).join(', ')}`)
        cfg.defaults.filters[k] = kept
      }
    }
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

/** "Made by" and who made the app: their logo when the app ships one (`logo`, an image path), else their name. */
function credit({ name, logo } = {}) {
  if (!name) return null
  return h('span', { class: 'mast__credit' }, h('span', { text: 'Made by' }),
    logo ? h('img', { src: logo, alt: name, class: 'mast__credit-logo' }) : h('span', { class: 'mast__credit-name', text: name }))
}

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
  const more = new Map()   // names of values found by asking the model
  const name = (v) => more.get(String(v)) ?? (f()?.format ? f().format(v) : String(v))
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
    list.replaceChildren(...xs.map((v, i) => h('li', { id: `mast-found-${i}`, role: 'option', 'aria-selected': String(i === 0), text: name(v), onpointerdown: (ev) => (ev.preventDefault(), pick(v)) })))
    if (xs.length) input.setAttribute('aria-activedescendant', 'mast-found-0')
    else input.removeAttribute('aria-activedescendant')
  }
  // The names loaded with the filter come first; when that list was cut at its limit, the model is asked too.
  let asked = 0
  input.oninput = () => {
    const q = input.value.trim().toLowerCase()
    const vals = S.known[f()?.key] || []
    const local = q ? vals.filter((v) => name(v).toLowerCase().includes(q)).sort((a, b) => name(a).toLowerCase().indexOf(q) - name(b).toLowerCase().indexOf(q)).slice(0, 8) : []
    show(local)
    const cut = vals.length >= (f()?.limit || (f()?.hidden ? 2000 : 500))
    const mine = ++asked   // any answer still on its way is now stale
    clearTimeout(input.__t)
    if (!q.replace(/[%_\\]/g, '') || !cut || local.length >= 8) return
    const by = e.nameField || e.field
    input.__t = setTimeout(() => query(`run: ${S.src()} -> { where: lower(${by}::string) ~ ${S.lit(`%${q.replace(/[%_\\]/g, '')}%`)}; group_by: v is ${e.field}${e.nameField ? `, nm is ${e.nameField}` : ''}; aggregate: n is count(); order_by: n desc; limit: 8 }`).then((rows) => {
      if (mine !== asked) return
      for (const r of rows) if (r.nm !== undefined) more.set(String(r.v), r.nm)
      const seen = new Set(local.map(String))
      show([...local, ...rows.map((r) => r.v).filter((v) => !seen.has(String(v)))].slice(0, 8))
    }, () => {}), 200)
  }
  input.onkeydown = (ev) => {
    if (ev.key === 'Enter' && hits.length && input.value.trim()) pick(hits[at])
    else if (ev.key === 'Escape') (asked++, show([]))
    else if ((ev.key === 'ArrowDown' || ev.key === 'ArrowUp') && hits.length) {
      ev.preventDefault()
      at = (at + (ev.key === 'ArrowDown' ? 1 : -1) + hits.length) % hits.length
      list.querySelectorAll('li').forEach((li, i) => li.setAttribute('aria-selected', String(i === at)))
      input.setAttribute('aria-activedescendant', `mast-found-${at}`)
    }
  }
  input.onblur = () => setTimeout(() => (asked++, show([])), 100)
  return h('div', { class: 'mast__search' }, input, list)
}

const missingDefaults = []
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
  // Blocks are drawn again in place only when every key is its own: two blocks sharing one would update one element
  // twice and leave the other stale, so a collision takes the replace path.
  const unique = (cs) => new Set(cs.map((c) => c.dataset.key)).size === cs.length
  if (same && fresh.length === here.length && unique(fresh) && unique(here) && fresh.every((c) => byKey.has(c.dataset.key))) {
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
    // In a frame as tall as its content the host page scrolls, not this one: bring the app's top into its view.
    if (window.parent !== window) $('#app').scrollIntoView({ block: 'start' })
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
  fails.push(...missingDefaults)
  for (const c of document.querySelectorAll('#main .block')) if (c.querySelector('.state--error')) fails.push(`“${c.querySelector('.kicker')?.textContent}” failed: ${c.querySelector('.state--error').textContent}`)
  const text = $('#main').innerText
  for (const bad of [/\bundefined\b/, /\bNaN\b/, /\[object /, /\bnull\b/, /Infinity/]) if (bad.test(text)) fails.push(`The page says ${bad}`)
  document.body.dataset.check = fails.length ? 'fail' : 'ok'
  document.body.prepend(h('pre', { class: 'self-check', text: fails.length ? `Check failed:\n${fails.join('\n')}` : `Check passed: ${document.querySelectorAll('#main .block').length} blocks, no errors or placeholders.` }))
}
