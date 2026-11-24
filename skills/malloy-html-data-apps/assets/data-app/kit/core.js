// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT
//
// The kit's plumbing: an element builder, queries (one request per distinct text, at most four in flight, cached for
// the session), what Publisher said about each answer, the model's own docs, d3 loading, and theme and mode.

import { config } from './state.js'

export const $ = (sel, root = document) => root.querySelector(sel)

/** h('div', { class: 'x', text: 'y', onclick }, ...children). Never innerHTML: every value is text. */
export function h(tag, props = {}, ...kids) {
  const el = document.createElement(tag)
  for (const [k, v] of Object.entries(props || {})) {
    if (v === null || v === undefined || v === false) continue
    if (k === 'text') el.textContent = v
    else if (k === 'class') el.className = v
    else if (k === 'style' && typeof v === 'object') for (const [p, x] of Object.entries(v)) p.startsWith('--') ? el.style.setProperty(p, String(x)) : (el.style[p] = x)
    else if (k.startsWith('on')) el.addEventListener(k.slice(2), v)
    else el.setAttribute(k, v === true ? '' : v)
  }
  el.append(...kids.flat().filter((k) => k !== null && k !== undefined && k !== false))
  return el
}
const NS = 'http://www.w3.org/2000/svg'
export function svgEl(tag, attrs = {}, ...kids) {
  const el = document.createElementNS(NS, tag)
  for (const [k, v] of Object.entries(attrs)) el.setAttribute(k, v)
  el.append(...kids)
  return el
}

/** Prose with bold parts: prose(['Revenue rose ', { b: '12%' }, '.']) or a string. */
export const prose = (parts) =>
  !parts ? [] : typeof parts === 'string' ? [parts] : parts.map((p) => (p && typeof p === 'object' ? h('strong', { text: String(p.b ?? '') }) : String(p ?? '')))
export const asText = (parts) => (Array.isArray(parts) ? parts.map((p) => (p && typeof p === 'object' ? p.b : p)).join('') : String(parts ?? ''))

/** What a failed query says: the server's reason when it gave one. */
export const reason = (err) => err?.response?.message ?? err?.message ?? String(err)

// ---- What Publisher says about each answer ----
// A query's response carries where it was answered from (a table built ahead of time, or the raw data just now), how
// long it took, and which source and fields it read. Publisher.query hands back rows only, so the kit reads the rest
// off the same response: one wrapper around fetch, for query requests only, which changes nothing about the request.
export const answered = new Map() // query text -> { servedFrom, ms, path }
let modelAt = null
const pageFetch = window.fetch.bind(window)
window.fetch = async (input, init) => {
  const res = await pageFetch(input, init)
  try {
    const url = typeof input === 'string' ? input : input.url
    if (init?.method === 'POST' && init.body && /\/models\/[^/]+\/query(\?|$)/.test(url)) {
      const q = JSON.parse(init.body).query
      if (q) {
        modelAt ||= { url: url.replace(/\/query(\?.*)?$/, ''), init: { credentials: init.credentials, headers: Object.fromEntries(Object.entries(init.headers || {}).filter(([k]) => k.toLowerCase() !== 'content-type')) } }
        res.clone().json().then((j) => answered.set(q, { servedFrom: j.servedFrom ?? null, ms: j.executionTimeMs, path: j.path }), () => {})
      }
    }
  } catch {
    // Not a query, or not JSON: nothing to record.
  }
  return res
}

/** The model's own words: each source's and field's #(doc), fetched once, when a reader first turns a card over. */
let docsP = null
export function modelDocs() {
  if (!docsP) {
    docsP = (modelAt ? pageFetch(modelAt.url, modelAt.init).then((r) => (r.ok ? r.json() : null)) : Promise.resolve(null))
      .then((m) => {
        const docs = new Map()
        const infos = (m?.sourceInfos || []).map((raw) => (typeof raw === 'string' ? JSON.parse(raw) : raw))
        const lines = (anns) => (anns || []).map((a) => a.value.trim()).filter((v) => v.startsWith('#(doc)'))
        // A doc can run over several #(doc) lines. A source that extends another lists the parent's lines first, and
        // nothing marks them: drop a leading run equal to another source's whole doc (or the same field's in another
        // source), then join what is left.
        const own = (ls, others) => {
          let cut = 0
          for (const o of others) if (o.length > cut && o.length < ls.length && o.every((x, i) => ls[i] === x)) cut = o.length
          return ls.slice(cut).map((v) => v.replace(/^#\(doc\)\s*/, '')).join(' ').trim() || undefined
        }
        const srcLines = infos.map((si) => lines(si.annotations))
        const fieldLines = new Map()
        const walk = (fs, prefix, out) => {
          for (const f of fs || []) {
            out.push([prefix + f.name, lines(f.annotations)])
            if (f.schema?.fields && prefix.split('.').length < 3) walk(f.schema.fields, `${prefix}${f.name}.`, out)
          }
          return out
        }
        const perSource = infos.map((si) => walk(si.schema?.fields, '', []))
        for (const list of perSource) for (const [name, ls] of list) (fieldLines.get(name) || fieldLines.set(name, []).get(name)).push(ls)
        infos.forEach((si, i) => {
          const fields = new Map(perSource[i].map(([name, ls]) => [name, own(ls, fieldLines.get(name).filter((o) => o !== ls))]))
          docs.set(si.name, { doc: own(srcLines[i], srcLines.filter((o) => o !== srcLines[i])), fields })
        })
        return docs
      })
      .catch(() => new Map())
  }
  return docsP
}

/**
 * What Publisher built for this package, for the back of a block: each table by its id (a stored source, or a
 * rollup with its base source, grain and measures) and when the last build finished. Fetched once, when a reader
 * first turns a block over; null where the host does not answer, and the back then says less.
 */
let servedP = null
export function servedTables() {
  if (!servedP) {
    const base = modelAt?.url.replace(/\/models\/.*$/, '')
    const get = (path) => pageFetch(`${base}/${path}`, modelAt.init).then((r) => (r.ok ? r.json() : null)).catch(() => null)
    servedP = !base ? Promise.resolve(null) : Promise.all([get('build-plan'), get('materializations')]).then(([plan, runs]) => {
      if (!plan?.sources) return null
      const tables = new Map(Object.values(plan.sources).map((src) => [src.sourceEntityId, { name: src.name, origin: src.origin, rollup: src.preaggregate || null }]))
      const done = (Array.isArray(runs) ? runs : []).filter((r) => r.status === 'MANIFEST_FILE_READY' && r.completedAt).sort((a, b) => String(b.completedAt).localeCompare(String(a.completedAt)))[0]
      return { tables, builtAt: done?.completedAt || null }
    })
  }
  return servedP
}

// ---- Queries ----

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
      queue.push(() => Publisher.query(config.model, q).then(resolve, reject))
      pump()
    })
    p.catch(() => cache.delete(q))
    cache.set(q, p)
  }
  return cache.get(q)
}

/** d3, loaded once beside the first queries rather than ahead of them; nothing draws before it is in. */
export const d3Ready = window.d3
  ? Promise.resolve()
  : new Promise((resolve, reject) => {
      const s = document.createElement('script')
      s.src = new URL('./vendor/d3.min.js', import.meta.url).href
      s.onload = resolve
      s.onerror = () => reject(Object.assign(new Error('Could not load the chart library (kit/vendor/d3.min.js).'), { whole: true }))
      document.head.append(s)
    })

// ---- Theme and mode: data-theme on <html> is the app's theme; data-mode fixes light or dark, else the reader's
// system setting decides. ?theme= and ?mode= override both, for a preview or a screenshot. ----
{
  const p = new URLSearchParams(location.search)
  if (p.get('theme')) document.documentElement.dataset.theme = p.get('theme')
  if (['light', 'dark'].includes(p.get('mode'))) document.documentElement.dataset.mode = p.get('mode')
}
// The host's mode. Credible shows a data app in a frame and tells it which mode its reader chose
// (credible:color-scheme, on load and on every change). The kit follows it unless the address fixes one (?mode=),
// and answers credible:color-scheme-applied, so the host knows this app draws dark itself and stops holding its
// frame light. It also asks once on start, in case the host spoke before the kit was listening.
if (window.parent !== window) {
  const fixed = ['light', 'dark'].includes(new URLSearchParams(location.search).get('mode'))
  addEventListener('message', (e) => {
    const d = e.data
    if (e.source !== window.parent || !d || d.type !== 'credible:color-scheme' || !['light', 'dark'].includes(d.mode)) return
    if (!fixed) document.documentElement.dataset.mode = d.mode
    e.source.postMessage({ type: 'credible:color-scheme-applied', mode: fixed ? document.documentElement.dataset.mode : d.mode }, e.origin === 'null' ? '*' : e.origin)
  })
  window.parent.postMessage({ type: 'credible:color-scheme-request' }, '*')
}

/** Charts read their colours when they draw, so a new theme or mode draws every loaded card again, in place. */
function recolour() {
  for (const c of document.querySelectorAll('#main .block')) if (c.__loaded && c.__spec) c.__update?.(c.__spec)
}
matchMedia('(prefers-color-scheme: dark)').addEventListener('change', recolour)
new MutationObserver(recolour).observe(document.documentElement, { attributes: true, attributeFilter: ['data-theme', 'data-mode'] })

// ---- Credible's agent: a reader who wants to go further asks it, from the block they were reading. ----
/** True inside Credible's viewer: in a frame, on a Credible data host ("data" or "<env>-data"). */
export function inCredible(loc = location) {
  const parts = loc.host.split('.')
  const plane = parts[1] || ''
  return window.parent !== window && parts.length >= 3 && (plane === 'data' || plane.endsWith('-data'))
}
/** Opens Credible's chat with `prompt` as the reader's question, after attaching `context` (what they were looking
 * at and the queries behind it). Only inside Credible's viewer; anywhere else it does nothing and returns false. */
export function askCredible(prompt, context) {
  const text = String(prompt ?? '').trim().slice(0, 4000)
  if (!text || !inCredible()) return false
  const parts = location.host.split('.')
  parts[1] = parts[1].replace(/data$/, 'app')
  const origin = `${location.protocol}//${parts.join('.')}`
  if (context) {
    // Credible drops a context over 8,000 characters whole, so shed the least useful parts until it fits:
    // the later queries, then the filter values, then everything but what the reader was looking at.
    const fits = (c) => JSON.stringify(c).length <= 7800
    let c = context
    while (!fits(c) && (c.items || []).length > 1) c = { ...c, items: c.items.slice(0, -1) }
    if (!fits(c)) c = { ...c, givens: Object.fromEntries(Object.entries(c.givens || {}).map(([k, v]) => [k, Array.isArray(v) && v.length > 5 ? [...v.slice(0, 5), `and ${v.length - 5} more`] : v])) }
    if (!fits(c)) c = { scope: c.scope, view: c.view }
    window.parent.postMessage({ type: 'credible:chat-context', context: c }, origin)
  }
  window.parent.postMessage({ type: 'credible:chat-message', prompt: text }, origin)
  return true
}
