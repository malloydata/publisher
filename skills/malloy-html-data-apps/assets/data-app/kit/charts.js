// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT
/* global d3, topojson */
// The chart module every app copies; reference/charts.md in the malloy-html-data-apps skill documents it.
// The app's charts, drawn with d3 into SVG. Colours are token names from the stylesheet ('--accent', '--c2'), so the
// look carries through. Every chart that stands for something can be clicked (onPick) and says what it is on hover.

// ---- Numbers and colours ----

const cvs = typeof document !== 'undefined' ? document.createElement('canvas').getContext('2d', { willReadFrequently: true }) : null
const resolved = new Map()
/** A CSS colour as rgb() that d3 can interpolate: color-mix() and color() come back from the browser unparsed. */
function rgbOf(v) {
  if (!v || /^(#|rgb|hsl)/.test(v) || !cvs) return v
  if (!resolved.has(v)) {
    cvs.clearRect(0, 0, 1, 1)
    cvs.fillStyle = '#000'
    cvs.fillStyle = v
    cvs.fillRect(0, 0, 1, 1)
    const [r, g, b, a] = cvs.getImageData(0, 0, 1, 1).data
    resolved.set(v, a === 255 ? `rgb(${r}, ${g}, ${b})` : `rgba(${r}, ${g}, ${b}, ${(a / 255).toFixed(3)})`)
  }
  return resolved.get(v)
}
export const token = (n) => rgbOf(getComputedStyle(document.documentElement).getPropertyValue(n).trim())
const nf = new Intl.NumberFormat('en-US')
export const fmt = (v) => (Number.isFinite(v) ? nf.format(v) : 'n/a')
export const pct = (v, d = 1) => (Number.isFinite(v) ? `${(v * 100).toFixed(d)}%` : 'n/a')
const compact = new Intl.NumberFormat('en-US', { notation: 'compact', maximumSignificantDigits: 3 })
const num = (v) => typeof v === 'number' && Number.isFinite(v)
const paint = (c) => (String(c).startsWith('--') ? token(c) : c)

/** The next round number at or above v. */
function niceTop(v) {
  if (!(v > 0)) return 0
  const p = 10 ** Math.floor(Math.log10(v))
  return [1, 1.2, 1.5, 2, 2.5, 3, 4, 5, 6, 8, 10].map((k) => k * p).find((t) => t >= v * 0.999)
}
/** A domain that holds zero and every value, with round ends. */
function zeroSpan(values) {
  const hi = Math.max(0, d3.max(values.filter(num)) ?? 0)
  const lo = Math.min(0, d3.min(values.filter(num)) ?? 0)
  const top = niceTop(hi * 1.04)
  const bottom = lo < 0 ? -niceTop(-lo * 1.04) : 0
  return top === bottom ? [0, 1] : [bottom, top]
}

/** Move label positions apart so none sits within `gap` px of the next, keeping them inside [lo, hi]. */
function dodge(ys, gap, lo, hi) {
  const order = ys.map((y, i) => [y, i]).sort((a, b) => a[0] - b[0])
  const out = order.map(([y]) => y)
  for (let k = 1; k < out.length; k++) out[k] = Math.max(out[k], out[k - 1] + gap)
  const over = out.length ? out.at(-1) - hi : 0
  if (over > 0) for (let k = out.length - 1; k >= 0; k--) out[k] = Math.max(lo, k === out.length - 1 ? out[k] - over : Math.min(out[k], out[k + 1] - gap))
  const res = []
  order.forEach(([, i], k) => (res[i] = out[k]))
  return res
}

const lum = (color) => {
  const c = d3.rgb(color)
  return [c.r, c.g, c.b].map((v) => ((v /= 255) <= 0.03928 ? v / 12.92 : ((v + 0.055) / 1.055) ** 2.4)).reduce((s, v, i) => s + v * [0.2126, 0.7152, 0.0722][i], 0)
}
const ratio = (a, b) => (Math.max(lum(a), lum(b)) + 0.05) / (Math.min(lum(a), lum(b)) + 0.05)
/** Text on a fill: white or the page's darkest text, whichever has the higher contrast. */
function inkOn(fill) {
  const dark = token('--text-strong') || '#1a1714'
  return ratio(fill, '#ffffff') >= ratio(fill, dark) ? '#ffffff' : dark
}
/** A series colour darkened until it reads as text on the page, at 4.5:1. */
function readable(color) {
  const bg = token('--bg') || '#ffffff'
  let c = d3.rgb(color)
  for (let i = 0; i < 20 && ratio(c.formatHex(), bg) < 4.5; i++) c = c.darker(0.15)
  return c.formatHex()
}
/** A change's class: good, bad or neutral, by which way is good news ('rise' or 'fall'); neutral when not said. */
const tone = (v, good) => (good === null || good === undefined || !num(v) || v === 0 ? '' : (v > 0) === (good === 'rise') ? 'fx-good' : 'fx-bad')
const toneFill = (v, good) => ({ 'fx-good': '--down', 'fx-bad': '--up' })[tone(v, good)] || '--c-bar'
const ramp = () => ['--seq-1', '--seq-2', '--seq-3', '--seq-4', '--seq-5'].map(token)
const noData = () => token('--nodata') || token('--surface-2')

/** Up to five classes by quantile, so a few large values do not wash out the rest. */
function classes(values) {
  const v = values.filter(num).sort(d3.ascending)
  const breaks = [...new Set([0.2, 0.4, 0.6, 0.8].map((q) => d3.quantileSorted(v, q)).filter(num))].filter((b) => b < v.at(-1))
  const all = ramp()
  const colors = breaks.length ? breaks.map((_, i) => all[Math.round((i * 4) / breaks.length)]).concat(all[4]) : v.length ? [all[2]] : []
  const at = (x) => breaks.filter((b) => x > b).length
  const ranges = colors.map((_, i) => { const m = v.filter((x) => at(x) === i); return [m[0], m.at(-1)] })
  return { colors, ranges, at: (x) => colors[at(x)] }
}

// ---- Elements ----

/** h('div', { class: 'x', text: 'y', style: {} }, ...children): an HTML element, text never parsed as markup. */
function h(tag, props = {}, ...kids) {
  const el = document.createElement(tag)
  for (const [k, v] of Object.entries(props)) {
    if (v === null || v === undefined || v === false) continue
    if (k === 'text') el.textContent = v
    else if (k === 'class') el.className = v
    else if (k === 'style') Object.assign(el.style, v)
    else el.setAttribute(k, v)
  }
  el.append(...kids.filter((x) => x !== null && x !== undefined && x !== false))
  return el
}
/** A detached <svg> as a d3 selection, w by h, named for screen readers. */
const svgIn = (w, ht, label) => d3.create('svg').attr('width', w).attr('height', ht).attr('viewBox', `0 0 ${w} ${ht}`).attr('role', 'img').attr('aria-label', label)

const CAPS = 13
const textW = (t, px = 11) => String(t).length * px * 0.6
/** Text cut with an ellipsis to fit `width` px at `px` font size. */
function fit(t, width, px = 11) {
  const str = String(t ?? '')
  if (textW(str, px) <= width) return str
  return `${str.slice(0, Math.max(1, Math.floor(width / (px * 0.6)) - 1))}…`
}
/** A halo label placed only where it overlaps none of `boxes` (labels already placed); returns whether it was. */
function put(boxes, g, x, y, text, attrs = {}, px = 11.5) {
  const w = textW(text, px)
  const x0 = attrs.anchor === 'end' ? x - w : attrs.anchor === 'middle' ? x - w / 2 : x
  const box = { x0, x1: x0 + w, y0: y - px, y1: y + 3 }
  if (!text || boxes.some((b) => box.x0 < b.x1 && box.x1 > b.x0 && box.y0 < b.y1 && box.y1 > b.y0)) return false
  boxes.push(box)
  halo(g, x, y, text, attrs)
  return true
}
/** A label with a halo of the page colour, so it reads over marks. attrs: anchor, base, cls, fill. */
function halo(g, x, y, text, attrs = {}) {
  return g.append('text').attr('x', x).attr('y', y).attr('class', `fx-halo ${attrs.cls || ''}`).attr('text-anchor', attrs.anchor || null).attr('dominant-baseline', attrs.base || null).style('fill', attrs.fill || null).text(text)
}

let uid = 0
const nextId = (stem) => `${stem}-${++uid}`

/** Claim `el` for one chart: drop any earlier chart's observer and class. */
function claim(el, cls) {
  el.__ro?.disconnect()
  if (el.__fx) el.classList.remove(el.__fx)
  el.__fx = cls
  el.classList.add('fx', cls)
}
/** Draw with paint(width) -> node(s), and again when the box changes width; give focus back after a keyboard pick.
 * Drawn again into a box that holds the same form, the marks move from where they were to where they are now. */
function mount(el, cls, paintFn) {
  const before = MOTION && el.__fx === cls ? el.querySelector('svg') : null
  const entering = MOTION && !el.__fx
  claim(el, cls)
  let last = 0
  let first = true
  const draw = () => {
    const w = Math.round(el.clientWidth)
    if (!w || Math.abs(w - last) < 2) return
    last = w
    tipHide()
    el.replaceChildren(...[paintFn(w)].flat().filter(Boolean))
    for (const svg of el.querySelectorAll('svg')) if (svg.querySelector('[role="button"]')) svg.setAttribute('role', 'group')
    if (first && before) morph(before, el.querySelector('svg'))
    else if (first && entering) enter(el)
    first = false
    relink()
    restoreFocus(el)
  }
  draw()
  el.__ro = new ResizeObserver(draw)
  el.__ro.observe(el)
}
// ---- Motion: a mark keeps what it stands for from one draw to the next ----

/** Off when the reader asks for less motion, or the address carries ?motion=0. */
export const MOTION = typeof matchMedia === 'function' && !matchMedia('(prefers-reduced-motion: reduce)').matches && !/[?&]motion=0\b/.test(location.search)
/** How long a mark takes to move, in ms. */
export const DUR = 380
const ATTRS = ['x', 'y', 'width', 'height', 'cx', 'cy', 'r', 'x1', 'x2', 'y1', 'y2', 'd', 'transform', 'points']
const STYLES = ['fill', 'opacity', 'stroke', 'fill-opacity']
/** What a mark stands for, from the datum d3 bound to it: a label, an id, a place, a cell. */
function datumKey(d) {
  if (d === null || d === undefined) return null
  if (typeof d !== 'object') return String(d)
  if (Array.isArray(d)) return d.key !== undefined ? String(d.key) : d.data ? datumKey(d.data) : null
  if (d.key !== undefined && typeof d.key !== 'object') return String(d.key)
  for (const f of ['id', 'label', 'name']) if (d[f] !== undefined && d[f] !== null && typeof d[f] !== 'object') return String(d[f])
  if (d.properties?.name) return d.properties.name
  if (d.r !== undefined && d.c !== undefined) return `${d.r}|${d.c}`
  return d.data ? datumKey(d.data) : null
}
/** Each element of an svg by a key that survives a redraw: its groups' and its own first class, and what it stands for. */
function marksOf(svg) {
  const out = new Map()
  const seen = new Map()
  const cls = (e) => (e.getAttribute('class') || '').split(' ')[0] // later classes mark a state a redraw changes
  for (const n of svg.querySelectorAll('*')) {
    let sig = `${n.tagName}.${cls(n)}`
    for (let p = n.parentElement; p && p !== svg; p = p.parentElement) sig = `${p.tagName}.${cls(p)}/${sig}`
    let k = datumKey(n.__data__)
    if (k === null) {
      const i = seen.get(sig) || 0
      seen.set(sig, i + 1)
      k = `#${i}`
    }
    if (!out.has(`${sig}|${k}`)) out.set(`${sig}|${k}`, n)
  }
  return out
}
const numbersIn = (s) => (s.match(/-?\d*\.?\d+(?:e-?\d+)?/g) || []).length
/** Move each mark of the new drawing from where its match was in the old one; a mark with no match fades in. */
function morph(oldSvg, newSvg) {
  if (!oldSvg || !newSvg) return
  const before = marksOf(oldSvg)
  const matched = new Set()
  for (const [key, n] of marksOf(newSvg)) {
    const o = before.get(key)
    if (!o) {
      if (n.parentElement === newSvg || matched.has(n.parentElement)) fadeIn(n)
      continue
    }
    matched.add(n)
    let t = null
    const go = () => (t ||= d3.select(n).transition().duration(DUR).ease(d3.easeCubicOut))
    for (const a of ATTRS) {
      const was = o.getAttribute(a)
      const now = n.getAttribute(a)
      if (was === null || now === null || was === now || (/^(d|points|transform)$/.test(a) && numbersIn(was) !== numbersIn(now))) continue
      n.setAttribute(a, was)
      go().attr(a, now)
    }
    for (const p of STYLES) {
      const was = o.style.getPropertyValue(p)
      const now = n.style.getPropertyValue(p)
      if (!was || !now || was === now) continue
      n.style.setProperty(p, was)
      go().style(p, now)
    }
    if (n.tagName === 'text' && !n.childElementCount && o.textContent !== n.textContent) countTo(n, o.textContent, n.textContent)
  }
}
function fadeIn(n) {
  const end = n.style.opacity
  d3.select(n).style('opacity', 0).transition().duration(DUR).style('opacity', end || 1).on('end', () => { if (!end) n.style.removeProperty('opacity') })
}
/** A chart's first appearance: it rises into place, and its lines draw from left to right. */
function enter(el) {
  el.animate([{ opacity: 0, transform: 'translateY(6px)' }, { opacity: 1, transform: 'none' }], { duration: DUR, easing: 'cubic-bezier(.2,.7,.3,1)' })
  for (const path of el.querySelectorAll('.fx-series path')) {
    const len = path.getTotalLength?.() || 0
    if (len) path.animate([{ strokeDasharray: `${len} ${len}`, strokeDashoffset: len }, { strokeDasharray: `${len} ${len}`, strokeDashoffset: 0 }], { duration: DUR * 2.2, easing: 'cubic-bezier(.3,.6,.3,1)' })
  }
}
const NUMBER = /^(.*?)([−-]?)([\d,]*\.?\d+)(.*)$/s
/**
 * Count a number written as text from its old value to its new one: countTo(node, '1,204', '3,880'). The new
 * text's prefix, suffix and decimals are kept; text that is not the same number in the same words is left alone.
 */
export function countTo(node, from, to) {
  const a = NUMBER.exec(String(from).trim())
  const b = NUMBER.exec(String(to).trim())
  if (!MOTION || !a || !b || a[1] !== b[1] || a[4] !== b[4]) return
  const v0 = Number(a[3].replace(/,/g, '')) * (a[2] ? -1 : 1)
  const v1 = Number(b[3].replace(/,/g, '')) * (b[2] ? -1 : 1)
  if (!num(v0) || !num(v1) || v0 === v1) return
  const dp = (b[3].split('.')[1] || '').length
  const nf = new Intl.NumberFormat('en-US', { minimumFractionDigits: dp, maximumFractionDigits: dp, useGrouping: /,/.test(a[3] + b[3]) })
  const say = (v) => `${b[1]}${v < 0 ? b[2] || '−' : ''}${nf.format(Math.abs(v))}${b[4]}`
  const i = d3.interpolateNumber(v0, v1)
  node.textContent = say(v0)
  const t = d3.timer((ms) => {
    const k = Math.min(1, ms / (DUR * 1.4))
    node.textContent = k < 1 ? say(i(d3.easeCubicOut(k))) : to
    if (k >= 1) t.stop()
  })
}

// ---- Linked hover: a mark lights what it stands for in every chart and ranked table on the page ----

let linked = null
function linkOn(key) {
  if (key === null || key === undefined || key === '' || key === linked) return
  linkOff()
  linked = key
  relink()
}
/** Light what is linked again, after a chart is drawn anew under the pointer. */
function relink() {
  if (linked === null) return
  const k = CSS.escape(linked)
  for (const n of document.querySelectorAll(`[data-link="${k}"], tr[data-id="${k}"]`)) {
    n.classList.add('fx-hot')
    n.closest('svg, table')?.classList.add('has-hot')
  }
}
function linkOff() {
  linked = null
  for (const n of document.querySelectorAll('.fx-hot')) n.classList.remove('fx-hot')
  for (const n of document.querySelectorAll('.has-hot')) n.classList.remove('has-hot')
}

/** With nothing to draw, say so in the card. */
function nothing(el, text = 'Nothing to draw for these filters.') {
  claim(el, 'fx-none')
  el.replaceChildren(h('p', { class: 'fx-empty', text }))
}
const leftOut = (text) => (text ? h('p', { class: 'fx-left-out', text }) : null)
const noValue = (n) => (n ? `${n} missing value${n > 1 ? 's are' : ' is'} not drawn.` : '')
const unsaid = (refs) => (refs.length ? `No value for ${refs.map((r) => r.label || 'a reference').join(', ')}, so not drawn.` : '')
const repeats = (ls) => { const t = [...new Set(ls.filter((l, i) => ls.indexOf(l) !== i))]; return t.length ? `Labels that repeat, drawn once: ${t.join(', ')}.` : '' }
const offBand = (x, band) => (band && (x(band.from) === undefined || x(band.to) === undefined) ? 'The selected period is not on this axis.' : '')
// Values missing inside a line; a line that starts late or ends early (a season still under way) has none missing.
const gaps = (labels, series) => noValue(d3.sum(series.filter((s) => s.gaps !== false), (s) => {
  const has = labels.map((_, i) => num(s.values[i]))
  const first = has.indexOf(true)
  return first < 0 ? 0 : has.slice(first, has.lastIndexOf(true) + 1).filter((x) => !x).length
}))
/** What a chart could not draw, under it in one line; nothing when there is nothing to say. */
const notes = (...lines) => leftOut(lines.filter(Boolean).join(' '))

// ---- Picking and the tooltip ----

let refocus = null
/** Make a mark clickable, and a named button a keyboard reaches unless keyboard is false (thousands of marks). */
function pickable(node, id, onPick, name, pressed = false, keyboard = true, focusKey = id) {
  if (!onPick) return node
  node.setAttribute('data-link', String(Array.isArray(id) ? id[0] : id && typeof id === 'object' ? id.name ?? '' : id))
  node.setAttribute('data-pick', '')
  node.setAttribute('data-key', String(focusKey))
  node.addEventListener('click', () => onPick(id))
  if (!keyboard) return node
  node.setAttribute('tabindex', '0')
  node.setAttribute('role', 'button')
  node.setAttribute('aria-label', name ?? String(id))
  node.setAttribute('aria-pressed', String(Boolean(pressed)))
  node.addEventListener('keydown', (e) => {
    if (e.key !== 'Enter' && e.key !== ' ') return
    e.preventDefault()
    refocus = { key: String(focusKey), form: node.closest('.fx')?.__fx, at: Date.now() }
    onPick(id)
  })
  return node
}
function restoreFocus(root) {
  if (!refocus) return
  const now = document.activeElement
  if (Date.now() - refocus.at > 4000 || (now && now !== document.body && !root.contains(now))) { refocus = null; return }
  if (root.__fx !== refocus.form) return
  const back = [...root.querySelectorAll('[data-key][tabindex]')].find((n) => n.dataset.key === refocus.key)
  if (back) { back.focus({ preventScroll: true }); refocus = null }
}

let tipEl = null
/** Show the tooltip: the first line bold, the rest plain. Placed on the page, never fixed, so it works in a frame. */
function tipShow(ev, lines) {
  if (!tipEl) {
    tipEl = h('div', { class: 'fx-tip', role: 'status' })
    document.body.append(tipEl)
  }
  tipEl.replaceChildren(...lines.filter((l) => l !== null && l !== undefined && l !== '').map((l, i) => h(i ? 'span' : 'strong', { text: String(l) })))
  tipEl.style.display = 'block'
  let x = ev.pageX
  let y = ev.pageY
  if (x === undefined || (!x && !y)) {
    const r = ev.target.getBoundingClientRect()
    x = r.left + r.width / 2 + window.scrollX
    y = r.top + window.scrollY
  }
  const w = tipEl.offsetWidth
  tipEl.style.left = `${Math.max(4, Math.min(x - w / 2, document.documentElement.scrollWidth - w - 4))}px`
  tipEl.style.top = `${Math.max(4, y - tipEl.offsetHeight - 12)}px`
}
const tipHide = () => { if (tipEl) tipEl.style.display = 'none' }
/** Hover shows lines(d) on a d3 selection, and so does keyboard focus on its marks that are buttons. Call it after
 * pickable: a focus listener alone would make Chrome tab to an SVG mark that is not a button. */
function hover(sel, lines) {
  sel.on('mousemove.tip', function (ev, d) { tipShow(ev, lines(d)); linkOn(this.getAttribute('data-link')) }).on('mouseleave.tip', () => { tipHide(); linkOff() })
  sel.filter(function () { return this.hasAttribute('tabindex') }).on('focus.tip', (ev, d) => tipShow(ev, lines(d))).on('blur.tip', tipHide)
}

// ---- Axes, legends and label placement ----

/** A value axis on the left with gridlines across the plot. */
function yAxis(g, y, width, format = fmt, ticks = 5) {
  g.attr('class', 'fx-ax').call(d3.axisLeft(y).ticks(ticks).tickSize(-width).tickPadding(6).tickFormat(format)).call((a) => a.select('.domain').remove())
}
/** A category or value axis along the bottom: tick labels only. */
function xAxis(g, x, { format, values } = {}) {
  const ax = d3.axisBottom(x).tickSize(0).tickPadding(8)
  if (format) ax.tickFormat(format)
  if (values) ax.tickValues(values)
  g.attr('class', 'fx-ax fx-ax--x').call(ax).call((a) => a.select('.domain').remove())
}
/** Inset a point scale's ends so its first and last tick labels fit inside [left, right]. */
const insetPoints = (labels, shown, left, right) => [left + Math.min(textW(shown[0] ?? '') / 2, 40), right - Math.min(textW(labels.at(-1) ?? '') / 2, 40)]
/** Every k-th label so at most `max` show along an axis. */
const everyNth = (labels, max) => labels.filter((_, i) => i % Math.max(1, Math.ceil(labels.length / max)) === 0)

/** A legend for classes() under a map or grid. extra: [[class, text]] swatches after the classes. */
function legend(c, title, extra = [], format = (v) => compact.format(v)) {
  const box = h('div', { class: 'map-legend' })
  if (title) box.append(h('strong', { class: 'caps', text: title }))
  c.colors.forEach((col, i) => {
    const [a, b] = c.ranges[i]
    if (!num(a)) return
    box.append(h('span', {}, h('i', { style: { background: col } }), a === b ? format(a) : `${format(a)}–${format(b)}`))
  })
  box.append(h('span', {}, h('i', { style: { background: noData() } }), 'No data'))
  for (const [cls, text] of extra) box.append(h('span', {}, h('i', { class: cls }), text))
  return box
}
/** A key of named colours, for parts too thin to name inside them. items: [{ name, color }]. */
const key = (items) => h('div', { class: 'legend' }, ...items.map((it) => h('span', {}, h('i', { style: { background: paint(it.color) } }), it.name)))

/** Keep the first position of each label that collides with no label kept before it. */
function placeLabels(cands, { bounds = [-Infinity, Infinity], top = -Infinity, avoid = [], px = 11 } = {}) {
  const kept = []
  const hit = (a, b) => a.x0 < b.x1 && a.x1 > b.x0 && a.y0 < b.y1 && a.y1 > b.y0
  for (const c of cands) {
    const w = textW(c.text, px)
    for (const pos of c.positions) {
      const x0 = pos.anchor === 'start' ? pos.x : pos.anchor === 'end' ? pos.x - w : pos.x - w / 2
      const box = { x0: x0 - 2, x1: x0 + w + 2, y0: pos.y - px, y1: pos.y + 3 }
      if (box.x0 < bounds[0] || box.x1 > bounds[1] || box.y0 < top) continue
      if (kept.some((k) => hit(box, k.box)) || (!c.force && avoid.some((a) => a !== c.own && hit(box, a)))) continue
      kept.push({ ...c, ...pos, box })
      break
    }
  }
  return kept
}

/** Shapes from a TopoJSON object, and the keys in values that have none. */
function shapesOf(topo, object, keyOf, values) {
  const features = topojson.feature(topo, topo.objects[object]).features
  const have = new Set(features.map(keyOf))
  const missing = [...values.keys()].filter((k) => !have.has(k))
  return { features, missing }
}
const notOnMap = (missing) => (missing.length ? `Not on this map, no shape matched: ${missing.join(', ')}.` : '')

/** Room the widest of these labels needs, in px. */
const room = (labels, px = 12) => Math.ceil(d3.max(labels, (l) => textW(l, px)) || 0) + 12

// ---- The standard charts ----

/**
 * Bars. rows: [{ label, value, color (a token name for this bar alone), ...extra }]. opts: color (a token name), horizontal, format, direct (each value
 * written at its bar's end, and no value axis), selected (the label in scope; the rest fade), onPick(label),
 * tip(row) -> extra tooltip lines, refs ([{ value, label }]), height (vertical bars), label.
 */
export function bars(el, rows, opts = {}) {
  const { color = '--c-bar', horizontal = false, format = fmt, direct = false, selected = null, onPick, tip, refs = [], height = 280, label = 'Bar chart' } = opts
  const data = rows.filter((r) => num(r.value))
  if (!data.length) return nothing(el)
  const dropped = rows.length - data.length
  const marks = refs.filter((r) => num(r.value))
  mount(el, 'fx-bars', (w) => {
    const ext = zeroSpan([...data.map((r) => r.value), ...marks.map((r) => r.value)])
    const lines = (r) => [r.label, format(r.value), ...(tip ? [tip(r)].flat() : [])]
    let svg
    if (horizontal) {
      const step = 26
      // With a reference line the labels' row needs room above the bars for its label.
      const m = { t: direct ? (marks.length ? 20 : 4) : 22, r: direct ? Math.min(room(data.map((r) => format(r.value))), w / 4) : Math.max(12, textW(format(ext[1])) / 2 + 4), b: 4, l: Math.min(room(data.map((r) => r.label)), w * 0.38) }
      const ht = m.t + step * data.length + m.b
      svg = svgIn(w, ht, label)
      const x = d3.scaleLinear(ext, [m.l, w - m.r])
      const y = d3.scaleBand(data.map((r) => r.label), [m.t, ht - m.b]).padding(0.25)
      if (!direct) svg.append('g').attr('class', 'fx-ax').attr('transform', `translate(0,${m.t - 4})`).call(d3.axisTop(x).ticks(Math.max(2, Math.floor(w / 110))).tickSize(-(ht - m.t)).tickFormat(format)).call((a) => a.select('.domain').remove())
      // Reference lines behind the bars, labelled above them, so no value is struck through.
      for (const ref of marks) refLine(svg, { x: x(ref.value), y0: m.t - (direct ? 2 : 0), y1: ht - m.b, label: ref.label, top: true })
      const g = svg.append('g').selectAll('g').data(data).join('g')
      g.append('text').attr('class', 'fx-lab').attr('x', m.l - 8).attr('y', (r) => y(r.label) + y.bandwidth() / 2).attr('dy', '0.35em').attr('text-anchor', 'end').text((r) => fit(r.label, m.l - 10, 12))
      g.append('rect').attr('class', 'fx-mark').attr('x', (r) => x(Math.min(0, r.value))).attr('y', (r) => y(r.label)).attr('height', y.bandwidth()).attr('width', (r) => Math.abs(x(r.value) - x(0))).attr('rx', 1)
        .style('fill', (r) => paint(r.color || color)).style('opacity', (r) => (selected === null || r.label === selected ? 1 : 0.35))
        .each(function (r) { pickable(this, r.label, onPick, `${r.label}: ${format(r.value)}`, r.label === selected) })
        .call(hover, lines)
      if (direct) g.append('text').attr('class', 'fx-num fx-halo').attr('x', (r) => x(Math.max(0, r.value)) + 6).attr('y', (r) => y(r.label) + y.bandwidth() / 2).attr('dy', '0.35em').text((r) => format(r.value))
    } else {
      const m = { t: direct ? 20 : 10, r: 8, b: 26, l: direct ? 4 : room([format(ext[0]), format(ext[1])], 11) }
      svg = svgIn(w, height, label)
      const x = d3.scaleBand(data.map((r) => r.label), [m.l, w - m.r]).padding(0.2)
      const y = d3.scaleLinear(ext, [height - m.b, m.t])
      if (!direct) yAxis(svg.append('g').attr('transform', `translate(${m.l},0)`), y, w - m.l - m.r, format)
      const shown = everyNth(x.domain(), Math.max(2, Math.floor((w - m.l) / 70)))
      xAxis(svg.append('g').attr('transform', `translate(0,${height - m.b})`), x, { values: shown, format: (l) => fit(l, Math.max(x.step() * Math.ceil(x.domain().length / shown.length) - 4, 30)) })
      svg.append('g').selectAll('rect').data(data).join('rect').attr('class', 'fx-mark').attr('x', (r) => x(r.label)).attr('width', x.bandwidth()).attr('y', (r) => y(Math.max(0, r.value))).attr('height', (r) => Math.abs(y(r.value) - y(0))).attr('rx', 1)
        .style('fill', (r) => paint(r.color || color)).style('opacity', (r) => (selected === null || r.label === selected ? 1 : 0.35))
        .each(function (r) { pickable(this, r.label, onPick, `${r.label}: ${format(r.value)}`, r.label === selected) })
        .call(hover, lines)
      if (direct && x.bandwidth() > 24) svg.append('g').selectAll('text').data(data).join('text').attr('class', 'fx-num').attr('x', (r) => x(r.label) + x.bandwidth() / 2).attr('y', (r) => y(Math.max(0, r.value)) - 4).attr('text-anchor', 'middle').text((r) => format(r.value))
      for (const ref of marks) refLine(svg, { y: y(ref.value), x0: m.l, x1: w - m.r, label: ref.label })
    }
    return [svg.node(), notes(noValue(dropped), unsaid(refs.filter((r) => !num(r.value))), repeats(data.map((r) => r.label)))]
  })
}

/** A dashed reference line with its label: horizontal at y, vertical at x. */
function refLine(svg, { x, y, x0, x1, y0, y1, label, color = '--text-muted', top = false }, boxes = []) {
  const right = Number(svg.attr('width')) || 600
  const g = svg.append('g').attr('class', 'fx-ref')
  if (y !== undefined) g.append('line').attr('x1', x0).attr('x2', x1).attr('y1', y).attr('y2', y)
  else g.append('line').attr('x1', x).attr('x2', x).attr('y1', y0).attr('y2', y1)
  g.select('line').style('stroke', paint(color))
  if (!label) return
  if (y !== undefined) { halo(g, x1 - 4, y - 5, fit(label, x1 - x0 - 10, 11.5), { cls: 'fx-note', anchor: 'end' }); return }
  // A vertical line's label goes on whichever side has room for it, the right first.
  const room = (x1 ?? right) - x - 10
  const left = room < textW(label, 11.5) && x - (x0 ?? 0) - 10 > room
  put(boxes, g, left ? x - 4 : x + 4, top ? y0 - 6 : y1 - 6, fit(label, (left ? x - (x0 ?? 0) : (x1 ?? right) - x) - 10, 11.5), { cls: 'fx-note', anchor: left ? 'end' : undefined })
}

/** The shaded selected period on a category axis, labelled at its foot. */
function periodBand(svg, x, from, to, top, bottom, text = 'Selected', boxes = []) {
  const half = x.step() / 2
  const a = x(from)
  const b = x(to)
  if (a === undefined || b === undefined) return
  const pad = Math.min(half, 8)
  const l = Math.max(x.range()[0] - pad, Math.min(a, b) - half)
  const r = Math.min(x.range()[1] + pad, Math.max(a, b) + half)
  svg.insert('rect', ':first-child').attr('class', 'fx-period').attr('x', l).attr('y', top).attr('width', r - l).attr('height', bottom - top)
  put(boxes, svg, l + 4, top + 12, fit(text.toUpperCase(), Math.max(r - l - 6, 60), 12), { cls: 'fx-caps' })
}

/** A brush across a category axis: onBrush(fromLabel, toLabel), or (null, null) when cleared. */
function brushAcross(svg, x, top, bottom, labels, band, onBrush) {
  const near = (px) => labels[Math.max(0, Math.min(labels.length - 1, Math.round((px - x.range()[0]) / x.step())))]
  const x0 = x.range()[0] - x.step() / 2
  // Never narrower than a pixel: a redraw at a passing width can otherwise give the overlay a negative width.
  const brush = d3.brushX().extent([[x0, top], [Math.max(x0 + 1, x.range()[1] + x.step() / 2), bottom]]).on('end', (e) => {
    if (!e.sourceEvent) return
    if (!e.selection) return onBrush(null, null)
    onBrush(near(e.selection[0]), near(e.selection[1]))
  })
  const g = svg.append('g').attr('class', 'fx-brush').call(brush)
  if (band && x(band.from) !== undefined && x(band.to) !== undefined) g.call(brush.move, [x(band.from) - x.step() / 2, x(band.to) + x.step() / 2])
  return g
}

const ISO_DAY = /^\d{4}-\d{2}(-\d{2})?/
/**
 * Calendar ticks for labels that are ISO dates (`2024-05-13`, `2024-05`): the first label of a year, every few years
 * to fit `max` ticks, shown as the year; within two years, the first label of each month, shown as "May 2024".
 * Null when the labels are not dates, so the caller keeps its own ticks. tickFormat(label) in opts overrides the text.
 */
function dateTicks(labels, max, tickFormat) {
  const uniq = [...new Set(labels)]
  if (uniq.length < 2 || !uniq.every((l) => ISO_DAY.test(String(l)))) return null
  const year = (l) => String(l).slice(0, 4)
  const month = (l) => String(l).slice(0, 7)
  const years = new Set(uniq.map(year)).size
  const key = years > 2 ? year : month
  const firsts = uniq.filter((l, i) => i === 0 || key(l) !== key(uniq[i - 1]))
  const step = Math.max(1, Math.ceil(firsts.length / Math.max(2, max)))
  const values = firsts.filter((_, i) => i % step === 0)
  const text = (l) => (years > 2 ? year(l) : new Date(`${month(l)}-01T00:00:00Z`).toLocaleDateString('en-US', { month: 'short', year: 'numeric', timeZone: 'UTC' }))
  return { values, format: tickFormat || text }
}


/**
 * Lines. labels: the x categories. series: [{ name, values, color (a token name), dashed, width }].
 * opts: format, endLabels (default: on when there are two or more series), band ({ from, to, label }: x labels),
 * refs ([{ y, label } | { x, label }]), beginAtZero (default true), points ([{ series, index, text }]: notes beside
 * points), corners ({ tl, tr, bl, br }), onPick(label), onBrush(fromLabel, toLabel) (drag to choose a period),
 * markers ([{ series, indexes, label, color }]: dots on chosen points, with a key), maxTicks, height, label, tickFormat(label) (axis text; ISO date labels get calendar ticks shown as years or months).
 */
export function lines(el, labels, series, opts = {}) {
  const { format = fmt, onPick, onBrush, maxTicks = 8, band, refs = [], beginAtZero = true, points = [], corners = {}, height = 280, label = 'Line chart', tickFormat, markers = [] } = opts
  const all = series.flatMap((s) => s.values.filter(num))
  if (!labels.length || !all.length) return nothing(el)
  const endLabels = opts.endLabels ?? series.length > 1
  mount(el, 'fx-lines', (w) => {
    const yr = refs.filter((r) => num(r.y))
    const xr = refs.filter((r) => r.y === undefined && r.x !== undefined)
    const span = [...all, ...yr.map((r) => r.y)]
    const lo = beginAtZero ? Math.min(0, d3.min(span)) : d3.min(span)
    const y0 = d3.scaleLinear([lo, d3.max(span)], [height - 26, 10]).nice()
    const m = { t: 10, b: 26, l: room(y0.ticks(5).map(format), 11), r: endLabels ? Math.min(room(series.map((s) => s.name)), Math.round(w / 3)) : 10 }
    const svg = svgIn(w, height, label)
    const fitTicks = Math.min(maxTicks, Math.floor((w - m.l - m.r) / 70))
    const dated = dateTicks(labels, fitTicks, tickFormat)
    const shown = dated ? dated.values : everyNth([...new Set(labels)], fitTicks)
    const x = d3.scalePoint(labels, insetPoints(labels, shown, m.l + 4, w - m.r - 4))
    const y = y0.range([height - m.b, m.t])
    yAxis(svg.append('g').attr('transform', `translate(${m.l},0)`), y, w - m.l - m.r, format)
    xAxis(svg.append('g').attr('transform', `translate(0,${height - m.b})`), x, { values: shown, ...(dated ? { format: dated.format } : tickFormat ? { format: tickFormat } : {}) })
    const boxes = []
    if (band) periodBand(svg, x, band.from, band.to, m.t, height - m.b, band.label, boxes)
    for (const ref of yr) refLine(svg, { y: y(ref.y), x0: m.l, x1: w - m.r, label: ref.label }, boxes)
    for (const ref of xr) if (x(ref.x) !== undefined) refLine(svg, { x: x(ref.x), x1: w - m.r, y0: m.t, y1: height - m.b, label: ref.label }, boxes)
    const few = labels.length < 24
    const line = d3.line().defined((d) => num(d[1])).x((d) => x(d[0])).y((d) => y(d[1])).curve(few ? d3.curveLinear : d3.curveMonotoneX)
    for (const s of series) {
      const pts = labels.map((l, i) => [l, s.values[i]])
      const g = svg.append('g').attr('class', 'fx-series')
      g.append('path').attr('d', line(pts)).style('fill', 'none').style('stroke', paint(s.color)).style('stroke-width', s.width || 2).style('stroke-dasharray', s.dashed ? '5 4' : null)
      if (few && s.dots !== false) g.selectAll('circle').data(pts.filter((p) => num(p[1]))).join('circle').attr('cx', (p) => x(p[0])).attr('cy', (p) => y(p[1])).attr('r', s.hollow ? 3 : 2.5).style('fill', s.hollow ? paint('--surface') : paint(s.color)).style('stroke', s.hollow ? paint(s.color) : null).style('stroke-width', s.hollow ? 1.5 : null)
    }
    if (endLabels) {
      const ends = series.map((s) => { let k = s.values.length - 1; while (k >= 0 && !num(s.values[k])) k--; return k < 0 ? null : { s, y: y(s.values[k]) } }).filter(Boolean)
      const ys = dodge(ends.map((e) => e.y), 15, m.t + 6, height - m.b - 6)
      ends.forEach((e, i) => halo(svg, w - m.r + 8, ys[i], fit(e.s.name, m.r - 10, 12), { base: 'middle', cls: 'fx-end', fill: readable(paint(e.s.color)) }))
    }
    // Markers: dots on chosen points of a series (every Thanksgiving week), with a one-line key at the top left.
    markers.forEach((mk, n) => {
      const s = series[mk.series ?? 0]
      const at = (mk.indexes || []).filter((i) => num(s?.values[i]))
      svg.append('g').attr('class', 'fx-markers').selectAll('circle').data(at).join('circle')
        .attr('cx', (i) => x(labels[i])).attr('cy', (i) => y(s.values[i])).attr('r', mk.r || 3.5)
        .style('fill', paint(mk.color || s.color)).style('stroke', paint('--bg')).style('stroke-width', 1.5)
      if (mk.label) {
        const key = svg.append('g').attr('class', 'fx-key').attr('transform', `translate(${m.l + 10},${m.t + 12 + n * 16})`)
        key.append('circle').attr('r', mk.r || 3.5).style('fill', paint(mk.color || s.color)).style('stroke', paint('--bg')).style('stroke-width', 1.5)
        halo(key, 9, 0, mk.label, { base: 'middle', cls: 'fx-note' })
        boxes.push({ x0: m.l + 4, x1: m.l + 24 + textW(mk.label, 11.5), y0: m.t + 2 + n * 16, y1: m.t + 18 + n * 16 })
      }
    })
    for (const p of points) {
      const s = series[p.series ?? 0]
      const v = s?.values[p.index]
      if (!num(v)) continue
      const px = x(labels[p.index])
      const right = px < (m.l + w - m.r) / 2
      put(boxes, svg, right ? px + 6 : px - 6, y(v) - 18 < m.t ? y(v) + 16 : y(v) - 6, fit(p.text, right ? w - m.r - px - 10 : px - m.l - 10, 11.5), { anchor: right ? 'start' : 'end', cls: 'fx-note' })
    }
    cornerNotes(svg, corners, m, w, height, boxes)
    const rule = svg.append('line').attr('class', 'fx-rule').attr('y1', m.t).attr('y2', height - m.b).style('opacity', 0)
    const dots = svg.append('g').style('pointer-events', 'none')
    const nearest = (px) => labels[Math.max(0, Math.min(labels.length - 1, Math.round((px - x.range()[0]) / x.step())))]
    const surface = onBrush ? brushAcross(svg, x, m.t, height - m.b, labels, band, onBrush).select('.overlay') : svg.append('rect').attr('x', m.l).attr('y', m.t).attr('width', w - m.l - m.r).attr('height', height - m.t - m.b).style('fill', 'transparent')
    surface.on('mousemove.tip', (ev) => {
      const l = nearest(d3.pointer(ev, svg.node())[0])
      const i = labels.indexOf(l)
      rule.attr('x1', x(l)).attr('x2', x(l)).style('opacity', 1)
      dots.selectAll('circle').data(series.filter((s) => num(s.values[i]))).join('circle').attr('cx', x(l)).attr('cy', (s) => y(s.values[i])).attr('r', 4).style('fill', (s) => paint(s.color))
      tipShow(ev, [l, ...series.filter((s) => s.tip !== false).map((s) => `${s.name}: ${num(s.values[i]) ? format(s.values[i]) : num(series.find((t) => t.tip === false && t.color === s.color)?.values[i]) ? `${format(series.find((t) => t.tip === false && t.color === s.color).values[i])} (partial)` : 'no value'}`), onPick ? 'Click to choose this period.' : onBrush ? 'Drag to zoom to a period.' : ''])
    }).on('mouseleave.tip', () => { rule.style('opacity', 0); dots.selectAll('*').remove(); tipHide() })
    if (onPick && !onBrush) surface.style('cursor', 'pointer').on('click', (ev) => onPick(nearest(d3.pointer(ev, svg.node())[0])))
    const off = xr.filter((r) => x(r.x) === undefined).map((r) => r.label || r.x)
    return [svg.node(), notes(gaps(labels, series), unsaid(refs.filter((r) => !yr.includes(r) && !xr.includes(r))), off.length ? `Not on this axis, so not drawn: ${off.join(', ')}.` : '', offBand(x, band), repeats(labels))]
  })
}

function cornerNotes(svg, corners, m, w, ht, boxes = []) {
  for (const [at, text] of Object.entries(corners || {})) {
    if (!text) continue
    const left = at[1] === 'l'
    put(boxes, svg, left ? m.l + 8 : w - m.r - 8, at[0] === 't' ? m.t + 14 : ht - m.b - 8, fit(text, (w - m.l - m.r) / 2 - 16, 12), { anchor: left ? 'start' : 'end', cls: 'fx-corner' }, 12)
  }
}

/**
 * Stacked areas: what a total is made of over time, each part named inside its band at the right.
 * labels: the x categories. series: [{ name, values, color }], bottom first. opts: format, band, onPick(name),
 * onBrush(fromLabel, toLabel), height, label.
 */
export function area(el, labels, series, opts = {}) {
  const { format = fmt, onPick, onBrush, maxTicks = 8, band, height = 280, label = 'Stacked area chart', tickFormat } = opts
  if (!labels.length || !series.some((s) => s.values.some(num))) return nothing(el)
  mount(el, 'fx-area', (w) => {
    const rows = labels.map((l, i) => Object.fromEntries([['label', l], ...series.map((s) => [s.name, Math.max(0, num(s.values[i]) ? s.values[i] : 0)])]))
    const stack = d3.stack().keys(series.map((s) => s.name))(rows)
    const top = d3.max(stack.at(-1), (d) => d[1]) || 1
    const y = d3.scaleLinear([0, top], [height - 26, 10]).nice()
    const m = { t: 10, b: 26, l: room(y.ticks(5).map(format), 11), r: 10 }
    const svg = svgIn(w, height, label)
    const dated = dateTicks(labels, Math.min(maxTicks, Math.floor((w - m.l) / 70)), tickFormat)
    const shown = dated ? dated.values : everyNth([...new Set(labels)], Math.min(maxTicks, Math.floor((w - m.l) / 70)))
    const x = d3.scalePoint(labels, insetPoints(labels, shown, m.l, w - m.r))
    yAxis(svg.append('g').attr('transform', `translate(${m.l},0)`), y, w - m.l - m.r, format)
    xAxis(svg.append('g').attr('transform', `translate(0,${height - m.b})`), x, { values: shown, ...(dated ? { format: dated.format } : tickFormat ? { format: tickFormat } : {}) })
    const shape = d3.area().x((d) => x(d.data.label)).y0((d) => y(d[0])).y1((d) => y(d[1]))
    const paths = svg.append('g').selectAll('path').data(stack).join('path').attr('class', 'fx-mark').attr('d', shape).style('fill', (d, i) => paint(series[i].color)).style('fill-opacity', 0.85)
      .each(function (d, i) { pickable(this, series[i].name, onPick, series[i].name) })
    hover(paths, (d) => { const s = series[d.index]; const last = s.values.findLast?.(num) ?? s.values.filter(num).at(-1); return [s.name, `Latest: ${format(last)}`, onPick ? 'Click to choose this part.' : ''] })
    for (const d of stack) {
      const end = d.at(-1)
      if (y(end[0]) - y(end[1]) < 16) continue
      const fill = paint(series[d.index].color)
      svg.append('text').attr('class', 'fx-inside').attr('x', w - m.r - 6).attr('y', (y(end[0]) + y(end[1])) / 2).attr('dy', '0.35em').attr('text-anchor', 'end').style('fill', inkOn(fill)).text(fit(series[d.index].name, w / 3, 12))
    }
    if (band) periodBand(svg, x, band.from, band.to, m.t, height - m.b, band.label)
    if (onBrush) brushAcross(svg, x, m.t, height - m.b, labels, band, onBrush)
    const below = d3.sum(series, (s) => s.values.filter((v) => num(v) && v < 0).length)
    return [svg.node(), key(series.filter((s, i) => y(stack[i].at(-1)[0]) - y(stack[i].at(-1)[1]) < 16).map((s) => ({ name: s.name, color: s.color }))), notes(gaps(labels, series), below ? `${below} negative values drawn as zero.` : '', offBand(x, band), repeats(labels))]
  })
}

/**
 * Stacked shares: each x category's parts as a share of its total. rows: [{ x, part, value }].
 * parts: [{ name, color }] in stack order. opts: selected (a part), onPick(part), height, label.
 */
export function stackedShare(el, rows, parts, opts = {}) {
  const { onPick, selected = null, height = 280, label = 'Stacked share chart' } = opts
  const xs = [...new Set(rows.map((r) => r.x))]
  if (!xs.length) return nothing(el)
  const get = (x, p) => rows.find((r) => r.x === x && r.part === p)?.value ?? 0
  const total = new Map(xs.map((x) => [x, d3.sum(parts, (p) => Math.max(0, get(x, p.name)))]))
  mount(el, 'fx-share', (w) => {
    const m = { t: 8, r: 8, b: 26, l: 40 }
    const svg = svgIn(w, height, label)
    const x = d3.scaleBand(xs, [m.l, w - m.r]).padding(0.2)
    const y = d3.scaleLinear([0, 1], [height - m.b, m.t])
    yAxis(svg.append('g').attr('transform', `translate(${m.l},0)`), y, w - m.l - m.r, (v) => pct(v, 0), 4)
    xAxis(svg.append('g').attr('transform', `translate(0,${height - m.b})`), x, { values: everyNth(xs, Math.floor((w - m.l) / 60)) })
    const stack = d3.stack().keys(parts.map((p) => p.name)).value((x0, k) => (total.get(x0) ? Math.max(0, get(x0, k)) / total.get(x0) : 0))(xs)
    stack.forEach((layer, i) => {
      const p = parts[i]
      svg.append('g').selectAll('rect').data(layer.map((d, j) => ({ d, x: xs[j] }))).join('rect').attr('class', 'fx-mark')
        .attr('x', (r) => x(r.x)).attr('width', x.bandwidth()).attr('y', (r) => y(r.d[1])).attr('height', (r) => Math.max(0, y(r.d[0]) - y(r.d[1])))
        .style('fill', paint(p.color)).style('opacity', selected === null || selected === p.name ? 1 : 0.35)
        .each(function (r) { pickable(this, p.name, onPick, `${p.name}, ${r.x}: ${pct(r.d[1] - r.d[0])}`, selected === p.name, xs.length * parts.length <= 60, `${p.name}|${r.x}`) })
        .call(hover, (r) => [`${p.name}, ${r.x}`, `${pct(r.d[1] - r.d[0])}, ${fmt(get(r.x, p.name))}`, `Total ${fmt(total.get(r.x))}`])
    })
    const off = new Set(rows.filter((r) => !parts.some((p) => p.name === r.part)).map((r) => r.part))
    return [svg.node(), key(parts), notes(off.size ? `Not drawn, not among the parts: ${[...off].join(', ')}.` : '')]
  })
}

/**
 * Then against now: one point per item, its earlier value on x and its later value on y, on the same scale, with
 * the diagonal where nothing changed. points: [{ id, label, x, y }]. opts: xTitle, yTitle, format, labelTop (how
 * many of the biggest movers to name, default 6; the two largest are always named), name (ids always named),
 * selected (id), corners ({ tl, br } text, such as '↖ more now'), rise and fall (the token names a rise and a fall
 * are coloured with: neutral by default; '--down' for the good direction and '--up' for the bad one), onPick(id).
 */
export function scatterChange(el, points, opts = {}) {
  const { xTitle = 'Then', yTitle = 'Now', format = fmt, labelTop = 6, name = [], selected = null, corners = {}, rise = '--text-soft', fall = '--text-soft', onPick, label = 'Then against now' } = opts
  const pts = points.filter((p) => num(p.x) && num(p.y))
  if (!pts.length) return nothing(el)
  const movers = [...pts].sort((a, b) => Math.abs(b.y - b.x) - Math.abs(a.y - a.x)).slice(0, labelTop).map((p) => p.id)
  const largest = [...pts].sort((a, b) => b.y - a.y).slice(0, 2).map((p) => p.id)
  const named = new Set([...largest, ...movers, ...name, selected].filter((v) => v !== null && v !== undefined))
  const color = (p) => paint(p.id === selected ? '--accent' : p.y > p.x ? rise : p.y < p.x ? fall : '--text-soft')
  mount(el, 'fx-scatter', (w) => {
    const ht = Math.min(w, 460)
    const ext = zeroSpan(pts.flatMap((p) => [p.x, p.y]))
    const m = { t: 12, r: 14, b: 40, l: room(ext.map(format), 11) + 16 }
    const svg = svgIn(w, ht, label)
    const x = d3.scaleLinear(ext, [m.l, w - m.r])
    const y = d3.scaleLinear(ext, [ht - m.b, m.t])
    yAxis(svg.append('g').attr('transform', `translate(${m.l},0)`), y, w - m.l - m.r, format)
    xAxis(svg.append('g').attr('transform', `translate(0,${ht - m.b + 4})`), x, { format, values: x.ticks(Math.max(2, Math.floor(w / 110))) })
    svg.append('text').attr('class', 'fx-axis-title').attr('x', w - m.r).attr('y', ht - 4).attr('text-anchor', 'end').text(xTitle)
    svg.append('text').attr('class', 'fx-axis-title').attr('transform', `translate(12,${m.t}) rotate(-90)`).attr('text-anchor', 'end').text(yTitle)
    const d0 = Math.max(ext[0], ext[0])
    const d1 = ext[1]
    svg.append('line').attr('class', 'fx-diagonal').attr('x1', x(d0)).attr('y1', y(d0)).attr('x2', x(d1)).attr('y2', y(d1))
    halo(svg, x(d1) - 4, y(d1) + 14, 'No change', { anchor: 'end', cls: 'fx-note' })
    const dots = svg.append('g').selectAll('circle').data([...pts].sort((a, b) => (a.id === selected) - (b.id === selected))).join('circle').attr('class', 'fx-mark fx-pt')
      .attr('cx', (p) => x(p.x)).attr('cy', (p) => y(p.y)).attr('r', (p) => (p.id === selected ? 7 : named.has(p.id) ? 5 : 4)).style('fill', color).style('fill-opacity', (p) => (p.id === selected ? 1 : 0.75))
      .each(function (p) { pickable(this, p.id, onPick, `${p.label}: ${format(p.x)} then, ${format(p.y)} now`, p.id === selected, pts.length <= 60) })
    hover(dots, (p) => [p.label, `${format(p.x)} → ${format(p.y)}`])
    const avoid = pts.map((p) => ({ x0: x(p.x) - 4, x1: x(p.x) + 4, y0: y(p.y) - 4, y1: y(p.y) + 4 }))
    const cands = pts.filter((p) => named.has(p.id)).map((p) => ({ text: p.label, force: p.id === selected, positions: [{ x: x(p.x) + 7, y: y(p.y) + 4, anchor: 'start' }, { x: x(p.x) - 7, y: y(p.y) + 4, anchor: 'end' }, { x: x(p.x), y: y(p.y) - 8 }] }))
    for (const l of placeLabels(cands, { bounds: [m.l, w - m.r], top: m.t, avoid })) halo(svg, l.x, l.y, l.text, { anchor: l.anchor || 'middle', cls: 'fx-pin' })
    cornerNotes(svg, { tl: corners.tl, br: corners.br }, m, w, ht)
    return [svg.node(), notes(noValue(points.length - pts.length))]
  })
}

/**
 * With and without: for each row, a dot for each of two values on one line, joined, and the difference written
 * at the right. rows: [{ label, a, b, id }]. opts: aName, bName, aColor, bColor (token names), format,
 * diff(row) -> text, diffLabel (a heading over that column), selected (id or label), onPick(id or label), label.
 */
export function dumbbell(el, rows, opts = {}) {
  const { aName = 'Without', bName = 'With', aColor = '--text-soft', bColor = '--accent', format = fmt, diff, diffLabel, selected = null, onPick, label = 'Dumbbell chart' } = opts
  const both = (r) => num(r.a) && num(r.b)
  const data = rows.filter((r) => num(r.a) || num(r.b))
  if (!data.length) return nothing(el)
  const say = (r) => (!both(r) ? '' : diff ? diff(r) : `${r.b > r.a ? '+' : r.b < r.a ? '−' : ''}${format(Math.abs(r.b - r.a))}`)
  mount(el, 'fx-dumbbell', (w) => {
    const step = 26
    const m = { t: 30, b: 22, l: Math.min(room(data.map((r) => r.label)), w * 0.36), r: Math.min(Math.max(room(data.map(say)), diffLabel ? room([diffLabel], 11) : 0), w / 4) }
    const ht = m.t + step * data.length + m.b
    const svg = svgIn(w, ht, label)
    const x = d3.scaleLinear(zeroSpan(data.flatMap((r) => [r.a, r.b])), [m.l, w - m.r])
    svg.append('g').attr('class', 'fx-ax').attr('transform', `translate(0,${ht - m.b})`).call(d3.axisBottom(x).ticks(Math.max(2, Math.floor(w / 120))).tickSize(-(ht - m.t - m.b)).tickFormat(format)).call((a) => a.select('.domain').remove())
    const id = (r) => r.id ?? r.label
    const g = svg.append('g').selectAll('g').data(data).join('g').attr('transform', (r, i) => `translate(0,${m.t + i * step + step / 2})`)
    g.append('rect').attr('class', 'fx-row').attr('x', 0).attr('y', -step / 2).attr('width', w).attr('height', step).classed('is-on', (r) => id(r) === selected)
      .each(function (r) { pickable(this, id(r), onPick, `${r.label}: ${aName} ${format(r.a)}, ${bName} ${format(r.b)}`, id(r) === selected) })
    hover(g.select('rect'), (r) => [r.label, `${aName}: ${format(r.a)}`, `${bName}: ${format(r.b)}`, say(r)])
    g.append('text').attr('class', 'fx-lab').attr('x', m.l - 10).attr('dy', '0.35em').attr('text-anchor', 'end').text((r) => fit(r.label, m.l - 12, 12))
    g.filter(both).append('line').attr('class', 'fx-join').style('pointer-events', 'none').attr('x1', (r) => x(r.a)).attr('x2', (r) => x(r.b))
    g.filter((r) => num(r.a)).append('circle').attr('cx', (r) => x(r.a)).attr('r', 5).style('fill', paint(aColor))
    g.filter((r) => num(r.b)).append('circle').attr('cx', (r) => x(r.b)).attr('r', 5.5).style('fill', paint(bColor))
    g.append('text').attr('class', 'fx-num').attr('x', w - m.r + 8).attr('dy', '0.35em').text(say)
    let lx = m.l
    for (const [c, t] of [[aColor, aName], [bColor, bName]]) {
      svg.append('circle').attr('cx', lx + 5).attr('cy', 12).attr('r', 5).style('fill', paint(c))
      svg.append('text').attr('class', 'fx-lab').attr('x', lx + 14).attr('y', 12).attr('dy', '0.35em').text(t)
      lx += 30 + textW(t, 12)
    }
    // What the right-hand column is: a heading over it, so a reader is not left to guess.
    if (diffLabel) svg.append('text').attr('class', 'fx-lab fx-diff-head').attr('x', w - m.r + 8).attr('y', 12).attr('dy', '0.35em').text(fit(diffLabel, m.r - 8, 11))
    return svg.node()
  })
}

/** A sparkline into an <svg>: values, numbers in order, as a line in color (a token name) with its last point marked.
 * opts: band ([i0, i1] shades the selected period). */
export function sparkline(svg, values, color = '--accent', opts = {}) {
  const w = svg.clientWidth || Number(svg.getAttribute('width')) || 160
  const ht = svg.clientHeight || Number(svg.getAttribute('height')) || 36
  const s = d3.select(svg).attr('viewBox', `0 0 ${w} ${ht}`)
  s.selectAll('*').remove()
  const pts = values.map((v, i) => [i, v]).filter(([, v]) => num(v))
  if (pts.length < 2) return
  const x = d3.scaleLinear([0, values.length - 1], [2, w - 2])
  const y = d3.scaleLinear([Math.min(0, d3.min(pts, (p) => p[1])), Math.max(0, d3.max(pts, (p) => p[1]))], [ht - 3, 3])
  if (opts.band) s.append('rect').attr('x', x(opts.band[0]) - 1).attr('y', 0).attr('width', Math.max(2, x(opts.band[1]) - x(opts.band[0]) + 2)).attr('height', ht).style('fill', token('--period'))
  s.append('path').attr('d', d3.area().x((p) => x(p[0])).y0(y(0)).y1((p) => y(p[1]))(pts)).style('fill', paint(color)).style('opacity', 0.12)
  s.append('path').attr('d', d3.line().x((p) => x(p[0])).y((p) => y(p[1]))(pts)).style('fill', 'none').style('stroke', paint(color)).style('stroke-width', 1.5)
  s.append('circle').attr('cx', x(pts.at(-1)[0])).attr('cy', y(pts.at(-1)[1])).attr('r', 2.5).style('fill', paint(color))
}

/**
 * A heatmap: rows by columns, each cell's colour from the sequential ramp by the square root of its value, so one
 * large cell does not wash out the rest. rowsOrder, colsOrder: the row and column names, in order. cells: [{ row, col,
 * value }]. opts: format, selected ({ row, col }), onPick(row, col), onRow(row), label.
 */
export function heatmap(el, rowsOrder, colsOrder, cells, opts = {}) {
  const { format = fmt, onPick, onRow, selected = null, label = 'Heatmap' } = opts
  if (!rowsOrder.length || !colsOrder.length) return nothing(el)
  const at = new Map(cells.map((c) => [`${c.row}|${c.col}`, c.value]))
  const max = d3.max(cells, (c) => (num(c.value) ? c.value : 0)) || 1
  const seq = ['--seq-0', '--seq-1', '--seq-2', '--seq-3', '--seq-4', '--seq-5'].map(token)
  const color = d3.scaleSequentialSqrt([0, max], d3.interpolateRgbBasis(seq))
  mount(el, 'fx-heat', (w) => {
    const m = { t: 20, l: Math.min(room(rowsOrder.map(String), 11), w * 0.3), r: 2 }
    const cw = (w - m.l - m.r) / colsOrder.length
    const ch = Math.max(14, Math.min(cw * 0.75, 34))
    const ht = m.t + ch * rowsOrder.length + 4
    const svg = svgIn(w, ht, label)
    // Label every nth column, n wide enough that the longest label clears its neighbour.
    const every = Math.max(Math.ceil(colsOrder.length / Math.max(1, Math.floor((w - m.l) / 28))), Math.ceil((d3.max(colsOrder, (c) => String(c).length) * 6.6 + 10) / cw))
    colsOrder.forEach((c, i) => i % every === 0 && svg.append('text').attr('class', 'fx-lab fx-small').attr('x', m.l + i * cw + cw / 2).attr('y', 12).attr('text-anchor', 'middle').text(fit(c, cw * every - 2, 11)))
    rowsOrder.forEach((r, j) => {
      const t = svg.append('text').attr('class', 'fx-lab fx-small').attr('x', m.l - 6).attr('y', m.t + j * ch + ch / 2).attr('dy', '0.35em').attr('text-anchor', 'end').text(fit(r, m.l - 8, 11))
      if (onRow) pickable(t.node(), r, onRow, `${r}: every column`)
    })
    const data = rowsOrder.flatMap((r, j) => colsOrder.map((c, i) => ({ r, c, i, j, v: at.get(`${r}|${c}`) })))
    const keyboard = data.length <= 200
    const sel = (d) => selected && String(selected.row) === String(d.r) && String(selected.col) === String(d.c)
    const rects = svg.append('g').selectAll('rect').data(data).join('rect').attr('class', (d) => `fx-hc${sel(d) ? ' is-on' : ''}`)
      .attr('x', (d) => m.l + d.i * cw + 1).attr('y', (d) => m.t + d.j * ch + 1).attr('width', Math.max(1, cw - 2)).attr('height', ch - 2).attr('rx', 1)
      .style('fill', (d) => (num(d.v) ? color(Math.max(0, d.v)) : noData()))
      .each(function (d) { if (num(d.v)) pickable(this, [d.r, d.c], onPick && (([r, c]) => onPick(r, c)), `${d.r}, ${d.c}: ${format(d.v)}`, sel(d), keyboard, `${d.r}|${d.c}`) })
    hover(rects, (d) => [`${d.r}, ${d.c}`, num(d.v) ? format(d.v) : 'No data'])
    const off = cells.filter((c) => !rowsOrder.includes(c.row) || !colsOrder.includes(c.col)).length
    return [svg.node(), notes(off ? `${off} cells outside these rows and columns left out.` : '')]
  })
}

// ---- The ranked table ----

/**
 * A ranked table. rows: [{ id, name, sub, value, trend: [numbers], change }], already sorted.
 * opts: valueLabel, format, trendLabel, changeLabel, changeFormat, good ('rise' | 'fall' | null: which way of the
 * change is good news; null keeps it neutral), limit (rows shown before "Show all", default 10), band ([i0, i1] of
 * the trend to shade), selected (id), onOpen(id) (a row's click: on a view with filters, filter by it), href(id) (the
 * name becomes a link, to open the entity's page), inCompare(id) and onCompare(id) (both or neither).
 */
/** The + last pressed on a ranked list. A page that redraws its cards on a pick replaces the table, so its new
 *  drawing takes the focus back, as restoreFocus does for a chart's marks. */
let rtRefocus = null
export function rankTable(el, rows, opts = {}) {
  const { valueLabel = 'Value', format = fmt, trendLabel = 'Trend', changeLabel = 'Change', changeFormat = (v) => pct(v, 0), good = null, limit = 10, band, selected = null, onOpen, href, inCompare, onCompare } = opts
  const hasTrend = rows.some((r) => r.trend?.length)
  const hasChange = rows.some((r) => num(r.change))
  const max = Math.max(...rows.map((r) => r.value || 0)) || 1
  const maxChange = Math.max(...rows.map((r) => (num(r.change) ? Math.abs(r.change) : 0))) || 1
  const toneOf = (c) => (good === null || !c ? '--text-muted' : (c > 0) === (good === 'rise') ? '--down' : '--up')
  let all = el.__rtAll || false
  const render = () => {
    const shown = all ? rows : rows.slice(0, limit)
    const head = h('tr', {}, h('th', { class: 'rt__rank', text: '#' }), h('th', { text: 'Name' }), h('th', { class: 'rt__bar', text: valueLabel }), hasTrend ? h('th', { class: 'rt__spark', text: trendLabel }) : null, hasChange ? h('th', { class: 'rt__change', text: changeLabel }) : null, onCompare ? h('th', { class: 'rt__act' }, h('span', { class: 'visually-hidden', text: 'Compare' })) : null)
    const body = h('tbody')
    shown.forEach((r, i) => {
      const tr = h('tr', { class: r.id === selected ? 'is-selected' : '', 'data-id': r.id })
      tr.addEventListener('mouseenter', () => linkOn(String(r.id)))
      tr.addEventListener('mouseleave', linkOff)
      if (onOpen) {
        tr.tabIndex = 0
        tr.addEventListener('click', () => onOpen(r.id))
        tr.addEventListener('keydown', (e) => e.target === tr && e.key === 'Enter' && onOpen(r.id))
      }
      const fill = h('i', { style: { width: `calc((100% - 5.5em) * ${Math.max(0, r.value || 0) / max})` } })
      tr.append(h('td', { class: 'rt__rank', text: String(i + 1) }), h('td', { class: 'rt__name' }, href ? h('a', { href: href(r.id), text: r.name, onclick: (e) => e.stopPropagation() }) : r.name, r.sub ? h('small', { text: r.sub }) : null), h('td', { class: 'rt__bar' }, h('div', {}, fill, h('span', { text: format(r.value) }))))
      if (hasTrend) {
        const svg = document.createElementNS('http://www.w3.org/2000/svg', 'svg')
        svg.setAttribute('width', '120')
        svg.setAttribute('height', '26')
        svg.setAttribute('aria-hidden', 'true')
        tr.append(h('td', { class: 'rt__spark' }, svg))
        requestAnimationFrame(() => sparkline(svg, r.trend || [], '--c-bar', { band }))
      }
      if (hasChange) {
        const td = h('td', { class: 'rt__change' })
        if (num(r.change)) {
          const wd = (Math.abs(r.change) / maxChange) * 50
          td.append(h('div', { class: 'rt__cbar' }, h('i', { style: { width: `${wd}%`, left: r.change >= 0 ? '50%' : `${50 - wd}%`, background: token(toneOf(r.change)) } })), h('span', { text: `${r.change > 0 ? '+' : r.change < 0 ? '−' : ''}${changeFormat(Math.abs(r.change))}`, style: { color: token(toneOf(r.change)) } }))
        }
        tr.append(td)
      }
      if (onCompare) {
        const on = Boolean(inCompare?.(r.id))
        const btn = h('button', { type: 'button', 'aria-pressed': String(on), 'aria-label': `${on ? 'Remove' : 'Add'} ${r.name} ${on ? 'from' : 'to'} compare`, text: on ? '✓' : '+' })
        btn.addEventListener('click', (e) => {
          e.stopPropagation()
          rtRefocus = { id: String(r.id), at: Date.now() }
          onCompare(r.id)
          render()
        })
        tr.append(h('td', { class: 'rt__act' }, btn))
      }
      body.append(tr)
    })
    const kids = [h('div', { class: 'rt-wrap' }, h('table', { class: 'rt' }, h('thead', {}, head), body))]
    if (rows.length > limit) {
      const more = h('button', { type: 'button', class: 'btn rt-more', text: all ? `Show the top ${limit}` : `Show all ${fmt(rows.length)}` })
      more.addEventListener('click', () => {
        all = el.__rtAll = !all
        render()
        el.querySelector('.rt-more')?.focus()
      })
      kids.push(more)
    }
    el.replaceChildren(...kids)
    const now = document.activeElement
    if (rtRefocus && Date.now() - rtRefocus.at < 4000 && (!now || now === document.body || el.contains(now) || !now.isConnected)) {
      el.querySelector(`tr[data-id="${CSS.escape(rtRefocus.id)}"] .rt__act button`)?.focus({ preventScroll: true })
    }
  }
  render()
}

// ---- Maps ----

const files = new Map()
/** Fetch a boundary file once per session. path: relative to the page, such as './vendor/maps/states-albers-10m.json'. */
export function loadTopo(path) {
  if (!files.has(path)) {
    const p = fetch(path).then((r) => {
      if (!r.ok) throw new Error(`${path}: HTTP ${r.status}`)
      return r.json()
    })
    p.catch(() => files.delete(path))
    files.set(path, p)
  }
  return files.get(path)
}

/**
 * A choropleth. topo: a loaded TopoJSON file. values: Map of key -> number.
 * opts: object (the TopoJSON object to fill: 'states', 'counties', 'countries'), borders (an object drawn as a mesh
 * over the fill, such as 'states' over counties), key (shape -> key; default its name), world (true for longitude and
 * latitude files), format (hover), legendFormat (the legend's class edges; compact by default), legendTitle,
 * selected (key), onPick(key), linkOf(key) (the name other charts use for the shape, for linked hover), tip(key,
 * value) -> extra hover lines, label.
 */
export function choropleth(el, topo, values, opts = {}) {
  const { object, borders, world = false, format = fmt, legendFormat, legendTitle = '', selected = null, onPick, tip, label = 'Map' } = opts
  const keyOf = opts.key || (object === 'counties' ? (f) => f.id : (f) => f.properties.name)
  const { features, missing } = shapesOf(topo, object, keyOf, values)
  const c = classes([...values.values()])
  mount(el, 'fx-choropleth', () => {
    const [w, ht] = world ? [960, 500] : [975, 610]
    const path = d3.geoPath(world ? d3.geoEqualEarth().fitSize([w, ht], { type: 'Sphere' }) : null)
    const svg = d3.create('svg').attr('viewBox', `0 0 ${w} ${ht}`).attr('role', 'img').attr('aria-label', label)
    const shapes = svg.append('g').selectAll('path').data(features).join('path').attr('d', path)
      .attr('class', (f) => `area${keyOf(f) === selected ? ' is-selected' : ''}`)
      .style('fill', (f) => (num(values.get(keyOf(f))) ? c.at(values.get(keyOf(f))) : noData()))
      .each(function (f) { const k = keyOf(f); if (num(values.get(k))) { pickable(this, k, onPick, `${k}: ${format(values.get(k))}`, k === selected, object !== 'counties'); if (opts.linkOf && onPick) this.setAttribute('data-link', opts.linkOf(k)) } })
    hover(shapes, (f) => { const k = keyOf(f); const v = values.get(k); return [k, num(v) ? format(v) : 'No data', ...(tip ? [tip(k, v)].flat() : [])] })
    if (borders) svg.append('path').attr('class', 'mesh').attr('d', path(topojson.mesh(topo, topo.objects[borders], (a, b) => a !== b)))
    const on = features.find((f) => keyOf(f) === selected)
    if (on) svg.append('path').attr('class', 'area-ring').attr('d', path(on)).attr('aria-hidden', 'true')
    el.classList.add('map')
    return [svg.node(), legend(c, legendTitle, [], legendFormat), notes(notOnMap(missing))]
  })
}

/** Hexagons of about `r` px over points projected to x, y: [{ x, y, n }] -> [{ x, y, n }]. */
function hexbin(points, r) {
  const dx = r * 2 * Math.sin(Math.PI / 3)
  const dy = r * 1.5
  const bins = new Map()
  for (const p of points) {
    const py = p.y / dy
    let pj = Math.round(py)
    const px = p.x / dx - (pj & 1) / 2
    let pi = Math.round(px)
    const py1 = py - pj
    if (Math.abs(py1) * 3 > 1) {
      const px1 = px - pi
      const pi2 = pi + (px < pi ? -1 : 1) / 2
      const pj2 = pj + (py < pj ? -1 : 1)
      const px2 = px - pi2
      const py2 = py - pj2
      if (px1 * px1 + py1 * py1 > px2 * px2 + py2 * py2) { pi = pi2 + (pj & 1 ? 1 : -1) / 2; pj = pj2 }
    }
    const k = `${pi},${pj}`
    let bin = bins.get(k)
    if (!bin) bins.set(k, (bin = { x: (pi + (pj & 1) / 2) * dx, y: pj * dy, n: 0 }))
    bin.n += p.n
  }
  return [...bins.values()]
}
const hexPath = (r) => `M${d3.range(6).map((i) => { const a = (i * Math.PI) / 3; return `${(Math.sin(a) * r).toFixed(2)},${(-Math.cos(a) * r).toFixed(2)}` }).join('L')}Z`

/**
 * A map of points binned into hexagons, for places with coordinates and no boundary file. points: [{ lat, lon,
 * value }] (a count per location). opts: base (the same for every location, unfiltered, drawn grey underneath), area
 * ('south,west,north,east' of the selected hexagon), onPick(area), format, height, legendTitle, label.
 * Colours are capped at the 98th percentile, so one heavy spot does not wash out the rest; the legend says so.
 * Hexagons are clicked, not tabbed to: put a ranked table beside the map.
 */
export function hexMap(el, points, opts = {}) {
  const { base = points, area: picked = null, onPick, format = fmt, height = 520, legendTitle = '', label = 'Map' } = opts
  // On the globe, and not 0,0, where a failed geocode lands.
  const ok = (p) => num(p.lat) && num(p.lon) && Math.abs(p.lat) <= 85 && Math.abs(p.lon) <= 180 && (p.lat || p.lon)
  const located = points.filter(ok)
  if (!located.length) return nothing(el)
  mount(el, 'fx-hexmap', (w) => {
    const at = (base.some(ok) ? base : located).filter(ok).map((p) => [p.lon, p.lat])
    // One place has no extent to fit; a small box around it does.
    if (d3.max(at, (c) => c[0]) - d3.min(at, (c) => c[0]) + d3.max(at, (c) => c[1]) - d3.min(at, (c) => c[1]) < 1e-6) at.push([at[0][0] - 0.05, at[0][1] - 0.05], [at[0][0] + 0.05, at[0][1] + 0.05])
    const projection = d3.geoMercator().fitExtent([[10, 10], [w - 10, height - 10]], { type: 'MultiPoint', coordinates: at })
    const r = Math.max(3, w / 200)
    const bin = (ps) => hexbin(ps.filter(ok).map((p) => { const [x, y] = projection([p.lon, p.lat]); return { x, y, n: num(p.value) ? p.value : 0 } }), r)
    const svg = svgIn(w, height, label)
    const g = svg.append('g')
    g.append('g').selectAll('path').data(bin(base)).join('path').attr('class', 'fx-hex-base').attr('d', hexPath(r * 0.92)).attr('transform', (d) => `translate(${d.x},${d.y})`)
    const bins = bin(located)
    const max = d3.max(bins, (d) => d.n) || 1
    const cap = Math.max(1, d3.quantile(bins.map((d) => d.n).sort(d3.ascending), 0.98) || max)
    const color = d3.scaleSequentialSqrt([0, cap], d3.interpolateRgbBasis(['--seq-0', ...['--seq-1', '--seq-2', '--seq-3', '--seq-4', '--seq-5']].map(token))).clamp(true)
    const box = (d) => { const [wl, n] = projection.invert([d.x - r, d.y - r]); const [e, s] = projection.invert([d.x + r, d.y + r]); return [s, wl, n, e].map((v) => v.toFixed(4)).join(',') }
    const hexes = g.append('g').selectAll('path').data(bins).join('path').attr('class', 'fx-hex').attr('d', hexPath(r * 0.96)).attr('transform', (d) => `translate(${d.x},${d.y})`).style('fill', (d) => color(d.n))
      .each(function (d) { pickable(this, box(d), onPick, null, false, false) })
    hover(hexes, (d) => { const [lon, lat] = projection.invert([d.x, d.y]); return [format(d.n), `near ${lat.toFixed(3)}, ${lon.toFixed(3)}`, onPick ? 'Click to choose this spot.' : ''] })
    if (picked) {
      const [s, wl, n, e] = picked.split(',').map(Number)
      const [x0, y0] = projection([wl, n])
      const [x1, y1] = projection([e, s])
      if ([x0, y0, x1, y1].every(num)) g.append('rect').attr('class', 'fx-hex-box').attr('x', x0 - 3).attr('y', y0 - 3).attr('width', x1 - x0 + 6).attr('height', y1 - y0 + 6)
    }
    const zoom = d3.zoom().scaleExtent([1, 14]).translateExtent([[0, 0], [w, height]]).filter((ev) => ev.type !== 'wheel' || ev.ctrlKey || ev.metaKey).on('zoom', (ev) => g.attr('transform', ev.transform))
    svg.call(zoom)
    const tools = h('div', { class: 'fx-map-tools' })
    for (const [name, text, act] of [['Zoom in', '+', () => svg.transition().call(zoom.scaleBy, 1.6)], ['Zoom out', '−', () => svg.transition().call(zoom.scaleBy, 1 / 1.6)], ['Whole map', '⤢', () => svg.transition().call(zoom.transform, d3.zoomIdentity)]]) {
      const b = h('button', { class: 'btn', type: 'button', 'aria-label': name, text })
      b.addEventListener('click', act)
      tools.append(b)
    }
    const c = { colors: d3.range(5).map((i) => color((cap * (i + 1)) / 5)), ranges: d3.range(5).map((i) => [(cap * i) / 5, (cap * (i + 1)) / 5]) }
    return [h('div', { class: 'fx-map-wrap' }, svg.node(), tools), legend(c, legendTitle, [], (v) => compact.format(Math.round(v))), notes(max > cap ? `Colours stop at ${format(Math.round(cap))}; the busiest hexagon holds ${format(max)}.` : '', noValue(points.length - located.length).replace('with no value', 'with no location'))]
  })
}

// ---- Rings, flows and treemaps ----

/**
 * A radial heatmap, such as a week clock: rings (inner to outer) by slots around the circle. cells: [{ ring, slot,
 * value }], slot an integer from 0. opts: rings (names, inner first), slots (how many, default 24), slotLabel(i),
 * every (label every n-th slot, default 3), format, selected ({ ring, slot }), onPick(ring, slot), onRing(ring),
 * centre (text in the middle; default the busiest cell), label.
 */
export function clock(el, cells, opts = {}) {
  const { rings = [...new Set(cells.map((c) => c.ring))], slots = 24, slotLabel = (i) => String(i), every = 3, format = fmt, selected = null, onPick, onRing, label = 'Radial heatmap' } = opts
  const data = cells.filter((c) => num(c.value) && rings.includes(c.ring) && Number.isInteger(c.slot) && c.slot >= 0 && c.slot < slots)
  if (!data.length) return nothing(el)
  mount(el, 'fx-clock', (w) => {
    const size = Math.min(w, 480)
    const R = size / 2 - 30
    const r0 = R * 0.3
    const ring = (R - r0) / rings.length
    const gap = 0.36
    const ang = (i) => gap / 2 + (i / slots) * (2 * Math.PI - gap)
    const svg = svgIn(size, size, label).attr('viewBox', `${-size / 2} ${-size / 2} ${size} ${size}`)
    const color = d3.scaleSequential(d3.extent(data, (c) => c.value), d3.interpolateRgbBasis(['--seq-0', '--seq-1', '--seq-2', '--seq-3', '--seq-4', '--seq-5'].map(token)))
    const arc = d3.arc().padAngle(0.01).padRadius(R)
    const total = d3.sum(data, (c) => c.value)
    const on = (c) => selected && selected.ring === c.ring && Number(selected.slot) === c.slot
    const marks = svg.append('g').selectAll('path').data(data).join('path').attr('class', (c) => `fx-hc${on(c) ? ' is-on' : ''}`)
      .attr('d', (c) => { const k = rings.indexOf(c.ring); return arc({ innerRadius: r0 + k * ring + 0.5, outerRadius: r0 + (k + 1) * ring, startAngle: ang(c.slot), endAngle: ang(c.slot + 1) }) })
      .style('fill', (c) => color(c.value))
      .each(function (c) { pickable(this, [c.ring, c.slot], onPick && (([a, b]) => onPick(a, b)), null, false, false) })
    hover(marks, (c) => [`${c.ring}, ${slotLabel(c.slot)}`, `${format(c.value)}, ${pct(c.value / total)} of all`, onPick ? 'Click to choose it.' : ''])
    for (let i = 0; i < slots; i += every) {
      const a = ang(i)
      svg.append('text').attr('class', 'fx-lab fx-small').attr('x', Math.sin(a) * (R + 15)).attr('y', -Math.cos(a) * (R + 15)).attr('dy', '0.35em').attr('text-anchor', 'middle').text(slotLabel(i))
    }
    rings.forEach((name, k) => {
      const t = svg.append('text').attr('class', 'fx-lab fx-tiny').attr('y', -(r0 + k * ring + ring / 2)).attr('dy', '0.35em').attr('text-anchor', 'middle').text(fit(String(name).slice(0, 3).toUpperCase(), 30, 9))
      if (onRing) pickable(t.node(), name, onRing, `${name}: every slot`, selected?.ring === name && (selected.slot === null || selected.slot === undefined))
    })
    const peak = d3.greatest(data, (c) => c.value)
    svg.append('text').attr('class', 'fx-lab fx-small').attr('y', -6).attr('text-anchor', 'middle').text(opts.centre ? '' : 'Busiest')
    svg.append('text').attr('class', 'fx-centre').attr('y', 12).attr('text-anchor', 'middle').text(fit(opts.centre ?? `${String(peak.ring).slice(0, 3)} ${slotLabel(peak.slot)}`, r0 * 1.8, 14))
    const off = cells.length - data.length
    return [svg.node(), notes(off ? `${off} cells with no value, or outside the rings and slots, left out.` : '')]
  })
}

/**
 * A flow from one grouping to the next (a sankey): rows grouped by two or three stages, band widths by value.
 * rows: [{ ...one field per stage, value }]. opts: stages ([{ key: the row's field, title, color: (name) -> token
 * name }]), selected ({ [stage key]: name }),
 * onPick(stage key, name), format, height, label. Nodes are named buttons; bands show their share on hover.
 */
export function flow(el, rows, opts = {}) {
  const { stages, selected = {}, onPick, format = fmt, height = 420, label = 'Flow' } = opts
  const data = rows.filter((r) => num(r.value) && r.value > 0)
  if (!data.length || !stages?.length) return nothing(el)
  mount(el, 'fx-flow', (cw) => {
    const w = Math.max(cw, 760)
    const total = d3.sum(data, (r) => r.value)
    const nodeW = 10
    const pad = 8
    const top = 22
    const lw = Math.min(200, w * 0.24)
    const xs = stages.map((_, i) => (stages.length === 1 ? lw : lw + (i * (w - 2 * lw)) / (stages.length - 1)))
    const nodes = new Map()
    stages.forEach((st, si) => {
      const sums = d3.rollups(data, (v) => d3.sum(v, (r) => r.value), (r) => r[st.key] ?? 'Not given').sort((a, b) => b[1] - a[1])
      const k = (height - top - pad * (sums.length - 1)) / total
      let y = top
      for (const [name, value] of sums) { nodes.set(`${si}|${name}`, { si, st, name, value, x: xs[si], y, ht: Math.max(1, value * k), out: y, in: y, k }); y += value * k + pad }
    })
    const links = []
    for (let si = 0; si < stages.length - 1; si++) {
      for (const [a, ts] of d3.rollups(data, (v) => d3.sum(v, (r) => r.value), (r) => r[stages[si].key] ?? 'Not given', (r) => r[stages[si + 1].key] ?? 'Not given')) for (const [b, value] of ts) links.push({ s: nodes.get(`${si}|${a}`), t: nodes.get(`${si + 1}|${b}`), value })
    }
    links.sort((a, b) => a.s.y - b.s.y || a.t.y - b.t.y).forEach((l) => { l.w = l.value * l.s.k; l.y0 = l.s.out + l.w / 2; l.s.out += l.w })
    links.sort((a, b) => a.t.y - b.t.y || a.s.y - b.s.y).forEach((l) => { l.y1 = l.t.in + l.w / 2; l.t.in += l.w })
    const colorOf = (n) => paint(n.st.color ? n.st.color(n.name) : '--c-bar')
    const svg = svgIn(w, height, label)
    const bands = svg.append('g').selectAll('path').data(links).join('path').attr('class', 'fx-band')
      .attr('d', (l) => { const x0 = l.s.x + nodeW; const x1 = l.t.x; const xm = (x0 + x1) / 2; return `M${x0},${l.y0}C${xm},${l.y0} ${xm},${l.y1} ${x1},${l.y1}` })
      .style('stroke', (l) => colorOf(l.s.si === 0 ? l.s : l.t)).style('stroke-width', (l) => Math.max(1, l.w))
    const light = (test) => bands.classed('is-lit', (l) => !!test && test(l)).classed('is-dim', (l) => !!test && !test(l))
    bands.on('mouseenter.lit', (ev, l) => light((x) => x === l)).on('mouseleave.lit', () => light(null))
    hover(bands, (l) => [`${l.s.name} → ${l.t.name}`, `${format(l.value)}, ${pct(l.value / l.s.value)} of ${l.s.name}`])
    const ng = svg.append('g').selectAll('g').data([...nodes.values()]).join('g')
    ng.append('rect').attr('class', (n) => `fx-node${selected[n.st.key] === n.name ? ' is-on' : ''}`).attr('x', (n) => n.x).attr('y', (n) => n.y).attr('width', nodeW).attr('height', (n) => n.ht).style('fill', colorOf)
      .each(function (n) { pickable(this, n, onPick && ((x) => onPick(x.st.key, selected[x.st.key] === x.name ? null : x.name)), `${n.st.title || n.st.key}: ${n.name}, ${format(n.value)}`, selected[n.st.key] === n.name, true, `${n.si}|${n.name}`) })
      .on('mouseenter.lit', (ev, n) => light((l) => l.s === n || l.t === n)).on('mouseleave.lit', () => light(null))
    hover(ng.select('rect'), (n) => [n.name, `${format(n.value)}, ${pct(n.value / total)} of all`, onPick ? 'Click to filter to it.' : ''])
    ng.filter((n) => n.ht > 9).each(function (n) {
      const g = d3.select(this)
      const left = n.si === 0
      const tx = left ? n.x - 8 : n.x + nodeW + 6
      const space = left ? lw - 12 : n.si === stages.length - 1 ? w - tx - 4 : (xs[1] - xs[0]) / 2 - 16
      halo(g, tx, n.y + n.ht / 2 + (n.ht > 24 ? -2 : 4), fit(n.name, space, 12), { anchor: left ? 'end' : 'start', cls: 'fx-node-name' })
      if (n.ht > 24) halo(g, tx, n.y + n.ht / 2 + 12, `${format(n.value)} · ${pct(n.value / total, 0)}`, { anchor: left ? 'end' : 'start', cls: 'fx-small' })
    })
    const room1 = stages.length > 1 ? (xs[1] - xs[0]) / 2 : lw
    stages.forEach((st, i) => svg.append('text').attr('class', 'fx-caps').attr('x', i === 0 ? xs[i] + nodeW : i === stages.length - 1 ? xs[i] : xs[i] + nodeW / 2).attr('y', 11).attr('text-anchor', i === 0 ? 'end' : i === stages.length - 1 ? 'start' : 'middle').text(fit((st.title || st.key).toUpperCase(), i === 0 ? lw : i === stages.length - 1 ? w - xs[i] - 4 : room1, 12.5)))
    return [h('div', { class: 'fx-scroll' }, svg.node()), notes(rows.length > data.length ? `${rows.length - data.length} rows with no positive value left out.` : '')]
  })
}

/**
 * A treemap: parts of one whole, or many items in groups. items: [{ id, label, value, group, share, change,
 * color }]. opts: colorBy ('change': diverging by change, which way is good from `good`; 'color': each item's
 * token; default: the sequential ramp by value), good ('rise' | 'fall'), format, changeFormat, selected (id), onPick(id), onGroup(group), height, label.
 * A share is printed only when the model gives it.
 */
export function treemap(el, items, opts = {}) {
  const { colorBy = 'value', good = null, format = fmt, changeFormat = (v) => `${v > 0 ? '+' : v < 0 ? '−' : ''}${pct(Math.abs(v), 0)}`, selected = null, onPick, onGroup, height = 420, label = 'Treemap' } = opts
  const data = items.filter((d) => num(d.value) && d.value > 0)
  if (!data.length) return nothing(el)
  const grouped = data.some((d) => d.group !== undefined && d.group !== null)
  mount(el, 'fx-treemap', (w) => {
    const root = d3.hierarchy(grouped ? { children: d3.groups(data, (d) => d.group).map(([g, ds]) => ({ group: g, children: ds })) } : { children: data }).sum((d) => d.value || 0).sort((a, b) => b.value - a.value)
    d3.treemap().size([w, height]).paddingOuter(1).paddingTop((d) => (grouped && d.depth === 1 ? 20 : 1)).paddingInner(2).round(true)(root)
    const svg = svgIn(w, height, label)
    const changeAbs = d3.max(data, (d) => (num(d.change) ? Math.abs(d.change) : 0)) || 0.01
    const div = d3.scaleDiverging([-changeAbs, 0, changeAbs], d3.interpolateRgbBasis([token('--down'), token('--surface-2'), token('--up')]))
    const seq = d3.scaleSequentialSqrt([0, d3.max(data, (d) => d.value)], d3.interpolateRgbBasis(ramp()))
    const fill = (d) => (colorBy === 'change' ? (num(d.change) && good ? div(good === 'rise' ? -d.change : d.change) : noData()) : colorBy === 'color' ? paint(d.color || '--c-bar') : seq(d.value))
    if (grouped) {
      const gh = svg.append('g').selectAll('g').data(root.children).join('g').attr('transform', (d) => `translate(${d.x0},${d.y0})`)
      const t = gh.append('text').attr('class', 'fx-group').attr('x', 2).attr('y', 14).text((d) => fit(`${d.data.group} · ${format(d.value)}`, d.x1 - d.x0 - 4, 12))
      if (onGroup) t.each(function (d) { pickable(this, d.data.group, onGroup, `${d.data.group}: ${format(d.value)}`) })
    }
    const leaf = svg.append('g').selectAll('g').data(root.leaves()).join('g').attr('transform', (d) => `translate(${d.x0},${d.y0})`)
    leaf.append('rect').attr('class', (d) => `fx-tmap${d.data.id === selected ? ' is-on' : ''}`).attr('width', (d) => d.x1 - d.x0).attr('height', (d) => d.y1 - d.y0).style('fill', (d) => fill(d.data))
      .each(function (d) { pickable(this, d.data.id, onPick, `${d.data.label}: ${format(d.data.value)}`, d.data.id === selected, data.length <= 60) })
    hover(leaf.select('rect'), (d) => [d.data.label, format(d.data.value), num(d.data.share) ? `${pct(d.data.share)} of the whole` : '', num(d.data.change) ? `Change ${changeFormat(d.data.change)}` : ''])
    leaf.filter((d) => d.x1 - d.x0 > 50 && d.y1 - d.y0 > 30).each(function (d) {
      const g = d3.select(this)
      const ink = inkOn(fill(d.data))
      const wd = d.x1 - d.x0 - 10
      g.append('text').attr('class', 'fx-tm-name').attr('x', 5).attr('y', 15).style('fill', ink).text(fit(d.data.label, wd, 12))
      const sub = [format(d.data.value), num(d.data.share) ? pct(d.data.share, 0) : '', colorBy === 'change' && num(d.data.change) ? changeFormat(d.data.change) : ''].filter(Boolean).join(' · ')
      g.append('text').attr('class', 'fx-small').attr('x', 5).attr('y', 29).style('fill', ink).text(fit(sub, wd, 11))
    })
    const out = [svg.node()]
    if (colorBy === 'change') out.push(h('div', { class: 'map-legend' }, h('span', {}, h('i', { style: { background: token(good === 'fall' ? '--down' : '--up') } }), 'Fell'), h('span', {}, h('i', { style: { background: token(good === 'fall' ? '--up' : '--down') } }), 'Rose'), h('span', {}, h('i', { style: { background: noData() } }), 'No change shown')))
    out.push(notes(noValue(items.length - data.length)))
    return out
  })
}

const told = (c) => (typeof c === 'string' ? { text: c } : c || {})

const TILE_GRID = {
  AK: [0, 0], ME: [10, 0], VT: [9, 1], NH: [10, 1],
  WA: [0, 2], ID: [1, 2], MT: [2, 2], ND: [3, 2], MN: [4, 2], IL: [5, 2], WI: [6, 2], MI: [7, 2], NY: [8, 2], RI: [9, 2], MA: [10, 2],
  OR: [0, 3], NV: [1, 3], WY: [2, 3], SD: [3, 3], IA: [4, 3], IN: [5, 3], OH: [6, 3], PA: [7, 3], NJ: [8, 3], CT: [9, 3],
  CA: [0, 4], UT: [1, 4], CO: [2, 4], NE: [3, 4], MO: [4, 4], KY: [5, 4], WV: [6, 4], VA: [7, 4], MD: [8, 4], DE: [9, 4],
  AZ: [1, 5], NM: [2, 5], KS: [3, 5], AR: [4, 5], TN: [5, 5], NC: [6, 5], SC: [7, 5], DC: [8, 5],
  OK: [3, 6], LA: [4, 6], MS: [5, 6], AL: [6, 6], GA: [7, 6],
  HI: [0, 7], TX: [3, 7], FL: [8, 7],
}
export const ABBR = {
  Alabama: 'AL', Alaska: 'AK', Arizona: 'AZ', Arkansas: 'AR', California: 'CA', Colorado: 'CO', Connecticut: 'CT', Delaware: 'DE',
  'District of Columbia': 'DC', Florida: 'FL', Georgia: 'GA', Hawaii: 'HI', Idaho: 'ID', Illinois: 'IL', Indiana: 'IN', Iowa: 'IA',
  Kansas: 'KS', Kentucky: 'KY', Louisiana: 'LA', Maine: 'ME', Maryland: 'MD', Massachusetts: 'MA', Michigan: 'MI', Minnesota: 'MN',
  Mississippi: 'MS', Missouri: 'MO', Montana: 'MT', Nebraska: 'NE', Nevada: 'NV', 'New Hampshire': 'NH', 'New Jersey': 'NJ',
  'New Mexico': 'NM', 'New York': 'NY', 'North Carolina': 'NC', 'North Dakota': 'ND', Ohio: 'OH', Oklahoma: 'OK', Oregon: 'OR',
  Pennsylvania: 'PA', 'Rhode Island': 'RI', 'South Carolina': 'SC', 'South Dakota': 'SD', Tennessee: 'TN', Texas: 'TX', Utah: 'UT',
  Vermont: 'VT', Virginia: 'VA', Washington: 'WA', 'West Virginia': 'WV', Wisconsin: 'WI', Wyoming: 'WY',
}

/**
 * rows: [{ id (a US state's full name), value, trend: [numbers] }]; states not in rows are drawn as no data, and ids
 * that are not states are named under the map. opts: marked (ids that get a ring, such as plant states), markedLabel,
 * selected, onPick(id), tip(id, value), legendTitle, format (the tooltip), legendFormat (the tiles and the legend,
 * default compact: 1.2K), label. The legend's edges are the values of the places in each class.
 */
export function tileMap(el, rows, opts = {}) {
  const { marked = [], markedLabel = 'Marked', selected = null, onPick, tip, legendTitle = '', format = fmt, legendFormat = (v) => compact.format(v), label = 'Tile map' } = opts
  const byId = new Map(rows.map((r) => [r.id, r]))
  const c = classes(rows.map((r) => r.value))
  const missing = rows.map((r) => r.id).filter((id) => !(id in ABBR))
  const tiles = Object.entries(ABBR).map(([name, ab]) => {
    const { value: v, trend } = byId.get(name) || {}
    const fill = num(v) ? c.at(v) : noData()
    return { name, ab, v, has: num(v), fill, ink: inkOn(fill), tr: (trend || []).filter(num), at: TILE_GRID[ab] }
  })
  const say = (d) => (tip ? [tip(d.name, d.v)].flat() : [d.name, d.has ? format(d.v) : 'No data'])
  mount(el, 'fx-tiles', (w) => {
    const gap = 3
    const t = Math.max(18, Math.min(66, Math.floor((w - gap * 10) / 11)))
    const big = t >= 44
    const svg = svgIn(t * 11 + gap * 10, t * 8 + gap * 7, label)
    const g = svg.selectAll('g').data(tiles).join('g').attr('class', (d) => `fx-tile${d.name === selected ? ' is-selected' : ''}`).attr('transform', (d) => `translate(${d.at[0] * (t + gap)},${d.at[1] * (t + gap)})`)
    g.append('rect').attr('width', t).attr('height', t).attr('rx', 3).style('fill', (d) => d.fill)
    g.append('text').attr('class', 'fx-tile__ab').attr('x', 5).attr('y', 13).style('fill', (d) => d.ink).text((d) => d.ab)
    const has = g.filter((d) => d.has)
    if (t >= 52) has.append('text').attr('class', 'fx-tile__v').attr('x', 5).attr('y', 26).style('fill', (d) => d.ink).text((d) => fit(legendFormat(d.v), t - 8, 10))
    if (big) has.filter((d) => d.tr.length > 1).append('path').attr('class', 'fx-tile__spark').style('stroke', (d) => d.ink)
      .attr('d', (d) => d3.line((v, i) => 5 + (i / (d.tr.length - 1)) * (t - 10), d3.scaleLinear(d3.extent(d.tr), [t - 6, t * 0.72 - 6]))(d.tr))
    g.filter((d) => marked.includes(d.name)).append('circle').attr('class', 'fx-tile__mark').attr('cx', big ? t - 7 : t - 6).attr('cy', big ? 7 : t - 6).attr('r', big ? 3.5 : 3)
    has.each(function (d) { pickable(this, d.name, onPick, say(d).join(': '), d.name === selected) })
    hover(g, say)
    return [svg.node(), legend(c, legendTitle, marked.length ? [['fx-key-ring', markedLabel]] : [], legendFormat), leftOut(missing.length ? `Not US states, so not on this map: ${missing.join(', ')}.` : '')]
  })
}

/**
 * rows: [{ id, label, a, b }]. opts: aName, bName, format, emph (ids drawn in the accent), change(row) -> text, or
 * { text, cls } with cls 'fx-good' or 'fx-bad', written after the right label; onPick(id), selected, label.
 */
export function slope(el, rows, opts = {}) {
  const { aName = 'Before', bName = 'After', format = fmt, emph = [], change, onPick, selected = null, label = 'Slope chart' } = opts
  const ok = rows.filter((r) => num(r.a) && num(r.b))
  if (!ok.length) return nothing(el)
  const on = (r) => emph.includes(r.id) || r.id === selected
  const tell = (r) => `${format(r.a)} → ${format(r.b)}`
  let [lo, hi] = d3.extent(ok.flatMap((r) => [r.a, r.b]))
  if (hi === lo) { lo -= Math.abs(lo) * 0.1 || 1; hi += Math.abs(hi) * 0.1 || 1 }
  const order = onPick ? ok : [...ok].sort((a, b) => on(a) - on(b))
  mount(el, 'fx-slope', (w) => {
    const narrow = w < 520
    const ht = Math.max(220, ok.length * 30 + 40)
    const m = { t: 30, b: 14 }
    const x0 = narrow ? Math.max(64, d3.max(ok, (r) => textW(format(r.a))) + 14) : Math.min(230, w * 0.32)
    const x1 = narrow ? w - 150 : w - Math.min(260, w * 0.36)
    const y = d3.scaleLinear([lo - (hi - lo) * 0.05, hi], [ht - m.b, m.t])
    const svg = svgIn(w, ht, label)
    for (const [x, name, room] of [[x0, aName, Math.min(2 * x0, x1 - x0)], [x1, bName, Math.min(2 * (w - x1), x1 - x0)]]) {
      svg.append('text').attr('class', 'fx-head').attr('x', x).attr('y', 14).attr('text-anchor', 'middle').text(fit(name, room - 8, CAPS))
      svg.append('line').attr('class', 'fx-base').attr('x1', x).attr('x2', x).attr('y1', m.t - 6).attr('y2', ht - m.b)
    }
    const ly0 = dodge(ok.map((r) => y(r.a)), 14, m.t, ht - m.b)
    const ly1 = dodge(ok.map((r) => y(r.b)), 14, m.t, ht - m.b)
    const g = svg.append('g').selectAll('g').data(order).join('g').attr('class', (r) => `fx-slope__row${on(r) ? ' is-on' : ''}`)
    g.append('line').attr('x1', x0).attr('x2', x1).attr('y1', (r) => y(r.a)).attr('y2', (r) => y(r.b))
    g.append('circle').attr('cx', x0).attr('cy', (r) => y(r.a)).attr('r', 3.5)
    g.append('circle').attr('cx', x1).attr('cy', (r) => y(r.b)).attr('r', 3.5)
    g.each(function (r) {
      const i = ok.indexOf(r)
      const row = d3.select(this)
      const c = told(change?.(r))
      halo(row, x0 - 8, ly0[i], narrow ? format(r.a) : fit(`${r.label}  ${format(r.a)}`, x0 - 12), { anchor: 'end', base: 'middle' })
      const full = w - x1 - 12
      const extra = c.text && full - textW(`  ${c.text}`) > 40 ? textW(`  ${c.text}`) : 0
      const right = halo(row, x1 + 8, ly1[i], fit(`${format(r.b)}  ${r.label}`, full - extra), { anchor: 'start', base: 'middle' })
      if (extra) right.append('tspan').attr('class', c.cls).text(`  ${c.text}`)
      pickable(this, r.id, onPick, `${r.label}: ${tell(r)}`, r.id === selected)
    })
    hover(g, (r) => [r.label, tell(r)])
    return [svg.node(), notes(noValue(rows.length - ok.length))]
  })
}

/**
 * xs: x labels (years). series: [{ id, label, ranks: [rank or null per x] }], ranks from the model, 1 the top; a tie
 * draws both on one spot. opts: emph (ids in the accent), onPick(id), selected, label.
 */
export function bump(el, xs, series, opts = {}) {
  const { emph = [], onPick, selected = null, label = 'Rank chart' } = opts
  const rank = (r) => (num(r) && r >= 1 ? r : null)
  const all = series.map((sr) => ({ ...sr, ranks: xs.map((_, i) => rank(sr.ranks[i])) }))
  const ok = all.filter((sr) => sr.ranks.some(num))
  const unranked = all.filter((sr) => !sr.ranks.some(num)).map((sr) => sr.label)
  const bad = d3.sum(series, (sr) => sr.ranks.filter((r) => r !== null && r !== undefined && !rank(r)).length)
  if (!xs.length || !ok.length) return nothing(el)
  const depth = Math.min(d3.max(ok, (sr) => d3.max(sr.ranks)), Math.max(ok.length, 10) + 2)
  ok.forEach((sr, k) => Object.assign(sr, { k, first: sr.ranks.findIndex(num), last: sr.ranks.findLastIndex(num), on: emph.includes(sr.id) || sr.id === selected }))
  const order = onPick ? ok : [...ok].sort((a, b) => a.on - b.on)
  // Tied dots share a spot: only the top one prints a number.
  const top = new Map()
  for (const sr of order) sr.ranks.forEach((r, i) => num(r) && top.set(`${i}:${Math.min(r, depth)}`, sr))
  const dots = (sr) => sr.ranks.flatMap((r, i) => (num(r) ? [{ sr, r, i }] : []))
  const said = (sr) => [sr.label, `#${sr.ranks[sr.last]} in ${xs[sr.last]}`]
  mount(el, 'fx-bump', (w) => {
    const narrow = w < 520
    const rowH = narrow ? 24 : 28
    const ht = depth * rowH + 40
    const m = { t: 24, r: narrow ? 110 : 150, b: 16, l: narrow ? 22 : 120 }
    const svg = svgIn(w, ht, label)
    const x = d3.scalePoint(d3.range(xs.length), [m.l, w - m.r])
    const y = d3.scaleLinear([1, depth], [m.t + rowH / 2, m.t + (depth - 0.5) * rowH]).clamp(true)
    const step = x.step()
    const roomy = step >= 24
    const every = Math.ceil(36 / step)
    const last = xs.length - 1
    xAxis(svg.append('g'), x, { values: d3.range(xs.length).filter((i) => i === last || (!(i % every) && last - i >= every)), format: (i) => fit(xs[i], Math.min(every * step, 2 * (x(i) - 2), 2 * (w - x(i) - 2)) - 6, CAPS) })
    const firstY = dodge(ok.map((sr) => y(sr.ranks[sr.first])), 13, m.t, ht - m.b)
    const lastY = dodge(ok.map((sr) => y(sr.ranks[sr.last])), 13, m.t, ht - m.b)
    const line = d3.line((r, i) => x(i), y).defined(num).curve(d3.curveBumpX)
    const g = svg.append('g').selectAll('g').data(order).join('g').attr('class', (sr) => `fx-bump__row${sr.on ? ' is-on' : ''}`)
    hover(g.append('path').attr('d', (sr) => line(sr.ranks)), said)
    hover(g.selectAll('circle').data(dots).join('circle').attr('cx', (d) => x(d.i)).attr('cy', (d) => y(d.r)).attr('r', roomy ? (narrow ? 8 : 10) : 3.5), (d) => [d.sr.label, `${xs[d.i]}: #${d.r}`])
    if (roomy) {
      g.selectAll('text').data((sr) => dots(sr).filter((d) => top.get(`${d.i}:${Math.min(d.r, depth)}`) === sr)).join('text').attr('class', 'fx-bump__n').attr('pointer-events', 'none')
        .attr('x', (d) => x(d.i)).attr('y', (d) => y(d.r)).attr('text-anchor', 'middle').attr('dominant-baseline', 'central').text((d) => d.r)
    }
    const gapL = roomy ? 16 : 8
    g.each(function (sr) {
      const row = d3.select(this)
      if (!narrow) halo(row, x(sr.first) - gapL, firstY[sr.k], fit(sr.label, m.l - gapL - 4), { anchor: 'end', base: 'middle' })
      halo(row, x(sr.last) + gapL, lastY[sr.k], fit(sr.label, w - x(sr.last) - gapL - 4), { anchor: 'start', base: 'middle' })
      pickable(this, sr.id, onPick, said(sr).join(': '), sr.id === selected)
    })
    g.filter('[tabindex]').on('focus.tip', (ev, sr) => tipShow(ev, said(sr))).on('blur.tip', tipHide)
    const deep = ok.some((sr) => sr.ranks.some((r) => r > depth))
    return [svg.node(), notes(deep ? `Ranks below ${depth} are drawn on the bottom row.` : '', unranked.length ? `No rank in any period: ${unranked.join(', ')}.` : '', bad ? `${bad} rank${bad > 1 ? 's' : ''} below 1 or not a number left out.` : '')]
  })
}

/** Dot offsets off the axis, none overlapping; same xs, same offsets. */
function swarm(xs, r) {
  const order = d3.range(xs.length).sort((a, b) => xs[a] - xs[b] || a - b)
  const ys = []
  const d2 = (2 * r) ** 2
  let lo = 0
  order.forEach((i, k) => {
    while (xs[order[lo]] <= xs[i] - 2 * r) lo++
    const near = order.slice(lo, k)
    const cands = [0, ...near.flatMap((p) => { const off = Math.sqrt(Math.max(0, d2 - (xs[p] - xs[i]) ** 2)) + 0.01; return [ys[p] + off, ys[p] - off] })].sort((a, b) => Math.abs(a) - Math.abs(b))
    ys[i] = cands.find((c) => near.every((p) => (xs[p] - xs[i]) ** 2 + (ys[p] - c) ** 2 >= d2 - 0.01)) ?? 0
  })
  return ys
}

/**
 * points: [{ id, label, value }]. opts: log (log scale; values at or below zero are then left out, and the chart says
 * how many), ticks, format, axisTitle, emph (ids in the accent, labelled), labelIds (ids to label), onPick(id),
 * selected, label. Use it for a continuous value: a count that takes only a few values stacks into towers. Over 60
 * dots are clicked, not tabbed to: put a table beside it for the keyboard.
 */
export function beeswarm(el, points, opts = {}) {
  const { log = false, format = fmt, axisTitle = '', emph = [], labelIds = [], onPick, selected = null, label = 'Beeswarm' } = opts
  const pts = points.filter((p) => num(p.value) && (!log || p.value > 0)).map((p, k) => ({ ...p, k }))
  const missing = points.filter((p) => !num(p.value)).length
  const dropped = points.length - missing - pts.length
  if (!pts.length) return nothing(el)
  const [vMin, vMax] = d3.extent(pts, (p) => p.value)
  const a = Math.floor(Math.log10(vMin))
  const b = Math.max(Math.ceil(Math.log10(vMax)), a + 1)
  const bottom = log ? 10 ** a : vMin < 0 ? -niceTop(-vMin) : 0
  const top = log ? 10 ** b : vMax > 0 ? niceTop(vMax) : 0
  const dom = [bottom, top === bottom ? top + 1 : top]
  const ticks = opts.ticks || (log ? d3.range(a, b + 1).flatMap((k) => [1, 2, 5].map((n) => n * 10 ** k)).filter((t) => t <= top) : d3.ticks(...dom, 4))
  const on = (p) => emph.includes(p.id) || p.id === selected
  const order = onPick ? pts : [...pts].sort((a, b) => on(a) - on(b))
  mount(el, 'fx-swarm', (w) => {
    const x = (log ? d3.scaleLog : d3.scaleLinear)(dom, [16, w - 16])
    const cx = pts.map((p) => x(p.value))
    let r = w < 520 ? 3 : 3.6
    let ys = swarm(cx, r + 0.6)
    for (let k = 0; k < 2 && d3.max(ys, Math.abs) > 130; k++) { r *= 0.7; ys = swarm(cx, r + 0.4) }
    const spread = d3.max(ys, Math.abs)
    const mid = Math.max(70, spread + 40)
    const ht = mid * 2 + 44
    const svg = svgIn(w, ht, label)
    const anchor = (t) => (t === ticks[0] ? 'start' : t === ticks.at(-1) ? 'end' : 'middle')
    let right = -Infinity
    const shown = ticks.filter((t) => { const tw = textW(format(t)); const left = x(t) - tw * { start: 0, middle: 0.5, end: 1 }[anchor(t)]; return left >= right + 6 && (right = left + tw) })
    svg.append('g').selectAll('line').data(shown).join('line').attr('class', 'fx-grid').attr('x1', x).attr('x2', x).attr('y1', 12).attr('y2', ht - 36)
    xAxis(svg.append('g').attr('transform', `translate(0,${ht - 36})`), x, { values: shown, format })
    svg.selectAll('.tick text').attr('text-anchor', anchor)
    if (axisTitle) svg.append('text').attr('class', 'fx-head').attr('x', 16).attr('y', ht - 4).text(fit(axisTitle, w - 32, CAPS))
    const dots = svg.append('g').selectAll('circle').data(order).join('circle').attr('class', (p) => `fx-dot${on(p) ? ' is-on' : ''}`).attr('cx', (p) => cx[p.k]).attr('cy', (p) => mid + ys[p.k]).attr('r', r)
      .each(function (p) { pickable(this, p.id, onPick, `${p.label}: ${format(p.value)}`, p.id === selected, pts.length <= 60) })
    hover(dots, (p) => [p.label, format(p.value)])
    const up = mid - spread - 18
    const dn = mid + spread + 22
    const ps = ['middle', 'end', 'start'].flatMap((a) => [[up, a], [dn, a]])
    const want = pts.filter((p) => labelIds.includes(p.id) || emph.includes(p.id)).sort((a, b) => a.value - b.value)
    const cands = want.map((p, k) => ({ p, text: p.label, force: emph.includes(p.id), positions: (k % 2 ? [ps[1], ...ps] : ps).map(([ly, a]) => ({ x: cx[p.k], y: ly, anchor: a })) }))
    for (const c of placeLabels(cands, { bounds: [0, w], px: 11.5 })) {
      const above = c.y < mid
      svg.append('line').attr('class', 'fx-leader').attr('x1', c.x).attr('x2', c.x).attr('y1', mid + ys[c.p.k] + (above ? -r : r)).attr('y2', c.y + (above ? 4 : -10))
      halo(svg, c.x, c.y, c.text, { anchor: c.anchor, cls: c.force ? 'fx-note fx-accent' : 'fx-note' })
    }
    return [svg.node(), notes(dropped ? `${dropped} at or below zero left out: a log scale cannot place them.` : '', noValue(missing))]
  })
}

/**
 * rows: [{ id, label, a, b }]. opts: aName, bName, format, change(row) -> text or { text, cls }, onPick(id), selected,
 * height, label.
 */
export function arrows(el, rows, opts = {}) {
  const { aName = 'Before', bName = 'After', format = fmt, change, onPick, selected = null, height = 320, label = 'Arrow chart' } = opts
  const ok = rows.filter((r) => num(r.a) && num(r.b))
  if (!ok.length) return nothing(el)
  const tell = (r) => `${format(r.a)} → ${format(r.b)}`
  const [vMin, vMax] = d3.extent(ok.flatMap((r) => [r.a, r.b]))
  const step = vMax > vMin ? d3.tickStep(vMin, vMax, 4) : Math.abs(vMin) * 0.1 || 1
  const m = { t: 54, r: 8, b: 44 }
  const y = d3.scaleLinear([Math.floor(vMin / step) * step - step, Math.ceil(vMax / step) * step + (vMax === vMin ? step : 0)], [height - m.b, m.t])
  m.l = Math.max(44, ...y.ticks(5).map((t) => textW(format(t)) + 12))
  mount(el, 'fx-arrows', (w) => {
    const svg = svgIn(w, height, label)
    yAxis(svg.append('g').attr('transform', `translate(${m.l},0)`), y, w - m.l - m.r, format, 5)
    const x = d3.scaleBand(d3.range(ok.length), [m.l, w - m.r])
    const bw = x.bandwidth()
    const g = svg.append('g').selectAll('g').data(ok).join('g').attr('class', (r) => `fx-arrow${selected !== null && selected !== r.id ? ' is-dim' : ''}`).attr('transform', (r, i) => `translate(${x(i) + bw / 2},0)`)
    const dir = (r) => (r.b >= r.a ? 1 : -1)
    g.append('line').attr('class', 'fx-arrow__shaft').attr('y1', (r) => y(r.a)).attr('y2', (r) => y(r.b) + 7 * dir(r))
    g.append('path').attr('class', 'fx-arrow__head').attr('d', (r) => `M-6,${y(r.b) + 9 * dir(r)}L0,${y(r.b)}L6,${y(r.b) + 9 * dir(r)}Z`)
    g.append('circle').attr('class', 'fx-arrow__from').attr('cy', (r) => y(r.a)).attr('r', 4)
    g.each(function (r) {
      const row = d3.select(this)
      if (textW(format(r.b)) <= bw - 4) halo(row, 0, y(r.b) - 16 * dir(r) + 4, format(r.b), { anchor: 'middle', cls: 'fx-strong' })
      const c = told(change?.(r))
      if (c.text && bw >= 24) halo(row, 0, y(r.b) - 30 * dir(r) + 4, fit(c.text, bw - 2), { anchor: 'middle', cls: c.cls })
      const words = String(r.label).split(' ')
      const lines = textW(r.label, 12) > bw - 8 ? [words.splice(0, Math.ceil(words.length / 2)).join(' '), words.join(' ')] : [r.label]
      if (bw >= 24) lines.forEach((t, k) => row.append('text').attr('class', 'fx-cat').attr('y', height - 26 + k * 13).attr('text-anchor', 'middle').text(fit(t, bw - 8, 12)))
      pickable(this, r.id, onPick, `${r.label}: ${tell(r)}`, r.id === selected)
    })
    hover(g, (r) => [r.label, tell(r)])
    const key = svg.append('g').attr('transform', `translate(${m.l},12)`)
    key.append('circle').attr('class', 'fx-arrow__from').attr('cx', 4).attr('r', 4)
    const aKey = fit(aName, (w - m.l) / 2 - 40)
    const kx = 12 + textW(aKey) + 18
    key.append('text').attr('x', 12).attr('dominant-baseline', 'middle').text(aKey)
    key.append('path').attr('class', 'fx-arrow__head').attr('d', `M${kx},4L${kx + 6},-5L${kx + 12},4Z`)
    key.append('text').attr('x', kx + 18).attr('dominant-baseline', 'middle').text(fit(bName, w - m.l - kx - 24))
    return [svg.node(), notes(noValue(rows.length - ok.length))]
  })
}

/**
 * xs: x labels. series: [{ name, color (token), values: change per x, from the model; null for none }]. opts: nets
 * (each period's net change, from the model: printed above its column; without them the bar marks the net and no
 * number is printed), format (signed change), unit (in the tooltip, such as 'jobs'), onPick(name), selected, height,
 * label. A period with no values keeps its place on the axis.
 */
export function contrib(el, xs, series, opts = {}) {
  const { nets = [], format = (v) => `${v > 0 ? '+' : v < 0 ? '−' : ''}${compact.format(Math.abs(v))}`, unit = '', onPick, selected = null, height = 300, label = 'Contribution chart' } = opts
  const u = unit ? ` ${unit}` : ''
  const cols = xs.map((x, i) => {
    const c = { x, net: nets[i], up: 0, dn: 0 }
    c.segs = series.filter((sr) => num(sr.values[i])).map((sr) => {
      const v = sr.values[i]
      const from = v >= 0 ? c.up : c.dn
      c[v >= 0 ? 'up' : 'dn'] += v
      return { sr, v, c, from, to: from + v }
    })
    return c
  })
  const n = d3.sum(cols, (c) => c.segs.length)
  if (!n) return nothing(el)
  const empty = cols.filter((c) => !c.segs.length).length
  mount(el, 'fx-contrib', (w) => {
    const lo = Math.min(0, d3.min(cols, (c) => c.dn) * 1.25)
    const y = d3.scaleLinear([lo, Math.max(0, d3.max(cols, (c) => c.up) * 1.1) || +!lo], [height - 26, 22]).nice()
    const m = { r: 8, b: 26, l: Math.max(40, d3.max(y.ticks(5), (t) => textW(format(t))) + 12) }
    const x = d3.scaleBand(xs, [m.l, w - m.r])
    const band = x.step()
    const bw = Math.min(56, band * 0.56)
    const every = Math.ceil(40 / band)
    const svg = svgIn(w, height, label)
    yAxis(svg.append('g').attr('transform', `translate(${m.l},0)`), y, w - m.l - m.r, format)
    svg.append('line').attr('class', 'fx-base').attr('x1', m.l).attr('x2', w - m.r).attr('y1', y(0)).attr('y2', y(0))
    inside(svg.append('g').attr('transform', `translate(0,${height - m.b})`).call(xAxis, x, { values: xs.filter((_, i) => (i % every === 0 && xs.length - 1 - i >= every) || i === xs.length - 1), format: (t) => fit(t, band * every - 4) }), w)
    const g = svg.append('g').selectAll('g').data(cols).join('g').attr('transform', (c) => `translate(${x(c.x) + band / 2},0)`)
    g.selectAll('rect').data((c) => c.segs).join('rect').attr('class', (d) => `fx-seg${selected && selected !== d.sr.name ? ' is-dim' : ''}`)
      .attr('x', -bw / 2).attr('width', bw).attr('y', (d) => y(Math.max(d.from, d.to))).attr('height', (d) => Math.max(0.5, Math.abs(y(d.from) - y(d.to)))).style('fill', (d) => paint(d.sr.color))
      .each(function (d) { pickable(this, d.sr.name, onPick, `${d.sr.name}, ${d.c.x}: ${format(d.v)}${u}`, d.sr.name === selected, n <= 60, `${d.sr.name}|${d.c.x}`) })
      .call(hover, (d) => [`${d.sr.name}, ${d.c.x}`, format(d.v) + u, num(d.c.net) ? `Net ${format(d.c.net)}${u}` : ''])
    g.filter((c) => c.segs.length).append('line').attr('class', 'fx-net').attr('x1', -bw / 2 - 4).attr('x2', bw / 2 + 4).attr('transform', (c) => `translate(0,${y(c.up + c.dn)})`)
    const net = (c) => (w < 520 ? format(c.net) : `${format(c.net)} net`)
    g.filter((c) => num(c.net) && textW(net(c)) < band - 4).each(function (c) { halo(d3.select(this), 0, y(c.up) - 7, net(c), { anchor: 'middle', cls: 'fx-strong' }) })
    return [Object.assign(key(series), { className: 'legend fx-key' }), svg.node(), notes(empty ? `${empty} period${empty > 1 ? 's have' : ' has'} no values.` : '')]
  })
}

/** Pull a bottom axis's end labels inside [2, w - 2]. */
function inside(ax, w) {
  ax.selectAll('.tick text').each(function () {
    const cx = this.parentNode.transform.baseVal.consolidate().matrix.e
    const half = textW(this.textContent) / 2
    const end = cx + half > w - 2
    if (end || cx - half < 2) d3.select(this).attr('text-anchor', end ? 'end' : 'start').attr('x', end ? w - 2 - cx : 2 - cx)
  })
}

/**
 * parts: [{ name, color (token), share (0..1, from the model), value }]. When the shares make a whole, the squares
 * add to exactly 100; when they do not (a top five), the rest are drawn as "Everything else", with no number. The
 * percentages written are the model's shares, rounded. opts: format, unit (after the value, such as 'jobs'),
 * restLabel, onPick(name), selected.
 */
export function waffle(el, parts, opts = {}) {
  const { format = fmt, unit = '', restLabel = 'Everything else', onPick, selected = null } = opts
  const ok = parts.filter((p) => num(p.share) && p.share > 0)
  if (!ok.length) return nothing(el)
  const skipped = parts.filter((p) => !ok.includes(p)).map((p) => p.name)
  const total = d3.sum(ok, (p) => p.share)
  const raw = ok.map((p) => (p.share / Math.max(total, 1)) * 100)
  const n = raw.map(Math.floor)
  let left = Math.round(Math.abs(total - 1) < 0.02 ? 100 : Math.min(100, total * 100)) - d3.sum(n)
  d3.sort(raw.keys(), (i) => n[i] - raw[i]).forEach((i) => { if (left > 0) { n[i]++; left-- } })
  const rest = 100 - d3.sum(n)
  const cells = ok.flatMap((p, i) => Array(n[i]).fill(p)).concat(Array(rest).fill(null))
  const sh = (p) => pct(p.share, 0)
  const dim = (p) => (selected && selected !== p.name ? 'is-dim' : null)
  const said = (p) => (num(p.value) ? format(p.value) + (unit && ` ${unit}`) : '')
  mount(el, 'fx-waffle', () => {
    const grid = d3.create('div').attr('class', 'fx-waffle__grid').attr('role', 'img').attr('aria-label', ok.map((p) => `${p.name} ${sh(p)}`).join(', '))
    grid.selectAll('i').data(cells).join('i').attr('class', (p) => (p ? dim(p) : 'is-rest')).style('background', (p) => (p ? paint(p.color) : null))
      .call(hover, (p) => (p ? [p.name, sh(p), said(p)] : [restLabel]))
    const keys = h('ul', { class: 'fx-waffle__keys' }, ...ok.map((p) => pickable(h('li', { class: dim(p) },
      h('i', { style: { background: paint(p.color) } }),
      h('span', { class: 'fx-waffle__pct', text: sh(p) }),
      h('span', { text: p.name }, h('small', { text: said(p) }))), p.name, onPick, `${p.name}, ${sh(p)}`, p.name === selected)))
    if (rest) keys.append(h('li', {}, h('i', { class: 'is-rest' }), h('span', { class: 'fx-waffle__pct' }), h('span', { text: restLabel })))
    return [grid.node(), keys, notes(skipped.length ? `Left out, with no share above zero: ${skipped.join(', ')}.` : '', total > 1.005 ? 'The shares add to more than the whole, so the squares are scaled to fit.' : '')]
  })
}

/** panels: [{ title, xs, values, accent }]. opts: format(v) for the end values. A panel with no numbers says so. */
export function multiples(el, panels, opts = {}) {
  const { format = fmt } = opts
  if (!panels.length) return nothing(el)
  claim(el, 'fx-multiples')
  el.replaceChildren(...panels.map((p) => {
    const pts = p.values.map((v, i) => [i, v]).filter(([, v]) => num(v))
    const box = h('figure', { class: `fx-mult${p.accent ? ' is-accent' : ''}` })
    if (!pts.length) {
      box.append(h('figcaption', {}, h('span', { class: 'caps', text: p.title }), h('small', { text: 'No data' })))
      return box
    }
    const [first, last] = [pts[0], pts.at(-1)]
    const holder = h('div')
    box.append(h('figcaption', {}, h('span', { class: 'caps', text: p.title }), h('b', { text: format(last[1]) }), h('small', { text: `from ${format(first[1])} in ${p.xs[first[0]]}` })), holder)
    requestAnimationFrame(() => mount(holder, 'fx-mult__chart', (w) => {
      const ht = 64
      const svg = svgIn(w, ht, `${p.title}, ${p.xs[first[0]]} to ${p.xs[last[0]]}`)
      const x = d3.scaleLinear([0, p.values.length - 1], [2, w - 2])
      const [lo, hi] = d3.extent([0, ...pts.map((d) => d[1])])
      const y = d3.scaleLinear([lo, hi > lo ? hi : lo + 1], [ht - 2, 6])
      svg.append('path').attr('class', 'fx-mult__area').attr('d', d3.area().x((d) => x(d[0])).y0(y(0)).y1((d) => y(d[1]))(pts))
      svg.append('path').attr('class', 'fx-mult__line').attr('d', d3.line().x((d) => x(d[0])).y((d) => y(d[1]))(pts))
      svg.append('circle').attr('class', 'fx-mult__dot').attr('r', 2.8).attr('cx', x(last[0])).attr('cy', y(last[1]))
      svg.append('rect').attr('width', w).attr('height', ht).style('fill', 'transparent').on('mouseleave.tip', tipHide).on('mousemove.tip', (ev) => {
        const [i, v] = d3.least(pts, (d) => Math.abs(x(d[0]) - d3.pointer(ev)[0]))
        tipShow(ev, [p.title, `${p.xs[i]}: ${format(v)}`])
      })
      return svg.node()
    }))
    return box
  }))
}

/**
 * pts: [{ x, y, label }] in time order. opts: xTitle, yTitle, xFmt, yFmt, labelIf(point, i) -> bool (the ends are
 * labelled first, when there is room), label, tip(point) -> text. An axis starts at zero only when its values come
 * near it. A period with no value breaks the path there.
 */
export function connectedScatter(el, pts, opts = {}) {
  const { xTitle = '', yTitle = '', xFmt = fmt, yFmt = fmt, labelIf = () => false, label = 'Connected scatter', tip = (p) => p.label } = opts
  const has = (p) => p && num(p.x) && num(p.y)
  const ok = pts.filter(has)
  if (!ok.length) return nothing(el)
  const missing = pts.length - ok.length
  const span = (vs) => {
    const [lo, hi] = d3.extent(vs)
    const pad = (hi - lo) * 0.08 || Math.abs(hi) * 0.1 || 1
    return [lo >= 0 && lo < hi / 3 ? 0 : lo - pad, hi <= 0 && hi > lo / 3 ? 0 : hi + pad]
  }
  mount(el, 'fx-cscatter', (w) => {
    const ht = Math.round(Math.max(300, Math.min(440, w * 0.5)))
    const y = d3.scaleLinear(span(ok.map((p) => p.y)), [ht - 42, 26])
    const m = { l: Math.max(40, d3.max(y.ticks(5), (t) => textW(yFmt(t))) + 12), r: 18, t: 26, b: 42 }
    const x = d3.scaleLinear(span(ok.map((p) => p.x)), [m.l, w - m.r])
    const svg = svgIn(w, ht, label)
    yAxis(svg.append('g').attr('transform', `translate(${m.l},0)`), y, w - m.l - m.r, yFmt)
    let xt = []
    for (let k = w < 500 ? 4 : 7; k > 0; k--) if ((xt = x.ticks(k)).length < 2 || x(xt[1]) - x(xt[0]) > d3.max(xt, (t) => textW(xFmt(t))) + 24) break
    inside(svg.append('g').attr('transform', `translate(0,${ht - m.b})`).call(xAxis, x, { format: xFmt, values: xt }), w)
    svg.append('line').attr('class', 'fx-axis').attr('x1', m.l).attr('x2', w - m.r).attr('y1', ht - m.b).attr('y2', ht - m.b)
    if (xTitle) halo(svg, w - m.r, ht - m.b + 32, fit(xTitle, w - m.l - m.r), { anchor: 'end', cls: 'fx-axis-title' })
    if (yTitle) halo(svg, m.l - 6, m.t - 8, fit(yTitle, w - m.l), { cls: 'fx-axis-title' })
    svg.append('path').attr('class', 'fx-cs__path').attr('d', d3.line().defined(has).x((p) => x(p.x)).y((p) => y(p.y))(pts))
    const end = ok.length - 1
    svg.append('g').selectAll('circle').data(ok).join('circle').attr('class', (p, i) => (i ? 'fx-cs__dot' : 'fx-cs__start'))
      .attr('cx', (p) => x(p.x)).attr('cy', (p) => y(p.y)).attr('r', (p, i) => (i === end ? 5 : i ? 2.2 : 4))
      .call(hover, (p) => [tip(p), (xTitle && `${xTitle}: `) + xFmt(p.x), (yTitle && `${yTitle}: `) + yFmt(p.y)])
    const dots = ok.map((p) => ({ x0: x(p.x) - 3, x1: x(p.x) + 3, y0: y(p.y) - 3, y1: y(p.y) + 3 }))
    const cands = ok.map((p, i) => ({ p, i })).filter(({ p, i }) => i === 0 || i === end || labelIf(p, i))
      .sort((a, b) => (b.i === end) - (a.i === end) || (b.i === 0) - (a.i === 0))
      .map(({ p, i }) => {
        const [px, py] = [x(p.x), y(p.y)]
        return { text: p.label, strong: i === 0 || i === end, own: dots[i], positions: [[8, 4, 'start'], [-8, 4, 'end'], [0, -9, 'middle'], [0, 17, 'middle']].map(([dx, dy, anchor]) => ({ x: px + dx, y: py + dy, anchor })) }
      })
    for (const c of placeLabels(cands, { bounds: [m.l, w - m.r], avoid: dots })) halo(svg, c.x, c.y, c.text, { anchor: c.anchor, cls: `fx-label${c.strong ? ' fx-label--strong' : ''}` })
    return [svg.node(), notes(missing ? `${missing} period${missing > 1 ? 's' : ''} with no value: the path breaks there.` : '')]
  })
}

/**
 * rows: [{ id, label, value, strong }], in the order to draw, values from the model; the first 40 are drawn. opts:
 * format (default: a fraction as a signed percent, 0.12 as +12%), onPick(id), selected, label, good ('rise' | 'fall' |
 * null, the default: which side is good news; null keeps both neutral), rowH (row height, default 20). A value too wide
 * for a phone is left to the row's tooltip.
 */
export function divergingBars(el, rows, opts = {}) {
  const { format = (v) => `${v > 0 ? '+' : v < 0 ? '−' : ''}${pct(Math.abs(v), 0)}`, onPick, selected = null, label = 'Change', good = null, rowH = 20 } = opts
  const valued = rows.filter((r) => num(r.value))
  const ok = valued.slice(0, 40)
  if (!ok.length) return nothing(el)
  mount(el, 'fx-dbars', (w) => {
    const ht = ok.length * rowH + 22
    const [lo, hi] = d3.extent([0, ...ok.map((r) => r.value)])
    const room = Math.min(w * 0.2, d3.max(ok, (r) => textW(format(r.value), 12)) + 10)
    const labW = Math.max(40, Math.min(w * 0.42, d3.max(ok, (r) => textW(r.label, 12) * (r.strong ? 1.08 : 1)) + 14, w - 2 * room - w / 3))
    const x = d3.scaleLinear([lo, hi], [labW + (lo < 0 ? room : 4), w - (hi > 0 ? room : 4)])
    const svg = svgIn(w, ht, label)
    const g = svg.append('g').selectAll('g').data(ok).join('g').attr('class', (r) => `fx-row${r.id === selected ? ' is-selected' : ''}`).attr('transform', (r, i) => `translate(0,${4 + i * rowH + rowH / 2})`)
      .each(function (r) { pickable(this, r.id, onPick, `${r.label}: ${format(r.value)}`, r.id === selected) })
      .call(hover, (r) => [r.label, format(r.value)])
    g.append('rect').attr('class', 'fx-rowbg').attr('y', -rowH / 2).attr('width', w).attr('height', rowH)
    g.append('text').attr('class', (r) => `fx-name${r.strong ? ' fx-name--strong' : ''}`).attr('x', labW - 8).attr('y', 4).attr('text-anchor', 'end').text((r) => fit(r.label, labW - 10, 12))
    g.append('rect').attr('x', (r) => x(Math.min(0, r.value))).attr('y', -6).attr('width', (r) => Math.max(1, Math.abs(x(r.value) - x(0)))).attr('height', 12).attr('rx', 1).style('fill', (r) => token(toneFill(r.value, good)))
    g.filter((r) => textW(format(r.value), 12) <= room - 6).append('text').attr('class', 'fx-value').attr('y', 4)
      .attr('x', (r) => x(r.value) + (r.value >= 0 ? 5 : -5)).attr('text-anchor', (r) => (r.value >= 0 ? 'start' : 'end')).text((r) => format(r.value))
    svg.append('line').attr('class', 'fx-zero').attr('x1', x(0)).attr('x2', x(0)).attr('y2', ht - 16)
    const nc = textW('no change') / 2
    svg.append('text').attr('class', 'fx-tick').attr('x', Math.min(Math.max(x(0), nc + 2), w - nc - 2)).attr('y', ht - 4).attr('text-anchor', 'middle').text('no change')
    return [svg.node(), notes(valued.length > 40 ? `The first 40 of ${valued.length} are drawn.` : '', noValue(rows.length - valued.length))]
  })
}

/** Postal codes, two letters at twice the state FIPS id (the feature id). */
const POSTAL = '  ALAK  AZARCA  COCTDEDCFLGA  HIIDILINIAKSKYLAMEMDMAMIMNMSMOMTNENVNHNJNMNYNCNDOHOKORPA  RISCSDTNTXUTVTVA  WAWVWIWY'

function hatch(svg, size) {
  const id = nextId('fx-hatch')
  const p = svg.append('defs').append('pattern').attr('id', id).attr('width', size).attr('height', size).attr('patternUnits', 'userSpaceOnUse').attr('patternTransform', 'rotate(45)')
  p.append('rect').attr('width', size).attr('height', size).style('fill', noData())
  p.append('line').attr('y2', size).style('stroke', paint('--border-strong')).attr('stroke-width', size / 4)
  return `url(#${id})`
}

/**
 * topo: a pre-projected US TopoJSON from reference/maps.md (975 x 610). values: Map(key -> amount); amounts at or
 * below zero get no circle. opts: object (the topology object, default 'states'), key (feature -> the values' key,
 * default its name; for counties, the FIPS id, `(f) => f.id`), color (the circles' token, default '--c-bar'), emph
 * (keys ringed and labelled), labelTop, selected, onPick(key), tip(key, value), format (the tooltip), keyFormat (the
 * labels and the size key, default compact: 1.2K, so pass one with the unit, such as '$'), legendTitle, label. Keys that
 * match no shape are named under the map. County shapes are clicked, not tabbed to, as in maps.md.
 */
export function circleMap(el, topo, values, opts = {}) {
  const { object = 'states', key = (f) => f.properties.name, color = '--c-bar', emph = [], labelTop = 4, selected = null, onPick, tip, format = fmt, keyFormat = (v) => compact.format(v), legendTitle = '', label: aria = 'Map' } = opts
  const { features, missing } = shapesOf(topo, object, key, values)
  const path = d3.geoPath()
  const pos = (v) => num(v) && v > 0
  const amounts = [...values.values()].filter(pos)
  const hiV = d3.max(amounts) || 0
  const noCircle = values.size - amounts.length
  const lines = (n) => { const v = values.get(n); return tip ? [tip(n, v)].flat() : [n, num(v) ? format(v) : 'No data'] }
  mount(el, 'fx-cmap', (w) => {
    const k = w / 975
    const ht = Math.round(610 * k)
    const svg = svgIn(w, ht, aria)
    svg.append('g').attr('transform', `scale(${k})`).selectAll('path').data(features).join('path').attr('d', path)
      .attr('class', (f) => `fx-cmap__state${key(f) === selected ? ' is-selected' : ''}`)
      .each(function (f) { const n = key(f); if (num(values.get(n))) pickable(this, n, onPick, lines(n).join(': '), n === selected, object !== 'counties') })
      .call(hover, (f) => lines(key(f)))
    const rMax = Math.max(14, 34 * Math.sqrt(k))
    const r = d3.scaleSqrt([0, hiV || 1], [0, rMax])
    const dots = features.filter((f) => pos(values.get(key(f)))).map((f) => {
      const [cx, cy] = path.centroid(f)
      return { name: key(f), v: values.get(key(f)), cx: cx * k, cy: cy * k, ab: POSTAL.slice(f.id * 2, f.id * 2 + 2).trim() }
    }).sort((a, b) => b.v - a.v)
    svg.append('g').attr('pointer-events', 'none').selectAll('circle').data(dots).join('circle').attr('cx', (d) => d.cx).attr('cy', (d) => d.cy).attr('r', (d) => r(d.v))
      .attr('class', (d) => `fx-cmap__dot${emph.includes(d.name) ? ' is-on' : ''}${d.name === selected ? ' is-selected' : ''}`).style('fill', paint(color))
    const named = new Set([...dots.slice(0, labelTop).map((d) => d.name), ...emph])
    const cands = dots.filter((d) => named.has(d.name)).map((d) => {
      const rr = r(d.v)
      return { d, text: w < 520 ? d.ab || d.name : `${d.name} ${keyFormat(d.v)}`, force: emph.includes(d.name), positions: [[0, -rr - 5, 'middle'], [rr + 4, 4, 'start'], [-rr - 4, 4, 'end'], [0, rr + 13, 'middle']].map(([x, y, anchor]) => ({ x: d.cx + x, y: d.cy + y, anchor })) }
    }).sort((a, b) => b.force - a.force)
    for (const c of placeLabels(cands, { bounds: [0, w], top: 0, px: 11.5 })) halo(svg, c.x, c.y, c.text, { anchor: c.anchor, cls: `fx-note${c.force ? ' fx-accent' : ''}` })
    if (hiV) {
      const kx = Math.min(w * 0.66, w - rMax - 56)
      const ky = ht - 8
      const roundDown = (v) => { const p = 10 ** Math.floor(Math.log10(v)); return ([5, 2, 1].find((m) => m * p <= v) || 1) * p }
      const sizes = [...new Set([hiV, hiV / 4, hiV / 16].map(roundDown))].slice(0, w < 520 ? 2 : 3)
      const g = svg.append('g').attr('class', 'fx-cmap__key')
      const row = g.selectAll('g').data(sizes).join('g').attr('transform', (v) => `translate(${kx},${ky - 2 * r(v)})`)
      row.append('circle').attr('cy', r).attr('r', r)
      row.append('line').attr('class', 'fx-leader').attr('x2', rMax + 6)
      row.append('text').attr('x', rMax + 9).attr('dominant-baseline', 'middle').text(keyFormat)
      if (legendTitle) g.append('text').attr('class', 'fx-head').attr('x', kx - rMax).attr('y', ky - 2 * rMax - 12).text(fit(legendTitle, w - kx + rMax - 4, CAPS))
    }
    return [svg.node(), notes(hiV ? '' : 'No amounts above zero to draw.', hiV && noCircle ? `${noCircle} at or below zero, or with no value, have no circle.` : '', notOnMap(missing))]
  })
}

/** Thirds of a list of numbers: { cuts, at(v) -> 0, 1 or 2 }. */
export const tercile = (values) => {
  const v = values.filter(num).sort(d3.ascending)
  const cuts = [v[Math.floor((v.length - 1) / 3)], v[Math.floor((2 * (v.length - 1)) / 3)]]
  return { cuts, at: (x) => cuts.filter((c) => x > c).length }
}
const BI = (a, b) => paint(`--bi-${a}${b}`)

/**
 * A map whose colour carries two measures, each split into thirds. topo: a pre-projected US TopoJSON from
 * reference/maps.md. values: Map key -> { a, b } (a value with either one null is drawn hatched, as withheld; a key
 * not in the map is plain no-data). opts: object (default 'states'), key (feature -> key, default its name), aLabel
 * and bLabel (the key's axis words, such as 'Jobs' and 'Growth'), aFmt, bFmt (the class edges), note (one sentence on
 * how to read the darkest and the warmest cells), withheldLabel, noneLabel, selected, onPick(key), tip(key, v), label.
 * The nine colours are --bi-<a third><b third>, mixed from --accent and --c2 in the styles. County shapes are clicked,
 * not tabbed to, as in maps.md; for counties pass `key: (f) => f.id` and FIPS keys.
 */
export function bivariateMap(el, topo, values, opts = {}) {
  const { object = 'states', key = (f) => f.properties.name, aLabel = 'More', bLabel = 'More', aFmt = fmt, bFmt = fmt, note = '', withheldLabel = 'Withheld', noneLabel = 'No data', selected = null, onPick, tip, label: aria = 'Map' } = opts
  const { features, missing } = shapesOf(topo, object, key, values)
  const path = d3.geoPath()
  const ok = (v) => v && num(v.a) && num(v.b)
  const full = [...values.values()].filter(ok)
  const Ta = tercile(full.map((v) => v.a))
  const Tb = tercile(full.map((v) => v.b))
  const lines = (k) => { const v = values.get(k); return tip ? [tip(k, v)].flat() : ok(v) ? [k, `${aLabel} ${aFmt(v.a)}`, `${bLabel} ${bFmt(v.b)}`] : [k, v ? withheldLabel : noneLabel] }
  mount(el, 'map', () => {
    const svg = svgIn(975, 610, aria)
    const hatched = hatch(svg, 6)
    svg.append('g').selectAll('path').data(features).join('path').attr('d', path).attr('class', (f) => `area${key(f) === selected ? ' is-selected' : ''}`)
      .style('fill', (f) => { const v = values.get(key(f)); return ok(v) ? BI(Ta.at(v.a), Tb.at(v.b)) : v ? hatched : noData() })
      .each(function (f) { const k = key(f); if (values.get(k)) pickable(this, k, onPick, lines(k).join(', '), k === selected, object !== 'counties') })
      .call(hover, (f) => lines(key(f)))
    svg.append('path').attr('class', 'mesh').attr('d', path(topojson.mesh(topo, topo.objects[object], (p, q) => p !== q)))
    const on = features.find((f) => key(f) === selected)
    if (on) svg.append('path').attr('class', 'area-ring').attr('d', path(on)).attr('aria-hidden', 'true')
    const axis = (t, cls) => h('span', { class: `bi-key__${cls}`, text: `${fit(t, 70, CAPS)} →` })
    return [svg.node(), h('div', { class: 'bi-legend' },
      h('div', { class: 'bi-key' }, axis(bLabel, 'y'), h('div', { class: 'bi-grid' }, ...[2, 1, 0].flatMap((b) => [0, 1, 2].map((a) => h('i', { style: { background: BI(a, b) } })))), axis(aLabel, 'x')),
      h('div', { class: 'bi-words' },
        h('span', { text: full.length ? `${aLabel} split into thirds at ${Ta.cuts.filter(num).map(aFmt).join(' and ')}; ${bLabel.toLowerCase()} at ${Tb.cuts.filter(num).map(bFmt).join(' and ')}.` : 'No place has both values, so none is coloured.' }),
        note ? h('span', { text: note }) : null,
        h('span', { class: 'bi-none' }, h('i', { class: 'fx-hatch-key' }), withheldLabel),
        h('span', { class: 'bi-none' }, h('i', { style: { background: noData() } }), noneLabel))), notes(notOnMap(missing))]
  })
}

/**
 * cols: [{ id, label, short, total, parts: [{ name, value, share }], rest }] where share is the part's share of its
 * column from the model, printed in the band (without it the band is drawn and no number printed), rest is the part of
 * the total in no part (withheld), drawn hatched, and short is the label on phones. opts: colors (name -> token), order
 * (part names, bottom first; default, the order they first appear), onPick(id), onPickPart(name), selected,
 * selectedPart, format, unit (in the tooltip), restLabel, label.
 */
export function marimekko(el, cols, opts = {}) {
  const { colors = {}, onPick, onPickPart, selected = null, selectedPart = null, format = fmt, unit = '', restLabel = 'Withheld', label: aria = 'Marimekko' } = opts
  const ok = cols.filter((c) => num(c.total) && c.total > 0)
  if (!ok.length) return nothing(el)
  const noTotal = cols.filter((c) => !ok.includes(c)).map((c) => c.label)
  const negative = d3.sum(ok, (c) => c.parts.filter((p) => num(p.value) && p.value < 0).length)
  const order = [...new Set([...(opts.order || []), ...ok.flatMap((c) => c.parts.map((p) => p.name))])]
  const rest = (c) => (num(c.rest) && c.rest > 0 ? c.rest : 0)
  const over = ok.filter((c) => d3.sum(c.parts, (p) => Math.max(0, p.value) || 0) + rest(c) > c.total * 1.001).map((c) => c.label)
  const u = unit ? ` ${unit}` : ''
  const colorOf = (n) => paint(colors[n] || '--c-bar')
  mount(el, 'fx-mekko', (w) => {
    const narrow = w < 560
    const m = { l: 38, r: 4, t: 10, b: narrow ? 92 : 64 }
    const ht = narrow ? 380 : 360
    const gap = 2
    const plotW = w - m.l - m.r - gap * (ok.length - 1)
    const bottom = ht - m.b
    const svg = svgIn(w, ht, aria)
    const hatched = hatch(svg, 5)
    const y = d3.scaleLinear([0, 1], [bottom, m.t])
    yAxis(svg.append('g').attr('transform', `translate(${m.l},0)`), y, w - m.l - m.r, d3.format('.0%'), 4)
    const cw = d3.scaleLinear([0, d3.sum(ok, (c) => c.total)], [0, plotW])
    let x0 = m.l
    for (const c of ok) {
      const wd = cw(c.total)
      const lx = x0 + wd / 2
      const g = svg.append('g').attr('class', `fx-mk-col${c.id === selected ? ' is-selected' : ''}`).attr('text-anchor', 'middle')
      const given = [...order.map((n) => c.parts.find((p) => p.name === n)).filter((p) => p && num(p.value) && p.value > 0), ...(rest(c) ? [{ name: restLabel, value: c.rest, rest: true }] : [])]
      const scale = Math.max(c.total, d3.sum(given, (p) => p.value))
      let acc = 0
      const parts = given.map((p) => ({ ...p, y1: y(acc / scale), y0: y((acc += p.value) / scale), fill: p.rest ? hatched : colorOf(p.name), t: `${p.name}: ${format(p.value)}${u}` }))
      g.selectAll('rect').data(parts).join('rect').attr('x', x0).attr('y', (p) => p.y0).attr('width', Math.max(0.5, wd)).attr('height', (p) => Math.max(0, p.y1 - p.y0 - 1))
        .style('fill', (p) => p.fill).attr('class', (p) => (!p.rest && selectedPart && selectedPart !== p.name ? 'is-dim' : null))
        .each(function (p) { if (!p.rest) pickable(this, p.name, onPickPart, `${c.label}, ${p.t}`, p.name === selectedPart, true, `${c.id}|${p.name}`) })
        .call(hover, (p) => [c.label, p.t, num(p.share) ? `${pct(p.share, 0)} of ${c.label}` : ''])
      if (wd > 30) g.selectAll('text').data(parts.filter((p) => num(p.share) && p.y1 - p.y0 > 15)).join('text').attr('class', 'fx-cell').attr('pointer-events', 'none').attr('x', lx).attr('y', (p) => (p.y0 + p.y1) / 2 + 4).style('fill', (p) => inkOn(p.fill)).text((p) => pct(p.share, 0))
      const name = narrow && c.short ? c.short : c.label
      const flat = wd > textW(name) + 4
      if (flat) g.append('text').attr('class', 'fx-name').attr('x', lx).attr('y', bottom + 15).text(name)
      else if (wd >= 13) g.append('text').attr('class', 'fx-name').attr('text-anchor', 'end').attr('transform', `translate(${lx + 4},${bottom + 6}) rotate(-90)`).text(fit(name, m.b - 18, 12))
      if (flat && wd > textW(format(c.total)) + 4) g.append('text').attr('class', 'fx-tick').attr('x', lx).attr('y', bottom + 29).text(format(c.total))
      if (onPick) g.append('rect').attr('class', 'fx-mk-hit').attr('x', x0).attr('y', bottom + 2).attr('width', wd).attr('height', m.b - 4)
        .each(function () { pickable(this, c.id, onPick, `${c.label}: ${format(c.total)}${u}`, c.id === selected) }).call(hover, () => [c.label, format(c.total) + u])
      x0 += wd + gap
    }
    return [h('div', { class: 'legend fx-legend' },
      ...[...order].reverse().map((n) => h('span', {}, h('i', { style: { background: colorOf(n) } }), n)),
      ok.some(rest) ? h('span', {}, h('i', { class: 'fx-hatch-key' }), restLabel) : null), svg.node(), notes(over.length ? `The parts add to more than the total in ${over.join(', ')}, so those columns are scaled to fit.` : '', noTotal.length ? `No total above zero, so no column: ${noTotal.join(', ')}.` : '', negative ? `${negative} part${negative > 1 ? 's' : ''} below zero left out.` : '')]
  })
}

/**
 * steps: [{ label, delta, strong }] between start and end, all from the model; the last step is usually "All others",
 * so the steps add up. Each bar starts where the one before ended; the numbers printed are the totals and steps as
 * given, and when the steps do not reach the end total the chart says so. opts: startLabel, start, endLabel, end,
 * format (the totals), deltaFormat (the steps), good ('rise' | 'fall' | null, the default: which way is good news),
 * label. Drawn sideways, so labels read at any width; the axis starts near the smaller total, and says so.
 */
export function waterfall(el, steps, opts = {}) {
  const { startLabel = 'Start', start, endLabel = 'End', end, format = fmt, deltaFormat = (v) => `${v > 0 ? '+' : v < 0 ? '−' : ''}${format(Math.abs(v))}`, good = null, label: aria = 'Waterfall' } = opts
  if (!num(start) || !num(end)) return nothing(el)
  const ok = steps.filter((st) => num(st.delta))
  const blank = steps.filter((st) => !num(st.delta)).map((st) => st.label)
  let run = start
  const rows = [{ label: startLabel, total: start }, ...ok.map((st) => ({ ...st, from: run, to: (run += st.delta) })), { label: endLabel, total: end }]
  const off = Math.abs(run - end) > Math.max(1e-9, Math.abs(end) * 0.001)
  const isTotal = (r) => r.from === undefined
  const text = (r) => (isTotal(r) ? format(r.total) : deltaFormat(r.delta))
  mount(el, 'fx-waterfall', (w) => {
    const labW = Math.min(w * 0.42, d3.max(rows, (r) => textW(r.label, 12)) + 12)
    const rowH = 22
    const [lo, hi] = d3.extent(rows.flatMap((r) => (isTotal(r) ? [r.total] : [r.from, r.to])))
    const pad = (hi - lo) * 0.18 || Math.abs(hi) * 0.1 || 1
    const room = Math.max(56, d3.max(rows, (r) => textW(text(r)) + 10))
    const x = d3.scaleLinear([lo - pad, hi + pad], [labW + 6, w - room])
    const svg = svgIn(w, rows.length * rowH + 6, aria)
    const g = svg.selectAll('g').data(rows).join('g').attr('transform', (r, i) => `translate(0,${i * rowH + rowH / 2 + 2})`)
    g.append('text').attr('class', (r) => `fx-name${isTotal(r) || r.strong ? ' fx-name--strong' : ''}`).attr('x', labW - 4).attr('y', 4).attr('text-anchor', 'end').text((r) => fit(r.label, labW - 8, 12))
    const a = (r) => (isTotal(r) ? labW + 6 : Math.min(x(r.from), x(r.to)))
    const b = (r) => (isTotal(r) ? x(r.total) : Math.max(x(r.from), x(r.to)))
    g.append('rect').attr('x', a).attr('y', -7).attr('height', 14).attr('width', (r) => Math.max(1.5, b(r) - a(r)))
      .style('fill', (r) => paint(isTotal(r) ? '--c-bar' : toneFill(r.delta, good))).attr('opacity', (r) => (isTotal(r) ? 0.85 : null))
      .call(hover, (r) => [r.label, text(r)])
    g.filter(isTotal).append('path').attr('class', 'fx-break').attr('d', `M${labW + 14},-9l-4,18M${labW + 19},-9l-4,18`)
    g.filter((r) => !isTotal(r)).append('line').attr('class', 'fx-link').attr('x1', (r) => x(r.to)).attr('x2', (r) => x(r.to)).attr('y1', 7).attr('y2', rowH - 7)
    g.append('text').attr('class', 'fx-value').attr('x', (r) => b(r) + 5).attr('y', 4).text(text)
    return [svg.node(), notes('The axis does not start at zero: the bars show the change, not the size.', blank.length ? `No value, so left out: ${blank.join(', ')}.` : '', off ? `The steps do not reach the end total${blank.length ? '' : ': a contributor is missing from the list the model gave'}.` : '')]
  })
}
