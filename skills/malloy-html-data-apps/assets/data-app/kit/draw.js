// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT
//
// The kit's own charts, drawn with d3 to the house style: hairline grid, no axis lines, the subject in the accent at
// 2px, the comparison in grey, ends labelled in place of a legend, a hover rule that lists every series, and a click
// on every chart. charts.js has the long tail of forms; these are the ones every dashboard uses.

const css = (n) => getComputedStyle(document.documentElement).getPropertyValue(n).trim()
export const MOTION = typeof matchMedia === 'function' && !matchMedia('(prefers-reduced-motion: reduce)').matches && !/[?&]motion=0\b/.test(location.search)

// ---- One tooltip for the page ----
let tipEl = null
export function tip(ev, build, anchor) {
  if (!tipEl) {
    tipEl = document.createElement('div')
    tipEl.className = 'tip'
    tipEl.setAttribute('role', 'status')
    document.body.append(tipEl)
  }
  tipEl.replaceChildren(...build())
  tipEl.style.opacity = '1'
  const r = (anchor || ev.target).getBoundingClientRect()
  const x = anchor ? r.left + anchor.__x : ev.clientX
  const w = tipEl.offsetWidth
  const left = x + 16 + w > innerWidth - 8 ? x - 16 - w : x + 16
  tipEl.style.left = `${Math.max(8, left) + scrollX}px`
  tipEl.style.top = `${(anchor ? r.top + 8 : ev.clientY - tipEl.offsetHeight - 12) + scrollY}px`
}
export function untip() {
  if (tipEl) tipEl.style.opacity = '0'
}
export function tipRow(color, label, value, kind = 'line') {
  const d = document.createElement('div')
  d.className = 'tip__r'
  const key = document.createElement('i')
  key.className = `tip__key tip__key--${kind}`
  key.style.background = color
  const l = document.createElement('span')
  l.textContent = label
  const v = document.createElement('strong')
  v.textContent = value
  d.append(key, l, v)
  return d
}
export const tipHead = (text) => Object.assign(document.createElement('div'), { className: 'tip__h', textContent: text })
export const tipFoot = (text, cls = '') => Object.assign(document.createElement('div'), { className: `tip__d ${cls}`, textContent: text })

/** Redraw on resize, at most once a frame; the first draw is synchronous. */
export function sized(el, paint) {
  let last = 0
  const draw = () => {
    const w = Math.round(el.clientWidth)
    if (!w || Math.abs(w - last) < 2) return
    last = w
    el.replaceChildren()
    paint(w)
  }
  el.__ro?.disconnect()
  draw()
  el.__ro = new ResizeObserver(() => requestAnimationFrame(draw))
  el.__ro.observe(el)
}

/**
 * A line chart of one subject series and, optionally, its comparison, over buckets.
 *   line(el, { labels: [...], ticks: [...], subject: { name, values }, compare: { name, values }, format, height,
 *              partial: [bool...], tipLabel: (i) => 'Week of 8–14 Nov', delta: (a, b) => ({ text, tone }),
 *              onPick: (i) => …, onRange: (i, j) => …, label })
 * Click a point to pick its bucket; drag across to pick a range. Ends are labelled with value and name.
 */
export function line(el, o) {
  sized(el, (w) => {
    const ht = o.height || 260
    const narrow = w < 520
    const a = o.subject.values
    const b = o.compare?.values || []
    const all = [...a, ...b].filter(Number.isFinite)
    if (!all.length) return
    const m = { t: 14, r: narrow ? 72 : 124, b: 28, l: 0 }
    let lo = Math.min(...all)
    let hi = Math.max(...all)
    if (o.zero !== false && lo >= 0 && (hi - lo) / hi > 0.6) lo = 0
    const pad = (hi - lo || Math.abs(hi) || 1) * 0.12
    const y = d3.scaleLinear([lo === 0 ? 0 : lo - pad * 2, hi + pad], [ht - m.b, m.t]).nice(4)
    const ticksY = y.ticks(4)
    const axis = o.axis || ((t) => o.format(t))
    m.l = Math.max(...ticksY.map((t) => axis(t).length)) * 7 + 12
    const x = d3.scaleLinear([0, Math.max(1, a.length - 1)], [m.l + 4, w - m.r])
    const svg = d3.select(el).append('svg').attr('width', w).attr('height', ht).attr('role', 'img').attr('aria-label', o.label || o.subject.name)
    svg.append('g').attr('class', 'grid').selectAll('line').data(ticksY).join('line').attr('x1', m.l).attr('x2', w - m.r + 8).attr('y1', y).attr('y2', y)
    svg.append('g').attr('class', 'ax').selectAll('text').data(ticksY).join('text').attr('x', m.l - 10).attr('y', y).attr('dy', '0.32em').attr('text-anchor', 'end').text((t) => axis(t))
    // Date ticks: as many as fit, counted back from the latest so it is always labelled.
    const every = Math.max(1, Math.ceil(a.length / Math.max(2, Math.floor((w - m.l - m.r) / 84))))
    const idx = a.map((_, i) => i).filter((i) => (a.length - 1 - i) % every === 0)
    svg.append('g').attr('class', 'ax').selectAll('text').data(idx).join('text').attr('x', x).attr('y', ht - 8).attr('text-anchor', (i) => (i === a.length - 1 && narrow ? 'end' : 'middle')).text((i) => o.ticks[i])
    const curve = a.length > 40 ? d3.curveLinear : d3.curveMonotoneX
    const path = (vals) => d3.line().defined(Number.isFinite).x((_, i) => x(i)).y(y).curve(curve)(vals)
    const solid = a.map((v, i) => (o.partial?.[i] ? null : v))
    if (b.length) svg.append('path').attr('class', 'ln ln--cmp').attr('d', path(b))
    const main = svg.append('path').attr('class', 'ln ln--subject').attr('d', path(solid))
    // A bucket the period only partly covers is an open point with a dotted join, so it never reads as a fall.
    o.partial?.forEach((p, i) => {
      if (!p || !Number.isFinite(a[i])) return
      const j = i === 0 ? 1 : i - 1
      if (Number.isFinite(a[j])) svg.append('path').attr('class', 'ln ln--partial').attr('d', d3.line()([[x(j), y(a[j])], [x(i), y(a[i])]]))
      svg.append('circle').attr('class', 'pt pt--partial').attr('cx', x(i)).attr('cy', y(a[i])).attr('r', 3.5)
    })
    if (MOTION && !el.__drawn) {
      const len = main.node().getTotalLength()
      main.attr('stroke-dasharray', `${len} ${len}`).attr('stroke-dashoffset', len).transition().duration(600).ease(d3.easeCubicOut).attr('stroke-dashoffset', 0).on('end', () => main.attr('stroke-dasharray', null))
    }
    el.__drawn = true
    // Ends: the value, then the name; kept apart when the two meet.
    const last = (vals) => {
      for (let i = vals.length - 1; i >= 0; i--) if (Number.isFinite(vals[i])) return i
      return -1
    }
    const ia = last(a)
    const ib = last(b)
    const ends = [{ i: ia, v: a[ia], name: o.subject.name, cls: 'subject' }, ...(ib >= 0 ? [{ i: ib, v: b[ib], name: o.compare.name, cls: 'cmp' }] : [])].filter((e) => e.i >= 0)
    const ys = ends.map((e) => y(e.v))
    if (ys.length === 2 && Math.abs(ys[0] - ys[1]) < 34) {
      const mid = (ys[0] + ys[1]) / 2
      const up = ys[0] <= ys[1] ? -1 : 1
      ys[0] = mid + up * 17
      ys[1] = mid - up * 17
    }
    ends.forEach((e, k) => {
      const g = svg.append('g').attr('class', `end end--${e.cls}`).attr('transform', `translate(${w - m.r + 14},${Math.max(m.t + 6, Math.min(ht - m.b - 14, ys[k]))})`)
      g.append('text').attr('class', 'end__v').attr('y', -2).text(o.format(e.v, false))
      g.append('text').attr('class', 'end__n').attr('y', 13).text(narrow ? (e.cls === 'subject' ? 'Now' : 'Before') : e.name)
    })
    if (ia >= 0) svg.append('circle').attr('class', 'pt pt--end').attr('cx', x(ia)).attr('cy', y(a[ia])).attr('r', 4)
    // Hover, click and drag on one surface.
    const rule = svg.append('line').attr('class', 'hover-rule').attr('y1', m.t).attr('y2', ht - m.b).style('opacity', 0)
    const band = svg.append('rect').attr('class', 'drag-band').attr('y', m.t).attr('height', ht - m.t - m.b).style('opacity', 0)
    const dotA = svg.append('circle').attr('class', 'pt pt--subject').attr('r', 4).style('opacity', 0)
    const dotB = svg.append('circle').attr('class', 'pt pt--cmp').attr('r', 3.5).style('opacity', 0)
    const near = (px) => Math.max(0, Math.min(a.length - 1, Math.round(x.invert(px))))
    let down = null
    const surface = svg.append('rect').attr('class', 'surface').attr('x', m.l).attr('y', m.t).attr('width', Math.max(1, w - m.l - m.r)).attr('height', ht - m.t - m.b)
      .attr('tabindex', o.onPick ? 0 : null).attr('role', o.onPick ? 'button' : null).attr('aria-label', o.onPick ? `${o.label || o.subject.name}: pick a point to zoom to it` : null)
    const show = (i) => {
      rule.attr('x1', x(i)).attr('x2', x(i)).style('opacity', 1)
      dotA.attr('cx', x(i)).attr('cy', y(a[i])).style('opacity', Number.isFinite(a[i]) ? 1 : 0)
      dotB.attr('cx', x(i)).attr('cy', y(b[i])).style('opacity', Number.isFinite(b[i]) ? 1 : 0)
      surface.node().__x = x(i) - 0
      tip(null, () => [
        tipHead(o.tipLabel(i)),
        tipRow(css('--accent'), o.subject.name, Number.isFinite(a[i]) ? o.format(a[i], true) : 'no value'),
        ...(b.length ? [tipRow(css('--cmp'), o.compare.name, Number.isFinite(b[i]) ? o.format(b[i], true) : 'no value')] : []),
        ...(b.length && o.delta && Number.isFinite(a[i]) && Number.isFinite(b[i]) ? [tipFoot(o.delta(a[i], b[i]).text, o.delta(a[i], b[i]).tone)] : []),
        ...(o.onPick ? [tipFoot(o.partial?.[i] ? 'Part of a period. Click to zoom to it.' : 'Click to zoom to it, or drag across several.', 'tip__hint')] : []),
      ], surface.node())
    }
    surface.on('pointermove', (ev) => {
      const px = d3.pointer(ev)[0]
      if (down !== null && o.onRange) {
        const x0 = Math.min(down, px)
        band.attr('x', x0).attr('width', Math.abs(px - down)).style('opacity', 1)
      }
      show(near(px))
    }).on('pointerleave', () => {
      rule.style('opacity', 0)
      dotA.style('opacity', 0)
      dotB.style('opacity', 0)
      untip()
    }).on('pointerdown', (ev) => {
      down = d3.pointer(ev)[0]
      surface.node().setPointerCapture?.(ev.pointerId)
    }).on('pointerup', (ev) => {
      if (down === null) return
      const px = d3.pointer(ev)[0]
      const [i, j] = [near(Math.min(down, px)), near(Math.max(down, px))]
      down = null
      band.style('opacity', 0)
      untip()
      if (i !== j && o.onRange) o.onRange(i, j)
      else if (o.onPick) o.onPick(i)
    })
    // Keyboard: arrows move the hover point, Enter picks it.
    let at = a.length - 1
    surface.on('keydown', (ev) => {
      if (ev.key === 'ArrowLeft' || ev.key === 'ArrowRight') {
        ev.preventDefault()
        at = Math.max(0, Math.min(a.length - 1, at + (ev.key === 'ArrowLeft' ? -1 : 1)))
        show(at)
      } else if (ev.key === 'Enter' && o.onPick) o.onPick(at)
    }).on('blur', () => {
      rule.style('opacity', 0)
      untip()
    })
  })
}

/** A sparkline: the period in the accent, the comparison in grey, on a range at least 6% of the mean wide. */
export function spark(values, cmp = []) {
  const svg = d3.create('svg').attr('class', 'spark').attr('viewBox', '0 0 100 26').attr('preserveAspectRatio', 'none').attr('aria-hidden', 'true')
  const all = [...values, ...cmp].filter(Number.isFinite)
  if (values.filter(Number.isFinite).length < 2) return svg.node()
  const [lo, hi] = d3.extent(all)
  const mid = (lo + hi) / 2
  const half = Math.max((hi - lo) / 2, Math.abs(mid) * 0.06, 1e-9)
  const y = d3.scaleLinear([mid - half, mid + half], [24, 2])
  const x = d3.scaleLinear([0, Math.max(1, values.length - 1)], [0, 100])
  const l = d3.line().defined(Number.isFinite).x((_, i) => x(i)).y(y).curve(d3.curveMonotoneX)
  if (cmp.some(Number.isFinite)) svg.append('path').attr('class', 'spark__cmp').attr('d', l(cmp))
  svg.append('path').attr('class', 'spark__line').attr('d', l(values))
  return svg.node()
}

/**
 * A grid of cells, darker for more: rows by columns, such as weekday by hour. Every cell is a button.
 *   heat(el, { rows, cols, value: (r, c) => n | null, format, onPick: (r, c) => …, label, rowLabel, colLabel })
 */
export function heat(el, o) {
  sized(el, (w) => {
    const lw = Math.min(160, Math.max(...o.rows.map((r) => String(o.rowLabel ? o.rowLabel(r) : r).length)) * 7.5 + 14)
    const cw = (w - lw) / o.cols.length
    const ch = Math.max(18, Math.min(30, cw * 0.8))
    const vals = o.rows.flatMap((r) => o.cols.map((c) => o.value(r, c))).filter(Number.isFinite)
    const [lo, hi] = d3.extent(vals)
    const ramp = d3.scaleLinear([lo, hi], [0.06, 1])
    const svg = d3.select(el).append('svg').attr('width', w).attr('height', 22 + ch * o.rows.length).attr('role', 'group').attr('aria-label', o.label)
    // Every nth column labelled, n wide enough that the longest label clears its neighbour.
    const longest = d3.max(o.cols, (c) => String(o.colLabel ? o.colLabel(c) : c).length)
    const every = Math.max(Math.ceil(o.cols.length / Math.max(1, Math.floor((w - lw) / 34))), Math.ceil((longest * 6.6 + 10) / cw))
    const colTexts = svg.append('g').attr('class', 'ax').selectAll('text').data(o.cols.map((c, i) => [c, i]).filter(([, i]) => i % every === 0)).join('text')
      .attr('x', ([, i]) => (i === 0 ? lw : lw + i * cw + cw / 2)).attr('y', 12).attr('text-anchor', ([, i]) => (i === 0 ? 'start' : 'middle')).text(([c]) => (o.colLabel ? o.colLabel(c) : c))
    // Measured, not guessed: a label that would touch the one before it, or pass the edge, is left out.
    let right = -Infinity
    colTexts.each(function () {
      const b = this.getBBox()
      if (b.x < right + 6 || b.x + b.width > w) this.remove()
      else right = b.x + b.width
    })
    const fitRow = (t) => {
      const full = String(t)
      const room = Math.floor((lw - 14) / 7)
      return full.length > room ? `${full.slice(0, Math.max(1, room - 1))}…` : full
    }
    svg.append('g').attr('class', 'ax ax--rows').selectAll('text').data(o.rows).join('text').attr('x', lw - 10).attr('y', (_, j) => 22 + j * ch + ch / 2).attr('dy', '0.32em').attr('text-anchor', 'end').text((r) => fitRow(o.rowLabel ? o.rowLabel(r) : r))
      .append('title').text((r) => String(o.rowLabel ? o.rowLabel(r) : r))
    const accent = css('--accent')
    const paper = css('--paper')
    const cells = o.rows.flatMap((r, j) => o.cols.map((c, i) => ({ r, c, i, j, v: o.value(r, c) })))
    svg.append('g').selectAll('rect').data(cells).join('rect').attr('class', (d) => `cell${Number.isFinite(d.v) ? '' : ' cell--none'}`)
      .attr('x', (d) => lw + d.i * cw + 1).attr('y', (d) => 22 + d.j * ch + 1).attr('width', Math.max(1, cw - 2)).attr('height', ch - 2).attr('rx', 2)
      .style('fill', (d) => (Number.isFinite(d.v) ? d3.interpolateRgb(paper, accent)(ramp(d.v)) : null))
      .attr('tabindex', (d) => (o.onPick && Number.isFinite(d.v) && cells.length <= 400 ? 0 : null)).attr('role', o.onPick ? 'button' : null)
      .attr('aria-label', (d) => `${o.rowLabel ? o.rowLabel(d.r) : d.r}, ${o.colLabel ? o.colLabel(d.c) : d.c}: ${Number.isFinite(d.v) ? o.format(d.v) : 'no data'}`)
      .on('pointermove', function (ev, d) {
        tip(ev, () => [tipHead(`${o.rowLabel ? o.rowLabel(d.r) : d.r}, ${o.colLabel ? o.colLabel(d.c) : d.c}`), tipRow(accent, o.valueName || 'Value', Number.isFinite(d.v) ? o.format(d.v) : 'no data', 'box'), ...(o.onPick ? [tipFoot('Click to filter to it.', 'tip__hint')] : [])])
      })
      .on('pointerleave', untip)
      .on('click', (_, d) => o.onPick && Number.isFinite(d.v) && o.onPick(d.r, d.c))
      .on('keydown', (ev, d) => ev.key === 'Enter' && o.onPick && o.onPick(d.r, d.c))
  })
}
