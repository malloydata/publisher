// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT
//
// The live state and the only ways to change it. Every change writes the address and draws the page again: a new
// page pushes history, a new filter or period replaces it, so Back steps between pages, not between clicks.

import * as S from './state.js'

export let state = null
/** The last page shown that was not an entity's: where an entity page's breadcrumb goes back to. */
export let cameFrom = null
let draw = () => {}

export function init(s, render) {
  state = s
  draw = render
  history.replaceState(null, '', S.toHash(state))
  window.addEventListener('popstate', () => {
    state = S.fromHash(location.hash)
    draw({ toTop: true })
  })
}

export function go(next, { push = false, toTop = push } = {}) {
  if (state && !next.id && next.page !== state.page) cameFrom = null
  if (state && state.id === null) cameFrom = state.page
  state = next
  const hash = S.toHash(state)
  if (hash !== location.hash) history[push ? 'pushState' : 'replaceState'](null, '', hash)
  draw({ toTop })
}
const patch = (p) => go({ ...state, ...p })

export const setPeriod = (period) => patch({ period, grain: null })
export const setCompare = (compare) => patch({ compare })
export const setGrain = (grain) => patch({ grain })
export const setMetric = (metric) => patch({ metric })
export const setFilter = (key, values) => {
  const filters = { ...state.filters }
  if (values?.length) filters[key] = values
  else delete filters[key]
  patch({ filters })
}
/** Add a value to a filter, or take it out when it is the only one picked: what a click on a bar does. */
export function toggleFilter(key, value) {
  const now = state.filters[key] || []
  const has = now.some((v) => String(v) === String(value))
  setFilter(key, has ? now.filter((v) => String(v) !== String(value)) : [value])
}
export const clearFilters = () => patch({ filters: {} })
/** Open a page (with an id for a detail page), keeping the period and filters. */
export const open = (page, id = null) => go({ ...state, page, id }, { push: true })
/** The address of a page, with the current period and filters: for a link drawn as an <a>. */
export const linkTo = (page, id = null) => S.toHash({ ...state, page, id })
