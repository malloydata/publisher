// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT
//
// The data app kit: import everything from here. Copied whole into public/kit/ and never edited, so a fix to the kit
// is a copy over it. The app's own code is public/app.js (its config and pages) and, rarely, public/app.css.
//
// KIT_VERSION says which kit an app carries; the running page shows it as <meta name="data-app-kit">. A new major
// version changes what an app's config or its own blocks must say: compare the app's config with the new kit's worked
// app.js before copying it over an app built on an older major. A minor or patch version is a copy over, nothing else.

export { dataApp, KIT_VERSION } from './shell.js'
export { block, subject, rank, share, trend, table, dataTable, heat, watch, entityHead, pageHead, aboutPage, sourceList, newsList, news, blocks, figure, changeText, statusMark, downloadCsv, markReady } from './blocks.js'
export { h, prose, query, setHost, host } from './core.js'
export * as S from './state.js'
export * as P from './period.js'
export * as F from './format.js'
export * as store from './store.js'
export * as D from './draw.js'
export * as C from './charts.js'
