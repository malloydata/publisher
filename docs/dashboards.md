<!--
Copyright (c) Credible Data Inc.
SPDX-License-Identifier: MIT
-->

# Dashboards (moved)

Dashboards and notebooks are now one document type, the **report**: a `.malloy` file with an
`## artifact` tag, anywhere in a package, whose `layout=grid` or `layout=linear` is the only
difference. What this page used to cover is in **[reports.md](reports.md)**.

The sections that other pages linked to here:

- <a id="a-dashboard-from-one-query"></a>[A dashboard from one query](reports.md#a-dashboard-from-one-query)
- <a id="laying-out-the-grid"></a>[Laying out the grid](reports.md#laying-out-the-grid)
- <a id="text-tiles"></a>[Text tiles](reports.md#text-tiles)
- <a id="filter-controls"></a>[Filter controls](reports.md#filter-controls)
- <a id="apply"></a>[Apply, starting values, and the URL](reports.md#apply)
- <a id="drill"></a>[Drill](reports.md#drill)
- <a id="what-publisher-checks-at-load"></a>[What Publisher checks at load](reports.md#what-publisher-checks-at-load)
- <a id="editing-in-the-console"></a>[Editing in the Console](reports.md#editing-in-the-console)
- <a id="rendering-one-in-your-own-react-app"></a>[Rendering one in your own React app](reports.md#rendering-one-in-your-own-react-app)
- <a id="documents-held-as-text"></a>[Documents held as text](reports.md#documents-held-as-text)

Why the two became one, and what changed (reports anywhere, `layout=`, drill targets by path, the
`…/reports` routes), is in [reports-design.md](reports-design.md).
