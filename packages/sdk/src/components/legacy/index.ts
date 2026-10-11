// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * Deprecated surface kept for hosts that still use it: the by-name wrappers
 * over the deprecated `…/dashboards*` and `…/notebooks*` routes. Report code
 * imports nothing from here; removing a feature is deleting its module and its
 * line below.
 */
export { Dashboard, type DashboardProps } from "./Dashboard";
export { default as Notebook } from "./Notebook";
