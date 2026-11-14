// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * The builder's names from before it edited every report, kept for hosts that
 * still import them from `@malloy-publisher/sdk/builder`. Builder-only: the
 * main entry must not reach this module (see `mainEntry.spec.ts`).
 */
import {
   ReportBuilder,
   type ReportBuilderProps,
} from "../ReportBuilder/ReportBuilder";
import {
   ReportEditor,
   type ReportEditorProps,
} from "../ReportBuilder/ReportEditor";

/** @deprecated Use `ReportEditor`. */
export const DashboardEditor = ReportEditor;
/** @deprecated Use `ReportEditorProps`. */
export type DashboardEditorProps = ReportEditorProps;
/** @deprecated Use `ReportBuilder`. */
export const DashboardBuilder = ReportBuilder;
/** @deprecated Use `ReportBuilderProps`. */
export type DashboardBuilderProps = ReportBuilderProps;
