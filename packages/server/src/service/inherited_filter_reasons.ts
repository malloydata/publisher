// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * Why an inherited `where:` could not be verified on the executed source, as
 * the denial's reason class says it (never its text, which names the model's
 * fields): a field it reads is defined again (`rename:`, `except:`, a new
 * definition); it reads through a join whose inputs are not recorded; the
 * source that declares it could not be resolved from this model; or it is
 * nested past a resolution bound.
 */
export const INHERITED_FILTER_REASONS = [
   "redefined_field",
   "unrecorded_join",
   "unresolved_declaration",
   "resolution_bound",
] as const;
export type InheritedFilterReason = (typeof INHERITED_FILTER_REASONS)[number];
