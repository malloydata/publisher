// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

// The seed lists in materialization_allowlists.mjs, typed for the specs that
// read them.

type Step = string;
type Counted = Record<string, { count: number; step: Step }>;

export declare const MATERIALIZATION_IMPORT_ALLOWLIST: Record<string, Step>;
export declare const MATERIALIZATION_HEADER_ALLOWLIST: Record<string, Step>;
export declare const MATERIALIZATION_HISTORY_ALLOWLIST: Counted;
export declare const MATERIALIZATION_UNIMPORTED_EXPORTS: Record<string, Step>;
export declare const RATCHET_ANNOTATION_READS: Counted;
export declare const RATCHET_IR_READS: Counted;
export declare const RATCHET_CLOCK_READS: Counted;
export declare const RATCHET_DIALECT_LITERALS: Counted;
export declare const RATCHET_LONG_FUNCTIONS: Record<string, Step>;
export declare const RATCHET_SWITCH_READS: Counted;
export declare const RATCHET_TIER_DECISIONS: Counted;
export declare const RATCHET_TABLE_IDENTITY: Counted;
export declare const RATCHET_STORED_TYPE_PATTERNS: Counted;
export declare const RATCHET_SERVE_IO: Counted;
export declare const RATCHET_ADMISSION_IMPORTS: Record<string, Step>;
export declare const RATCHET_VOCABULARY_UNIONS: Counted;
export declare const DELETED_NAMES_PENDING: Record<string, Step>;
