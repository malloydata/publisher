// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * `#(partition)` annotation parsing.
 *
 * Annotation format:
 *   #(partition)  <field path> = $NAME   — source-level, on a single source
 *
 * Unlike `#(authorize)`, whose body is an arbitrary Malloy boolean expression
 * handed verbatim to the compiler, `#(partition)`'s body is a narrow grammar
 * publisher parses itself: exactly one field path (a single column, or a
 * dotted join path like `report_ref.report_id`), the `=` operator, and a
 * `$GIVEN` reference. This module never grafts anything onto a query — it
 * only answers "what (column, given) pairs does this source's own annotation
 * declare", which the dimensional-indexing partition step and (in a follow-up
 * change) the row-filter graft both need to agree on.
 *
 * As with `#(authorize)`, **what counts as the tag is Malloy's answer**: a
 * note is a partition marker iff Malloy routes it to `partition` ({@link
 * noteRoute}), never a text match of our own.
 */

import { payloadOf, routeOf } from "@malloydata/malloy";
import { ModelCompilationError } from "../errors";

/** The annotation route Malloy assigns a `partition` marker. */
const PARTITION_ROUTE = "partition";

/** Malloy's own routing for ONE note — see `authorize.ts`'s identical helper. */
function noteRoute(text: string): string | undefined {
   return routeOf({ value: text.trimStart() } as Parameters<typeof routeOf>[0]);
}

/**
 * Whether any of `texts` is a `#(partition)`-routed note, by Malloy's own
 * routing — same convention as `authorize.ts`'s `containsAuthorizeAnnotationTag`.
 * Presence-only (does not validate the body grammar), for a caller that just
 * needs to know a marker exists somewhere.
 */
export function containsPartitionAnnotationTag(texts: string[]): boolean {
   return texts.some((text) => noteRoute(text) === PARTITION_ROUTE);
}

/** The note's payload — the part after the prefix, dedented for a block note. */
function notePayload(text: string): string {
   return (
      payloadOf({ value: text.trimStart() } as Parameters<
         typeof payloadOf
      >[0]) ?? ""
   );
}

/**
 * Every way a `#(partition)` annotation is refused, named distinctly so a
 * caller (and a test) can tell which rule fired rather than pattern-matching
 * a message string. Most of these are {@link parsePartitionAnnotation}'s
 * grammar rules; `partitioned_composite` is a structural placement refusal
 * (see `gate_classification.ts`'s `assertPartitionAnnotationsValid`), not a
 * body-grammar one, but shares this error shape rather than inventing a
 * second one; so is `ancestry_unresolvable`, raised when the walk looking for
 * a marker cannot read the IR chain it would have to follow.
 */
export type PartitionAnnotationRejectionCause =
   | "empty_body"
   | "compound_boolean"
   | "in_operator"
   | "negated_operator"
   | "comparison_operator"
   | "left_not_field_path"
   | "missing_given_reference"
   | "malformed_body"
   | "duplicate_given"
   | "partitioned_composite"
   | "ancestry_unresolvable";

/**
 * A `#(partition)` annotation that fails this module's grammar. Extends
 * {@link ModelCompilationError} so it maps to the same 424 an author already
 * gets from a malformed `#(authorize)` gate. `rejectionCause` is the
 * machine-readable half of the message, for a caller that wants to branch
 * on WHY rather than parse prose.
 */
export class PartitionAnnotationError extends ModelCompilationError {
   constructor(
      public readonly rejectionCause: PartitionAnnotationRejectionCause,
      message: string,
   ) {
      super({ message });
   }
}

/** One resolved `#(partition)` marker: a field path and the given it slices on. */
export interface PartitionPair {
   /** Dotted field path on the entry point's own surface, e.g. `"report_ref.report_id"`. */
   column: string;
   /** The given name, without its `$` sigil. */
   given: string;
}

const IDENT = "[A-Za-z_][A-Za-z0-9_]*";
/** A single column or a dotted join path — never a call, operator, or literal. */
const FIELD_PATH_RE = new RegExp(`^${IDENT}(?:\\.${IDENT})*$`);
/** `$NAME` and nothing else — no trailing text, no missing sigil. */
const GIVEN_REF_RE = new RegExp(`^\\$(${IDENT})$`);

// Checked in this order, ahead of the `=`-split below, so each rejection
// fires for its own reason rather than falling through to a generic
// "malformed body". Word-bounded (`\b`) so a column named `android_id` or
// `mint_id` doesn't false-positive on `and`/`in` as a substring.
const COMPOUND_BOOLEAN_RE = /\b(and|or|not)\b/i;
const IN_OPERATOR_RE = /\bin\b/i;
const NEGATED_OPERATOR_RE = /!=/;
const COMPARISON_OPERATOR_RE = /(>=|<=|>|<)/;

function rejectionMessage(
   sourceName: string,
   body: string,
   detail: string,
): string {
   return (
      `Source "${sourceName}" declares \`#(partition) ${body}\`: ${detail} ` +
      `#(partition) only accepts \`<column> = $GIVEN\`, where <column> is a ` +
      `single field or a dotted join path.`
   );
}

function reject(
   sourceName: string,
   body: string,
   cause: PartitionAnnotationRejectionCause,
   detail: string,
): never {
   throw new PartitionAnnotationError(
      cause,
      rejectionMessage(sourceName, body, detail),
   );
}

/**
 * Parse one annotation note's body against the `#(partition)` grammar.
 *
 * Returns `null` if the note is not partition-routed at all (an ordinary
 * annotation this function has no opinion on). Throws a {@link
 * PartitionAnnotationError} — never returns a best-effort guess — for a
 * partition-routed note whose body doesn't fit the grammar, so a malformed
 * tag is refused rather than silently dropped.
 */
export function parsePartitionAnnotation(
   sourceName: string,
   annotationText: string,
): PartitionPair | null {
   if (noteRoute(annotationText) !== PARTITION_ROUTE) return null;
   const body = notePayload(annotationText).trim();
   if (body.length === 0) {
      reject(sourceName, body, "empty_body", "the expression body is empty.");
   }
   if (COMPOUND_BOOLEAN_RE.test(body)) {
      reject(
         sourceName,
         body,
         "compound_boolean",
         "a compound boolean (`and`/`or`/`not`) is not allowed — declare one " +
            "`#(partition)` marker per column.",
      );
   }
   if (IN_OPERATOR_RE.test(body)) {
      reject(
         sourceName,
         body,
         "in_operator",
         "the `in` operator is not allowed.",
      );
   }
   if (NEGATED_OPERATOR_RE.test(body)) {
      reject(sourceName, body, "negated_operator", "`!=` is not allowed.");
   }
   if (COMPARISON_OPERATOR_RE.test(body)) {
      reject(
         sourceName,
         body,
         "comparison_operator",
         "only `=` is allowed, not `<`/`>`/`<=`/`>=`.",
      );
   }
   const eq = body.indexOf("=");
   if (eq === -1) {
      reject(sourceName, body, "malformed_body", "no `=` was found.");
   }
   const left = body.slice(0, eq).trim();
   const right = body.slice(eq + 1).trim();
   if (!FIELD_PATH_RE.test(left)) {
      reject(
         sourceName,
         body,
         "left_not_field_path",
         `\`${left}\` is not a field path — the left side must be a single ` +
            "column or a dotted join path, not an expression.",
      );
   }
   const givenMatch = GIVEN_REF_RE.exec(right);
   if (!givenMatch) {
      reject(
         sourceName,
         body,
         "missing_given_reference",
         `\`${right}\` is not a given reference — the right side must be ` +
            "`$NAME`.",
      );
   }
   return { column: left, given: givenMatch[1] };
}

/**
 * Every `#(partition)` pair declared by ONE list of annotation notes,
 * preserving declaration order. Non-partition annotations are ignored.
 * Propagates the throw from a malformed marker.
 *
 * A source may declare several markers scoping on independent axes (e.g. org
 * AND list), so unlike a dedup this keeps every distinct one — but two
 * markers naming the SAME given can only be the same slicing dimension
 * declared twice (or a copy-paste column mistake), never two independent
 * axes, so that combination is refused rather than silently taking the
 * first or last.
 */
export function collectPartitionPairs(
   sourceName: string,
   annotationTexts: readonly string[],
): PartitionPair[] {
   const pairs: PartitionPair[] = [];
   const seenGivens = new Map<string, string>();
   for (const text of annotationTexts) {
      const pair = parsePartitionAnnotation(sourceName, text);
      if (pair === null) continue;
      const priorColumn = seenGivens.get(pair.given);
      if (priorColumn !== undefined) {
         throw new PartitionAnnotationError(
            "duplicate_given",
            `Source "${sourceName}" declares \`#(partition)\` on both ` +
               `\`${priorColumn}\` and \`${pair.column}\` for the same given ` +
               `\`$${pair.given}\` — a given can back at most one partition ` +
               `column per source.`,
         );
      }
      seenGivens.set(pair.given, pair.column);
      pairs.push(pair);
   }
   return pairs;
}
