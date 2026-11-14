// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import type { Given } from "../../client";
import type { GivenValue } from "../../hooks/givenValue";
import { isEmptyStringMeaningful } from "./paramCodec";

/**
 * Whether a given must be set before anything that reads it can run: the
 * server says it declares no default (`required`), and Malloy then refuses
 * the query ("Given 'X' has no value and no default").
 *
 * The explicit flag, not an absent `default`: a server or proxy that does not
 * send `required` leaves every tile running as before, where reading absence
 * would make every given look required on one that drops `default`.
 */
export const isRequiredGiven = (given: Given): boolean =>
   given.required === true;

/**
 * No value yet. An empty string is a value only where the codec says it means
 * something: for a `filter<…>` or a string it is "All", which a reader can
 * pick on a required filter and must then see run; for a number or a date it
 * is nothing.
 */
const unset = (value: GivenValue | undefined, type: string | undefined) =>
   value === undefined ||
   value === null ||
   (value === "" && !isEmptyStringMeaningful(type));

/**
 * The page's required controls that have no value yet, in row order. A tile
 * that reads one is not run; the page asks for them instead.
 */
export function missingRequiredGivens(
   specs: readonly Given[],
   values: ReadonlyMap<string, GivenValue>,
   /** Values the host sets itself, which count as set. */
   hostGivens?: Readonly<Record<string, unknown>>,
): Given[] {
   return specs.filter(
      (spec) =>
         spec.name !== undefined &&
         isRequiredGiven(spec) &&
         unset(values.get(spec.name), spec.type) &&
         (hostGivens?.[spec.name] === undefined ||
            hostGivens[spec.name] === null),
   );
}

/** The required givens among `missing` that a tile reading `reads` waits on. */
export function waitingOn(
   missing: readonly Given[],
   reads: readonly string[] | undefined,
): Given[] {
   if (!reads || missing.length === 0) return [];
   const names = new Set(reads);
   return missing.filter((spec) => spec.name && names.has(spec.name));
}

const labelOf = (given: Given) => given.label ?? given.name ?? "";

function joinAnd(labels: readonly string[]): string {
   return labels.length < 2
      ? labels.join("")
      : `${labels.slice(0, -1).join(", ")} and ${labels[labels.length - 1]}`;
}

/** What the page says while a required filter is unset: "Pick a value for Recall Year to see this report." */
export function requiredPrompt(missing: readonly Given[]): string {
   const labels = missing.map(labelOf);
   return `Pick a value for ${joinAnd(labels)} to see this report.`;
}

/** What a tile waiting on required filters says in place of its result. */
export function waitingText(waiting: readonly Given[]): string {
   return `Waiting for ${joinAnd(waiting.map(labelOf))}.`;
}
