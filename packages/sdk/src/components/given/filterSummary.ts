// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import type { Given } from "../../client";
import type { GivenValue } from "../../hooks/givenValue";
import {
   decodeAtLeast,
   decodeBetween,
   decodeDayRange,
   decodeFilterList,
   decodeTimePreset,
   filterInnerType,
   isFilterType,
   isPlainFilterList,
   TIME_PRESETS,
} from "./filterValue";
import { renderGivenDefault } from "./utils";

/** Longest summary a filter button shows before it shortens to "first +N". */
const MAX_SUMMARY_CHARS = 28;

const DAY = /^\d{4}-\d{2}-\d{2}$/;

const dayFormat = new Intl.DateTimeFormat("en-US", {
   month: "short",
   day: "numeric",
   year: "numeric",
   timeZone: "UTC",
});

/** `2023-01-01` → `Jan 1, 2023`; anything else unchanged. */
function formatDay(text: string): string {
   if (!DAY.test(text)) return text;
   const date = new Date(`${text}T00:00:00Z`);
   return Number.isNaN(date.getTime()) ? text : dayFormat.format(date);
}

/** A list of picked values, shortened to fit a button: `Outerwear +2`. */
function summarizeList(values: string[]): string {
   if (values.length === 0) return "";
   const joined = values.join(", ");
   if (joined.length <= MAX_SUMMARY_CHARS || values.length === 1)
      return truncate(joined);
   return `${truncate(values[0] ?? "")} +${values.length - 1}`;
}

function truncate(text: string): string {
   return text.length > MAX_SUMMARY_CHARS
      ? `${text.slice(0, MAX_SUMMARY_CHARS - 1)}…`
      : text;
}

/** What a given with no value of its own shows: its default, or that it does not narrow. */
function unsetSummary(given: Given): string {
   const type = given.type ?? "string";
   const shown = renderGivenDefault(type, given.default);
   if (shown !== undefined && shown !== "") return summarizeText(type, shown);
   const inner = filterInnerType(type) ?? type;
   return given.control === "select" ||
      given.control === "multiselect" ||
      inner === "string"
      ? "All"
      : "Any";
}

/** A value written as text, read the way its type reads. */
function summarizeText(type: string, text: string): string {
   if (!isFilterType(type)) return truncate(formatDay(text));
   const inner = filterInnerType(type);
   if (inner === "string" && isPlainFilterList(text))
      return summarizeList(decodeFilterList(text));
   if (inner === "number") {
      const between = decodeBetween(text);
      if (between) return `${between[0]}–${between[1]}`;
      const atLeast = decodeAtLeast(text);
      if (atLeast !== undefined) return `≥ ${atLeast}`;
   }
   if (inner === "date" || inner === "timestamp" || inner === "timestamptz") {
      const preset = decodeTimePreset(text);
      if (preset)
         return TIME_PRESETS.find((p) => p.key === preset)?.label ?? text;
      const days = decodeDayRange(text);
      if (days)
         return `${formatDay(days.firstDay)} – ${formatDay(days.lastDay)}`;
   }
   return truncate(text);
}

/**
 * The few words a filter button shows for its given's current value: `All`,
 * `Outerwear +2`, `Jan 1, 2023`, `≥ 50`, `Last 30 days`. Unset, the model
 * default stands in, read the same way, so a default looks like the value it
 * is rather than a hint under the control. The full value is the popover's.
 */
export function summarizeGivenValue(
   given: Given,
   value: GivenValue | undefined,
): string {
   if (value === null || value === undefined) return unsetSummary(given);
   if (value instanceof Date)
      return Number.isNaN(value.getTime()) ? "" : dayFormat.format(value);
   if (typeof value === "boolean") return value ? "Yes" : "No";
   if (typeof value === "number") return String(value);
   if (value === "") return "(empty)";
   return summarizeText(given.type ?? "string", value);
}
