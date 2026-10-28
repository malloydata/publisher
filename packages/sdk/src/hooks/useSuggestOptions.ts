// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { useQueries } from "@tanstack/react-query";
import { useMemo, useRef } from "react";
import type { Given } from "../client";
import { withPreamble } from "../components/Dashboard/textSource";
import { malloyIdentifier } from "../components/ModelViewer/malloyRunText";
import {
   givensToRequest,
   withHostGivens,
} from "../components/given/paramCodec";
import { useServer } from "../components/ServerProvider";
import type { GivenValue, HostGivenValue } from "./givenValue";

/**
 * How many options a generated `suggest { source=… dimension=… }` asks for.
 *
 * Stated here rather than left to the server's `DEFAULT_QUERY_ROW_LIMIT`, which
 * is also 1000 but caps silently and in whatever order the warehouse returns.
 * An explicit limit with an explicit order makes a truncated list the same list
 * every time, and makes the cap something a reader of this file can see.
 * The query asks for one row more than this, so a list that was cut off is
 * known to be (`truncated`), not guessed from a count that happens to land on
 * the limit. Past the limit, typing in the picker searches the whole dimension
 * on the server instead of filtering the first thousand: see
 * {@link OptionSearch}.
 */
export const SUGGEST_OPTION_LIMIT = 1000;

/** How many matches a search of the server returns: enough to scan, and the hint says to keep typing past it. */
export const SUGGEST_SEARCH_LIMIT = 200;

/** One fetch of options: the values, and whether there were more than it holds. */
export interface OptionPage {
   values: string[];
   /** More rows matched than the page holds, so values past it are not offered. */
   truncated: boolean;
}

/**
 * Search a truncated option list on the server: for a control whose suggest
 * came back cut off at {@link SUGGEST_OPTION_LIMIT}, so filtering in the
 * browser would only ever find values in the first thousand.
 *
 * `key` identifies the suggest (its report, version, source, dimension, the
 * givens it carries and a text source's definitions); a caller caches each
 * search under it plus the text searched for. `run` sends the same suggest with
 * a case-insensitive "contains" on the dimension.
 *
 * Only a suggest over a dimension has one: a named `suggest { query=… }` is a
 * whole query, with nowhere to add a condition, so its list is filtered in the
 * browser and says it was cut off.
 */
export interface OptionSearch {
   key: readonly unknown[];
   run: (text: string, signal?: AbortSignal) => Promise<OptionPage>;
}

/**
 * The name a `group_by` gives its output column, which is the last segment of a
 * field path.
 *
 * `order_by` takes an output field name, not a path: `order_by:
 * products.category` is refused outright ("order_by takes the name of a field in
 * the query output, not a path"), while `group_by: products.category` names its
 * column `category`. A joined dimension is the ordinary case for a `suggest`, so
 * getting this wrong would have failed the option query for most of them.
 */
function outputFieldName(dimension: string): string {
   const segments = dimension.split(".");
   return segments[segments.length - 1];
}

/**
 * A name as the query writes it: backtick-quoted, so a reserved word (`year`,
 * `source`) or a name with a space is still read as a name. One the author
 * already quoted is kept as written.
 */
function quotedName(name: string): string {
   return /^`.*`$/.test(name) ? name : malloyIdentifier(name);
}

/** A field path with each of its segments quoted ({@link quotedName}). */
function quotedPath(path: string): string {
   return path.split(".").map(quotedName).join(".");
}

/**
 * The query a `suggest { source=… dimension=… }` runs.
 *
 * Exported so the spec can assert the text this hook actually sends. A test that
 * rebuilds the same string alongside it proves only that the test agrees with
 * itself, and this text has already been wrong once: `order_by` takes an output
 * field name, not a path, so a joined dimension failed the query outright.
 */
export function buildSuggestQuery(
   source: string,
   dimension: string,
   /** Text the values must contain, ignoring case: a search of a truncated list. */
   search?: string,
): string {
   // One row past the limit, so a list that was cut off says so.
   const limit =
      (search === undefined ? SUGGEST_OPTION_LIMIT : SUGGEST_SEARCH_LIMIT) + 1;
   const field = quotedPath(dimension);
   return (
      `run: ${quotedName(source)} -> {\n` +
      `  group_by: ${field}\n` +
      (search === undefined
         ? ""
         : `  where: ${containsCondition(field, search)}\n`) +
      `  order_by: ${quotedName(outputFieldName(dimension))} asc\n` +
      `  limit: ${limit}\n` +
      `}`
   );
}

/**
 * "The dimension contains this text, ignoring case", in Malloy every dialect
 * compiles: `strpos` and `lower` are standard functions each dialect maps to
 * its own SQL (`STRPOS`, `POSITION`, `LOCATE`, `CHARINDEX`). Not a regular
 * expression, whose inline `(?i)` Snowflake does not read, and not `LIKE`,
 * whose wildcards (`%`, `_`) a value can contain and whose escape character
 * differs by database: the text is a plain string literal, matched as written.
 *
 * Cast to string so a numeric or date dimension searches its text too. The text
 * is lowercased here, where it is known, and the column in SQL.
 */
function containsCondition(field: string, search: string): string {
   return `strpos(lower(${field}::string), ${malloyStringLiteral(search.toLowerCase())}) > 0`;
}

/**
 * Text as a Malloy string literal, whatever it holds: a backslash and a
 * single quote escaped, in that order, so neither can end the literal early.
 * Control characters are dropped: a search box has no use for them, and a raw
 * newline is not legal inside a literal. Nothing else is special inside
 * single quotes, so `%`, `_`, `{`, `$` and the rest stay literal text.
 */
export function malloyStringLiteral(text: string): string {
   const escaped = text
      // eslint-disable-next-line no-control-regex
      .replace(/[\u0000-\u001f\u007f]/g, "")
      .replace(/\\/g, "\\\\")
      .replace(/'/g, "\\'");
   return `'${escaped}'`;
}

/**
 * Resolve the option lists behind `select` / `multiselect` controls.
 *
 * A `suggest` is an ordinary query: either a named one (`suggest { query=… }`)
 * or a grouping over a dimension (`suggest { source=… dimension=… }`), so this
 * runs it on the same governed query endpoint everything else uses. No new
 * endpoint, and the row caps and authorize gates that apply to a surface's own
 * queries apply to its dropdowns too.
 *
 * Takes plain `Given`s: `suggest` is declared on the given, so a notebook's
 * Parameters panel and a dashboard's control row fill the same dropdown from
 * the same declaration.
 *
 * Requested with `compactJson` because these results are never rendered: the
 * flat row form is what the widget needs, and asking for the full Malloy result
 * would mean parsing rendering metadata to get at a list of strings.
 *
 * The request carries the givens the suggest query NEEDS and no others. An
 * `#(authorize)` gate is evaluated against the request's givens, so a suggest
 * over a gated source was denied outright and its dropdown read "Options
 * unavailable". Sending every applied value would have fixed that but made the
 * option list depend on the page's other filters, which this hook's caching
 * deliberately avoids. So the server says, per suggest, which givens its source
 * is gated or scoped by (`suggest.givenNames`), and {@link suggestRequestGivens}
 * sends the current values of exactly those; the cache is keyed on them too.
 * A server too old to name them sends none, and the request is as before.
 */
/**
 * The current values a suggest query has to carry: the applied values of the
 * givens the server named on it, encoded for the request. Undefined when the
 * suggest names none, or when the caller has no applied values to offer.
 */
export function suggestRequestGivens(
   spec: Given,
   applied: AppliedGivens | undefined,
): Record<string, unknown> | undefined {
   const names = spec.suggest?.givenNames;
   if (!applied || !names || names.length === 0) return undefined;
   const request = withHostGivens(
      givensToRequest(applied.values, applied.declaredTypes, names),
      // A control's own value wins, so only what no control declares is added.
      hostOnly(applied),
      names,
   );
   return Object.keys(request).length > 0 ? request : undefined;
}

/** The applied control row and the declared types that encode it. */
export interface AppliedGivens {
   values: ReadonlyMap<string, GivenValue>;
   declaredTypes: ReadonlyMap<string, string | undefined>;
   /** Givens the host sets with no control: a gated source's suggest needs them, and they may be lists. */
   host?: Readonly<Record<string, HostGivenValue>>;
}

function hostOnly(
   applied: AppliedGivens,
): Record<string, HostGivenValue> | undefined {
   if (!applied.host) return undefined;
   return Object.fromEntries(
      Object.entries(applied.host).filter(
         ([name]) => !applied.declaredTypes.has(name),
      ),
   );
}

export function useSuggestOptions(
   environmentName: string,
   packageName: string,
   modelPath: string | undefined,
   specs: Given[],
   // Trailing and optional because this hook is exported from the package, so
   // a slot beside `packageName` where it belongs would break every caller.
   versionId?: string,
   /**
    * The page's applied values, so a suggest can carry the givens its source
    * is gated or scoped by. Optional for the same reason as `versionId`; a
    * caller that omits it gets the pre-gating behaviour.
    */
   applied?: AppliedGivens,
   /**
    * Text-source mode: the document's definitions, sent ahead of the suggest as
    * the viewer's own text. Absent, a suggest runs by name or by view as before.
    */
   preamble?: string,
   /**
    * Only these givens' options are fetched, for a page that loads a picker's
    * list when it is opened rather than on render (the model viewer, where
    * nothing runs until the reader asks). Absent, every picker's list is.
    */
   only?: ReadonlySet<string>,
): {
   options: Map<string, string[]>;
   isLoading: boolean;
   /**
    * Givens whose suggest query failed.
    *
    * Reported separately from an empty option list because the two look
    * identical in a dropdown and mean opposite things: "this dimension has no
    * values" is data, "the query did not run" is a fault, and a reader offered
    * an empty picker has no way to tell which they are looking at.
    */
   failed: Set<string>;
   /**
    * Givens whose list was cut off: more values exist than were fetched. For
    * a suggest over a dimension, `search` can find the rest; for a named
    * query it cannot, and the picker says the list is partial.
    */
   truncated: Set<string>;
   /** Server search, per given, for a truncated suggest over a dimension. */
   search: Map<string, OptionSearch>;
} {
   const { apiClients } = useServer();

   // Only a picker needs options; a text box or a slider would just be issuing
   // a query whose answer it never shows.
   const suggestable = useMemo(
      () =>
         specs.filter(
            (spec) =>
               spec.name !== undefined &&
               (spec.control === "select" || spec.control === "multiselect") &&
               (spec.suggest?.query !== undefined ||
                  (spec.suggest?.source !== undefined &&
                     spec.suggest?.dimension !== undefined)),
         ),
      [specs],
   );

   // What identifies each suggest, and how to run it with or without a search:
   // one definition, so the list and its searches always send the same query
   // and carry the same givens.
   const suggests = suggestable.map((spec) => {
      const givens = suggestRequestGivens(spec, applied);
      const suggest = spec.suggest ?? {};
      const identity = [
         environmentName,
         packageName,
         versionId,
         modelPath,
         spec.name,
         suggest.query,
         suggest.source,
         suggest.dimension,
         // Only the givens the suggest carries, so a change to any OTHER
         // control leaves the cached list alone.
         givens === undefined ? null : JSON.stringify(givens),
         // Only a text source keys on its definitions, so no other key changes shape.
         ...(preamble === undefined ? [] : [preamble]),
      ];
      const run = async (
         search: string | undefined,
         signal?: AbortSignal,
      ): Promise<OptionPage> => {
         const named = suggest.query !== undefined;
         const response = await apiClients.models.executeQueryModel(
            environmentName,
            packageName,
            modelPath as string,
            named && preamble === undefined
               ? {
                    queryName: suggest.query,
                    compactJson: true,
                    versionId,
                    givens,
                 }
               : {
                    query: withPreamble(
                       preamble ?? "",
                       named
                          ? `run: ${suggest.query}`
                          : buildSuggestQuery(
                               suggest.source as string,
                               suggest.dimension as string,
                               search,
                            ),
                    ),
                    compactJson: true,
                    versionId,
                    givens,
                 },
            undefined,
            { signal },
         );
         return readOptionPage(
            response.data.result,
            suggest.dimension === undefined
               ? undefined
               : outputFieldName(suggest.dimension),
            // A generated query asked for one row past its limit. A named
            // query's own limit is unknown: a full server cap is the best sign
            // it was cut off.
            named
               ? { limit: SUGGEST_OPTION_LIMIT, cutOffAt: SUGGEST_OPTION_LIMIT }
               : {
                    limit:
                       search === undefined
                          ? SUGGEST_OPTION_LIMIT
                          : SUGGEST_SEARCH_LIMIT,
                 },
         );
      };
      return { spec, identity, run, searchable: suggest.query === undefined };
   });

   const results = useQueries({
      queries: suggests.map(({ spec, identity, run }) => {
         return {
            queryKey: ["givenSuggest", ...identity],
            enabled:
               modelPath !== undefined &&
               (only === undefined || only.has(spec.name as string)),
            // Option lists change with the data, not with the filters, so they
            // are cached well past a single control interaction.
            staleTime: 5 * 60 * 1000,
            refetchOnWindowFocus: false,
            queryFn: ({ signal }: { signal: AbortSignal }) =>
               run(undefined, signal),
         };
      }),
   });

   const options = useMemo(() => {
      const byName = new Map<string, string[]>();
      suggestable.forEach((spec, index) => {
         const value = results[index]?.data;
         if (spec.name && value) byName.set(spec.name, value.values);
      });
      return byName;
      // `results` is a fresh array each render; its data identity is what
      // matters, so depend on that rather than the wrapper objects.
      //
      // `JSON.stringify`, not `join("|")`. `join` flattens a nested array with
      // commas and renders undefined as empty, so genuinely different option
      // lists produced the SAME key: `[["a","b"]]` and `[["a,b"]]` both give
      // `a,b`, and `[undefined]`, `[[]]` and `[[""]]` all give `""`. A refetch
      // between two colliding lists left the previous dropdown in place.
      // eslint-disable-next-line react-hooks/exhaustive-deps
   }, [suggestable, JSON.stringify(results.map((result) => result.data))]);

   const failed = useMemo(() => {
      const names = new Set<string>();
      suggestable.forEach((spec, index) => {
         if (spec.name && results[index]?.isError) names.add(spec.name);
      });
      return names;
      // Same reasoning as `options`: `results` is a fresh array each render.
      // eslint-disable-next-line react-hooks/exhaustive-deps
   }, [suggestable, results.map((result) => result.isError).join("|")]);

   // Same reasoning as `options`: compared by what each list says, not by the
   // fresh wrapper objects.
   const truncatedKey = results
      .map((result) => !!result.data?.truncated)
      .join("|");
   const truncated = useMemo(() => {
      const names = new Set<string>();
      suggestable.forEach((spec, index) => {
         if (spec.name && results[index]?.data?.truncated) names.add(spec.name);
      });
      return names;
      // eslint-disable-next-line react-hooks/exhaustive-deps
   }, [suggestable, truncatedKey]);

   // A handle per truncated suggest over a dimension. Rebuilt only when what
   // identifies a suggest changes, so a picker searching with one does not
   // start over on every render.
   const identities = JSON.stringify(suggests.map((s) => s.identity));
   const latestRuns = useRef(suggests);
   latestRuns.current = suggests;
   const search = useMemo(() => {
      const byName = new Map<string, OptionSearch>();
      suggests.forEach(({ spec, identity, searchable }, index) => {
         if (!spec.name || !searchable || !truncated.has(spec.name)) return;
         byName.set(spec.name, {
            key: ["givenSuggestSearch", ...identity],
            run: (text, signal) => latestRuns.current[index].run(text, signal),
         });
      });
      return byName;
      // eslint-disable-next-line react-hooks/exhaustive-deps
   }, [identities, truncated]);

   return {
      options,
      isLoading: results.some((result) => result.isLoading),
      failed,
      truncated,
      search,
   };
}

/**
 * One fetch's options and whether it was cut off. `limit` is what the page
 * holds; a generated query asked for one row more, so a row past it means
 * more exist. `cutOffAt` instead marks a page as cut off once it reaches that
 * many rows, for a named query whose own limit is unknown.
 */
export function readOptionPage(
   result: string | undefined,
   dimension: string | undefined,
   { limit, cutOffAt }: { limit: number; cutOffAt?: number },
): OptionPage {
   const rows = readRows(result);
   return {
      values: optionValues(rows.slice(0, limit), dimension),
      truncated:
         cutOffAt === undefined ? rows.length > limit : rows.length >= cutOffAt,
   };
}

/**
 * Pull the option strings out of a compact-JSON result.
 *
 * Prefers the named column and otherwise takes the first, which covers a
 * `suggest { query=… }` whose column is named something else. Nulls are
 * dropped: "no value" is expressed by clearing the control, not by picking an
 * empty option.
 *
 * The name to pass is the OUTPUT column, so a joined `products.category` is
 * looked up as `category`. Passing the path found nothing and fell through to
 * the first column, which happened to be right only because the generated query
 * selects exactly one.
 */
export function readOptionValues(
   result: string | undefined,
   dimension: string | undefined,
): string[] {
   return optionValues(readRows(result), dimension);
}

/** A compact-JSON result as rows, or a thrown error when it is not one. */
function readRows(result: string | undefined): unknown[] {
   // Absent is tolerated: `result` is optional in the API contract, so a
   // missing one is a shape the server is allowed to send and "no values" is
   // the honest reading of it.
   if (!result) return [];

   // Corrupt is NOT tolerated, and the difference matters. This hook exists to
   // separate "this dimension has no values" from "the option query failed",
   // and returning `[]` for a response that could not be read collapsed the
   // second into the first: the control rendered "No matching values" and told
   // the reader a populated dimension was empty. Thrown, so react-query marks
   // the query errored and `failed` reports it, which is what puts "Options
   // unavailable" on the control.
   let rows: unknown;
   try {
      rows = JSON.parse(result);
   } catch {
      throw new Error("Suggest query returned a result that is not JSON.");
   }
   if (!Array.isArray(rows)) {
      throw new Error(
         "Suggest query returned JSON that is not a list of rows.",
      );
   }
   return rows;
}

/** The option strings in some rows: see {@link readOptionValues}. */
function optionValues(
   rows: unknown[],
   dimension: string | undefined,
): string[] {
   const values: string[] = [];
   const seen = new Set<string>();
   for (const row of rows) {
      if (row === null || typeof row !== "object") continue;
      const record = row as Record<string, unknown>;
      const keys = Object.keys(record);
      // `hasOwnProperty`, not `in`: `record` comes from `JSON.parse`, so `in`
      // walks its prototype and reports `constructor`, `toString` and friends
      // as present. A dimension with one of those names would select a
      // function off the prototype instead of falling back to the first
      // column. The same hazard was fixed in the app's URL merge; this is the
      // other place the rule lives.
      const key =
         dimension !== undefined &&
         Object.prototype.hasOwnProperty.call(record, dimension)
            ? dimension
            : keys[0];
      const value = key === undefined ? undefined : record[key];
      if (value === null || value === undefined) continue;
      const text = String(value);
      if (seen.has(text)) continue;
      seen.add(text);
      values.push(text);
   }
   return values;
}
