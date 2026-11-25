// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * Where a rollup's table is placed. A leaf: the two rollup readers
 * (`#@ preaggregate` and `#(rollup)`) and the inheritance from a base's
 * `name=` all apply it.
 *
 * A rollup's `name=` places and prefixes its table: everything before its
 * last dot is the schema or dataset the table is created in, and its leaf
 * takes the place of the readable prefix of the name Publisher gives the
 * table, as a `name=` does under `scope: "version"`; the name's identity and
 * tail stay Publisher's. `namespace=` is the container's older, deprecated
 * spelling, read for one more release.
 */
import { hasReservedTableSuffix, readableSegment } from "../domain/table_name";

/**
 * One segment of a container: a plain identifier, plus the hyphen.
 *
 * {@link assertSafeSqlIdentifier}'s `[A-Za-z_][A-Za-z0-9_$]*` plus the hyphen:
 * that rule excludes hyphens because BigQuery never reaches it, but a rollup
 * container does reach BigQuery, where `my-project.analytics` is ordinary.
 *
 * Everything else is out: a space or a quote cannot be spliced bare, and a name
 * needing quotes cannot be joined to a generated table name.
 */
const NAMESPACE_SEGMENT = /^[A-Za-z_][A-Za-z0-9_$-]*$/;

/**
 * Whether every dot-separated segment can be spliced into a generated name.
 *
 * Dots are the one separator that survives: BigQuery addresses a dataset as
 * `project.dataset`, so a container is a path, not a single identifier.
 *
 * An empty string is refused by the per-segment test, which `""` fails.
 */
function isSpliceableNamespace(namespace: string): boolean {
   return namespace.split(".").every((s) => NAMESPACE_SEGMENT.test(s));
}

/**
 * The container of a `name=`: everything before its LAST dot.
 *
 * `analytics.orders` yields `analytics`; `proj.analytics.orders` yields
 * `proj.analytics`; a bare `orders` yields undefined. Split on the last dot
 * because BigQuery names can carry a project as well as a dataset.
 *
 * **A quoted name yields nothing, deliberately.** `"My.Schema"` is one
 * identifier the last-dot rule would tear apart, and even `"A"."B"` would join
 * a quoted prefix to an unquoted segment, which the CREATE and bind sides quote
 * differently. A trailing dot yields nothing either: the leaf is empty, so the
 * name is malformed and inventing a container from it would hide that.
 */
export function nameContainer(name: string | undefined): string | undefined {
   if (!name) return undefined;
   const lastDot = name.lastIndexOf(".");
   if (lastDot <= 0) return undefined;
   if (name.slice(lastDot + 1).trim() === "") return undefined;
   const candidate = name.slice(0, lastDot);
   return isSpliceableNamespace(candidate) ? candidate : undefined;
}

/** A rollup line's placement key, read: its container, or why it is invalid. */
type RollupContainer =
   | {
        container?: string;
        /** The `name=`'s leaf: the readable prefix of the table's name. */
        prefix?: string;
        /** Read from the deprecated `namespace=`. */
        deprecatedNamespace?: true;
     }
   | { invalid: string };

/**
 * Where one rollup line places its table and what its name starts with,
 * from its `name=` (its container, if any, and its leaf, the readable
 * prefix) or its deprecated `namespace=` (a container only), never both; or
 * why the line is invalid: a `name=` whose container is not plain names, an
 * empty leaf, or a leaf ending the way Publisher's own names do; a
 * `namespace=` that is not plain dot-separated names. Neither key: nothing
 * of its own.
 */
export function readRollupContainer(keys: {
   name?: string;
   namespace?: string;
}): RollupContainer {
   const { name, namespace } = keys;
   if (name !== undefined && namespace !== undefined) {
      return {
         invalid:
            "names both name= and namespace=; namespace= is the deprecated spelling of name='s container, so keep only name=",
      };
   }
   if (name !== undefined) {
      const written = name.trim();
      const dot = written.lastIndexOf(".");
      const prefix = written.slice(dot + 1).trim();
      const container = dot > 0 ? nameContainer(written) : undefined;
      if (prefix === "" || (dot >= 0 && container === undefined)) {
         return {
            invalid: `has name="${name}", which is not a plain name: write a table name, with the schema or dataset to place it in before a dot if any (name="analytics.orders_rollup")`,
         };
      }
      // Written into the name as it reads, lowercased only: a leaf the name
      // would rewrite (a space, punctuation, a leading digit, `__`) is
      // refused rather than changed without notice.
      if (readableSegment(prefix) !== prefix.toLowerCase()) {
         return {
            invalid: `has name="${name}", whose table name is not a plain name: use letters, digits and single underscores, starting with a letter (it is lowercased in the table's name)`,
         };
      }
      if (hasReservedTableSuffix(prefix)) {
         return {
            invalid: `has name="${name}", whose table name ends the way Publisher's own table names do (\`__p<8 hex>\`); choose another`,
         };
      }
      return { ...(container ? { container } : {}), prefix };
   }
   if (namespace !== undefined) {
      const container = namespace.trim();
      return isSpliceableNamespace(container)
         ? { container, deprecatedNamespace: true }
         : {
              invalid: `has namespace="${namespace}", which is not plain dot-separated names`,
           };
   }
   return {};
}

/** The rewrite of a deprecated `namespace=`, for its warning. */
export function namespaceRewrite(container: string): string {
   return `namespace= is deprecated: write name="${container}.<table>" instead (its schema or dataset places the table, and <table> starts the table's name)`;
}
