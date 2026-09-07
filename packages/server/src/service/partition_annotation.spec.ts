// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { describe, expect, it } from "bun:test";
import {
   collectPartitionPairs,
   parsePartitionAnnotation,
   PartitionAnnotationError,
   type PartitionAnnotationRejectionCause,
} from "./partition_annotation";

/** Asserts the throw is a {@link PartitionAnnotationError} with this exact cause. */
function expectRejection(
   fn: () => unknown,
   cause: PartitionAnnotationRejectionCause,
): void {
   let thrown: unknown;
   try {
      fn();
   } catch (err) {
      thrown = err;
   }
   expect(thrown).toBeInstanceOf(PartitionAnnotationError);
   expect((thrown as PartitionAnnotationError).rejectionCause).toBe(cause);
}

describe("parsePartitionAnnotation — accept shapes", () => {
   it("returns null for a non-partition-routed note", () => {
      expect(
         parsePartitionAnnotation("orders", "#(authorize) $ROLE = 'a'"),
      ).toBe(null);
      expect(parsePartitionAnnotation("orders", "# bar_chart")).toBe(null);
   });

   it("parses a single-hop column", () => {
      expect(
         parsePartitionAnnotation("orders", "#(partition) tenant = $TENANT"),
      ).toEqual({ column: "tenant", given: "TENANT" });
   });

   it("parses a dotted multi-hop join path", () => {
      // A real customer's canonical model filters on a two-hop path — this is
      // required, not an edge case (see task doc).
      expect(
         parsePartitionAnnotation(
            "orders",
            "#(partition) report_ref.report_id = $REPORT",
         ),
      ).toEqual({ column: "report_ref.report_id", given: "REPORT" });
   });

   it("tolerates the trailing newline Malloy keeps on note text", () => {
      expect(
         parsePartitionAnnotation("orders", "#(partition) tenant = $TENANT\n"),
      ).toEqual({ column: "tenant", given: "TENANT" });
   });

   it("recognizes the ##(partition) file-level spelling and block form the same way authorize does", () => {
      expect(
         parsePartitionAnnotation("orders", "##(partition) tenant = $TENANT"),
      ).toEqual({ column: "tenant", given: "TENANT" });
   });
});

describe("parsePartitionAnnotation — reject shapes", () => {
   it("rejects a negated operator (!=)", () => {
      expectRejection(
         () =>
            parsePartitionAnnotation(
               "orders",
               "#(partition) tenant != $TENANT",
            ),
         "negated_operator",
      );
   });

   it("rejects comparison operators", () => {
      for (const op of [">", "<", ">=", "<="]) {
         expectRejection(
            () =>
               parsePartitionAnnotation(
                  "orders",
                  `#(partition) tenant ${op} $TENANT`,
               ),
            "comparison_operator",
         );
      }
   });

   it("rejects `in`", () => {
      expectRejection(
         () =>
            parsePartitionAnnotation(
               "orders",
               "#(partition) tenant in $TENANTS",
            ),
         "in_operator",
      );
   });

   it("does not false-positive `in`/`and` inside an identifier", () => {
      // "android_id" contains "and"; "domain" contains "in" — neither is the
      // keyword, and a naive substring match would wrongly reject both.
      expect(
         parsePartitionAnnotation("orders", "#(partition) android_id = $A"),
      ).toEqual({ column: "android_id", given: "A" });
      expect(
         parsePartitionAnnotation("orders", "#(partition) domain = $D"),
      ).toEqual({ column: "domain", given: "D" });
   });

   it("rejects a compound boolean (and/or/not)", () => {
      expectRejection(
         () =>
            parsePartitionAnnotation(
               "orders",
               "#(partition) tenant = $A and region = $B",
            ),
         "compound_boolean",
      );
      expectRejection(
         () =>
            parsePartitionAnnotation(
               "orders",
               "#(partition) tenant = $A or region = $B",
            ),
         "compound_boolean",
      );
      expectRejection(
         () =>
            parsePartitionAnnotation("orders", "#(partition) not tenant = $A"),
         "compound_boolean",
      );
   });

   it("rejects an expression on the left rather than a field path", () => {
      expectRejection(
         () =>
            parsePartitionAnnotation(
               "orders",
               "#(partition) upper(tenant) = $A",
            ),
         "left_not_field_path",
      );
      expectRejection(
         () =>
            parsePartitionAnnotation("orders", "#(partition) tenant + 1 = $A"),
         "left_not_field_path",
      );
   });

   it("rejects a missing `$` on the right", () => {
      expectRejection(
         () =>
            parsePartitionAnnotation("orders", "#(partition) tenant = TENANT"),
         "missing_given_reference",
      );
      expectRejection(
         () => parsePartitionAnnotation("orders", "#(partition) tenant = 'x'"),
         "missing_given_reference",
      );
      expectRejection(
         () => parsePartitionAnnotation("orders", "#(partition) tenant = 5"),
         "missing_given_reference",
      );
   });

   it("rejects a malformed body with no operator at all", () => {
      expectRejection(
         () => parsePartitionAnnotation("orders", "#(partition) tenant"),
         "malformed_body",
      );
   });

   it("rejects an empty body", () => {
      expectRejection(
         () => parsePartitionAnnotation("orders", "#(partition)"),
         "empty_body",
      );
   });

   it("every rejection names the source and quotes the offending body", () => {
      try {
         parsePartitionAnnotation("tenant_orders", "#(partition) tenant != $T");
         throw new Error("expected a throw");
      } catch (err) {
         expect((err as Error).message).toContain('"tenant_orders"');
         expect((err as Error).message).toContain("tenant != $T");
      }
   });
});

describe("collectPartitionPairs", () => {
   it("collects multiple markers scoping on independent axes", () => {
      expect(
         collectPartitionPairs("orders", [
            "#(partition) org_id = $ORG",
            "#(partition) list_id = $LIST",
         ]),
      ).toEqual([
         { column: "org_id", given: "ORG" },
         { column: "list_id", given: "LIST" },
      ]);
   });

   it("ignores non-partition annotations interleaved with real ones", () => {
      expect(
         collectPartitionPairs("orders", [
            "# some_render_tag",
            "#(partition) org_id = $ORG",
            "#(authorize) $ROLE = 'a'",
         ]),
      ).toEqual([{ column: "org_id", given: "ORG" }]);
   });

   it("rejects two markers naming the same given", () => {
      expectRejection(
         () =>
            collectPartitionPairs("orders", [
               "#(partition) org_id = $TENANT",
               "#(partition) alt_org_id = $TENANT",
            ]),
         "duplicate_given",
      );
   });

   it("returns [] for a source with no partition markers", () => {
      expect(collectPartitionPairs("orders", ["# a_render_tag"])).toEqual([]);
   });
});
