import { describe, expect, it } from "bun:test";
import {
   buildFilterClause,
   injectFilterRefinement,
   parseSourceFilterAnnotation,
   parseSourceFilters,
   SourceFilterValidationError,
   type SourceFilterDefinition,
   type SourceFilterParams,
} from "./source_filter";

describe("service/source_filter", () => {
   // -----------------------------------------------------------------------
   // parseSourceFilterAnnotation
   // -----------------------------------------------------------------------
   describe("parseSourceFilterAnnotation", () => {
      it("returns null for non-source_filter annotations", () => {
         expect(parseSourceFilterAnnotation("#(doc) Some docs")).toBeNull();
         expect(parseSourceFilterAnnotation("# bar_chart")).toBeNull();
         expect(parseSourceFilterAnnotation("")).toBeNull();
      });

      it("parses a minimal annotation (dimension + type)", () => {
         const result = parseSourceFilterAnnotation(
            "#(source_filter) dimension=status type=equal",
         );
         expect(result).toEqual({
            name: "status",
            dimension: "status",
            type: "equal",
            implicit: false,
            required: false,
         });
      });

      it("parses all fields including name, implicit, required", () => {
         const result = parseSourceFilterAnnotation(
            '#(source_filter) name="Customer ID" dimension=customer_id type=equal implicit required',
         );
         expect(result).toEqual({
            name: "Customer ID",
            dimension: "customer_id",
            type: "equal",
            implicit: true,
            required: true,
         });
      });

      it("parses type=in", () => {
         const result = parseSourceFilterAnnotation(
            "#(source_filter) dimension=region type=in",
         );
         expect(result).toEqual({
            name: "region",
            dimension: "region",
            type: "in",
            implicit: false,
            required: false,
         });
      });

      it("parses type=like", () => {
         const result = parseSourceFilterAnnotation(
            "#(source_filter) dimension=name type=like",
         );
         expect(result!.type).toBe("like");
      });

      it("parses type=greater_than", () => {
         const result = parseSourceFilterAnnotation(
            "#(source_filter) dimension=created_at type=greater_than",
         );
         expect(result!.type).toBe("greater_than");
      });

      it("parses type=less_than", () => {
         const result = parseSourceFilterAnnotation(
            "#(source_filter) dimension=created_at type=less_than",
         );
         expect(result!.type).toBe("less_than");
      });

      it("parses required without implicit", () => {
         const result = parseSourceFilterAnnotation(
            "#(source_filter) dimension=tenant_id type=equal required",
         );
         expect(result).toEqual({
            name: "tenant_id",
            dimension: "tenant_id",
            type: "equal",
            implicit: false,
            required: true,
         });
      });

      it("handles single-quoted name values", () => {
         const result = parseSourceFilterAnnotation(
            "#(source_filter) name='My Filter' dimension=col type=equal",
         );
         expect(result!.name).toBe("My Filter");
      });

      it("handles extra whitespace", () => {
         const result = parseSourceFilterAnnotation(
            "  #(source_filter)   dimension=status   type=equal   required  ",
         );
         expect(result).toEqual({
            name: "status",
            dimension: "status",
            type: "equal",
            implicit: false,
            required: true,
         });
      });

      it("throws on missing dimension", () => {
         expect(() =>
            parseSourceFilterAnnotation("#(source_filter) type=equal"),
         ).toThrow("missing required 'dimension'");
      });

      it("throws on missing type", () => {
         expect(() =>
            parseSourceFilterAnnotation("#(source_filter) dimension=status"),
         ).toThrow("missing required 'type'");
      });

      it("throws on invalid type", () => {
         expect(() =>
            parseSourceFilterAnnotation(
               "#(source_filter) dimension=status type=banana",
            ),
         ).toThrow('Invalid source_filter type "banana"');
      });

      it("throws on unknown parameter", () => {
         expect(() =>
            parseSourceFilterAnnotation(
               "#(source_filter) dimension=status type=equal foo=bar",
            ),
         ).toThrow('Unknown source_filter parameter "foo"');
      });

      it("throws on unknown flag", () => {
         expect(() =>
            parseSourceFilterAnnotation(
               "#(source_filter) dimension=status type=equal banana",
            ),
         ).toThrow('Unknown source_filter flag "banana"');
      });
   });

   // -----------------------------------------------------------------------
   // parseSourceFilters
   // -----------------------------------------------------------------------
   describe("parseSourceFilters", () => {
      it("extracts source_filter annotations from a mixed list", () => {
         const annotations = [
            "#(doc) This is a source for orders",
            "#(source_filter) dimension=status type=equal",
            "# bar_chart",
            "#(source_filter) dimension=region type=in required",
         ];
         const filters = parseSourceFilters(annotations);
         expect(filters).toHaveLength(2);
         expect(filters[0].dimension).toBe("status");
         expect(filters[1].dimension).toBe("region");
         expect(filters[1].required).toBe(true);
      });

      it("returns empty array when no source_filter annotations", () => {
         const filters = parseSourceFilters(["#(doc) some docs", "# hidden"]);
         expect(filters).toHaveLength(0);
      });
   });

   // -----------------------------------------------------------------------
   // buildFilterClause
   // -----------------------------------------------------------------------
   describe("buildFilterClause", () => {
      const equalFilter: SourceFilterDefinition = {
         name: "status",
         dimension: "status",
         type: "equal",
         implicit: false,
         required: false,
      };

      const inFilter: SourceFilterDefinition = {
         name: "region",
         dimension: "region",
         type: "in",
         implicit: false,
         required: false,
      };

      const likeFilter: SourceFilterDefinition = {
         name: "name_search",
         dimension: "customer_name",
         type: "like",
         implicit: false,
         required: false,
      };

      const gtFilter: SourceFilterDefinition = {
         name: "start_date",
         dimension: "created_at",
         type: "greater_than",
         implicit: false,
         required: false,
      };

      const ltFilter: SourceFilterDefinition = {
         name: "end_date",
         dimension: "created_at",
         type: "less_than",
         implicit: false,
         required: false,
      };

      const requiredFilter: SourceFilterDefinition = {
         name: "tenant_id",
         dimension: "tenant_id",
         type: "equal",
         implicit: true,
         required: true,
      };

      it("returns empty string when no params provided", () => {
         const clause = buildFilterClause([equalFilter], {});
         expect(clause).toBe("");
      });

      it("returns empty string when param is empty string", () => {
         const clause = buildFilterClause([equalFilter], { status: "" });
         expect(clause).toBe("");
      });

      it("returns empty string when param is empty array", () => {
         const clause = buildFilterClause([inFilter], { region: [] });
         expect(clause).toBe("");
      });

      it("builds equal predicate", () => {
         const clause = buildFilterClause([equalFilter], {
            status: "active",
         });
         expect(clause).toBe("`status` = 'active'");
      });

      it("equal uses first element if given array", () => {
         const clause = buildFilterClause([equalFilter], {
            status: ["active", "pending"],
         });
         expect(clause).toBe("`status` = 'active'");
      });

      it("builds in predicate with single value", () => {
         const clause = buildFilterClause([inFilter], {
            region: ["US"],
         });
         expect(clause).toBe("`region` = 'US'");
      });

      it("builds in predicate with multiple values", () => {
         const clause = buildFilterClause([inFilter], {
            region: ["US", "EU", "APAC"],
         });
         expect(clause).toBe(
            "(`region` = 'US' or `region` = 'EU' or `region` = 'APAC')",
         );
      });

      it("builds like predicate", () => {
         const clause = buildFilterClause([likeFilter], {
            name_search: "%smith%",
         });
         expect(clause).toBe("`customer_name` ~ '%smith%'");
      });

      it("builds greater_than predicate", () => {
         const clause = buildFilterClause([gtFilter], {
            start_date: "2024-01-01",
         });
         expect(clause).toBe("`created_at` > @2024-01-01");
      });

      it("builds less_than predicate", () => {
         const clause = buildFilterClause([ltFilter], {
            end_date: "2024-12-31",
         });
         expect(clause).toBe("`created_at` < @2024-12-31");
      });

      it("combines multiple filters with AND", () => {
         const params: SourceFilterParams = {
            status: "active",
            region: ["US", "EU"],
         };
         const clause = buildFilterClause([equalFilter, inFilter], params);
         expect(clause).toBe(
            "`status` = 'active' and (`region` = 'US' or `region` = 'EU')",
         );
      });

      it("skips optional filters with no value", () => {
         const params: SourceFilterParams = {
            status: "active",
         };
         const clause = buildFilterClause([equalFilter, inFilter], params);
         expect(clause).toBe("`status` = 'active'");
      });

      it("throws on missing required filter", () => {
         expect(() => buildFilterClause([requiredFilter], {})).toThrow(
            SourceFilterValidationError,
         );
         expect(() => buildFilterClause([requiredFilter], {})).toThrow(
            'Required filter "tenant_id"',
         );
      });

      it("builds clause for required filter when value provided", () => {
         const clause = buildFilterClause([requiredFilter], {
            tenant_id: "abc123",
         });
         expect(clause).toBe("`tenant_id` = 'abc123'");
      });

      it("escapes single quotes in values", () => {
         const clause = buildFilterClause([equalFilter], {
            status: "it's active",
         });
         expect(clause).toBe("`status` = 'it\\'s active'");
      });

      it("escapes backslashes in values", () => {
         const clause = buildFilterClause([likeFilter], {
            name_search: "foo\\bar",
         });
         expect(clause).toBe("`customer_name` ~ 'foo\\\\bar'");
      });

      it("ignores params that don't match any filter", () => {
         const clause = buildFilterClause([equalFilter], {
            status: "active",
            unknown_param: "ignored",
         });
         expect(clause).toBe("`status` = 'active'");
      });
   });

   // -----------------------------------------------------------------------
   // injectFilterRefinement
   // -----------------------------------------------------------------------
   describe("injectFilterRefinement", () => {
      it("returns original query when clause is empty", () => {
         const query = "run: orders -> summary";
         expect(injectFilterRefinement(query, "")).toBe(query);
      });

      it("appends refinement to named view query", () => {
         const query = "run: orders -> summary";
         const clause = "`status` = 'active'";
         expect(injectFilterRefinement(query, clause)).toBe(
            "run: orders -> summary + {where: `status` = 'active'}",
         );
      });

      it("appends refinement to ad-hoc query", () => {
         const query =
            "run: orders -> { group_by: status; aggregate: order_count }";
         const clause = "`region` = 'US'";
         expect(injectFilterRefinement(query, clause)).toBe(
            "run: orders -> { group_by: status; aggregate: order_count } + {where: `region` = 'US'}",
         );
      });

      it("trims trailing whitespace before appending", () => {
         const query = "run: orders -> summary   \n  ";
         const clause = "`status` = 'active'";
         expect(injectFilterRefinement(query, clause)).toBe(
            "run: orders -> summary + {where: `status` = 'active'}",
         );
      });
   });
});
