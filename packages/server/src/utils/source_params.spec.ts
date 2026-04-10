import { describe, expect, it } from "bun:test";
import type * as Malloy from "@malloydata/malloy-interfaces";
import { BadRequestError } from "../errors";
import {
   buildMalloyParamClause,
   getSourceParams,
   paramValueToMalloyLiteral,
   validateRequiredParams,
} from "./source_params";

// ---------------------------------------------------------------------------
// Helpers to build ParameterInfo fixtures
// ---------------------------------------------------------------------------

function makeParam(
   name: string,
   kind: Malloy.ParameterTypeType,
   defaultValue?: Malloy.LiteralValue,
): Malloy.ParameterInfo {
   return {
      name,
      type: { kind } as Malloy.ParameterType,
      default_value: defaultValue,
   };
}

function makeStringParam(
   name: string,
   defaultValue?: string,
): Malloy.ParameterInfo {
   return makeParam(
      name,
      "string_type",
      defaultValue !== undefined
         ? { kind: "string_literal", string_value: defaultValue }
         : undefined,
   );
}

function makeNumberParam(
   name: string,
   defaultValue?: number,
): Malloy.ParameterInfo {
   return makeParam(
      name,
      "number_type",
      defaultValue !== undefined
         ? { kind: "number_literal", number_value: defaultValue }
         : undefined,
   );
}

function makeBooleanParam(
   name: string,
   defaultValue?: boolean,
): Malloy.ParameterInfo {
   return makeParam(
      name,
      "boolean_type",
      defaultValue !== undefined
         ? { kind: "boolean_literal", boolean_value: defaultValue }
         : undefined,
   );
}

// ---------------------------------------------------------------------------
// paramValueToMalloyLiteral
// ---------------------------------------------------------------------------

describe("paramValueToMalloyLiteral", () => {
   it("should convert a string value to a quoted Malloy literal", () => {
      const param = makeStringParam("s");
      expect(paramValueToMalloyLiteral("hello", param)).toBe('"hello"');
   });

   it("should escape quotes and backslashes in string values", () => {
      const param = makeStringParam("s");
      expect(paramValueToMalloyLiteral('say "hi"', param)).toBe(
         '"say \\"hi\\""',
      );
      expect(paramValueToMalloyLiteral("back\\slash", param)).toBe(
         '"back\\\\slash"',
      );
   });

   it("should convert an integer string to an unquoted number literal", () => {
      const param = makeNumberParam("n");
      expect(paramValueToMalloyLiteral("42", param)).toBe("42");
   });

   it("should convert a decimal string to an unquoted number literal", () => {
      const param = makeNumberParam("n");
      expect(paramValueToMalloyLiteral("3.14", param)).toBe("3.14");
   });

   it("should convert negative numbers", () => {
      const param = makeNumberParam("n");
      expect(paramValueToMalloyLiteral("-100", param)).toBe("-100");
   });

   it("should throw for non-numeric value on a number parameter", () => {
      const param = makeNumberParam("n");
      expect(() => paramValueToMalloyLiteral("abc", param)).toThrow(
         BadRequestError,
      );
      expect(() => paramValueToMalloyLiteral("abc", param)).toThrow(
         /expects a number/,
      );
   });

   it('should convert "true"/"false" to boolean literals', () => {
      const param = makeBooleanParam("b");
      expect(paramValueToMalloyLiteral("true", param)).toBe("true");
      expect(paramValueToMalloyLiteral("false", param)).toBe("false");
   });

   it('should convert "1"/"0" to boolean literals', () => {
      const param = makeBooleanParam("b");
      expect(paramValueToMalloyLiteral("1", param)).toBe("true");
      expect(paramValueToMalloyLiteral("0", param)).toBe("false");
   });

   it("should be case-insensitive for boolean values", () => {
      const param = makeBooleanParam("b");
      expect(paramValueToMalloyLiteral("TRUE", param)).toBe("true");
      expect(paramValueToMalloyLiteral("False", param)).toBe("false");
   });

   it("should throw for invalid boolean value", () => {
      const param = makeBooleanParam("b");
      expect(() => paramValueToMalloyLiteral("yes", param)).toThrow(
         BadRequestError,
      );
      expect(() => paramValueToMalloyLiteral("yes", param)).toThrow(
         /expects a boolean/,
      );
   });

   // --- Native (non-string) value tests ---

   it("should accept native number values directly", () => {
      const param = makeNumberParam("n");
      expect(paramValueToMalloyLiteral(42, param)).toBe("42");
      expect(paramValueToMalloyLiteral(3.14, param)).toBe("3.14");
      expect(paramValueToMalloyLiteral(-100, param)).toBe("-100");
   });

   it("should throw for NaN native number", () => {
      const param = makeNumberParam("n");
      expect(() => paramValueToMalloyLiteral(NaN, param)).toThrow(
         /expects a number, got NaN/,
      );
   });

   it("should accept native boolean values directly", () => {
      const param = makeBooleanParam("b");
      expect(paramValueToMalloyLiteral(true, param)).toBe("true");
      expect(paramValueToMalloyLiteral(false, param)).toBe("false");
   });

   it("should convert date values to @-prefixed literals", () => {
      const param = makeParam("d", "date_type");
      expect(paramValueToMalloyLiteral("2024-01-15", param)).toBe(
         "@2024-01-15",
      );
   });

   it("should convert timestamp values to @-prefixed literals", () => {
      const param = makeParam("t", "timestamp_type");
      expect(paramValueToMalloyLiteral("2024-01-15 10:30:00", param)).toBe(
         "@2024-01-15 10:30:00",
      );
   });

   it("should convert timestamptz values to @-prefixed literals", () => {
      const param = makeParam("t", "timestamptz_type");
      expect(paramValueToMalloyLiteral("2024-01-15 10:30:00Z", param)).toBe(
         "@2024-01-15 10:30:00Z",
      );
   });

   it("should convert filter expression values to f'' literals", () => {
      const param = makeParam("f", "filter_expression_type");
      expect(
         paramValueToMalloyLiteral("last week for two days", param),
      ).toBe("f'last week for two days'");
   });

   it("should default to quoted string for unknown type kinds", () => {
      const param = makeParam("x", "json_type");
      expect(paramValueToMalloyLiteral('{"a":1}', param)).toBe(
         '"{\\\"a\\\":1}"',
      );
   });
});

// ---------------------------------------------------------------------------
// validateRequiredParams
// ---------------------------------------------------------------------------

describe("validateRequiredParams", () => {
   it("should not throw when all required params are provided", () => {
      const declared = [makeStringParam("a"), makeNumberParam("b")];
      expect(() =>
         validateRequiredParams(declared, { a: "val", b: "42" }),
      ).not.toThrow();
   });

   it("should not throw when params with defaults are missing", () => {
      const declared = [
         makeStringParam("a"),
         makeNumberParam("b", 10),
      ];
      // Only 'a' is required (no default); 'b' has a default
      expect(() => validateRequiredParams(declared, { a: "val" })).not.toThrow();
   });

   it("should not throw when there are no declared params", () => {
      expect(() => validateRequiredParams([], {})).not.toThrow();
   });

   it("should throw for a single missing required param", () => {
      const declared = [makeStringParam("required_param")];
      expect(() => validateRequiredParams(declared, {})).toThrow(
         BadRequestError,
      );
      expect(() => validateRequiredParams(declared, {})).toThrow(
         'Parameter "required_param" is required',
      );
   });

   it("should throw listing all missing required params", () => {
      const declared = [
         makeStringParam("a"),
         makeNumberParam("b"),
         makeBooleanParam("c", false), // has default — not required
      ];
      expect(() => validateRequiredParams(declared, {})).toThrow(
         BadRequestError,
      );
      expect(() => validateRequiredParams(declared, {})).toThrow(
         /Parameters "a", "b" are required/,
      );
   });

   it("should throw only for the params not provided", () => {
      const declared = [
         makeStringParam("a"),
         makeNumberParam("b"),
      ];
      expect(() => validateRequiredParams(declared, { a: "val" })).toThrow(
         'Parameter "b" is required',
      );
   });
});

// ---------------------------------------------------------------------------
// buildMalloyParamClause
// ---------------------------------------------------------------------------

describe("buildMalloyParamClause", () => {
   it("should return empty string when no params are provided", () => {
      const declared = [makeStringParam("a", "default")];
      expect(buildMalloyParamClause({}, declared)).toBe("");
   });

   it("should build a single-param clause", () => {
      const declared = [makeStringParam("name")];
      expect(buildMalloyParamClause({ name: "Alice" }, declared)).toBe(
         '(name is "Alice")',
      );
   });

   it("should build a multi-param clause with correct types", () => {
      const declared = [
         makeStringParam("label"),
         makeNumberParam("count"),
         makeBooleanParam("active"),
      ];
      const result = buildMalloyParamClause(
         { label: "test", count: "5", active: "true" },
         declared,
      );
      expect(result).toBe('(label is "test", count is 5, active is true)');
   });

   it("should throw for an unknown parameter name", () => {
      const declared = [makeStringParam("known")];
      expect(() =>
         buildMalloyParamClause({ unknown: "val" }, declared),
      ).toThrow(BadRequestError);
      expect(() =>
         buildMalloyParamClause({ unknown: "val" }, declared),
      ).toThrow(/Unknown source parameter "unknown"/);
   });

   it("should list available params in unknown-param error", () => {
      const declared = [makeStringParam("alpha"), makeNumberParam("beta")];
      expect(() =>
         buildMalloyParamClause({ gamma: "val" }, declared),
      ).toThrow(/Available parameters: alpha, beta/);
   });

   it("should throw for unknown param when declared list is empty", () => {
      expect(() =>
         buildMalloyParamClause({ x: "1" }, []),
      ).toThrow(/Available parameters: \(none\)/);
   });

   it("should handle native number and boolean values", () => {
      const declared = [
         makeNumberParam("limit"),
         makeBooleanParam("active"),
         makeStringParam("label"),
      ];
      const result = buildMalloyParamClause(
         { limit: 100, active: true, label: "test" },
         declared,
      );
      expect(result).toBe('(limit is 100, active is true, label is "test")');
   });
});

// ---------------------------------------------------------------------------
// getSourceParams
// ---------------------------------------------------------------------------

describe("getSourceParams", () => {
   const sources: Malloy.SourceInfo[] = [
      {
         name: "flights",
         schema: { fields: [] },
         parameters: [
            makeStringParam("carrier"),
            makeNumberParam("min_distance", 0),
         ],
      },
      {
         name: "airports",
         schema: { fields: [] },
         // no parameters
      },
   ];

   it("should return parameters for a known source", () => {
      const params = getSourceParams(sources, "flights");
      expect(params).toHaveLength(2);
      expect(params[0].name).toBe("carrier");
      expect(params[1].name).toBe("min_distance");
   });

   it("should return empty array for a source with no parameters", () => {
      expect(getSourceParams(sources, "airports")).toEqual([]);
   });

   it("should return empty array for an unknown source name", () => {
      expect(getSourceParams(sources, "nonexistent")).toEqual([]);
   });

   it("should return empty array when sourceInfos is undefined", () => {
      expect(getSourceParams(undefined, "flights")).toEqual([]);
   });

   it("should return empty array when sourceName is undefined", () => {
      expect(getSourceParams(sources, undefined)).toEqual([]);
   });
});
