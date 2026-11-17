// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * F-16 (campaign case P3-11): Snowflake big integers, pinned as they are.
 *
 * A stored read of a Snowflake source's `bigint` field keeps every digit,
 * because Malloy says live does: the Snowflake dialect keeps
 * `supportsBigIntPrecision`, so {@link liveKeepsBigIntegers} is true and a
 * `storage=` table reads the column through `::"HUGEINT"`. Live falls short
 * of that on every connection Publisher configures: Malloy's connection
 * registry, and Publisher's own key-pair builder, hand snowflake-sdk the
 * configured options without the connector's own `jsTreatIntegerAsBigInt`
 * default, so the driver returns a NUMBER(38,0) as a JavaScript number and a
 * live sum past 2^53 comes back rounded while the served answer is exact. The fix belongs
 * upstream, in the Snowflake connector; Publisher does not round served
 * answers to match (served never less exact than it can be). The docs say
 * so ("Snowflake: live and served differences", docs/materialization.md).
 */

import { describe, expect, it } from "bun:test";
import { capabilitiesOf } from "./domain/dialect";
import { bigIntegerReadOf, liveKeepsBigIntegers } from "./stored_column_types";

const bigintField = { name: "total", type: "number", numberType: "bigint" };

describe("F-16 P3-11: Snowflake big integers are read exactly when served", () => {
   it("treats live Snowflake as keeping every digit, as Malloy's dialect says", () => {
      expect(liveKeepsBigIntegers("snowflake")).toBe(true);
      // Not by Publisher's own driver override: Malloy's dialect flag decides.
      expect(capabilitiesOf("snowflake").driverKeepsBigIntegers).toBe(false);
   });

   it("reads a storage= column of a Snowflake bigint field through HUGEINT", () => {
      expect(
         bigIntegerReadOf(
            bigintField,
            { name: "total", type: "DECIMAL(38,0)" },
            { dialect: "snowflake" },
         ),
      ).toEqual({ column: "total", castTo: "HUGEINT" });
   });

   it("reads a colocated NUMBER(38,0) column through its own type", () => {
      expect(
         bigIntegerReadOf(
            bigintField,
            { name: "total", type: "DOUBLE" },
            {
               dialect: "snowflake",
               colocated: true,
               nativeTypeOf: () => "NUMBER(38,0)",
            },
         ),
      ).toEqual({ column: "total", castTo: "NUMBER(38,0)" });
   });
});
