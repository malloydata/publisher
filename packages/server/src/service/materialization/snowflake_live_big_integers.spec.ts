// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * F-16 (campaign case P3-11): Snowflake big integers, served and live.
 *
 * A stored read of a Snowflake source's `bigint` field keeps every digit,
 * because Malloy says live does: the Snowflake dialect keeps
 * `supportsBigIntPrecision`, so {@link liveKeepsBigIntegers} is true and a
 * `storage=` table reads the column through `::"HUGEINT"`. Live keeps them
 * too: every Snowflake connection Publisher builds reads integers with the
 * connector's own `jsTreatIntegerAsBigInt` default (snowflake_connection.ts,
 * whose spec pins the live half), so the two answers agree.
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

   it("reads a database NUMBER(38,0) column through its own type", () => {
      expect(
         bigIntegerReadOf(
            bigintField,
            { name: "total", type: "DOUBLE" },
            {
               dialect: "snowflake",
               database: true,
               nativeTypeOf: () => "NUMBER(38,0)",
            },
         ),
      ).toEqual({ column: "total", castTo: "NUMBER(38,0)" });
   });
});
