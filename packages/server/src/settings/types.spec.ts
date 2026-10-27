// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { describe, expect, it } from "bun:test";
import {
   boolean,
   integer,
   list,
   Mismatch,
   number,
   oneOf,
   string,
   structured,
   type SettingType,
} from "./types";

const reads = <T>(type: SettingType<T>, raw: string) => type.fromString(raw);
const refuses = <T>(type: SettingType<T>, raw: string) =>
   expect(() => type.fromString(raw)).toThrow(Mismatch);

describe("setting types", () => {
   it("say what they expect in the words of the golden messages", () => {
      // errors.spec.ts holds the full messages these phrases appear in.
      expect(boolean().expected).toBe(
         "a boolean (true/false, 1/0, yes/no or on/off)",
      );
      expect(integer({ min: 0, max: 65535 }).expected).toBe(
         "an integer from 0 to 65535",
      );
      expect(integer({ min: 0 }).expected).toBe("an integer of at least 0");
      expect(number({ above: 0, below: 1 }).expected).toBe(
         "a number greater than 0 and less than 1",
      );
      expect(number({ min: 0, below: 1 }).expected).toBe(
         "a number of at least 0 and less than 1",
      );
      expect(oneOf(["on-demand", "local-only"]).expected).toBe(
         'one of "on-demand" or "local-only"',
      );
      expect(oneOf(["any", "destinations-only", "off"]).expected).toBe(
         'one of "any", "destinations-only" or "off"',
      );
      expect(list().expected).toBe("a list of strings");
      expect(string().expected).toBe("a string");
   });

   it("read a boolean in every spelling, any case, and a JSON boolean", () => {
      for (const raw of ["1", "true", "YES", " on "]) {
         expect(reads(boolean(), raw)).toBe(true);
      }
      for (const raw of ["0", "False", "no", "OFF"]) {
         expect(reads(boolean(), raw)).toBe(false);
      }
      refuses(boolean(), "enabled");
      expect(boolean().fromJson(false)).toBe(false);
      expect(() => boolean().fromJson(1)).toThrow(Mismatch);
   });

   it("read a base-10 integer in range, and refuse anything else", () => {
      const port = integer({ min: 0, max: 65535 });
      expect(reads(port, " 4040 ")).toBe(4040);
      expect(reads(integer(), "-5")).toBe(-5);
      for (const raw of ["40a0", "1e3", "05", "+5", "4.0", "70000", "-1"]) {
         refuses(port, raw);
      }
      expect(port.fromJson(80)).toBe(80);
      expect(() => port.fromJson(80.5)).toThrow(Mismatch);
   });

   it("read a decimal number matched whole", () => {
      const fraction = number({ above: 0, below: 1 });
      for (const [raw, value] of [
         ["0.5", 0.5],
         [".5", 0.5],
         ["0.50", 0.5],
         ["5e-1", 0.5],
      ] as const) {
         expect(reads(fraction, raw)).toBe(value);
      }
      for (const raw of ["0.5abc", "1", "0", "half"]) refuses(fraction, raw);
   });

   it("read an enum in any case as its registered spelling", () => {
      const mode = oneOf(["off", "write-only", "on"]);
      expect(reads(mode, " Write-Only ")).toBe("write-only");
      refuses(mode, "always");
   });

   it("read a list from commas, or a JSON array of strings", () => {
      expect(reads(list(), " a , ,b")).toEqual(["a", "b"]);
      expect(reads(list(), ",")).toEqual([]);
      expect(list().fromJson(["a", " b "])).toEqual(["a", "b"]);
      expect(() => list().fromJson("a,b")).toThrow(Mismatch);
      expect(() => list().fromJson([1])).toThrow(Mismatch);
   });

   it("trim a string, and refuse one that is not a string in the file", () => {
      expect(reads(string(), " 127.0.0.1 ")).toBe("127.0.0.1");
      expect(() => string().fromJson(4100)).toThrow(Mismatch);
   });

   it("keep a structured value whole, and never read one from a variable", () => {
      const value = [{ name: "e" }];
      expect(structured().fromJson(value)).toBe(value);
      refuses(structured(), "[]");
   });
});
