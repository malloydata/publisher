// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * The value types in the configuration design's table
 * (docs/configuration-design.md, "Types, read the same way from every
 * source"). Each one reads a value as a variable or flag spells it
 * (`fromString`) and as the config file holds it (`fromJson`), and says what it
 * expects in the words a message uses.
 *
 * A type never sees a blank variable: blank is unset, and the loader handles
 * it before a type is asked.
 */
export abstract class SettingType<T> {
   /** What a value must be: "an integer from 0 to 65535". */
   abstract readonly expected: string;

   /** What a JSON value in the file that is not a string must be, where narrower than {@link expected}. */
   readonly expectedInFile?: string;

   /** A variable, a flag, or a string from the config file. */
   abstract fromString(raw: string): T;

   /** A value from the config file; a string goes to {@link fromString}. */
   fromJson(value: unknown): T {
      if (typeof value === "string") return this.fromString(value);
      throw new Mismatch();
   }

   /** Whether a value already read is one this type allows. */
   allows(_value: unknown): boolean {
      return true;
   }
}

/** Thrown by a type for a value it does not read; the loader names the setting. */
export class Mismatch extends Error {
   constructor() {
      super("value does not match the setting's type");
   }
}

const TRUE_SPELLINGS = ["1", "true", "yes", "on"];
const FALSE_SPELLINGS = ["0", "false", "no", "off"];

/** `true/false`, `1/0`, `yes/no`, `on/off`, any case; a JSON boolean in the file. */
export class BooleanType extends SettingType<boolean> {
   readonly expected = "a boolean (true/false, 1/0, yes/no or on/off)";
   readonly expectedInFile = "true or false";

   fromString(raw: string): boolean {
      const spelled = raw.trim().toLowerCase();
      if (TRUE_SPELLINGS.includes(spelled)) return true;
      if (FALSE_SPELLINGS.includes(spelled)) return false;
      throw new Mismatch();
   }

   fromJson(value: unknown): boolean {
      return typeof value === "boolean" ? value : super.fromJson(value);
   }
}

export interface Range {
   /** Inclusive lower bound. */
   min?: number;
   /** Inclusive upper bound. */
   max?: number;
   /** Exclusive lower bound. */
   above?: number;
   /** Exclusive upper bound. */
   below?: number;
}

function inRange(value: number, range: Range): boolean {
   return (
      (range.min === undefined || value >= range.min) &&
      (range.max === undefined || value <= range.max) &&
      (range.above === undefined || value > range.above) &&
      (range.below === undefined || value < range.below)
   );
}

/** "from 0 to 65535", "of at least 1", "greater than 0 and less than 1". */
function describeRange(range: Range): string {
   if (range.min !== undefined && range.max !== undefined) {
      return ` from ${range.min} to ${range.max}`;
   }
   const parts = [
      range.min !== undefined ? `of at least ${range.min}` : undefined,
      range.above !== undefined ? `greater than ${range.above}` : undefined,
      range.max !== undefined ? `of at most ${range.max}` : undefined,
      range.below !== undefined ? `less than ${range.below}` : undefined,
   ].filter((part) => part !== undefined);
   return parts.length > 0 ? ` ${parts.join(" and ")}` : "";
}

/** A base-10 integer, as written: no sign but `-`, no leading zeros, no exponent. */
const INTEGER = /^(0|-?[1-9]\d*)$/;

/** Whole numbers in a range; in the file, a JSON number. */
export class IntegerType extends SettingType<number> {
   readonly expected: string;

   constructor(private readonly range: Range = {}) {
      super();
      this.expected = `an integer${describeRange(range)}`;
   }

   fromString(raw: string): number {
      const trimmed = raw.trim();
      if (!INTEGER.test(trimmed)) throw new Mismatch();
      return this.checked(Number(trimmed));
   }

   fromJson(value: unknown): number {
      return typeof value === "number"
         ? this.checked(value)
         : super.fromJson(value);
   }

   allows(value: unknown): boolean {
      return (
         typeof value === "number" &&
         Number.isInteger(value) &&
         inRange(value, this.range)
      );
   }

   private checked(value: number): number {
      if (!this.allows(value)) throw new Mismatch();
      return value;
   }
}

/**
 * A decimal number, matched whole (`0.5abc` is not `0.5`); `.5`, `0.50` and
 * `1e-3` are fine.
 */
const DECIMAL = /^[+-]?(\d+\.?\d*|\.\d+)([eE][+-]?\d+)?$/;

/** Finite numbers in a range; in the file, a JSON number. */
export class NumberType extends SettingType<number> {
   readonly expected: string;

   constructor(private readonly range: Range = {}) {
      super();
      this.expected = `a number${describeRange(range)}`;
   }

   fromString(raw: string): number {
      const trimmed = raw.trim();
      if (!DECIMAL.test(trimmed)) throw new Mismatch();
      return this.checked(Number.parseFloat(trimmed));
   }

   fromJson(value: unknown): number {
      return typeof value === "number"
         ? this.checked(value)
         : super.fromJson(value);
   }

   allows(value: unknown): boolean {
      return (
         typeof value === "number" &&
         Number.isFinite(value) &&
         inRange(value, this.range)
      );
   }

   private checked(value: number): number {
      if (!this.allows(value)) throw new Mismatch();
      return value;
   }
}

/** One of a fixed set of strings, any case, read as the registered spelling. */
export class EnumType<V extends string> extends SettingType<V> {
   readonly expected: string;

   constructor(readonly values: readonly V[]) {
      super();
      const quoted = values.map((value) => JSON.stringify(value));
      this.expected =
         quoted.length === 1
            ? quoted[0]
            : `one of ${quoted.slice(0, -1).join(", ")} or ${quoted[quoted.length - 1]}`;
   }

   fromString(raw: string): V {
      const spelled = raw.trim().toLowerCase();
      const match = this.values.find(
         (value) => value.toLowerCase() === spelled,
      );
      if (match === undefined) throw new Mismatch();
      return match;
   }
}

/** Strings, comma-separated in a variable; in the file, a JSON array of strings. */
export class ListType extends SettingType<string[]> {
   readonly expected = "a list of strings";

   fromString(raw: string): string[] {
      return raw
         .split(",")
         .map((item) => item.trim())
         .filter((item) => item.length > 0);
   }

   fromJson(value: unknown): string[] {
      if (
         !Array.isArray(value) ||
         !value.every((item) => typeof item === "string")
      ) {
         throw new Mismatch();
      }
      return value.map((item) => item.trim()).filter((item) => item.length > 0);
   }
}

/** Any text, trimmed. */
export class StringType extends SettingType<string> {
   readonly expected = "a string";

   fromString(raw: string): string {
      return raw.trim();
   }
}

/**
 * A setting the file holds as a whole object or array (`environments`,
 * `theme`, `retrieval`), validated by the code that reads it. It has no
 * variable or flag, so it is only ever read from JSON.
 */
export class StructuredType extends SettingType<unknown> {
   readonly expected = "a JSON value";

   fromString(): unknown {
      throw new Mismatch();
   }

   fromJson(value: unknown): unknown {
      return value;
   }
}

export const boolean = () => new BooleanType();
export const integer = (range?: Range) => new IntegerType(range);
export const number = (range?: Range) => new NumberType(range);
export const oneOf = <const V extends string>(values: readonly V[]) =>
   new EnumType(values);
export const list = () => new ListType();
export const string = () => new StringType();
export const structured = () => new StructuredType();
