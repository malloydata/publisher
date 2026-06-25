import { describe, expect, it } from "bun:test";
import { ModelCompilationError } from "../errors";
import { assertPersistNamesQuoted } from "./persist_annotation_validation";

describe("assertPersistNamesQuoted", () => {
   it("throws ModelCompilationError on a bare (unquoted) persist name", () => {
      expect(() =>
         assertPersistNamesQuoted(
            `#@ persist name=engaged_events\nsource: engaged_events is x -> { select: * }`,
            "m.malloy",
         ),
      ).toThrow(ModelCompilationError);
   });

   it("tolerates whitespace around = but still flags an unquoted value", () => {
      expect(() =>
         assertPersistNamesQuoted(
            `#@ persist name = engaged_events`,
            "m.malloy",
         ),
      ).toThrow(/must be quoted/);
   });

   it("accepts a double-quoted name", () => {
      expect(() =>
         assertPersistNamesQuoted(
            `#@ persist name="engaged_events"`,
            "m.malloy",
         ),
      ).not.toThrow();
   });

   it("accepts a single-quoted name", () => {
      expect(() =>
         assertPersistNamesQuoted(
            `#@ persist name='engaged_events'`,
            "m.malloy",
         ),
      ).not.toThrow();
   });

   it("accepts a dotted, quoted dialect path", () => {
      expect(() =>
         assertPersistNamesQuoted(
            `#@ persist name="my_dataset.engaged_events"`,
            "m.malloy",
         ),
      ).not.toThrow();
   });

   it("ignores non-persist annotations and persist lines with no name field", () => {
      expect(() =>
         assertPersistNamesQuoted(
            `# number=2\n#@ persist realization="COPY"\nsource: x is y -> { select: * }`,
            "m.malloy",
         ),
      ).not.toThrow();
   });

   it("does not mistake a neighbouring key for the name field", () => {
      expect(() =>
         assertPersistNamesQuoted(
            `#@ persist tablename=foo name="bar"`,
            "m.malloy",
         ),
      ).not.toThrow();
   });

   it("reports every offending annotation in the message", () => {
      expect(() =>
         assertPersistNamesQuoted(
            `#@ persist name=a\nsource: a is x -> { select: * }\n#@ persist name=b\nsource: b is x -> { select: * }`,
            "m.malloy",
         ),
      ).toThrow(/name=a.*name=b/s);
   });
});
