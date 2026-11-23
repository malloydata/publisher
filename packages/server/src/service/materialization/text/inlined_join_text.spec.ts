// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { describe, expect, it } from "bun:test";
import {
   dropJoinDefinition,
   inlinedJoinRenames,
   inlinedJoinStatements,
   intoFirstExtend,
   labelledRenames,
   renameBlock,
   type InlinedJoinText,
} from "./inlined_join_text";

const CUSTOMERS: InlinedJoinText = {
   join: "customers",
   columns: [
      { name: "name", column: "customers__name" },
      { name: "since", column: "customers__since", timeframe: "month" },
      {
         name: "big",
         column: "customers__big",
         nativeCast: "HUGEINT",
         timeframe: "day",
      },
      { name: "odd", column: "customers__odd", timeframe: "fortnight" },
   ],
};
const SECRET: InlinedJoinText = {
   join: "secret",
   access: "private",
   columns: [{ name: "ssn", column: "secret__ssn" }],
};
const INNER: InlinedJoinText = {
   join: "inner",
   access: "internal",
   columns: [{ name: "x", column: "inner__x" }],
};
const ALL = [CUSTOMERS, SECRET, INNER];

describe("inlinedJoinRenames", () => {
   it("names each stored column `<column>__stored`, numbered past a reserved or stored name", () => {
      const renames = inlinedJoinRenames(ALL, [
         "customers__name__stored",
         "secret__ssn__stored_2",
      ]);
      expect(renames).toEqual([
         {
            internal: "customers__name__stored_2",
            column: "customers__name",
            access: "internal",
         },
         {
            internal: "customers__since__stored",
            column: "customers__since",
            access: "internal",
         },
         {
            internal: "customers__big__stored",
            column: "customers__big",
            access: "internal",
         },
         {
            internal: "customers__odd__stored",
            column: "customers__odd",
            access: "internal",
         },
         // A hidden join's columns are private, whichever modifier hides it.
         {
            internal: "secret__ssn__stored",
            column: "secret__ssn",
            access: "private",
         },
         {
            internal: "inner__x__stored",
            column: "inner__x",
            access: "private",
         },
      ]);
   });

   it("never renames onto another stored column's name", () => {
      const renames = inlinedJoinRenames(
         [
            {
               join: "j",
               columns: [
                  { name: "a", column: "j__a" },
                  { name: "a_stored", column: "j__a__stored" },
               ],
            },
         ],
         [],
      );
      expect(renames.map((r) => r.internal)).toEqual([
         "j__a__stored_2",
         "j__a__stored__stored",
      ]);
   });
});

describe("labelledRenames and renameBlock", () => {
   const renames = inlinedJoinRenames(ALL, []);

   it("writes the internal renames, then the private ones, each under its label", () => {
      expect(labelledRenames(renames)).toBe(
         "\n  internal rename:" +
            "\n    customers__name__stored is customers__name" +
            "\n    customers__since__stored is customers__since" +
            "\n    customers__big__stored is customers__big" +
            "\n    customers__odd__stored is customers__odd" +
            "\n  private rename:" +
            "\n    secret__ssn__stored is secret__ssn" +
            "\n    inner__x__stored is inner__x",
      );
   });

   it("writes nothing for no renames, and no empty label", () => {
      expect(labelledRenames([])).toBe("");
      expect(labelledRenames(renames.slice(0, 1))).not.toContain("private");
      expect(renameBlock([])).toBe("");
   });

   it("writes one unlabelled extend block to open ahead of an include", () => {
      expect(renameBlock(renames.slice(0, 2))).toBe(
         " extend {\n  rename:" +
            "\n    customers__name__stored is customers__name" +
            "\n    customers__since__stored is customers__since\n}",
      );
   });
});

describe("inlinedJoinStatements", () => {
   const renames = inlinedJoinRenames(ALL, []);

   it("writes a record per join over its renamed columns: public, then internal, then private", () => {
      expect(inlinedJoinStatements(ALL, renames, false)).toBe(
         "\n  dimension:" +
            "\n    customers is {" +
            "\n      name is customers__name__stored," +
            "\n      since is customers__since__stored.month," +
            '\n      big is customers__big__stored::"HUGEINT",' +
            "\n      odd is customers__odd__stored" +
            "\n    }" +
            "\n  internal dimension:" +
            "\n    `inner` is {" +
            "\n      x is inner__x__stored" +
            "\n    }" +
            "\n  private dimension:" +
            "\n    secret is {" +
            "\n      ssn is secret__ssn__stored" +
            "\n    }",
      );
   });

   it("reads a big integer through its cast over any timeframe, and drops a timeframe it does not know", () => {
      const text = inlinedJoinStatements([CUSTOMERS], renames, false);
      expect(text).toContain('big is customers__big__stored::"HUGEINT",');
      expect(text).not.toContain("customers__big__stored.day");
      expect(text).toContain("odd is customers__odd__stored\n");
      expect(text).not.toContain("fortnight");
   });

   it("writes the renames first when asked to", () => {
      const text = inlinedJoinStatements([CUSTOMERS], renames, true);
      expect(text.startsWith(labelledRenames(renames))).toBe(true);
      expect(text.slice(labelledRenames(renames).length)).toBe(
         inlinedJoinStatements([CUSTOMERS], renames, false),
      );
   });

   it("writes nothing for no joins", () => {
      expect(inlinedJoinStatements([], [], false)).toBe("");
   });
});

describe("dropJoinDefinition", () => {
   const TEXT = `extend {
  join_one: customers is cust on customer_id = customers.id
  dimension: d is 1
} extend {
  join_one: a is x on a.id = id, customers2 is y on customers2.id = id
}`;

   it("takes out a join_one's whole statement when it is the only definition", () => {
      expect(dropJoinDefinition(TEXT, "customers")).toBe(`extend {

  dimension: d is 1
} extend {
  join_one: a is x on a.id = id, customers2 is y on customers2.id = id
}`);
   });

   it("takes out only its definition from a statement that defines others", () => {
      expect(dropJoinDefinition(TEXT, "a")).toBe(`extend {
  join_one: customers is cust on customer_id = customers.id
  dimension: d is 1
} extend {
  join_one: customers2 is y on customers2.id = id
}`);
   });

   it("takes out the annotation lines written on it, and reads a quoted name", () => {
      expect(
         dropJoinDefinition(
            "extend {\n  # label\n  join_one: `my join` is x on `my join`.id = id\n}",
            "my join",
         ),
      ).toBe("extend {\n\n}");
   });

   it("takes out a block doc written on it", () => {
      const out = dropJoinDefinition(
         "extend {\n  dimension: d is 1\n  #|(doc)\n  The join.\n  |#\n  join_one: c is x on c.id = id\n}",
         "c",
      )!;
      expect(out).toContain("dimension: d is 1");
      expect(out).not.toContain("join_one");
      expect(out).not.toContain("|#");
      expect(out).not.toContain("The join.");
   });

   it("leaves the text unchanged when it declares no such join, or only a field of that name", () => {
      expect(dropJoinDefinition(TEXT, "nope")).toBe(TEXT);
      expect(
         dropJoinDefinition("extend { dimension: items is 1 }", "items"),
      ).toBe("extend { dimension: items is 1 }");
   });

   it("refuses a name another kind of join defines, and text that does not lex", () => {
      expect(
         dropJoinDefinition(
            "extend { join_many: items is i on items.oid = id }",
            "items",
         ),
      ).toBeUndefined();
      expect(
         dropJoinDefinition('extend { dimension: s is "abc }', "s"),
      ).toBeUndefined();
   });
});

describe("intoFirstExtend", () => {
   const STATEMENTS = "\n  dimension: z is 1";

   it("writes a block of its own for an empty run", () => {
      expect(intoFirstExtend("", STATEMENTS)).toBe(
         "extend {\n  dimension: z is 1\n}",
      );
   });

   it("opens the first extend block with the statements, the author's first statement on its own line", () => {
      expect(
         intoFirstExtend("extend {\n  dimension: a is 1\n}", STATEMENTS),
      ).toBe("extend {\n  dimension: z is 1\n  dimension: a is 1\n}");
      expect(intoFirstExtend("extend { dimension: a is 1 }", STATEMENTS)).toBe(
         "extend {\n  dimension: z is 1\n dimension: a is 1 }",
      );
   });

   it("writes after the last include, into the block that follows it or a new one", () => {
      expect(
         intoFirstExtend(
            "include { public: * } extend {\n  dimension: a is 1\n}",
            STATEMENTS,
         ),
      ).toBe(
         "include { public: * } extend {\n  dimension: z is 1\n  dimension: a is 1\n}",
      );
      expect(
         intoFirstExtend(
            "extend { dimension: a is 1 } include { public: * }",
            STATEMENTS,
         ),
      ).toBe(
         "extend { dimension: a is 1 } include { public: * } extend {\n  dimension: z is 1\n}",
      );
   });

   it("refuses an include that never closes, a run with no block, and text that does not lex", () => {
      expect(
         intoFirstExtend("include { public: *", STATEMENTS),
      ).toBeUndefined();
      expect(intoFirstExtend("dimension", STATEMENTS)).toBeUndefined();
      expect(
         intoFirstExtend('extend { dimension: s is "abc }', STATEMENTS),
      ).toBeUndefined();
   });
});
