// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

// Finding F-3: identifier injection through the materialization storage path.
//
// The author-controlled `#@ persist name=` value is bound into the
// `CREATE OR REPLACE TABLE <path>` / `DROP TABLE IF EXISTS <path>` DDL the
// materialization builder emits. On the storage path the value is routed through
// `quoteManifestTablePath`, which passes an already-quoted string through
// unchanged (so the build side and the serve-side FROM-bind stay byte-identical).
// That pass-through is correct for a canonical, control-plane-supplied path, but
// the `#@ persist name=` value is UNTRUSTED author input: a value whose quoted
// content carries an embedded quote or backtick closes the identifier and appends
// arbitrary DDL.
//
// The fix is at the publish-time trust boundary, not in the shared quoting helper:
// `assertPersistNamesQuoted` already rejects an UNQUOTED name; it is extended to
// also reject a quoted name whose content is not a plain dialect identifier path
// (letters/digits/underscore segments joined by dots). This keeps the byte-identical
// CREATE/serve quoting contract untouched while making a malicious name un-publishable.
//
// These assertions pin the extended validator contract. They are red until the
// content check lands, which is the proof the injection is reachable.

import { describe, expect, it } from "bun:test";
import { ModelCompilationError } from "../errors";
import { assertPersistNamesQuoted } from "./persist_annotation_validation";

describe("assertPersistNamesQuoted identifier-injection hardening (F-3)", () => {
   // Each payload is a quoted persist name whose content breaks out of the
   // identifier when inlined into DDL. Rejecting them at publish is the fix.
   const injectionPayloads: Array<{ desc: string; line: string }> = [
      {
         desc: "embedded double-quote closing the identifier and appending DDL",
         line: `#@ persist name="orders\\"; DROP TABLE x; --"`,
      },
      {
         desc: "embedded backtick (backtick-dialect break-out)",
         // eslint-disable-next-line prettier/prettier -- keep double-quoted so the embedded backtick reads as the injection payload
         line: "#@ persist name=\"orders`; DROP TABLE x; --\"",
      },
      {
         desc: "bare embedded double-quote",
         line: `#@ persist name="a\\"b"`,
      },
      {
         desc: "semicolon / statement terminator in the value",
         line: `#@ persist name="orders; DROP TABLE x"`,
      },
      {
         desc: "whitespace in the value (not a bare identifier segment)",
         line: `#@ persist name="orders x"`,
      },
      {
         // The tag parser keeps only one of a repeated key, so a scan that
         // validated only the first `name=` would pass this and still deliver the
         // malicious value downstream. Every occurrence is validated to close it.
         desc: "a safe first value shadowing an unsafe duplicate key",
         line: `#@ persist name="ok" name="evil\\"; DROP TABLE t; --"`,
      },
      {
         desc: "an unsafe first value before a safe duplicate key",
         line: `#@ persist name="evil x" name="ok"`,
      },
   ];

   for (const { desc, line } of injectionPayloads) {
      it(`rejects a quoted persist name with ${desc}`, () => {
         expect(() => assertPersistNamesQuoted(line, "m.malloy")).toThrow(
            ModelCompilationError,
         );
      });
   }

   it("names the offending annotation and the allowed shape in the error", () => {
      let caught: Error | undefined;
      try {
         assertPersistNamesQuoted(`#@ persist name="a\\"b"`, "m.malloy");
      } catch (e) {
         caught = e as Error;
      }
      expect(caught).toBeInstanceOf(ModelCompilationError);
      // Actionable: the message must name the offending value and the allowed shape.
      expect(caught?.message).toContain("m.malloy");
      expect(caught?.message).toMatch(/letters|identifier|hyphen/);
   });

   // The grammar mirrors the control plane's physical-name allowlist
   // (`[A-Za-z0-9_-]` segments joined by dots), so it must NOT regress any name
   // the two services legitimately support: a simple identifier, a dotted dialect
   // path, a HYPHENATED BigQuery-style container path, a leading-digit segment,
   // and a three-part project.dataset.table path all stay publishable.
   const legitimate: string[] = [
      `#@ persist name="engaged_events"`,
      `#@ persist name='engaged_events'`,
      `#@ persist name="my_dataset.engaged_events"`,
      `#@ persist name="_private.tbl2"`,
      `#@ persist name="my-proj.mydataset.engaged_events"`,
      `#@ persist name="2024_events"`,
      `#@ persist name="proj-123.ds.tbl"`,
   ];
   for (const line of legitimate) {
      it(`still accepts the legitimate name ${line}`, () => {
         expect(() => assertPersistNamesQuoted(line, "m.malloy")).not.toThrow();
      });
   }
});
