// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { describe, expect, test } from "bun:test";
import * as fs from "node:fs";
import * as os from "node:os";
import * as path from "node:path";
import { buildConnection } from "./connection";
import { formatSuccess } from "./index";
import { scaffold } from "./scaffold";

/**
 * A guard for the prose, not for the code.
 *
 * Everything this tool says about an unset `${VAR}` is a claim about a SERVER
 * this package does not contain and cannot import. The claim was written from a
 * measurement of one server version, and a pending server change alters how that
 * failure is reported: today it reads `load_errors=0` with the variable named
 * nowhere, afterwards the variable is named in `loadErrors`. The underlying
 * behaviour, an environment that does not load so the server serves nothing, is
 * true either way.
 *
 * So the rule these tests enforce is: describe the BEHAVIOUR, never assert one
 * version's reading of it as the thing the user will see. Without them the
 * wording drifts back to whichever reading the author last observed, and it
 * drifts in the two worst possible places: text printed into somebody else's
 * terminal, and text written into somebody else's AGENTS.md. Both are read long
 * after anyone remembers which server version was current, and neither is
 * anywhere a future author would think to grep.
 *
 * These assertions are deliberately about wording. That is the point: the prose
 * is the artifact under test, because nothing else can fail when it goes stale.
 */

const postgres = () =>
   buildConnection({
      connection: "postgres",
      pgHost: "db.example.com",
      pgDatabase: "analytics",
      pgUser: "reader",
   });

function scaffoldInto(): { agents: string; output: string } {
   const tmp = fs.mkdtempSync(path.join(os.tmpdir(), "cmp-prose-"));
   try {
      const result = scaffold({
         name: "sales",
         cwd: tmp,
         host: "claude-code",
         force: false,
         connection: postgres(),
      });
      return {
         agents: fs.readFileSync(path.join(tmp, "AGENTS.md"), "utf8"),
         output: formatSuccess(result),
      };
   } finally {
      fs.rmSync(tmp, { recursive: true, force: true });
   }
}

/**
 * Phrasings that present ONE version's reading as the thing the user will see.
 *
 * Deliberately not a ban on the words "no load errors": the correct wording
 * names both readings, so one of them is that phrase. What must not appear is a
 * reading offered as the symptom to look for, with no version attached. The
 * first draft of this file banned the phrase itself and failed against prose
 * that was right, which is its own small lesson about testing wording.
 */
const ASSERTED_AS_DEFINITIVE = [
   /that is what an unset one looks like/i,
   /no environments, no packages, and no load errors/i,
   /`loadErrors` is empty, that is/i,
];

describe("unset-variable prose survives the server changing how it reports", () => {
   test("the CLI warning describes the behaviour, not one version's reading", () => {
      const { output } = scaffoldInto();
      // It must still warn: dropping the warning would "pass" this test while
      // losing the thing it exists to protect.
      expect(output).toContain("MALLOY_POSTGRES_PASSWORD");
      expect(output).toMatch(/does not load|serving nothing/i);
      for (const stale of ASSERTED_AS_DEFINITIVE) {
         expect(output).not.toMatch(stale);
      }
   });

   test("the generated AGENTS.md does the same", () => {
      const { agents } = scaffoldInto();
      expect(agents).toContain("MALLOY_POSTGRES_PASSWORD");
      expect(agents).toMatch(/does not load|serving nothing/i);
      for (const stale of ASSERTED_AS_DEFINITIVE) {
         expect(agents).not.toMatch(stale);
      }
   });

   test("both name the two readings rather than picking one", () => {
      // The wording that stays true whichever of #1047 and #1049 lands first:
      // older servers report it one way, newer ones another.
      const { agents, output } = scaffoldInto();
      for (const text of [agents, output]) {
         expect(text).toMatch(/older/i);
         expect(text).toMatch(/newer/i);
         expect(text).toMatch(/loadErrors/);
      }
   });
});

/**
 * Claims about the leak must be hedged the same way claims about load_errors
 * are, and for the same reason: #1071 redacts at the single serializer every
 * connection-returning route builds from, so a flat "Publisher serves your
 * password over its API" becomes false the moment it lands.
 *
 * This class escaped the guard once, in the worst possible way: one sentence
 * hedged the load_errors clause correctly and then asserted the leak clause
 * flat, one clause later. Fixing an instance and missing its sibling in the
 * same sentence is exactly what a guard is for.
 */
const LEAK_ASSERTED_AS_FACT = [
   /a running Publisher serves connection config(?!.*may)/i,
   /is served (?:by|from) .*unauthenticated/i,
   /your (?:password|credential) is (?:readable|exposed|served)/i,
];

describe("leak claims are hedged, because a pending fix removes the leak", () => {
   test("the generated AGENTS.md hedges it", () => {
      const { agents } = scaffoldInto();
      for (const flat of LEAK_ASSERTED_AS_FACT) {
         expect(agents).not.toMatch(flat);
      }
      // And it must still carry the advice, which does not rest on the leak.
      expect(agents).toMatch(/localhost|gateway|authenticat/i);
   });

   test("the .env.example header hedges it", () => {
      const tmp = fs.mkdtempSync(path.join(os.tmpdir(), "cmp-prose-leak-"));
      try {
         scaffold({
            name: "sales",
            cwd: tmp,
            host: "claude-code",
            force: false,
            connection: postgres(),
         });
         const env = fs.readFileSync(path.join(tmp, ".env.example"), "utf8");
         for (const flat of LEAK_ASSERTED_AS_FACT) {
            expect(env).not.toMatch(flat);
         }
         expect(env).toMatch(/may serve|depending on the server version/i);
      } finally {
         fs.rmSync(tmp, { recursive: true, force: true });
      }
   });
});

describe(".env.example does not invite a secret into a file nothing reads", () => {
   test("it never tells the user to copy it to .env", () => {
      // Publisher has no --env-file and no dotenv, and the generated start
      // script does no shell sourcing, so a populated .env is a secret at rest
      // attached to a server that still will not boot.
      const tmp = fs.mkdtempSync(path.join(os.tmpdir(), "cmp-prose-env-"));
      try {
         scaffold({
            name: "sales",
            cwd: tmp,
            host: "claude-code",
            force: false,
            connection: postgres(),
         });
         const env = fs.readFileSync(path.join(tmp, ".env.example"), "utf8");
         expect(env).not.toMatch(/copy this file/i);
         expect(env).not.toMatch(/copy .*to \.env/i);
         expect(env).toMatch(/export MALLOY_POSTGRES_PASSWORD=/);
         // The three assertions below are about PROPERTIES of the claim, not
         // about vocabulary. Three earlier versions of this guard asserted
         // particular words ("status", then "unauthenticated") and each one
         // went red against a correct rewrite, which is a guard punishing the
         // fix it exists to protect. What must hold is: the scope is stated,
         // the leak is not asserted as fact, and the advice is about the API.
         expect(env).toMatch(
            /not in the config file|not in your shell history|whole of it/i,
         );
         expect(env).toMatch(/localhost|gateway|authenticat/i);
         expect(env).not.toMatch(/only .*\/api\/v0\/status/i);
      } finally {
         fs.rmSync(tmp, { recursive: true, force: true });
      }
   });
});
