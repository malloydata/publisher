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
         // The limit on what ${VAR} buys, stated where the person handling the
         // credential is looking rather than only in the skill.
         expect(env).toMatch(/unauthenticated/i);
         // Substance, not a specific route. The first version of this assertion
         // required the string "status", which pinned the disclosure to one
         // endpoint; when the text was corrected to stop naming a single path
         // (four leak it, not one) the guard failed the fix. Assert that the
         // caveat covers the API rather than that it names any one route.
         expect(env).toMatch(/REST endpoints|the whole API|API/i);
         expect(env).not.toMatch(/only .*\/api\/v0\/status/i);
      } finally {
         fs.rmSync(tmp, { recursive: true, force: true });
      }
   });
});
