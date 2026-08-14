import { describe, expect, it } from "bun:test";
import { AccessDeniedError } from "../errors";
import { getMalloyErrorDetails } from "./error_messages";

describe("getMalloyErrorDetails — access-denied branch", () => {
   it("recognizes an authorize denial and gives access-relevant (not syntax) advice", () => {
      const details = getMalloyErrorDetails(
         "executeQuery",
         "env/pkg/model.malloy",
         new AccessDeniedError('Access denied for source "gated".'),
      );

      // Message carries the source name, never the gate expression.
      expect(details.message).toContain('Access denied for source "gated".');

      // The suggestion is about satisfying access (givens/role), and the
      // generic Malloy-syntax suggestions are replaced, not appended.
      expect(details.suggestions).toHaveLength(1);
      expect(details.suggestions[0]).toMatch(/given|authorize|restricted/i);
      // Not the generic "check the database connection / consult the language
      // docs" advice that an unrecognized error would yield.
      expect(details.suggestions.join(" ")).not.toMatch(
         /database connection configuration|language documentation/i,
      );
   });

   it("still falls back to generic suggestions for an unrecognized error", () => {
      const details = getMalloyErrorDetails(
         "executeQuery",
         "env/pkg/model.malloy",
         new Error("something unexpected"),
      );
      expect(details.suggestions.length).toBeGreaterThan(1);
   });
});

describe("getMalloyErrorDetails — restricted-mode branch", () => {
   /** A MalloyError-shaped throwable: an Error carrying `problems`. */
   function malloyErrorWith(
      message: string,
      problems: Array<{ code?: string; message?: string }>,
   ): Error {
      const error = new Error(message);
      (error as Error & { problems: unknown }).problems = problems;
      return error;
   }

   it("returns ONLY the restricted diagnostic when one construct cascades", () => {
      // The observed shape (QA field notes F5): one forbidden construct
      // produced five diagnostics, three of them actively misleading —
      // "'ACADEMIC_YEAR' is not defined" sends the caller to check warehouse
      // column names when the real cause is the refused conn.table(...).
      const details = getMalloyErrorDetails(
         "executeQuery",
         "env/pkg/model.malloy",
         malloyErrorWith(
            [
               "`beaver_dw.table(...)` cannot be used in a restricted query",
               "'ACADEMIC_YEAR' is not defined",
               "'TERM_START_DATE' is not defined",
               "Reference to undefined object '_probe'",
            ].join("\n"),
            [
               {
                  code: "restricted-construct-forbidden",
                  message:
                     "`beaver_dw.table(...)` cannot be used in a restricted query",
               },
               { code: "not-found", message: "'ACADEMIC_YEAR' is not defined" },
               {
                  code: "not-found",
                  message: "'TERM_START_DATE' is not defined",
               },
               {
                  code: "not-found",
                  message: "Reference to undefined object '_probe'",
               },
            ],
         ),
      );
      expect(details.message).toContain(
         "`beaver_dw.table(...)` cannot be used in a restricted query",
      );
      // The cascade fallout is suppressed, not passed through.
      expect(details.message).not.toContain("ACADEMIC_YEAR");
      expect(details.message).not.toContain("_probe");
      // One suggestion: where the construct IS allowed and the loop to use.
      expect(details.suggestions).toHaveLength(1);
      expect(details.suggestions[0]).toContain("model file");
      expect(details.suggestions[0]).toContain("malloy_reloadPackage");
      // The generic syntax advice must not attach — it misdirects here.
      expect(details.suggestions.join(" ")).not.toMatch(
         /Verify the structure and syntax/i,
      );
   });

   it("falls back to a message sniff for a re-wrapped error without problems", () => {
      const details = getMalloyErrorDetails(
         "executeQuery",
         "env/pkg/model.malloy",
         new Error(
            "`str_to_date!date(...)` cannot be used in a restricted query",
         ),
      );
      expect(details.message).toContain("restricted query");
      expect(details.suggestions).toHaveLength(1);
      expect(details.suggestions[0]).toContain("restricted mode");
   });

   it("does not hijack an error that merely mentions a restricted-looking name", () => {
      const details = getMalloyErrorDetails(
         "executeQuery",
         "env/pkg/model.malloy",
         new Error("Source 'restricted_data' not found"),
      );
      // The source-not-found branch should handle this, not the restricted one.
      expect(details.suggestions.join(" ")).toContain("restricted_data");
      expect(details.suggestions.join(" ")).not.toContain("restricted mode");
   });

   it("leaves a non-restricted problems-bearing error to the normal branches", () => {
      const details = getMalloyErrorDetails(
         "executeQuery",
         "env/pkg/model.malloy",
         malloyErrorWith("no viable alternative at input 'runx'", [
            { code: "syntax", message: "no viable alternative at input" },
         ]),
      );
      expect(details.suggestions.join(" ")).toMatch(/syntax error/i);
   });
});
