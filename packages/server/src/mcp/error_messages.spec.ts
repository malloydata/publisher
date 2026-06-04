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
