import { describe, expect, it } from "bun:test";
import { MalloyError } from "@malloydata/malloy";
import { classifyToolError } from "./handler_utils";
import {
   AccessDeniedError,
   BadRequestError,
   ModelCompilationError,
   PackageNotFoundError,
   ServiceUnavailableError,
} from "../errors";

/**
 * classifyToolError decides what an agent is told to do about a failure, so
 * every branch is pinned by the advice it produces, not just by "an error came
 * back". Asserting only that an error exists is what let every class funnel
 * through the Malloy helper unnoticed.
 *
 * The routing is by error CLASS on purpose. getMalloyErrorDetails refines on
 * message patterns the shipped engine does not emit, so "did it refine?" would
 * send real compile errors to the internal branch.
 */
describe("classifyToolError", () => {
   const advice = (e: unknown) =>
      JSON.stringify(classifyToolError("op", "env/pkg", e).suggestions);

   it("homes a missing package as not-found, not as Malloy", () => {
      const details = classifyToolError(
         "op",
         "env/pkg",
         new PackageNotFoundError("Package 'nope' not found"),
      );
      expect(details.message).toContain("Resource not found");
      expect(JSON.stringify(details.suggestions)).not.toContain("Malloy file");
   });

   it("homes back-pressure as retryable, not as Malloy", () => {
      const details = classifyToolError(
         "op",
         "env/pkg",
         new ServiceUnavailableError("Memory limit reached"),
      );
      expect(details.message).toContain("Memory limit reached");
      expect(JSON.stringify(details.suggestions)).toContain("Retry");
      expect(JSON.stringify(details.suggestions)).not.toContain("Malloy file");
   });

   it("keeps Malloy advice for a raw engine error", () => {
      // The regression guard for executeQuery. A bad query throws MalloyError,
      // NOT ModelCompilationError: the engine's error reaches the tool's catch
      // unwrapped. Route it by the internal branch and a syntax error tells the
      // agent an "unexpected internal error occurred", which is worse than the
      // bug this classifier was written to fix.
      const details = classifyToolError(
         "executeQuery",
         "env/pkg/m.malloy",
         new MalloyError("unexpected '@'", []),
      );
      expect(details.message).not.toContain("unexpected internal error");
      expect(advice(new MalloyError("unexpected '@'", []))).toContain("Malloy");
   });

   it("keeps Malloy advice for a wrapped compile error", () => {
      // What a failed reload throws. #890's promise is that a failed reload
      // returns the compile errors, so this must not read as an internal fault.
      const details = classifyToolError(
         "reloadPackage",
         "env/pkg",
         new ModelCompilationError({
            message: "Error(s) compiling model: unexpected '@'",
         }),
      );
      expect(details.message).not.toContain("unexpected internal error");
   });

   it("keeps Malloy advice for an authorize denial and a malformed request", () => {
      expect(
         classifyToolError(
            "op",
            "env/pkg",
            new AccessDeniedError('Access denied for source "orders"'),
         ).message,
      ).not.toContain("unexpected internal error");
      expect(
         classifyToolError(
            "op",
            "env/pkg",
            new BadRequestError("Invalid query request."),
         ).message,
      ).not.toContain("unexpected internal error");
   });

   it("reports anything else as internal rather than blaming the Malloy", () => {
      // A bug in our own code, a filesystem failure, or a worker crash. Telling
      // the agent to check its syntax sends it to edit a model that is fine.
      for (const error of [
         new TypeError("cannot read properties of undefined"),
         Object.assign(new Error("EACCES: permission denied"), {
            code: "EACCES",
         }),
      ]) {
         const details = classifyToolError("op", "env/pkg", error);
         expect(details.message).toContain("unexpected internal error");
         expect(JSON.stringify(details.suggestions)).not.toContain(
            "Malloy file",
         );
      }
   });
});
