import { describe, expect, it } from "bun:test";
import { MalloyError } from "@malloydata/malloy";
import { classifyToolError } from "./handler_utils";
import {
   AccessDeniedError,
   BadRequestError,
   InvalidArgumentError,
   ModelCompilationError,
   PackageNotFoundError,
   PayloadTooLargeError,
   ResponseUnserializableError,
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
      // Both assert the advice they expect, not merely that some error came
      // back. The previous version asserted the message lacked "unexpected
      // internal error", which is true of every branch except one, so it passed
      // whichever branch these landed in and did not notice when BadRequestError
      // was moved off the Malloy branch entirely.
      // An authorize denial reaches the Malloy helper and is REFINED there: the
      // message pattern replaces the generic suggestion list outright, so the
      // marker is the authorize advice, not "Malloy file".
      expect(
         advice(new AccessDeniedError('Access denied for source "orders"')),
      ).toContain("#(authorize)");
      expect(advice(new BadRequestError("Invalid query request."))).toContain(
         "Malloy file",
      );
   });

   it("keeps Malloy advice for a compile failure wrapped as BadRequestError", () => {
      // model.ts throws a plain BadRequestError for compile errors that are not
      // ModelCompilationError ("Model compilation failed: ..."). That is a real
      // Malloy problem and wants the syntax guidance and doc links, which is
      // why the narrower InvalidArgumentError exists rather than reclassifying
      // BadRequestError wholesale.
      expect(
         advice(
            new BadRequestError("Model compilation failed: unexpected '@'"),
         ),
      ).toContain("Malloy file");
   });

   it("homes a malformed argument as the caller's to fix, not as Malloy", () => {
      const details = classifyToolError(
         "searchDatabaseSchema",
         "env/conn",
         new InvalidArgumentError(
            'DuckDB schema name must be qualified as "catalog.schema", got "main".',
         ),
      );
      // The message is already specific, so it is passed through rather than
      // wrapped in "Error during <op> for resource <id>".
      expect(details.message).toBe(
         'DuckDB schema name must be qualified as "catalog.schema", got "main".',
      );
      const suggestions = JSON.stringify(details.suggestions);
      expect(suggestions).not.toContain("Malloy file");
      expect(suggestions).not.toContain("source:");
      expect(suggestions).toContain("This is not transient");
   });

   it("routes InvalidArgumentError ahead of the BadRequestError it extends", () => {
      // Subclass, so branch ORDER is what keeps these apart. Swap the two
      // branches and this is the only test that fails.
      expect(new InvalidArgumentError("x")).toBeInstanceOf(BadRequestError);
      expect(advice(new InvalidArgumentError("x"))).not.toContain(
         "Malloy file",
      );
      expect(advice(new BadRequestError("x"))).toContain("Malloy file");
   });

   it("does not offer a cap raise for a response that cannot be serialized", () => {
      // The shared oversize branch tells the agent to raise the cap named in
      // the message. For this subclass that is advice to change a setting and
      // hit the identical failure, which is the same confidently-wrong answer
      // the 413 itself was added to remove.
      const details = classifyToolError(
         "executeQuery",
         "env/pkg",
         new ResponseUnserializableError(
            "Query response exceeded 50000000 bytes: the 25356-row result is too large to serialize. Project fewer columns, add a LIMIT, or filter wide values.",
         ),
      );
      expect(details.message).toContain("too large to serialize");
      const suggestions = JSON.stringify(details.suggestions);
      expect(suggestions).toContain("will not help");
      expect(suggestions).toContain("not transient");
      expect(suggestions).not.toContain("raise the configured cap");
   });

   it("still offers the cap raise for an ordinary oversize response", () => {
      // The parent class keeps its advice: there the cap really is the limit
      // that fired, so raising it is a real remedy.
      expect(
         advice(
            new PayloadTooLargeError(
               "Query response exceeded 50000000 bytes (was 306326576). Project fewer columns, add a LIMIT, or raise PUBLISHER_MAX_RESPONSE_BYTES.",
            ),
         ),
      ).toContain("raise the configured cap");
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
