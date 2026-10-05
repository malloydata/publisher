// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { MalloyError } from "@malloydata/malloy";
import {
   errnoWireFields,
   ModelCompilationError,
   PackageManifestError,
} from "../errors";
import type { SerializedError } from "./protocol";

/**
 * The error shape that crosses the package-load worker boundary, in either
 * direction. Both sides use the same pair so a classification one side
 * attaches is the one the other side restores.
 */

/**
 * Flatten an error for postMessage. The class is carried as flags, because
 * structured clone keeps neither the prototype nor non-enumerable fields:
 * a Malloy compile error and a `ModelCompilationError` keep their
 * compilation-error classification (the main thread answers 424, not a
 * generic 500), an unusable publisher.json keeps its manifest-error
 * classification, and a Node errno error keeps the fields that identify a
 * refused filesystem access.
 */
export function serializeError(error: unknown): SerializedError {
   const serialized = serializeErrorShape(error);
   const errno = error instanceof Error ? errnoWireFields(error) : undefined;
   return errno ? { ...serialized, errno } : serialized;
}

function serializeErrorShape(error: unknown): SerializedError {
   if (error instanceof MalloyError) {
      return {
         name: error.name,
         message: error.message,
         stack: error.stack,
         malloyProblems: error.problems as unknown[],
         isCompilationError: true,
      };
   }
   if (error instanceof ModelCompilationError) {
      return {
         name: error.name,
         message: error.message,
         stack: error.stack,
         isCompilationError: true,
      };
   }
   if (error instanceof PackageManifestError) {
      return {
         name: error.name,
         message: error.message,
         stack: error.stack,
         isManifestError: true,
      };
   }
   if (error instanceof Error) {
      return {
         name: error.name,
         message: error.message,
         stack: error.stack,
      };
   }
   return { name: "Error", message: String(error) };
}

/**
 * Reconstitute an Error from its serialized shape, re-wrapping by the flags
 * `serializeError` attached so `instanceof` checks downstream (which decide
 * e.g. HTTP 424 vs 503) keep firing, and restoring errno fields.
 */
export function deserializeError(serialized: SerializedError): Error {
   const err = new Error(serialized.message);
   err.name = serialized.name;
   if (serialized.stack) err.stack = serialized.stack;
   if (serialized.errno) Object.assign(err, serialized.errno);
   if (serialized.malloyProblems) {
      (err as unknown as { problems: unknown }).problems =
         serialized.malloyProblems;
   }
   if (serialized.isCompilationError) {
      // ModelCompilationError's ctor expects a MalloyError-shaped input but
      // only reads `.message` at runtime. Cast through to satisfy the nominal
      // type without losing data.
      const wrapped = new ModelCompilationError(
         err as unknown as ConstructorParameters<
            typeof ModelCompilationError
         >[0],
      );
      if (serialized.stack) wrapped.stack = serialized.stack;
      return wrapped;
   }
   if (serialized.isManifestError) {
      const manifestError = new PackageManifestError(serialized.message);
      if (serialized.stack) manifestError.stack = serialized.stack;
      return manifestError;
   }
   return err;
}
