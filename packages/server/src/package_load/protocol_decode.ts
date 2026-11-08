// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * Receiving-side checks for the package-load worker protocol.
 *
 * A worker's result arrives as `unknown` structured-clone data and is hydrated
 * into live `Model`s through casts (`modelDef as ModelDef`, and so on). These
 * checks run first, so a result from a worker on another protocol version, or
 * a model whose envelope is the wrong shape, fails with a
 * {@link PackageLoadProtocolError} naming what was wrong, instead of surfacing
 * later as an unrelated `TypeError` deep inside hydration or a query.
 *
 * They check the envelope only: field presence and kinds, never the Malloy IR
 * inside `modelDef`, which the compiler that produced it already checked. They
 * never copy: a well-formed message is returned as the same object, because
 * `#(authorize)` enforcement depends on the aliasing structured clone preserved
 * inside it (see protocol.ts).
 */

import type { LoadPackageResult, SerializedModel } from "./protocol";
import { PACKAGE_LOAD_PROTOCOL_VERSION } from "./protocol";

/**
 * A package-load worker message this thread cannot read: a protocol version
 * mismatch or a malformed payload. An internal failure (a deployment or
 * programming error, never the author's), so it is deliberately not one of the
 * classes `loadOutcomeOrThrow` maps to a 4xx or a retryable 503.
 */
export class PackageLoadProtocolError extends Error {
   constructor(message: string) {
      super(`Package-load worker protocol error: ${message}`);
      this.name = "PackageLoadProtocolError";
   }
}

function isRecord(value: unknown): value is Record<string, unknown> {
   return typeof value === "object" && value !== null && !Array.isArray(value);
}

function kindOf(value: unknown): string {
   if (value === null) return "null";
   if (Array.isArray(value)) return "array";
   return typeof value;
}

type Expect = "string" | "number" | "object" | "array" | "string[]";

function matches(value: unknown, expect: Expect): boolean {
   switch (expect) {
      case "string":
         return typeof value === "string";
      case "number":
         return typeof value === "number";
      case "object":
         return isRecord(value);
      case "array":
         return Array.isArray(value);
      case "string[]":
         return (
            Array.isArray(value) && value.every((v) => typeof v === "string")
         );
   }
}

/** Optional fields of a SerializedModel and the kind each must have when set. */
const OPTIONAL_MODEL_FIELDS: ReadonlyArray<[keyof SerializedModel, Expect]> = [
   ["modelDef", "object"],
   ["modelInfo", "object"],
   ["sourceInfos", "array"],
   ["sources", "array"],
   ["queries", "array"],
   ["filterMap", "array"],
   ["givens", "array"],
   ["notebookCells", "array"],
   ["modelSourceText", "string"],
   ["dataStyles", "object"],
   ["compileDurationMs", "number"],
   ["problems", "array"],
   ["compilationError", "object"],
   ["authorizeWarnings", "string[]"],
   ["renderTagResults", "array"],
   ["preaggregateCompanion", "object"],
];

/**
 * Check one model's envelope and return it, typed, as the same object.
 * Throws {@link PackageLoadProtocolError} naming the first problem found.
 */
export function decodeSerializedModel(raw: unknown): SerializedModel {
   if (!isRecord(raw)) {
      throw new PackageLoadProtocolError(
         `a serialized model must be an object, got ${kindOf(raw)}`,
      );
   }
   if (typeof raw.modelPath !== "string" || raw.modelPath === "") {
      throw new PackageLoadProtocolError(
         `a serialized model has no modelPath (got ${kindOf(raw.modelPath)})`,
      );
   }
   const where = `serialized model "${raw.modelPath}"`;
   if (raw.modelType !== "model" && raw.modelType !== "notebook") {
      throw new PackageLoadProtocolError(
         `${where} has modelType ${JSON.stringify(raw.modelType) ?? "undefined"}; expected "model" or "notebook"`,
      );
   }
   for (const [field, expect] of OPTIONAL_MODEL_FIELDS) {
      const value = raw[field];
      if (value !== undefined && !matches(value, expect)) {
         throw new PackageLoadProtocolError(
            `${where} has ${field} of kind ${kindOf(value)}; expected ${expect}`,
         );
      }
   }
   if (Array.isArray(raw.filterMap)) {
      for (const entry of raw.filterMap) {
         if (
            !Array.isArray(entry) ||
            entry.length !== 2 ||
            typeof entry[0] !== "string" ||
            !Array.isArray(entry[1])
         ) {
            throw new PackageLoadProtocolError(
               `${where} has a filterMap entry that is not a [sourceName, filters[]] pair`,
            );
         }
      }
   }
   if (Array.isArray(raw.notebookCells)) {
      for (const cell of raw.notebookCells) {
         if (
            !isRecord(cell) ||
            (cell.type !== "code" && cell.type !== "markdown") ||
            typeof cell.text !== "string"
         ) {
            throw new PackageLoadProtocolError(
               `${where} has a notebook cell without a code/markdown type and text`,
            );
         }
      }
   }
   if (isRecord(raw.compilationError)) {
      const { name, message } = raw.compilationError;
      if (typeof name !== "string" || typeof message !== "string") {
         throw new PackageLoadProtocolError(
            `${where} has a compilationError without a name and message`,
         );
      }
   }
   return raw as unknown as SerializedModel;
}

/**
 * Check a `load-package-result` message: its protocol version first, then its
 * envelope and every model in it. Returns the same object.
 */
export function checkLoadPackageResult(raw: unknown): LoadPackageResult {
   if (!isRecord(raw)) {
      throw new PackageLoadProtocolError(
         `a load-package-result must be an object, got ${kindOf(raw)}`,
      );
   }
   if (raw.protocolVersion !== PACKAGE_LOAD_PROTOCOL_VERSION) {
      throw new PackageLoadProtocolError(
         `the worker speaks protocol version ${JSON.stringify(raw.protocolVersion) ?? "undefined"}, ` +
            `this server ${PACKAGE_LOAD_PROTOCOL_VERSION}; the worker bundle ` +
            `was built from a different revision than the server`,
      );
   }
   if (!isRecord(raw.packageMetadata)) {
      throw new PackageLoadProtocolError(
         `a load-package-result has packageMetadata of kind ${kindOf(raw.packageMetadata)}; expected object`,
      );
   }
   if (!Array.isArray(raw.models)) {
      throw new PackageLoadProtocolError(
         `a load-package-result has models of kind ${kindOf(raw.models)}; expected array`,
      );
   }
   if (typeof raw.loadDurationMs !== "number" || !isRecord(raw.timings)) {
      throw new PackageLoadProtocolError(
         `a load-package-result is missing loadDurationMs or timings`,
      );
   }
   for (const model of raw.models) decodeSerializedModel(model);
   return raw as unknown as LoadPackageResult;
}
